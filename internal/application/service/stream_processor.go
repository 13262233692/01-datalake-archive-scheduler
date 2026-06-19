package service

import (
	"context"
	"fmt"
	"io"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"datalake-archive-scheduler/internal/domain/model"
	"datalake-archive-scheduler/internal/domain/repository"
	domainservice "datalake-archive-scheduler/internal/domain/service"
	"datalake-archive-scheduler/pkg/logger"

	"go.uber.org/zap"
)

type StreamProcessor struct {
	sourceDB      repository.SourceDBRepository
	ossRepo       repository.OSSRepository
	cleaningSvc   domainservice.DataCleaningService
	maskingSvc    domainservice.DataMaskingService
	serialSvc     domainservice.DataSerializationService
	memoryLimit   int64
	batchSize     int
}

func NewStreamProcessor(
	sourceDB repository.SourceDBRepository,
	ossRepo repository.OSSRepository,
	cleaningSvc domainservice.DataCleaningService,
	maskingSvc domainservice.DataMaskingService,
	serialSvc domainservice.DataSerializationService,
	memoryLimit int64,
	batchSize int,
) *StreamProcessor {
	return &StreamProcessor{
		sourceDB:    sourceDB,
		ossRepo:     ossRepo,
		cleaningSvc: cleaningSvc,
		maskingSvc:  maskingSvc,
		serialSvc:   serialSvc,
		memoryLimit: memoryLimit,
		batchSize:   batchSize,
	}
}

type ShardResult struct {
	ShardIndex  int
	RecordCount int64
	OSSPath     string
	Error       error
}

func (p *StreamProcessor) ProcessShard(ctx context.Context, shard *model.ArchiveShard, tableName string, coldDate time.Time) ShardResult {
	result := ShardResult{
		ShardIndex: shard.ShardIndex,
	}

	logger.Info("processing shard started",
		zap.Int("shard_index", shard.ShardIndex),
		zap.Int64("start_id", shard.StartID),
		zap.Int64("end_id", shard.EndID),
	)

	iterator, err := p.sourceDB.FetchShard(ctx, tableName, shard.StartID, shard.EndID, coldDate, p.batchSize)
	if err != nil {
		result.Error = fmt.Errorf("fetch shard failed: %w", err)
		return result
	}
	defer iterator.Close()

	recordChan := make(chan *model.DataRecord, p.batchSize)
	cleanedChan := make(chan *model.DataRecord, p.batchSize)
	maskedChan := make(chan *model.DataRecord, p.batchSize)

	var wg sync.WaitGroup
	var recordCount int64
	var memUsed int64

	wg.Add(3)

	go p.extractWorker(ctx, iterator, recordChan, &wg)
	go p.cleanWorker(ctx, recordChan, cleanedChan, &wg)
	go p.maskWorker(ctx, cleanedChan, maskedChan, &wg)

	ossPath := fmt.Sprintf("%s/shard_%04d.jsonl",
		coldDate.Format("2006/01/02"),
		shard.ShardIndex,
	)

	pr, pw := io.Pipe()

	uploadErrChan := make(chan error, 1)
	go func() {
		defer pw.Close()
		for rec := range maskedChan {
			line := p.serialSvc.SerializeLine(rec)
			_, err := pw.Write(line)
			if err != nil {
				uploadErrChan <- fmt.Errorf("write pipe failed: %w", err)
				return
			}
			atomic.AddInt64(&recordCount, 1)
			atomic.AddInt64(&memUsed, -int64(len(line)))
		}
		uploadErrChan <- nil
	}()

	uploadCtx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()

	err = p.ossRepo.UploadStream(uploadCtx, ossPath, pr, 0)
	if err != nil {
		result.Error = fmt.Errorf("upload to oss failed: %w", err)
		return result
	}

	if err := <-uploadErrChan; err != nil {
		result.Error = fmt.Errorf("stream writer error: %w", err)
		return result
	}

	wg.Wait()

	result.RecordCount = atomic.LoadInt64(&recordCount)
	result.OSSPath = ossPath

	logger.Info("processing shard completed",
		zap.Int("shard_index", shard.ShardIndex),
		zap.Int64("record_count", result.RecordCount),
		zap.String("oss_path", ossPath),
	)

	return result
}

func (p *StreamProcessor) extractWorker(ctx context.Context, iterator repository.DataRecordIterator, out chan<- *model.DataRecord, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(out)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			if !iterator.HasNext() {
				return
			}
			rec, err := iterator.Next()
			if err != nil {
				logger.Warn("extract record error", zap.Error(err))
				return
			}
			if rec != nil {
				select {
				case out <- rec:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

func (p *StreamProcessor) cleanWorker(ctx context.Context, in <-chan *model.DataRecord, out chan<- *model.DataRecord, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(out)

	batch := make([]*model.DataRecord, 0, p.batchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		cleaned, err := p.cleaningSvc.CleanBatch(batch)
		if err != nil {
			logger.Warn("clean batch error", zap.Error(err))
		}
		for _, rec := range cleaned {
			select {
			case out <- rec:
			case <-ctx.Done():
				return
			}
		}
		batch = batch[:0]
	}

	for rec := range in {
		batch = append(batch, rec)
		if len(batch) >= p.batchSize {
			flush()
		}
	}
	flush()
}

func (p *StreamProcessor) maskWorker(ctx context.Context, in <-chan *model.DataRecord, out chan<- *model.DataRecord, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(out)

	batch := make([]*model.DataRecord, 0, p.batchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		masked := p.maskingSvc.MaskBatch(batch)
		for _, rec := range masked {
			select {
			case out <- rec:
			case <-ctx.Done():
				return
			}
		}
		batch = batch[:0]
	}

	for rec := range in {
		batch = append(batch, rec)
		if len(batch) >= p.batchSize {
			flush()
		}
	}
	flush()
}

func (p *StreamProcessor) ProcessShardSafe(ctx context.Context, shard *model.ArchiveShard, tableName string, coldDate time.Time) ShardResult {
	result := ShardResult{
		ShardIndex: shard.ShardIndex,
	}

	logger.Info("processing shard (safe mode)",
		zap.Int("shard_index", shard.ShardIndex),
		zap.Int64("start_id", shard.StartID),
		zap.Int64("end_id", shard.EndID),
	)

	iterator, err := p.sourceDB.FetchShard(ctx, tableName, shard.StartID, shard.EndID, coldDate, p.batchSize)
	if err != nil {
		result.Error = fmt.Errorf("fetch shard failed: %w", err)
		return result
	}

	iteratorClosed := false
	safeCloseIterator := func() {
		if !iteratorClosed {
			iteratorClosed = true
			if closeErr := iterator.Close(); closeErr != nil {
				logger.Warn("iterator close error", zap.Error(closeErr))
			}
		}
	}
	defer safeCloseIterator()

	recordChan := make(chan *model.DataRecord, p.batchSize)
	cleanedChan := make(chan *model.DataRecord, p.batchSize)
	maskedChan := make(chan *model.DataRecord, p.batchSize)

	channelsClosed := false
	safeCloseChannels := func() {
		if !channelsClosed {
			channelsClosed = true
			close(recordChan)
			close(cleanedChan)
			close(maskedChan)
		}
	}

	var recordCount int64

	defer func() {
		if r := recover(); r != nil {
			stack := debug.Stack()
			logger.Error("ProcessShardSafe internal panic recovered",
				zap.Int("shard_index", shard.ShardIndex),
				zap.Any("panic", r),
				zap.String("stack", string(stack)),
			)
			safeCloseIterator()
			safeCloseChannels()
			result.Error = fmt.Errorf("internal panic: %v", r)
		}
	}()

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("extractWorker panic recovered",
					zap.Int("shard_index", shard.ShardIndex),
					zap.Any("panic", r),
				)
			}
			wg.Done()
		}()
		p.extractWorkerSafe(ctx, iterator, recordChan, shard.ShardIndex)
	}()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("cleanWorker panic recovered",
					zap.Int("shard_index", shard.ShardIndex),
					zap.Any("panic", r),
				)
			}
			wg.Done()
		}()
		p.cleanWorkerSafe(ctx, recordChan, cleanedChan, shard.ShardIndex)
	}()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("maskWorker panic recovered",
					zap.Int("shard_index", shard.ShardIndex),
					zap.Any("panic", r),
				)
			}
			wg.Done()
		}()
		p.maskWorkerSafe(ctx, cleanedChan, maskedChan, shard.ShardIndex)
	}()

	ossPath := fmt.Sprintf("%s/shard_%04d.jsonl",
		coldDate.Format("2006/01/02"),
		shard.ShardIndex,
	)

	pr, pw := io.Pipe()

	uploadErrChan := make(chan error, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("upload writer panic recovered",
					zap.Int("shard_index", shard.ShardIndex),
					zap.Any("panic", r),
				)
				uploadErrChan <- fmt.Errorf("upload writer panic: %v", r)
			}
			pw.Close()
		}()

		for {
			select {
			case <-ctx.Done():
				uploadErrChan <- ctx.Err()
				return
			case rec, ok := <-maskedChan:
				if !ok {
					uploadErrChan <- nil
					return
				}
				line := p.serialSvc.SerializeLine(rec)
				if _, err := pw.Write(line); err != nil {
					uploadErrChan <- fmt.Errorf("write pipe failed: %w", err)
					return
				}
				atomic.AddInt64(&recordCount, 1)
			}
		}
	}()

	uploadCtx, uploadCancel := context.WithTimeout(ctx, 30*time.Minute)
	defer uploadCancel()

	err = p.ossRepo.UploadStream(uploadCtx, ossPath, pr, 0)
	if err != nil {
		safeCloseIterator()
		safeCloseChannels()
		result.Error = fmt.Errorf("upload to oss failed: %w", err)
		return result
	}

	if err := <-uploadErrChan; err != nil {
		safeCloseIterator()
		result.Error = fmt.Errorf("stream writer error: %w", err)
		return result
	}

	workerDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(workerDone)
	}()

	select {
	case <-workerDone:
	case <-time.After(5 * time.Minute):
		logger.Warn("worker goroutines timeout waiting, forcing close",
			zap.Int("shard_index", shard.ShardIndex),
		)
		safeCloseChannels()
	}

	safeCloseIterator()

	result.RecordCount = atomic.LoadInt64(&recordCount)
	result.OSSPath = ossPath

	logger.Info("processing shard (safe mode) completed",
		zap.Int("shard_index", shard.ShardIndex),
		zap.Int64("record_count", result.RecordCount),
		zap.String("oss_path", ossPath),
	)

	return result
}

func (p *StreamProcessor) extractWorkerSafe(ctx context.Context, iterator repository.DataRecordIterator, out chan<- *model.DataRecord, shardIndex int) {
	defer close(out)

	for {
		select {
		case <-ctx.Done():
			logger.Debug("extractWorker cancelled by context",
				zap.Int("shard_index", shardIndex),
				zap.Error(ctx.Err()),
			)
			return
		default:
			if !iterator.HasNext() {
				return
			}

			func() {
				defer func() {
					if r := recover(); r != nil {
						logger.Error("Next() panic recovered",
							zap.Int("shard_index", shardIndex),
							zap.Any("panic", r),
						)
					}
				}()

				rec, err := iterator.Next()
				if err != nil {
					logger.Warn("extract record error",
						zap.Int("shard_index", shardIndex),
						zap.Error(err),
					)
					return
				}

				if rec != nil {
					select {
					case out <- rec:
					case <-ctx.Done():
						return
					}
				}
			}()
		}
	}
}

func (p *StreamProcessor) cleanWorkerSafe(ctx context.Context, in <-chan *model.DataRecord, out chan<- *model.DataRecord, shardIndex int) {
	defer close(out)

	batch := make([]*model.DataRecord, 0, p.batchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}

		defer func() {
			if r := recover(); r != nil {
				logger.Error("CleanBatch panic recovered",
					zap.Int("shard_index", shardIndex),
					zap.Any("panic", r),
				)
			}
		}()

		cleaned, err := p.cleaningSvc.CleanBatch(batch)
		if err != nil {
			logger.Warn("clean batch error",
				zap.Int("shard_index", shardIndex),
				zap.Error(err),
			)
		}

		for _, rec := range cleaned {
			select {
			case out <- rec:
			case <-ctx.Done():
				return
			}
		}
		batch = batch[:0]
	}

	for {
		select {
		case rec, ok := <-in:
			if !ok {
				flush()
				return
			}
			batch = append(batch, rec)
			if len(batch) >= p.batchSize {
				flush()
			}
		case <-ctx.Done():
			return
		}
	}
}

func (p *StreamProcessor) maskWorkerSafe(ctx context.Context, in <-chan *model.DataRecord, out chan<- *model.DataRecord, shardIndex int) {
	defer close(out)

	batch := make([]*model.DataRecord, 0, p.batchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}

		defer func() {
			if r := recover(); r != nil {
				logger.Error("MaskBatch panic recovered",
					zap.Int("shard_index", shardIndex),
					zap.Any("panic", r),
				)
			}
		}()

		masked := p.maskingSvc.MaskBatch(batch)
		for _, rec := range masked {
			select {
			case out <- rec:
			case <-ctx.Done():
				return
			}
		}
		batch = batch[:0]
	}

	for {
		select {
		case rec, ok := <-in:
			if !ok {
				flush()
				return
			}
			batch = append(batch, rec)
			if len(batch) >= p.batchSize {
				flush()
			}
		case <-ctx.Done():
			return
		}
	}
}
