package service

import (
	"bytes"
	"context"
	"fmt"
	"io"
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

func (p *StreamProcessor) ProcessShardSimple(ctx context.Context, shard *model.ArchiveShard, tableName string, coldDate time.Time) ShardResult {
	result := ShardResult{
		ShardIndex: shard.ShardIndex,
	}

	logger.Info("processing shard (simple)",
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

	var buf bytes.Buffer
	var recordCount int64

	for iterator.HasNext() {
		rec, err := iterator.Next()
		if err != nil {
			break
		}

		cleanedRec, err := p.cleaningSvc.Clean(rec)
		if err != nil {
			logger.Warn("clean record failed", zap.Error(err), zap.Int64("record_id", rec.ID))
			continue
		}

		maskedRec := p.maskingSvc.Mask(cleanedRec)

		line := p.serialSvc.SerializeLine(maskedRec)
		buf.Write(line)

		recordCount++

		if int64(buf.Len()) > p.memoryLimit/4 {
			logger.Debug("buffer flushing",
				zap.Int("buffer_size", buf.Len()),
				zap.Int64("records", recordCount),
			)
		}
	}

	ossPath := fmt.Sprintf("%s/shard_%04d.jsonl",
		coldDate.Format("2006/01/02"),
		shard.ShardIndex,
	)

	err = p.ossRepo.UploadStream(ctx, ossPath, bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		result.Error = fmt.Errorf("upload to oss failed: %w", err)
		return result
	}

	result.RecordCount = recordCount
	result.OSSPath = ossPath

	logger.Info("processing shard completed",
		zap.Int("shard_index", shard.ShardIndex),
		zap.Int64("record_count", recordCount),
		zap.String("oss_path", ossPath),
	)

	return result
}
