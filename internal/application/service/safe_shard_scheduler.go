package service

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"datalake-archive-scheduler/internal/domain/model"
	"datalake-archive-scheduler/pkg/logger"

	"go.uber.org/zap"
)

type ShardTaskFunc func(ctx context.Context, shard *model.ArchiveShard) ShardResult

type SafeWaitGroup struct {
	wg     sync.WaitGroup
	mu     sync.Mutex
	done   bool
	cancel context.CancelFunc
}

func NewSafeWaitGroup(cancel context.CancelFunc) *SafeWaitGroup {
	return &SafeWaitGroup{
		cancel: cancel,
	}
}

func (swg *SafeWaitGroup) Add(delta int) {
	swg.mu.Lock()
	defer swg.mu.Unlock()
	if !swg.done {
		swg.wg.Add(delta)
	}
}

func (swg *SafeWaitGroup) Done() {
	swg.mu.Lock()
	defer swg.mu.Unlock()
	swg.wg.Done()
}

func (swg *SafeWaitGroup) WaitWithTimeout(timeout time.Duration) error {
	done := make(chan struct{})
	go func() {
		swg.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		swg.mu.Lock()
		swg.done = true
		swg.mu.Unlock()
		return nil
	case <-time.After(timeout):
		swg.mu.Lock()
		swg.done = true
		if swg.cancel != nil {
			swg.cancel()
		}
		swg.mu.Unlock()
		return fmt.Errorf("wait group timeout after %v", timeout)
	}
}

type Semaphore struct {
	ch       chan struct{}
	capacity int
	acquired int64
	released int64
}

func NewSemaphore(capacity int) *Semaphore {
	return &Semaphore{
		ch:       make(chan struct{}, capacity),
		capacity: capacity,
	}
}

func (s *Semaphore) Acquire(ctx context.Context) error {
	select {
	case s.ch <- struct{}{}:
		atomic.AddInt64(&s.acquired, 1)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Semaphore) Release() {
	select {
	case <-s.ch:
		atomic.AddInt64(&s.released, 1)
	default:
		logger.Warn("semaphore release called but no token acquired")
	}
}

func (s *Semaphore) Leaks() int64 {
	return atomic.LoadInt64(&s.acquired) - atomic.LoadInt64(&s.released)
}

func (s *Semaphore) Close() {
	close(s.ch)
}

type ShardSchedulerConfig struct {
	MaxConcurrency    int
	ShardTimeout      time.Duration
	GlobalTimeout     time.Duration
	PanicThreshold    int
	EnableDeadlockDetect bool
	DeadlockTimeout   time.Duration
}

func DefaultShardSchedulerConfig() ShardSchedulerConfig {
	return ShardSchedulerConfig{
		MaxConcurrency:     5,
		ShardTimeout:       30 * time.Minute,
		GlobalTimeout:      12 * time.Hour,
		PanicThreshold:     3,
		EnableDeadlockDetect: true,
		DeadlockTimeout:    10 * time.Minute,
	}
}

type ShardSchedulerStats struct {
	TotalShards      int64
	CompletedShards  int64
	FailedShards     int64
	PanicCount       int64
	ActiveGoroutines int64
	MaxGoroutines    int64
	StartTime        time.Time
	LastActivityTime time.Time
}

type SafeShardScheduler struct {
	config       ShardSchedulerConfig
	processor    *StreamProcessor
	stats        ShardSchedulerStats
	panicCount   int64
	goroutineCount int64
	mu           sync.RWMutex
}

func NewSafeShardScheduler(processor *StreamProcessor, config ShardSchedulerConfig) *SafeShardScheduler {
	if config.MaxConcurrency <= 0 {
		config.MaxConcurrency = 5
	}
	if config.ShardTimeout <= 0 {
		config.ShardTimeout = 30 * time.Minute
	}
	if config.GlobalTimeout <= 0 {
		config.GlobalTimeout = 12 * time.Hour
	}
	if config.DeadlockTimeout <= 0 {
		config.DeadlockTimeout = 10 * time.Minute
	}

	return &SafeShardScheduler{
		config:    config,
		processor: processor,
		stats: ShardSchedulerStats{
			StartTime: time.Now(),
		},
	}
}

func (s *SafeShardScheduler) ProcessShards(
	ctx context.Context,
	job *model.ArchiveJob,
	shards []*model.ArchiveShard,
	tableName string,
	coldDate time.Time,
) error {
	logger.Info("safe shard scheduler started",
		zap.String("job_id", job.ID),
		zap.Int("total_shards", len(shards)),
		zap.Int("max_concurrency", s.config.MaxConcurrency),
	)

	globalCtx, globalCancel := context.WithTimeout(ctx, s.config.GlobalTimeout)
	defer globalCancel()

	swg := NewSafeWaitGroup(globalCancel)
	sem := NewSemaphore(job.Concurrency)
	defer sem.Close()

	errChan := make(chan error, len(shards))
	resultChan := make(chan ShardResult, len(shards))

	s.resetStats(len(shards))

	if s.config.EnableDeadlockDetect {
		go s.deadlockDetector(globalCtx, job.ID, sem, swg)
	}

	go s.goroutineMonitor(globalCtx, job.ID)

	var processed int64
	var failed int64

	for _, shard := range shards {
		if err := globalCtx.Err(); err != nil {
			logger.Warn("context cancelled, stopping shard scheduling",
				zap.String("job_id", job.ID),
				zap.Error(err),
			)
			break
		}

		if err := sem.Acquire(globalCtx); err != nil {
			logger.Warn("semaphore acquire failed",
				zap.String("job_id", job.ID),
				zap.Int("shard_index", shard.ShardIndex),
				zap.Error(err),
			)
			atomic.AddInt64(&failed, 1)
			errChan <- fmt.Errorf("semaphore acquire failed: %w", err)
			continue
		}

		swg.Add(1)
		s.incGoroutineCount()

		go s.safeProcessShard(
			globalCtx,
			shard,
			job,
			tableName,
			coldDate,
			sem,
			swg,
			resultChan,
			errChan,
			&processed,
			&failed,
		)
	}

	logger.Info("all shards dispatched, waiting for completion",
		zap.String("job_id", job.ID),
		zap.Int64("active_goroutines", atomic.LoadInt64(&s.goroutineCount)),
	)

	if err := swg.WaitWithTimeout(s.config.GlobalTimeout); err != nil {
		logger.Error("shard processing timed out",
			zap.String("job_id", job.ID),
			zap.Error(err),
			zap.Int64("semaphore_leaks", sem.Leaks()),
		)

		leakCount := sem.Leaks()
		for i := int64(0); i < leakCount; i++ {
			sem.Release()
		}

		return fmt.Errorf("shard processing timeout: %w", err)
	}

	close(resultChan)
	close(errChan)

	failedCount := atomic.LoadInt64(&failed)
	if failedCount > 0 {
		logger.Warn("some shards failed",
			zap.String("job_id", job.ID),
			zap.Int64("failed_count", failedCount),
			zap.Int64("completed_count", atomic.LoadInt64(&processed)),
		)
		return fmt.Errorf("%d shards failed", failedCount)
	}

	if atomic.LoadInt64(&s.panicCount) > 0 {
		logger.Warn("some shards experienced panics but recovered",
			zap.String("job_id", job.ID),
			zap.Int64("panic_count", atomic.LoadInt64(&s.panicCount)),
		)
	}

	logger.Info("all shards processed successfully",
		zap.String("job_id", job.ID),
		zap.Int64("total_records", atomic.LoadInt64(&processed)),
		zap.Int64("panic_count", atomic.LoadInt64(&s.panicCount)),
		zap.Int64("max_goroutines", s.stats.MaxGoroutines),
	)

	return nil
}

func (s *SafeShardScheduler) safeProcessShard(
	ctx context.Context,
	shard *model.ArchiveShard,
	job *model.ArchiveJob,
	tableName string,
	coldDate time.Time,
	sem *Semaphore,
	swg *SafeWaitGroup,
	resultChan chan<- ShardResult,
	errChan chan<- error,
	processed *int64,
	failed *int64,
) {
	defer func() {
		if r := recover(); r != nil {
			stack := debug.Stack()
			atomic.AddInt64(&s.panicCount, 1)

			logger.Error("shard panic recovered",
				zap.String("job_id", job.ID),
				zap.Int("shard_index", shard.ShardIndex),
				zap.Any("panic", r),
				zap.String("stack", string(stack)),
			)

			shard.Fail(fmt.Sprintf("panic: %v", r))
			atomic.AddInt64(failed, 1)
			errChan <- fmt.Errorf("shard %d panic: %v", shard.ShardIndex, r)

			if atomic.LoadInt64(&s.panicCount) >= int64(s.config.PanicThreshold) {
				logger.Error("panic threshold exceeded, cancelling remaining shards",
					zap.String("job_id", job.ID),
					zap.Int64("panic_count", atomic.LoadInt64(&s.panicCount)),
				)
				if swg.cancel != nil {
					swg.cancel()
				}
			}
		}

		sem.Release()
		swg.Done()
		s.decGoroutineCount()

		s.updateActivityTime()
	}()

	shardCtx, shardCancel := context.WithTimeout(ctx, s.config.ShardTimeout)
	defer shardCancel()

	logger.Debug("processing shard (safe mode)",
		zap.String("job_id", job.ID),
		zap.Int("shard_index", shard.ShardIndex),
		zap.Int64("start_id", shard.StartID),
		zap.Int64("end_id", shard.EndID),
	)

	shard.Start()

	result := s.processor.ProcessShardSafe(shardCtx, shard, tableName, coldDate)

	if result.Error != nil {
		shard.Fail(result.Error.Error())
		atomic.AddInt64(failed, 1)
		errChan <- result.Error

		logger.Error("shard processing failed",
			zap.String("job_id", job.ID),
			zap.Int("shard_index", shard.ShardIndex),
			zap.Error(result.Error),
		)
	} else {
		shard.Complete(result.RecordCount, result.OSSPath)
		atomic.AddInt64(processed, result.RecordCount)
		resultChan <- result

		logger.Info("shard processing completed",
			zap.String("job_id", job.ID),
			zap.Int("shard_index", shard.ShardIndex),
			zap.Int64("record_count", result.RecordCount),
		)
	}
}

func (s *SafeShardScheduler) deadlockDetector(ctx context.Context, jobID string, sem *Semaphore, swg *SafeWaitGroup) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			elapsed := time.Since(s.stats.LastActivityTime)
			leaks := sem.Leaks()

			if elapsed > s.config.DeadlockTimeout && leaks > 0 {
				logger.Error("deadlock detected",
					zap.String("job_id", jobID),
					zap.Duration("inactive_duration", elapsed),
					zap.Int64("semaphore_leaks", leaks),
					zap.Int64("active_goroutines", atomic.LoadInt64(&s.goroutineCount)),
				)

				if swg.cancel != nil {
					swg.cancel()
				}
				return
			}

			logger.Debug("deadlock check passed",
				zap.String("job_id", jobID),
				zap.Duration("inactive_duration", elapsed),
				zap.Int64("semaphore_leaks", leaks),
			)
		}
	}
}

func (s *SafeShardScheduler) goroutineMonitor(ctx context.Context, jobID string) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			current := atomic.LoadInt64(&s.goroutineCount)
			if current > s.stats.MaxGoroutines {
				s.stats.MaxGoroutines = current
			}

			logger.Debug("goroutine monitor",
				zap.String("job_id", jobID),
				zap.Int64("active_goroutines", current),
				zap.Int64("max_goroutines", s.stats.MaxGoroutines),
				zap.Int64("panic_count", atomic.LoadInt64(&s.panicCount)),
			)
		}
	}
}

func (s *SafeShardScheduler) incGoroutineCount() {
	atomic.AddInt64(&s.goroutineCount, 1)
	s.updateActivityTime()
}

func (s *SafeShardScheduler) decGoroutineCount() {
	atomic.AddInt64(&s.goroutineCount, -1)
	s.updateActivityTime()
}

func (s *SafeShardScheduler) updateActivityTime() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stats.LastActivityTime = time.Now()
}

func (s *SafeShardScheduler) resetStats(totalShards int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.stats = ShardSchedulerStats{
		TotalShards:      int64(totalShards),
		StartTime:        time.Now(),
		LastActivityTime: time.Now(),
	}
	atomic.StoreInt64(&s.panicCount, 0)
	atomic.StoreInt64(&s.goroutineCount, 0)
}

func (s *SafeShardScheduler) GetStats() ShardSchedulerStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stats := s.stats
	stats.ActiveGoroutines = atomic.LoadInt64(&s.goroutineCount)
	stats.PanicCount = atomic.LoadInt64(&s.panicCount)
	return stats
}
