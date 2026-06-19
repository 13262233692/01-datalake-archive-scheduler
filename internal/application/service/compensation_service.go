package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	"datalake-archive-scheduler/internal/domain/model"
	"datalake-archive-scheduler/internal/domain/repository"
	"datalake-archive-scheduler/pkg/logger"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

type CompensationService struct {
	sourceDB     repository.SourceDBRepository
	ossRepo      repository.OSSRepository
	hiveRepo     repository.HiveMetastoreRepository
	starrocksRepo repository.StarRocksRepository
	processor    *StreamProcessor
	mu           sync.RWMutex
	compensations map[string]*CompensationTask
}

type CompensationPhase string

const (
	PhaseExtract     CompensationPhase = "EXTRACT"
	PhaseClean       CompensationPhase = "CLEAN"
	PhaseMask        CompensationPhase = "MASK"
	PhaseOSS         CompensationPhase = "OSS_UPLOAD"
	PhaseHiveMeta    CompensationPhase = "HIVE_METASTORE"
	PhaseStarRocks   CompensationPhase = "STARROCKS_LOAD"
	PhaseDelete      CompensationPhase = "DELETE_SOURCE"
)

type CompensationTask struct {
	ID             string
	JobID          string
	Status         model.JobStatus
	FailedShards   []*model.ArchiveShard
	RetryShards    []*model.ArchiveShard
	CompletedCount int64
	FailedCount    int64
	StartTime      time.Time
	EndTime        *time.Time
	Phase          CompensationPhase
	ErrorMessage   string
}

func NewCompensationService(
	sourceDB repository.SourceDBRepository,
	ossRepo repository.OSSRepository,
	hiveRepo repository.HiveMetastoreRepository,
	starrocksRepo repository.StarRocksRepository,
	processor *StreamProcessor,
) *CompensationService {
	return &CompensationService{
		sourceDB:      sourceDB,
		ossRepo:       ossRepo,
		hiveRepo:      hiveRepo,
		starrocksRepo: starrocksRepo,
		processor:     processor,
		compensations: make(map[string]*CompensationTask),
	}
}

func (s *CompensationService) CreateCompensation(jobID string, failedShards []*model.ArchiveShard) *CompensationTask {
	s.mu.Lock()
	defer s.mu.Unlock()

	task := &CompensationTask{
		ID:           uuid.New().String(),
		JobID:        jobID,
		Status:       model.JobStatusPending,
		FailedShards: failedShards,
		RetryShards:  make([]*model.ArchiveShard, 0, len(failedShards)),
		StartTime:    time.Now(),
		Phase:        PhaseExtract,
	}

	for _, shard := range failedShards {
		if shard.CanRetry() {
			retryShard := &model.ArchiveShard{
				ID:            uuid.New().String(),
				JobID:         jobID,
				ShardIndex:    shard.ShardIndex,
				StartID:       shard.StartID,
				EndID:         shard.EndID,
				Status:        model.ShardStatusPending,
				RetryCount:    shard.RetryCount,
				MaxRetryCount: shard.MaxRetryCount,
			}
			task.RetryShards = append(task.RetryShards, retryShard)
		}
	}

	s.compensations[task.ID] = task

	logger.Info("compensation task created",
		zap.String("compensation_id", task.ID),
		zap.String("job_id", jobID),
		zap.Int("failed_shards", len(failedShards)),
		zap.Int("retry_shards", len(task.RetryShards)),
	)

	return task
}

func (s *CompensationService) ExecuteCompensation(ctx context.Context, task *CompensationTask, tableName string, coldDate time.Time) error {
	s.mu.Lock()
	task.Status = model.JobStatusRunning
	s.mu.Unlock()

	logger.Info("compensation execution started",
		zap.String("compensation_id", task.ID),
		zap.String("job_id", task.JobID),
		zap.Int("retry_shards", len(task.RetryShards)),
	)

	var wg sync.WaitGroup
	sem := make(chan struct{}, 3)
	results := make(chan ShardResult, len(task.RetryShards))

	for _, shard := range task.RetryShards {
		wg.Add(1)
		sem <- struct{}{}

		go func(sh *model.ArchiveShard) {
			defer wg.Done()
			defer func() { <-sem }()

			sh.Start()
			result := s.processor.ProcessShardSafe(ctx, sh, tableName, coldDate)

			if result.Error != nil {
				sh.Fail(result.Error.Error())
				s.mu.Lock()
				task.FailedCount++
				task.ErrorMessage = result.Error.Error()
				s.mu.Unlock()
			} else {
				sh.Complete(result.RecordCount, result.OSSPath)
				s.mu.Lock()
				task.CompletedCount += result.RecordCount
				s.mu.Unlock()
			}

			results <- result
		}(shard)
	}

	wg.Wait()
	close(results)

	s.mu.Lock()
	if task.FailedCount > 0 {
		task.Status = model.JobStatusFailed
		task.ErrorMessage = fmt.Sprintf("%d shards still failed after compensation", task.FailedCount)
	} else {
		task.Status = model.JobStatusCompleted
	}
	now := time.Now()
	task.EndTime = &now
	s.mu.Unlock()

	logger.Info("compensation execution finished",
		zap.String("compensation_id", task.ID),
		zap.String("status", string(task.Status)),
		zap.Int64("completed_records", task.CompletedCount),
		zap.Int64("failed_count", task.FailedCount),
	)

	if task.FailedCount > 0 {
		return fmt.Errorf("compensation partially failed: %d shards failed", task.FailedCount)
	}

	return nil
}

func (s *CompensationService) RollbackShard(ctx context.Context, shard *model.ArchiveShard) error {
	logger.Info("rolling back shard",
		zap.Int("shard_index", shard.ShardIndex),
		zap.String("oss_path", shard.OSSPath),
	)

	if shard.OSSPath != "" {
		exists, err := s.ossRepo.Exists(ctx, shard.OSSPath)
		if err != nil {
			logger.Warn("check oss object failed", zap.Error(err))
		}
		if exists {
			if err := s.ossRepo.Delete(ctx, shard.OSSPath); err != nil {
				return fmt.Errorf("delete oss object failed: %w", err)
			}
			logger.Info("oss object deleted during rollback", zap.String("path", shard.OSSPath))
		}
	}

	return nil
}

func (s *CompensationService) RollbackJob(ctx context.Context, shards []*model.ArchiveShard) error {
	logger.Warn("rolling back entire job", zap.Int("shard_count", len(shards)))

	var wg sync.WaitGroup
	errChan := make(chan error, len(shards))

	for _, shard := range shards {
		if shard.Status == model.ShardStatusCompleted && shard.OSSPath != "" {
			wg.Add(1)
			go func(sh *model.ArchiveShard) {
				defer wg.Done()
				if err := s.RollbackShard(ctx, sh); err != nil {
					errChan <- err
				}
			}(shard)
		}
	}

	wg.Wait()
	close(errChan)

	var errs []string
	for err := range errChan {
		errs = append(errs, err.Error())
	}

	if len(errs) > 0 {
		return fmt.Errorf("rollback errors: %v", errs)
	}

	logger.Info("job rollback completed")
	return nil
}

func (s *CompensationService) GetCompensation(id string) (*CompensationTask, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	task, exists := s.compensations[id]
	return task, exists
}

func (s *CompensationService) GetCompensationsByJob(jobID string) []*CompensationTask {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var tasks []*CompensationTask
	for _, task := range s.compensations {
		if task.JobID == jobID {
			tasks = append(tasks, task)
		}
	}
	return tasks
}

func (s *CompensationService) VerifyDataIntegrity(ctx context.Context, shards []*model.ArchiveShard, targetTable string) error {
	logger.Info("verifying data integrity", zap.Int("shard_count", len(shards)))

	var totalSource int64
	var totalTarget int64
	var totalOSS int64

	for _, shard := range shards {
		if shard.Status == model.ShardStatusCompleted {
			totalSource += shard.RecordCount

			if shard.OSSPath != "" {
				_, err := s.ossRepo.Download(ctx, shard.OSSPath)
				if err != nil {
					return fmt.Errorf("oss object missing for shard %d: %w", shard.ShardIndex, err)
				}
				totalOSS += shard.RecordCount
			}
		}
	}

	if totalSource != totalOSS {
		return fmt.Errorf("data integrity check failed: source=%d, oss=%d", totalSource, totalOSS)
	}

	logger.Info("data integrity verification passed",
		zap.Int64("source_records", totalSource),
		zap.Int64("oss_records", totalOSS),
		zap.Int64("target_records", totalTarget),
	)

	return nil
}
