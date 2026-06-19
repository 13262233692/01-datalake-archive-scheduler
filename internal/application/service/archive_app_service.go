package service

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"datalake-archive-scheduler/internal/application/dto"
	"datalake-archive-scheduler/internal/domain/model"
	"datalake-archive-scheduler/internal/domain/repository"
	domainservice "datalake-archive-scheduler/internal/domain/service"
	"datalake-archive-scheduler/internal/infrastructure/config"
	"datalake-archive-scheduler/pkg/logger"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

type ArchiveAppService struct {
	config         *config.ArchiveConfig
	sourceDB       repository.SourceDBRepository
	ossRepo        repository.OSSRepository
	hiveRepo       repository.HiveMetastoreRepository
	starrocksRepo  repository.StarRocksRepository
	processor      *StreamProcessor
	shardScheduler *SafeShardScheduler
	compensation   *CompensationService
	cleaningSvc    domainservice.DataCleaningService
	maskingSvc     domainservice.DataMaskingService
	mu             sync.RWMutex
	jobs           map[string]*model.ArchiveJob
	jobShards      map[string][]*model.ArchiveShard
	stats          *ArchiveStats
}

type ArchiveStats struct {
	TotalArchivedRecords int64
	TotalJobs            int64
	CompletedJobs        int64
	FailedJobs           int64
}

func NewArchiveAppService(
	cfg *config.ArchiveConfig,
	sourceDB repository.SourceDBRepository,
	ossRepo repository.OSSRepository,
	hiveRepo repository.HiveMetastoreRepository,
	starrocksRepo repository.StarRocksRepository,
	processor *StreamProcessor,
	compensation *CompensationService,
	cleaningSvc domainservice.DataCleaningService,
	maskingSvc domainservice.DataMaskingService,
) *ArchiveAppService {
	schedulerConfig := DefaultShardSchedulerConfig()
	schedulerConfig.MaxConcurrency = cfg.Concurrency

	shardScheduler := NewSafeShardScheduler(processor, schedulerConfig)

	return &ArchiveAppService{
		config:         cfg,
		sourceDB:       sourceDB,
		ossRepo:        ossRepo,
		hiveRepo:       hiveRepo,
		starrocksRepo:  starrocksRepo,
		processor:      processor,
		shardScheduler: shardScheduler,
		compensation:   compensation,
		cleaningSvc:    cleaningSvc,
		maskingSvc:     maskingSvc,
		jobs:           make(map[string]*model.ArchiveJob),
		jobShards:      make(map[string][]*model.ArchiveShard),
		stats:          &ArchiveStats{},
	}
}

func (s *ArchiveAppService) CreateJob(ctx context.Context, req *dto.CreateArchiveJobRequest) (*dto.CreateArchiveJobResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tableName := req.TableName
	if tableName == "" {
		tableName = s.config.TableName
	}

	shardCount := req.ShardCount
	if shardCount == 0 {
		shardCount = s.config.ShardCount
	}

	concurrency := req.Concurrency
	if concurrency == 0 {
		concurrency = s.config.Concurrency
	}

	coldDate := s.config.ColdDate()
	if req.ColdDateStr != "" {
		parsed, err := time.Parse("2006-01-02", req.ColdDateStr)
		if err != nil {
			return nil, fmt.Errorf("invalid cold_date format: %w", err)
		}
		coldDate = parsed
	}

	totalCount, err := s.sourceDB.CountColdData(ctx, tableName, coldDate)
	if err != nil {
		return nil, fmt.Errorf("count cold data failed: %w", err)
	}

	if totalCount == 0 {
		return nil, fmt.Errorf("no cold data found for date: %s", coldDate.Format("2006-01-02"))
	}

	job := model.NewArchiveJob(tableName, coldDate, shardCount, concurrency)
	job.TotalRecords = totalCount

	s.jobs[job.ID] = job
	atomic.AddInt64(&s.stats.TotalJobs, 1)

	logger.Info("archive job created",
		zap.String("job_id", job.ID),
		zap.String("job_name", job.JobName),
		zap.Int64("total_records", totalCount),
		zap.Time("cold_date", coldDate),
	)

	return &dto.CreateArchiveJobResponse{
		JobID:     job.ID,
		JobName:   job.JobName,
		Status:    string(job.Status),
		CreatedAt: job.CreatedAt,
	}, nil
}

func (s *ArchiveAppService) StartJob(ctx context.Context, jobID string) error {
	s.mu.RLock()
	job, exists := s.jobs[jobID]
	s.mu.RUnlock()

	if !exists {
		return fmt.Errorf("job not found: %s", jobID)
	}

	if job.Status != model.JobStatusPending && job.Status != model.JobStatusPaused {
		return fmt.Errorf("job cannot be started from status: %s", job.Status)
	}

	go s.executeJob(ctx, jobID)

	return nil
}

func (s *ArchiveAppService) executeJob(ctx context.Context, jobID string) {
	s.mu.Lock()
	job, exists := s.jobs[jobID]
	if !exists {
		s.mu.Unlock()
		return
	}
	job.Start()
	s.mu.Unlock()

	jobCtx, jobCancel := context.WithTimeout(ctx, 24*time.Hour)
	defer jobCancel()

	logger.Info("archive job started",
		zap.String("job_id", jobID),
		zap.String("job_name", job.JobName),
	)

	defer func() {
		if r := recover(); r != nil {
			stack := debug.Stack()
			logger.Error("job panic recovered",
				zap.String("job_id", jobID),
				zap.Any("panic", r),
				zap.String("stack", string(stack)),
			)
			s.failJob(jobID, fmt.Sprintf("panic: %v", r))
			jobCancel()
		}
	}()

	if err := s.prepareShards(jobCtx, job); err != nil {
		logger.Error("prepare shards failed", zap.Error(err))
		s.failJob(jobID, err.Error())
		return
	}

	if err := s.processShards(jobCtx, job); err != nil {
		logger.Error("process shards failed", zap.Error(err))

		if compErr := s.tryCompensate(jobCtx, job); compErr != nil {
			logger.Error("compensation failed", zap.Error(compErr))
			s.failJob(jobID, compErr.Error())
			return
		}
	}

	if err := s.registerHivePartition(jobCtx, job); err != nil {
		logger.Error("register hive partition failed", zap.Error(err))
		s.failJob(jobID, err.Error())
		return
	}

	if err := s.loadToStarRocks(jobCtx, job); err != nil {
		logger.Error("load to starrocks failed", zap.Error(err))
		s.failJob(jobID, err.Error())
		return
	}

	if err := s.cleanupSourceData(jobCtx, job); err != nil {
		logger.Warn("cleanup source data failed", zap.Error(err))
	}

	s.completeJob(jobID)
}

func (s *ArchiveAppService) prepareShards(ctx context.Context, job *model.ArchiveJob) error {
	minID, maxID, err := s.sourceDB.GetMinMaxID(ctx, job.TableName, job.ColdDate)
	if err != nil {
		return fmt.Errorf("get min max id failed: %w", err)
	}

	totalRange := maxID - minID + 1
	shardSize := totalRange / int64(job.ShardCount)
	if shardSize == 0 {
		shardSize = 1
	}

	shards := make([]*model.ArchiveShard, 0, job.ShardCount)
	currentStart := minID

	for i := 0; i < job.ShardCount; i++ {
		endID := currentStart + shardSize - 1
		if i == job.ShardCount-1 {
			endID = maxID
		}

		shard := &model.ArchiveShard{
			ID:            uuid.New().String(),
			JobID:         job.ID,
			ShardIndex:    i,
			StartID:       currentStart,
			EndID:         endID,
			Status:        model.ShardStatusPending,
			MaxRetryCount: s.config.MaxRetryCount,
		}
		shards = append(shards, shard)

		currentStart = endID + 1
	}

	s.mu.Lock()
	s.jobShards[job.ID] = shards
	s.mu.Unlock()

	logger.Info("shards prepared",
		zap.String("job_id", job.ID),
		zap.Int("shard_count", len(shards)),
		zap.Int64("min_id", minID),
		zap.Int64("max_id", maxID),
	)

	return nil
}

func (s *ArchiveAppService) processShards(ctx context.Context, job *model.ArchiveJob) error {
	s.mu.RLock()
	shards := s.jobShards[job.ID]
	s.mu.RUnlock()

	logger.Info("starting safe shard processing",
		zap.String("job_id", job.ID),
		zap.Int("total_shards", len(shards)),
		zap.Int("concurrency", job.Concurrency),
	)

	schedulerConfig := DefaultShardSchedulerConfig()
	schedulerConfig.MaxConcurrency = job.Concurrency
	schedulerConfig.ShardTimeout = time.Duration(job.ShardCount) * 10 * time.Minute
	if schedulerConfig.ShardTimeout < 30*time.Minute {
		schedulerConfig.ShardTimeout = 30 * time.Minute
	}
	schedulerConfig.GlobalTimeout = time.Duration(job.ShardCount) * 30 * time.Minute
	if schedulerConfig.GlobalTimeout < 2*time.Hour {
		schedulerConfig.GlobalTimeout = 2 * time.Hour
	}

	scheduler := NewSafeShardScheduler(s.processor, schedulerConfig)

	err := scheduler.ProcessShards(
		ctx,
		job,
		shards,
		job.TableName,
		job.ColdDate,
	)

	if err != nil {
		logger.Error("shard processing completed with errors",
			zap.String("job_id", job.ID),
			zap.Error(err),
		)
		return err
	}

	stats := scheduler.GetStats()
	logger.Info("shard processing completed",
		zap.String("job_id", job.ID),
		zap.Int64("total_shards", stats.TotalShards),
		zap.Int64("completed_shards", stats.CompletedShards),
		zap.Int64("failed_shards", stats.FailedShards),
		zap.Int64("panic_count", stats.PanicCount),
		zap.Int64("max_goroutines", stats.MaxGoroutines),
		zap.Duration("duration", time.Since(stats.StartTime)),
	)

	var totalArchived int64
	var failedCount int64

	s.mu.RLock()
	for _, shard := range shards {
		if shard.Status == model.ShardStatusCompleted {
			totalArchived += shard.RecordCount
		} else if shard.Status == model.ShardStatusFailed {
			failedCount++
		}
	}
	s.mu.RUnlock()

	s.mu.Lock()
	job.ArchivedCount = totalArchived
	job.FailedCount = failedCount
	s.mu.Unlock()

	if failedCount > 0 {
		return fmt.Errorf("%d shards failed", failedCount)
	}

	return nil
}

func (s *ArchiveAppService) tryCompensate(ctx context.Context, job *model.ArchiveJob) error {
	job.StartCompensation()

	s.mu.RLock()
	shards := s.jobShards[job.ID]
	s.mu.RUnlock()

	var failedShards []*model.ArchiveShard
	for _, shard := range shards {
		if shard.Status == model.ShardStatusFailed && shard.CanRetry() {
			failedShards = append(failedShards, shard)
		}
	}

	if len(failedShards) == 0 {
		return fmt.Errorf("no retriable failed shards")
	}

	logger.Info("starting compensation",
		zap.String("job_id", job.ID),
		zap.Int("failed_shards", len(failedShards)),
	)

	compTask := s.compensation.CreateCompensation(job.ID, failedShards)

	return s.compensation.ExecuteCompensation(ctx, compTask, job.TableName, job.ColdDate)
}

func (s *ArchiveAppService) registerHivePartition(ctx context.Context, job *model.ArchiveJob) error {
	s.mu.RLock()
	shards := s.jobShards[job.ID]
	s.mu.RUnlock()

	ossPath := fmt.Sprintf("oss://%s/%s/%s/",
		s.ossBucketName(),
		s.ossPathPrefix(),
		job.ColdDate.Format("2006/01/02"),
	)

	partition := model.NewDayPartition(
		s.config.TargetTable,
		job.TableName,
		job.ColdDate,
		ossPath,
	)

	if err := s.hiveRepo.AddPartition(ctx, partition); err != nil {
		return fmt.Errorf("add hive partition failed: %w", err)
	}

	logger.Info("hive partition registered",
		zap.String("job_id", job.ID),
		zap.String("partition", partition.PartitionName),
		zap.String("location", ossPath),
	)

	_ = shards
	return nil
}

func (s *ArchiveAppService) loadToStarRocks(ctx context.Context, job *model.ArchiveJob) error {
	s.mu.RLock()
	shards := s.jobShards[job.ID]
	s.mu.RUnlock()

	var totalLoaded int64
	partitionName := "dt=" + job.ColdDate.Format("2006-01-02")

	for _, shard := range shards {
		if shard.Status == model.ShardStatusCompleted && shard.OSSPath != "" {
			ossPath := fmt.Sprintf("oss://%s/%s/%s",
				s.ossBucketName(),
				s.ossPathPrefix(),
				shard.OSSPath,
			)

			err := s.starrocksRepo.LoadDataFromOSS(ctx, s.config.TargetTable, ossPath, partitionName)
			if err != nil {
				logger.Warn("load shard to starrocks failed",
					zap.Int("shard_index", shard.ShardIndex),
					zap.Error(err),
				)
				continue
			}

			totalLoaded += shard.RecordCount
		}
	}

	logger.Info("starrocks load completed",
		zap.String("job_id", job.ID),
		zap.String("target_table", s.config.TargetTable),
		zap.Int64("loaded_records", totalLoaded),
	)

	return nil
}

func (s *ArchiveAppService) cleanupSourceData(ctx context.Context, job *model.ArchiveJob) error {
	s.mu.RLock()
	shards := s.jobShards[job.ID]
	s.mu.RUnlock()

	var deleted int64
	for _, shard := range shards {
		if shard.Status == model.ShardStatusCompleted {
			if err := s.sourceDB.DeleteShard(ctx, job.TableName, shard.StartID, shard.EndID); err != nil {
				logger.Warn("delete shard source data failed",
					zap.Int("shard_index", shard.ShardIndex),
					zap.Error(err),
				)
				continue
			}
			deleted += shard.RecordCount
		}
	}

	logger.Info("source data cleanup completed",
		zap.String("job_id", job.ID),
		zap.Int64("deleted_records", deleted),
	)

	return nil
}

func (s *ArchiveAppService) completeJob(jobID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, exists := s.jobs[jobID]
	if !exists {
		return
	}

	job.Complete()
	atomic.AddInt64(&s.stats.CompletedJobs, 1)
	atomic.AddInt64(&s.stats.TotalArchivedRecords, job.ArchivedCount)

	logger.Info("archive job completed",
		zap.String("job_id", jobID),
		zap.Int64("archived_count", job.ArchivedCount),
		zap.Duration("duration", job.EndTime.Sub(job.StartTime)),
	)
}

func (s *ArchiveAppService) failJob(jobID string, errMsg string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, exists := s.jobs[jobID]
	if !exists {
		return
	}

	job.Fail(errMsg)
	atomic.AddInt64(&s.stats.FailedJobs, 1)

	logger.Error("archive job failed",
		zap.String("job_id", jobID),
		zap.String("error", errMsg),
	)
}

func (s *ArchiveAppService) GetJobStatus(jobID string) (*dto.JobStatusResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	job, exists := s.jobs[jobID]
	if !exists {
		return nil, fmt.Errorf("job not found: %s", jobID)
	}

	endTimeStr := ""
	if job.EndTime != nil {
		endTimeStr = job.EndTime.Format(time.RFC3339)
	}

	return &dto.JobStatusResponse{
		JobID:         job.ID,
		JobName:       job.JobName,
		TableName:     job.TableName,
		Status:        string(job.Status),
		TotalRecords:  job.TotalRecords,
		ArchivedCount: job.ArchivedCount,
		FailedCount:   job.FailedCount,
		Progress:      job.Progress(),
		ShardCount:    job.ShardCount,
		Concurrency:   job.Concurrency,
		StartTime:     job.StartTime,
		EndTime:       endTimeStr,
		ErrorMessage:  job.ErrorMessage,
	}, nil
}

func (s *ArchiveAppService) GetShardStatus(jobID string) ([]dto.ShardStatusResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	shards, exists := s.jobShards[jobID]
	if !exists {
		return nil, fmt.Errorf("job not found: %s", jobID)
	}

	result := make([]dto.ShardStatusResponse, 0, len(shards))
	for _, shard := range shards {
		result = append(result, dto.ShardStatusResponse{
			ShardIndex:   shard.ShardIndex,
			Status:       string(shard.Status),
			RecordCount:  shard.RecordCount,
			OSSPath:      shard.OSSPath,
			RetryCount:   shard.RetryCount,
			ErrorMessage: shard.ErrorMessage,
		})
	}

	return result, nil
}

func (s *ArchiveAppService) ListJobs(limit, offset int) (*dto.ListJobsResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	jobs := make([]*model.ArchiveJob, 0, len(s.jobs))
	for _, job := range s.jobs {
		jobs = append(jobs, job)
	}

	total := len(jobs)

	if offset >= total {
		return &dto.ListJobsResponse{Total: total, Jobs: []dto.JobStatusResponse{}}, nil
	}

	end := offset + limit
	if end > total {
		end = total
	}

	result := make([]dto.JobStatusResponse, 0, end-offset)
	for i := offset; i < end; i++ {
		job := jobs[i]
		endTimeStr := ""
		if job.EndTime != nil {
			endTimeStr = job.EndTime.Format(time.RFC3339)
		}
		result = append(result, dto.JobStatusResponse{
			JobID:         job.ID,
			JobName:       job.JobName,
			TableName:     job.TableName,
			Status:        string(job.Status),
			TotalRecords:  job.TotalRecords,
			ArchivedCount: job.ArchivedCount,
			FailedCount:   job.FailedCount,
			Progress:      job.Progress(),
			ShardCount:    job.ShardCount,
			Concurrency:   job.Concurrency,
			StartTime:     job.StartTime,
			EndTime:       endTimeStr,
			ErrorMessage:  job.ErrorMessage,
		})
	}

	return &dto.ListJobsResponse{
		Total: total,
		Jobs:  result,
	}, nil
}

func (s *ArchiveAppService) PauseJob(jobID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, exists := s.jobs[jobID]
	if !exists {
		return fmt.Errorf("job not found: %s", jobID)
	}

	if job.Status != model.JobStatusRunning {
		return fmt.Errorf("job cannot be paused from status: %s", job.Status)
	}

	job.Pause()
	logger.Info("job paused", zap.String("job_id", jobID))
	return nil
}

func (s *ArchiveAppService) ResumeJob(ctx context.Context, jobID string) error {
	s.mu.Lock()
	job, exists := s.jobs[jobID]
	s.mu.Unlock()

	if !exists {
		return fmt.Errorf("job not found: %s", jobID)
	}

	if job.Status != model.JobStatusPaused {
		return fmt.Errorf("job cannot be resumed from status: %s", job.Status)
	}

	go s.executeJob(ctx, jobID)
	logger.Info("job resumed", zap.String("job_id", jobID))
	return nil
}

func (s *ArchiveAppService) CompensateJob(ctx context.Context, jobID string) (*dto.CompensationResponse, error) {
	s.mu.RLock()
	job, exists := s.jobs[jobID]
	shards := s.jobShards[jobID]
	s.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("job not found: %s", jobID)
	}

	if job.Status != model.JobStatusFailed {
		return nil, fmt.Errorf("compensation only available for failed jobs, current status: %s", job.Status)
	}

	var failedShards []*model.ArchiveShard
	for _, shard := range shards {
		if shard.Status == model.ShardStatusFailed {
			failedShards = append(failedShards, shard)
		}
	}

	if len(failedShards) == 0 {
		return nil, fmt.Errorf("no failed shards to compensate")
	}

	compTask := s.compensation.CreateCompensation(jobID, failedShards)

	go s.compensation.ExecuteCompensation(ctx, compTask, job.TableName, job.ColdDate)

	return &dto.CompensationResponse{
		CompensationID:  compTask.ID,
		Status:          string(compTask.Status),
		RetryShardCount: len(compTask.RetryShards),
	}, nil
}

func (s *ArchiveAppService) GetStats() *dto.StatsResponse {
	var runningJobs int

	s.mu.RLock()
	for _, job := range s.jobs {
		if job.Status == model.JobStatusRunning {
			runningJobs++
		}
	}
	s.mu.RUnlock()

	return &dto.StatsResponse{
		TotalJobs:           int(atomic.LoadInt64(&s.stats.TotalJobs)),
		RunningJobs:         runningJobs,
		CompletedJobs:       int(atomic.LoadInt64(&s.stats.CompletedJobs)),
		FailedJobs:          int(atomic.LoadInt64(&s.stats.FailedJobs)),
		TotalArchivedRecords: atomic.LoadInt64(&s.stats.TotalArchivedRecords),
		TotalOSSSize:        0,
	}
}

func (s *ArchiveAppService) ossBucketName() string {
	return "archive-bucket"
}

func (s *ArchiveAppService) ossPathPrefix() string {
	return "archive"
}
