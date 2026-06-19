package service

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"datalake-archive-scheduler/internal/domain/model"
	"datalake-archive-scheduler/internal/domain/repository"
	"datalake-archive-scheduler/pkg/crypto/merkletree"
	"datalake-archive-scheduler/pkg/logger"
	"datalake-archive-scheduler/pkg/queue"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

type AuditConfig struct {
	Enabled              bool
	SampleRate           float64
	BatchSize            int
	Concurrency          int
	QueueCapacity        int
	WebhookURL           string
	WebhookTimeout       time.Duration
	MaxRetries           int
	AlertThresholdMiss   float64
	AlertThresholdMismatch float64
}

type AuditBatchTask struct {
	AuditJobID     string
	BatchIndex     int
	StartID        int64
	EndID          int64
	RecordIDs      []int64
	SourceTableName string
	TargetTableName string
	ColdDate       time.Time
}

type AuditService struct {
	config          AuditConfig
	sourceDB       repository.SourceDBRepository
	targetDB       repository.StarRocksRepository
	queue          *queue.BatchingQueue
	consumer       *queue.QueueConsumer
	jobs            map[string]*model.AuditJob
	batches         map[string][]*model.AuditBatch
	mu              sync.RWMutex
	running         int32
	auditCount     int64
	alertCount     int64
	passCount      int64
	webhookSender  *WebhookSender
	stats           AuditStats
}

type AuditStats struct {
	TotalAudits     int64
	PassedAudits    int64
	FailedAudits    int64
	AlertAudits     int64
	TotalSampled    int64
	TotalMismatches  int64
	TotalMissings    int64
}

func NewAuditService(
	cfg AuditConfig,
	sourceDB repository.SourceDBRepository,
	targetDB repository.StarRocksRepository,
) *AuditService {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 2
	}
	if cfg.QueueCapacity <= 0 {
		cfg.QueueCapacity = 10000
	}
	if cfg.SampleRate <= 0 || cfg.SampleRate > 1 {
		cfg.SampleRate = 0.1
	}

	svc := &AuditService{
		config:      cfg,
		sourceDB:    sourceDB,
		targetDB:    targetDB,
		jobs:        make(map[string]*model.AuditJob),
		batches:     make(map[string][]*model.AuditBatch),
		webhookSender: NewWebhookSender(cfg.WebhookURL, cfg.WebhookTimeout, cfg.MaxRetries),
	}

	svc.queue = queue.NewBatchingQueue(cfg.QueueCapacity, cfg.BatchSize)
	svc.consumer = queue.NewQueueConsumer(svc.queue, cfg.Concurrency, svc.processBatch)

	return svc
}

func (s *AuditService) Start() {
	if !atomic.CompareAndSwapInt32(&s.running, 0, 1) {
		return
	}

	s.queue.StartFlushTicker(100 * time.Millisecond)
	s.consumer.Start()

	logger.Info("audit service started",
		zap.Int("queue_capacity", s.config.QueueCapacity),
		zap.Int("concurrency", s.config.Concurrency),
		zap.Float64("sample_rate", s.config.SampleRate),
	)
}

func (s *AuditService) Stop() {
	if !atomic.CompareAndSwapInt32(&s.running, 1, 0) {
		return
	}

	s.consumer.Stop()
	s.queue.Stop()

	logger.Info("audit service stopped")
}

func (s *AuditService) SubmitAudit(archiveJobID, sourceTableName, targetTableName string, totalRecords int64, coldDate time.Time) (string, error) {
	if atomic.LoadInt32(&s.running) == 0 {
		return "", fmt.Errorf("audit service not running")
	}

	jobID := uuid.New().String()
	now := time.Now()

	job := &model.AuditJob{
		ID:           jobID,
		ArchiveJobID:  archiveJobID,
		TableName:     sourceTableName,
		TargetTableName: targetTableName,
		Status:        model.AuditStatusPending,
		TotalRecords:  totalRecords,
		CreatedAt:     now,
	}

	s.mu.Lock()
	s.jobs[jobID] = job
	s.batches[jobID] = make([]*model.AuditBatch, 0)
	s.mu.Unlock()

	go s.dispatchAuditBatches(job, coldDate)

	return jobID, nil
}

func (s *AuditService) dispatchAuditBatches(job *model.AuditJob, coldDate time.Time) {
	defer func() {
		if r := recover(); r != nil {
			logger.Error("audit dispatch panic recovered",
				zap.String("job_id", job.ID),
				zap.Any("panic", r),
			)
		}
	}()

	s.mu.Lock()
	job.Status = model.AuditStatusRunning
	now := time.Now()
	job.StartedAt = &now
	s.mu.Unlock()

	minID, maxID, err := s.sourceDB.GetMinMaxID(context.Background(), job.TableName, coldDate)
	if err != nil {
		s.failJob(job.ID, fmt.Sprintf("get min max id failed: %v", err))
		return
	}

	totalRange := maxID - minID + 1
	if totalRange <= 0 {
		s.failJob(job.ID, "no records to audit")
		return
	}

	sampleCount := int64(float64(totalRange) * s.config.SampleRate)
	if sampleCount < 10 {
		sampleCount = 10
	}
	if sampleCount > job.TotalRecords {
		sampleCount = job.TotalRecords
	}

	batchSize := s.config.BatchSize
	batchCount := int(sampleCount) / batchSize
	if int(sampleCount)%batchSize > 0 {
		batchCount++
	}

	sampledIDs := s.sampleRecordIDs(minID, maxID, int(sampleCount))
	sort.Slice(sampledIDs, func(i, j int) bool { return sampledIDs[i] < sampledIDs[j] })

	s.mu.Lock()
	job.BatchCount = batchCount
	job.SampledRecords = sampleCount
	s.mu.Unlock()

	for i := 0; i < batchCount; i++ {
		start := i * batchSize
		end := start + batchSize
		if end > len(sampledIDs) {
			end = len(sampledIDs)
		}

		batchIDs := sampledIDs[start:end]
		if len(batchIDs) == 0 {
			continue
		}

		task := &AuditBatchTask{
			AuditJobID:     job.ID,
			BatchIndex:     i,
			StartID:        batchIDs[0],
			EndID:          batchIDs[len(batchIDs)-1],
			RecordIDs:      batchIDs,
			SourceTableName: job.TableName,
			TargetTableName: job.TargetTableName,
			ColdDate:       coldDate,
		}

		batch := &model.AuditBatch{
			ID:          fmt.Sprintf("%s-%d", job.ID, i),
			AuditJobID:  job.ID,
			BatchIndex:  i,
			StartID:     batchIDs[0],
			EndID:       batchIDs[len(batchIDs)-1],
			RecordCount: len(batchIDs),
			Status:      "pending",
		}

		s.mu.Lock()
		s.batches[job.ID] = append(s.batches[job.ID], batch)
		s.mu.Unlock()

		if !s.queue.Enqueue(task) {
			logger.Warn("audit queue full, dropping batch",
				zap.String("job_id", job.ID),
				zap.Int("batch_index", i),
			)
			batch.Status = "skipped"
		}
	}

	go s.monitorJobCompletion(job.ID)
}

func (s *AuditService) sampleRecordIDs(minID, maxID int64, count int) []int64 {
	if count <= 0 {
		return nil
	}

	total := maxID - minID + 1
	if total <= int64(count) {
		ids := make([]int64, 0, total)
		for id := minID; id <= maxID; id++ {
			ids = append(ids, id)
		}
		return ids
	}

	ids := make([]int64, 0, count)
	step := float64(total) / float64(count)

	for i := 0; i < count; i++ {
		id := minID + int64(float64(i)*step+rand.Float64()*step)
		if id > maxID {
			id = maxID
		}
		ids = append(ids, id)
	}

	return ids
}

func (s *AuditService) processBatch(batch []interface{}) error {
	for _, item := range batch {
		task, ok := item.(*AuditBatchTask)
		if !ok {
			continue
		}

		s.processAuditBatch(task)
	}
	return nil
}

func (s *AuditService) processAuditBatch(task *AuditBatchTask) {
	defer func() {
		if r := recover(); r != nil {
			logger.Error("audit batch panic recovered",
				zap.String("job_id", task.AuditJobID),
				zap.Int("batch_index", task.BatchIndex),
				zap.Any("panic", r),
			)
		}
	}()

	ctx := context.Background()

	sourceHashMap := s.computeSourceHashes(ctx, task)
	targetHashMap := s.computeTargetHashes(ctx, task)

	matchedIDs, mismatchedIDs, missingIDs := s.compareHashMaps(sourceHashMap, targetHashMap)

	sourceHashes := make([]string, 0, len(sourceHashMap))
	for _, h := range sourceHashMap {
		sourceHashes = append(sourceHashes, h)
	}
	sort.Strings(sourceHashes)

	targetHashes := make([]string, 0, len(targetHashMap))
	for _, h := range targetHashMap {
		targetHashes = append(targetHashes, h)
	}
	sort.Strings(targetHashes)

	sourceTree := merkletree.NewMerkleTree(sourceHashes)
	targetTree := merkletree.NewMerkleTree(targetHashes)

	batch := s.getBatch(task.AuditJobID, task.BatchIndex)
	if batch != nil {
		now := time.Now()
		batch.SourceHash = sourceTree.GetRootHash()
		batch.TargetHash = targetTree.GetRootHash()
		batch.IsMatched = len(mismatchedIDs) == 0 && len(missingIDs) == 0
		batch.MismatchedIDs = mismatchedIDs
		batch.MissingIDs = missingIDs
		batch.Status = "completed"
		batch.ProcessedAt = &now
	}

	s.updateJobStats(task.AuditJobID, len(matchedIDs), len(mismatchedIDs), len(missingIDs))

	logger.Debug("audit batch processed",
		zap.String("job_id", task.AuditJobID),
		zap.Int("batch_index", task.BatchIndex),
		zap.Int("matched", len(matchedIDs)),
		zap.Int("mismatched", len(mismatchedIDs)),
		zap.Int("missing", len(missingIDs)),
	)
}

func (s *AuditService) computeSourceHashes(ctx context.Context, task *AuditBatchTask) map[int64]string {
	hashMap := make(map[int64]string, len(task.RecordIDs))

	for _, id := range task.RecordIDs {
		iter, err := s.sourceDB.FetchShard(ctx, task.SourceTableName, id, id, task.ColdDate, 1)
		if err != nil {
			continue
		}

		if iter.HasNext() {
			rec, err := iter.Next()
			if err == nil {
				fields := map[string]interface{}{
					"amount":     rec.Amount,
					"created_at": rec.CreatedAt,
					"updated_at": rec.UpdatedAt,
					"order_id":   rec.OrderID,
					"status":     rec.Status,
				}
				h := merkletree.RecordHash(rec.ID, fields)
				hashMap[rec.ID] = h
			}
		}

		iter.Close()
	}

	return hashMap
}

func (s *AuditService) computeTargetHashes(ctx context.Context, task *AuditBatchTask) map[int64]string {
	hashMap := make(map[int64]string, len(task.RecordIDs))

	for _, id := range task.RecordIDs {
		fields, found := s.targetDB.GetRecordByID(ctx, task.TargetTableName, id)
		if found {
			coreFields := map[string]interface{}{
				"amount":     fields["amount"],
				"created_at": fields["created_at"],
				"updated_at": fields["updated_at"],
				"order_id":   fields["order_id"],
				"status":     fields["status"],
			}
			h := merkletree.RecordHash(id, coreFields)
			hashMap[id] = h
		}
	}

	return hashMap
}

func (s *AuditService) compareHashMaps(sourceMap, targetMap map[int64]string) (matched []int64, mismatched []int64, missing []int64) {
	for id, sourceHash := range sourceMap {
		if targetHash, exists := targetMap[id]; exists {
			if sourceHash == targetHash {
				matched = append(matched, id)
			} else {
				mismatched = append(mismatched, id)
			}
		} else {
			missing = append(missing, id)
		}
	}

	return matched, mismatched, missing
}

func (s *AuditService) getBatch(jobID string, batchIndex int) *model.AuditBatch {
	s.mu.RLock()
	defer s.mu.RUnlock()

	batches, ok := s.batches[jobID]
	if !ok || batchIndex >= len(batches) {
		return nil
	}
	return batches[batchIndex]
}

func (s *AuditService) updateJobStats(jobID string, matchCount, mismatchCount, missingCount int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, ok := s.jobs[jobID]
	if !ok {
		return
	}

	job.MatchCount += matchCount
	job.MismatchCount += mismatchCount
	job.MissingCount += missingCount
}

func (s *AuditService) monitorJobCompletion(jobID string) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	timeout := time.After(30 * time.Minute)

	for {
		select {
		case <-timeout:
			logger.Warn("audit job timeout", zap.String("job_id", jobID))
			s.failJob(jobID, "audit timeout")
			return
		case <-ticker.C:
			if s.checkJobComplete(jobID) {
				return
			}
		}
	}
}

func (s *AuditService) checkJobComplete(jobID string) bool {
	s.mu.RLock()
	_, ok := s.jobs[jobID]
	if !ok {
		s.mu.RUnlock()
		return true
	}

	batches := s.batches[jobID]
	completedCount := 0
	skippedCount := 0
	for _, b := range batches {
		if b.Status == "completed" {
			completedCount++
		}
		if b.Status == "skipped" {
			skippedCount++
		}
	}
	s.mu.RUnlock()

	if completedCount+skippedCount >= len(batches) && len(batches) > 0 {
		s.finalizeJob(jobID)
		return true
	}

	return false
}

func (s *AuditService) finalizeJob(jobID string) {
	s.mu.Lock()
	job, ok := s.jobs[jobID]
	if !ok {
		s.mu.Unlock()
		return
	}

	batches := s.batches[jobID]
	sourceHashes := make([]string, 0, len(batches))
	targetHashes := make([]string, 0, len(batches))

	for _, b := range batches {
		if b.SourceHash != "" {
			sourceHashes = append(sourceHashes, b.SourceHash)
		}
		if b.TargetHash != "" {
			targetHashes = append(targetHashes, b.TargetHash)
		}
	}

	sourceRoot := ""
	targetRoot := ""
	if len(sourceHashes) > 0 {
		sourceTree := merkletree.NewMerkleTree(sourceHashes)
		sourceRoot = sourceTree.GetRootHash()
	}
	if len(targetHashes) > 0 {
		targetTree := merkletree.NewMerkleTree(targetHashes)
		targetRoot = targetTree.GetRootHash()
	}

	job.SourceRootHash = sourceRoot
	job.TargetRootHash = targetRoot

	totalChecked := job.MatchCount + job.MismatchCount + job.MissingCount
	mismatchRate := 0.0
	missingRate := 0.0
	if totalChecked > 0 {
		mismatchRate = float64(job.MismatchCount) / float64(totalChecked)
		missingRate = float64(job.MissingCount) / float64(totalChecked)
	}

	now := time.Now()
	job.CompletedAt = &now

	if job.MismatchCount == 0 && job.MissingCount == 0 {
		job.Status = model.AuditStatusPassed
		job.AlertLevel = "none"
		atomic.AddInt64(&s.passCount, 1)
	} else if mismatchRate >= s.config.AlertThresholdMismatch || missingRate >= s.config.AlertThresholdMiss {
		job.Status = model.AuditStatusAlert
		job.AlertLevel = "critical"
		atomic.AddInt64(&s.alertCount, 1)
	} else {
		job.Status = model.AuditStatusFailed
		job.AlertLevel = "warning"
	}

	duration := now.Sub(job.CreatedAt)
	job.DurationMs = duration.Milliseconds()

	atomic.AddInt64(&s.auditCount, 1)
	atomic.AddInt64(&s.stats.TotalAudits, 1)

	if job.Status == model.AuditStatusPassed {
		atomic.AddInt64(&s.stats.PassedAudits, 1)
	} else if job.Status == model.AuditStatusFailed {
		atomic.AddInt64(&s.stats.FailedAudits, 1)
	} else if job.Status == model.AuditStatusAlert {
		atomic.AddInt64(&s.stats.AlertAudits, 1)
	}

	atomic.AddInt64(&s.stats.TotalSampled, job.SampledRecords)
	atomic.AddInt64(&s.stats.TotalMismatches, int64(job.MismatchCount))
	atomic.AddInt64(&s.stats.TotalMissings, int64(job.MissingCount))

	needAlert := job.Status == model.AuditStatusAlert
	s.mu.Unlock()

	if needAlert {
		go s.triggerAlert(job)
	}

	logger.Info("audit job completed",
		zap.String("job_id", jobID),
		zap.String("status", string(job.Status)),
		zap.String("alert_level", job.AlertLevel),
		zap.Int64("sampled_records", job.SampledRecords),
		zap.Int("match_count", job.MatchCount),
		zap.Int("mismatch_count", job.MismatchCount),
		zap.Int("missing_count", job.MissingCount),
		zap.String("source_root_hash", job.SourceRootHash),
		zap.String("target_root_hash", job.TargetRootHash),
		zap.Int64("duration_ms", job.DurationMs),
	)
}

func (s *AuditService) triggerAlert(job *model.AuditJob) {
	alert := map[string]interface{}{
		"level":            "critical",
		"audit_job_id":     job.ID,
		"archive_job_id":   job.ArchiveJobID,
		"table_name":       job.TableName,
		"source_root_hash": job.SourceRootHash,
		"target_root_hash": job.TargetRootHash,
		"mismatch_count":   job.MismatchCount,
		"missing_count":    job.MissingCount,
		"sampled_records":  job.SampledRecords,
		"alert_type":       "data_inconsistency",
		"timestamp":        time.Now().Format(time.RFC3339),
		"message":          fmt.Sprintf("CRITICAL: Data integrity violation detected! %d mismatches, %d missing records out of %d sampled",
			job.MismatchCount, job.MissingCount, job.SampledRecords),
	}

	if err := s.webhookSender.Send(alert); err != nil {
		logger.Error("webhook alert failed",
			zap.String("job_id", job.ID),
			zap.Error(err),
		)
	} else {
		logger.Warn("webhook alert sent successfully",
			zap.String("job_id", job.ID),
			zap.String("level", "critical"),
		)
	}
}

func (s *AuditService) failJob(jobID string, errMsg string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, ok := s.jobs[jobID]
	if !ok {
		return
	}

	now := time.Now()
	job.Status = model.AuditStatusFailed
	job.ErrorMessage = errMsg
	job.CompletedAt = &now
	job.DurationMs = now.Sub(job.CreatedAt).Milliseconds()
}

func (s *AuditService) GetAuditJob(jobID string) (*model.AuditJob, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	job, ok := s.jobs[jobID]
	return job, ok
}

func (s *AuditService) GetAuditBatches(jobID string) ([]*model.AuditBatch, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	batches, ok := s.batches[jobID]
	if !ok {
		return nil, false
	}

	result := make([]*model.AuditBatch, len(batches))
	copy(result, batches)
	return result, true
}

func (s *AuditService) ListAuditJobs(limit, offset int) ([]*model.AuditJob, int) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	jobs := make([]*model.AuditJob, 0, len(s.jobs))
	for _, job := range s.jobs {
		jobs = append(jobs, job)
	}

	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].CreatedAt.After(jobs[j].CreatedAt)
	})

	total := len(jobs)

	if offset >= total {
		return []*model.AuditJob{}, total
	}

	end := offset + limit
	if end > total {
		end = total
	}

	return jobs[offset:end], total
}

func (s *AuditService) GetStats() AuditStats {
	return AuditStats{
		TotalAudits:    atomic.LoadInt64(&s.stats.TotalAudits),
		PassedAudits:   atomic.LoadInt64(&s.stats.PassedAudits),
		FailedAudits:   atomic.LoadInt64(&s.stats.FailedAudits),
		AlertAudits:    atomic.LoadInt64(&s.stats.AlertAudits),
		TotalSampled:   atomic.LoadInt64(&s.stats.TotalSampled),
		TotalMismatches: atomic.LoadInt64(&s.stats.TotalMismatches),
		TotalMissings:  atomic.LoadInt64(&s.stats.TotalMissings),
	}
}

func (s *AuditService) GetQueueStats() map[string]interface{} {
	return map[string]interface{}{
		"queue_size":  s.queue.Len(),
		"is_running": s.consumer.IsRunning(),
	}
}

func (s *AuditService) IsRunning() bool {
	return atomic.LoadInt32(&s.running) == 1
}
