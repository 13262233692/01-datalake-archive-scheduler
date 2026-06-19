package service

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"datalake-archive-scheduler/internal/application/dto"
	"datalake-archive-scheduler/internal/domain/model"
	"datalake-archive-scheduler/internal/domain/repository"
	domainservice "datalake-archive-scheduler/internal/domain/service"
	"datalake-archive-scheduler/internal/infrastructure/config"
	"datalake-archive-scheduler/internal/infrastructure/oss"
	"datalake-archive-scheduler/internal/infrastructure/persistance"
)

type FaultInjectingIterator struct {
	realIter  repository.DataRecordIterator
	panicProb float64
	errorProb float64
	index     int
}

func (it *FaultInjectingIterator) Next() (*model.DataRecord, error) {
	it.index++

	if rand.Float64() < it.panicProb {
		panic(fmt.Sprintf("simulated data parsing panic at record %d", it.index))
	}

	if rand.Float64() < it.errorProb {
		return nil, fmt.Errorf("simulated parsing error at record %d", it.index)
	}

	return it.realIter.Next()
}

func (it *FaultInjectingIterator) HasNext() bool {
	return it.realIter.HasNext()
}

func (it *FaultInjectingIterator) Close() error {
	return it.realIter.Close()
}

type FaultInjectingDB struct {
	*persistance.MockPolarDBRepository
	panicProb float64
	errorProb float64
}

func (db *FaultInjectingDB) FetchShard(ctx context.Context, tableName string, startID, endID int64, coldDate time.Time, batchSize int) (repository.DataRecordIterator, error) {
	realIter, err := db.MockPolarDBRepository.FetchShard(ctx, tableName, startID, endID, coldDate, batchSize)
	if err != nil {
		return nil, err
	}

	return &FaultInjectingIterator{
		realIter:  realIter,
		panicProb: db.panicProb,
		errorProb: db.errorProb,
	}, nil
}

func setupTestEnvironment() (context.Context, *ArchiveAppService, *persistance.MockPolarDBRepository) {
	cfg := &config.ArchiveConfig{
		ColdYears:       3,
		ShardCount:      10,
		Concurrency:     5,
		BatchSize:       100,
		MaxRetryCount:   3,
		MemoryLimitMB:   256,
		MaskingSalt:     "test-salt",
		TableName:       "order_detail",
		TargetTable:     "unicorn_pro_history",
	}

	sourceDB := persistance.NewMockPolarDBRepository(cfg.TableName)

	ossCfg := &config.OSSConfig{
		Bucket:     "test-bucket",
		PathPrefix: "archive",
	}
	ossRepo := oss.NewMockOSSRepository(ossCfg)

	hiveCfg := &config.HiveConfig{Database: "test"}
	hiveRepo := newMockHiveForTest(hiveCfg)

	srCfg := &config.StarRocksConfig{Database: "test"}
	srRepo := newMockStarRocksForTest(srCfg)

	cleaningSvc := domainservice.NewDataCleaningService()
	maskingSvc := domainservice.NewDataMaskingService(cfg.MaskingSalt)
	serialSvc := domainservice.NewJSONSerializationService()

	processor := NewStreamProcessor(
		sourceDB,
		ossRepo,
		cleaningSvc,
		maskingSvc,
		serialSvc,
		cfg.MemoryLimitBytes(),
		cfg.BatchSize,
	)

	compensation := NewCompensationService(
		sourceDB,
		ossRepo,
		hiveRepo,
		srRepo,
		processor,
	)

	appService := NewArchiveAppService(
		cfg,
		sourceDB,
		ossRepo,
		hiveRepo,
		srRepo,
		processor,
		compensation,
		cleaningSvc,
		maskingSvc,
	)

	return context.Background(), appService, sourceDB
}

func newMockHiveForTest(cfg *config.HiveConfig) *mockHive {
	return &mockHive{cfg: cfg}
}

type mockHive struct {
	cfg *config.HiveConfig
}

func (m *mockHive) GetTable(ctx context.Context, database, tableName string) (*model.PartitionInfo, error) {
	return nil, nil
}

func (m *mockHive) AddPartition(ctx context.Context, partition *model.PartitionInfo) error {
	return nil
}

func (m *mockHive) DropPartition(ctx context.Context, database, tableName, partitionName string) error {
	return nil
}

func (m *mockHive) GetPartition(ctx context.Context, database, tableName, partitionName string) (*model.PartitionInfo, error) {
	return nil, nil
}

func (m *mockHive) PartitionExists(ctx context.Context, database, tableName, partitionName string) (bool, error) {
	return false, nil
}

func (m *mockHive) AlterPartition(ctx context.Context, partition *model.PartitionInfo) error {
	return nil
}

func (m *mockHive) Close() error {
	return nil
}

func newMockStarRocksForTest(cfg *config.StarRocksConfig) *mockStarRocks {
	return &mockStarRocks{cfg: cfg}
}

type mockStarRocks struct {
	cfg *config.StarRocksConfig
}

func (m *mockStarRocks) LoadDataFromOSS(ctx context.Context, tableName string, ossPath string, partition string) error {
	return nil
}

func (m *mockStarRocks) ImportStream(ctx context.Context, tableName string, dataChannel <-chan []byte, format string) error {
	return nil
}

func (m *mockStarRocks) ExecuteSQL(ctx context.Context, sql string) error {
	return nil
}

func (m *mockStarRocks) TableExists(ctx context.Context, tableName string) (bool, error) {
	return true, nil
}

func (m *mockStarRocks) GetRecordCount(ctx context.Context, tableName string, partition string) (int64, error) {
	return 0, nil
}

func (m *mockStarRocks) Close() error {
	return nil
}

func TestSafeShardScheduler_NormalCase(t *testing.T) {
	ctx, appService, sourceDB := setupTestEnvironment()

	req := &dto.CreateArchiveJobRequest{
		TableName: "order_detail",
		ShardCount: 5,
		Concurrency: 3,
	}

	resp, err := appService.CreateJob(ctx, req)
	if err != nil {
		t.Fatalf("Failed to create job: %v", err)
	}

	if err := appService.StartJob(ctx, resp.JobID); err != nil {
		t.Fatalf("Failed to start job: %v", err)
	}

	timeout := time.After(60 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			t.Fatal("Job timed out")
		case <-ticker.C:
			status, err := appService.GetJobStatus(resp.JobID)
			if err != nil {
				t.Fatalf("Failed to get job status: %v", err)
			}

			if status.Status == string(model.JobStatusCompleted) {
				if status.ArchivedCount <= 0 {
					t.Fatalf("No records archived")
				}
				if status.FailedCount > 0 {
					t.Fatalf("Expected no failures, got %d", status.FailedCount)
				}
				activeIters := sourceDB.GetActiveIteratorCount()
				if activeIters != 0 {
					t.Fatalf("Iterator leak detected: %d active iterators", activeIters)
				}
				return
			}

			if status.Status == string(model.JobStatusFailed) {
				t.Fatalf("Job failed: %s", status.ErrorMessage)
			}
		}
	}
}

func TestSafeShardScheduler_WithPanics(t *testing.T) {
	cfg := &config.ArchiveConfig{
		ColdYears:       3,
		ShardCount:      10,
		Concurrency:     5,
		BatchSize:       100,
		MaxRetryCount:   3,
		MemoryLimitMB:   256,
		MaskingSalt:     "test-salt",
		TableName:       "order_detail",
		TargetTable:     "unicorn_pro_history",
	}

	baseDB := persistance.NewMockPolarDBRepository(cfg.TableName)
	faultDB := &FaultInjectingDB{
		MockPolarDBRepository: baseDB,
		panicProb:             0.02,
		errorProb:             0.03,
	}

	ossCfg := &config.OSSConfig{Bucket: "test-bucket", PathPrefix: "archive"}
	ossRepo := oss.NewMockOSSRepository(ossCfg)
	hiveRepo := newMockHiveForTest(&config.HiveConfig{Database: "test"})
	srRepo := newMockStarRocksForTest(&config.StarRocksConfig{Database: "test"})

	cleaningSvc := domainservice.NewDataCleaningService()
	maskingSvc := domainservice.NewDataMaskingService(cfg.MaskingSalt)
	serialSvc := domainservice.NewJSONSerializationService()

	processor := NewStreamProcessor(
		faultDB,
		ossRepo,
		cleaningSvc,
		maskingSvc,
		serialSvc,
		cfg.MemoryLimitBytes(),
		cfg.BatchSize,
	)

	compensation := NewCompensationService(
		faultDB,
		ossRepo,
		hiveRepo,
		srRepo,
		processor,
	)

	appService := NewArchiveAppService(
		cfg,
		faultDB,
		ossRepo,
		hiveRepo,
		srRepo,
		processor,
		compensation,
		cleaningSvc,
		maskingSvc,
	)

	ctx := context.Background()
	var wg sync.WaitGroup
	var completedJobs int64
	var failedJobs int64

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(jobNum int) {
			defer wg.Done()

			req := &dto.CreateArchiveJobRequest{
				TableName:   "order_detail",
				ShardCount:  8,
				Concurrency: 4,
			}

			resp, err := appService.CreateJob(ctx, req)
			if err != nil {
				atomic.AddInt64(&failedJobs, 1)
				t.Logf("Job %d create failed: %v", jobNum, err)
				return
			}

			if err := appService.StartJob(ctx, resp.JobID); err != nil {
				atomic.AddInt64(&failedJobs, 1)
				t.Logf("Job %d start failed: %v", jobNum, err)
				return
			}

			timeout := time.After(120 * time.Second)
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-timeout:
					t.Errorf("Job %d timed out", jobNum)
					atomic.AddInt64(&failedJobs, 1)

					leaked := baseDB.ForceCleanupLeakedIterators()
					if leaked > 0 {
						t.Errorf("Job %d leaked %d iterators after timeout", jobNum, leaked)
					}
					return
				case <-ticker.C:
					status, err := appService.GetJobStatus(resp.JobID)
					if err != nil {
						continue
					}

					if status.Status == string(model.JobStatusCompleted) || status.Status == string(model.JobStatusFailed) {
						if status.Status == string(model.JobStatusCompleted) {
							atomic.AddInt64(&completedJobs, 1)
						} else {
							atomic.AddInt64(&failedJobs, 1)
						}

						activeIters := baseDB.GetActiveIteratorCount()
						if activeIters > 0 {
							t.Errorf("Job %d finished with %d active iterators - potential leak", jobNum, activeIters)
							baseDB.ForceCleanupLeakedIterators()
						}
						return
					}
				}
			}
		}(i)
	}

	wg.Wait()

	activeIters := baseDB.GetActiveIteratorCount()
	if activeIters != 0 {
		t.Fatalf("Final iterator leak detected: %d active iterators", activeIters)
	}

	leakedIters := baseDB.GetLeakedIterators()
	if len(leakedIters) > 0 {
		t.Fatalf("Final leaked iterators: %d", len(leakedIters))
	}

	t.Logf("Test completed: %d completed, %d failed",
		atomic.LoadInt64(&completedJobs),
		atomic.LoadInt64(&failedJobs))
}

func TestSafeShardScheduler_ContextCancellation(t *testing.T) {
	ctx, appService, sourceDB := setupTestEnvironment()

	req := &dto.CreateArchiveJobRequest{
		TableName:   "order_detail",
		ShardCount:  20,
		Concurrency: 5,
	}

	resp, err := appService.CreateJob(ctx, req)
	if err != nil {
		t.Fatalf("Failed to create job: %v", err)
	}

	cancellableCtx, cancel := context.WithCancel(ctx)

	if err := appService.StartJob(cancellableCtx, resp.JobID); err != nil {
		t.Fatalf("Failed to start job: %v", err)
	}

	time.Sleep(1 * time.Second)

	cancel()
	t.Log("Context cancelled")

	time.Sleep(3 * time.Second)

	activeIters := sourceDB.GetActiveIteratorCount()
	if activeIters > 5 {
		t.Errorf("Too many active iterators after cancel: %d", activeIters)
	}

	leaked := sourceDB.ForceCleanupLeakedIterators()
	if leaked > 0 {
		t.Logf("Cleaned up %d leaked iterators after cancellation", leaked)
	}
}

func TestSafeShardScheduler_HighConcurrency(t *testing.T) {
	ctx, appService, sourceDB := setupTestEnvironment()

	concurrency := 10
	shardCount := 100

	req := &dto.CreateArchiveJobRequest{
		TableName:   "order_detail",
		ShardCount:  shardCount,
		Concurrency: concurrency,
	}

	resp, err := appService.CreateJob(ctx, req)
	if err != nil {
		t.Fatalf("Failed to create job: %v", err)
	}

	if err := appService.StartJob(ctx, resp.JobID); err != nil {
		t.Fatalf("Failed to start job: %v", err)
	}

	maxGoroutines := 0
	var mu sync.Mutex

	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			status, err := appService.GetJobStatus(resp.JobID)
			if err == nil && (status.Status == string(model.JobStatusCompleted) || status.Status == string(model.JobStatusFailed)) {
				return
			}

			activeIters := sourceDB.GetActiveIteratorCount()
			mu.Lock()
			if int(activeIters)+concurrency > maxGoroutines {
				maxGoroutines = int(activeIters) + concurrency
			}
			mu.Unlock()
		}
	}()

	timeout := time.After(120 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			t.Fatal("Job timed out")
		case <-ticker.C:
			status, err := appService.GetJobStatus(resp.JobID)
			if err != nil {
				continue
			}

			if status.Status == string(model.JobStatusCompleted) {
				mu.Lock()
				t.Logf("Max concurrent goroutines/iterators: ~%d", maxGoroutines)
				mu.Unlock()

				activeIters := sourceDB.GetActiveIteratorCount()
				if activeIters != 0 {
					t.Fatalf("Iterator leak: %d active after completion", activeIters)
				}
				return
			}

			if status.Status == string(model.JobStatusFailed) {
				t.Fatalf("Job failed: %s", status.ErrorMessage)
			}
		}
	}
}
