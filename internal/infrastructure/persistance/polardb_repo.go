package persistance

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"datalake-archive-scheduler/internal/domain/model"
	"datalake-archive-scheduler/internal/domain/repository"
	"datalake-archive-scheduler/pkg/logger"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

type MockPolarDBRepository struct {
	mu            sync.RWMutex
	data          map[int64]*model.DataRecord
	tableName     string
	maxID         int64
	activeIters   int64
	closedIters   int64
	leakDetectMu  sync.Mutex
	leakDetectMap map[string]string
}

func NewMockPolarDBRepository(tableName string) *MockPolarDBRepository {
	repo := &MockPolarDBRepository{
		data:          make(map[int64]*model.DataRecord),
		tableName:     tableName,
		leakDetectMap: make(map[string]string),
	}
	repo.generateMockData()
	return repo
}

func (r *MockPolarDBRepository) generateMockData() {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	totalRecords := 100000

	for i := 1; i <= totalRecords; i++ {
		id := int64(i)
		yearsAgo := rand.Intn(5) + 1
		daysAgo := rand.Intn(365)
		createdAt := now.AddDate(-yearsAgo, 0, -daysAgo)

		statuses := []string{"SUCCESS", "FAILED", "PENDING", "PROCESSING"}
		currencies := []string{"CNY", "USD", "EUR"}

		record := &model.DataRecord{
			ID:        id,
			UserID:    fmt.Sprintf("USER%08d", rand.Intn(10000)),
			OrderID:   fmt.Sprintf("ORD%012d", id),
			Amount:    float64(rand.Intn(100000)) / 100,
			Currency:  currencies[rand.Intn(len(currencies))],
			Status:    statuses[rand.Intn(len(statuses))],
			CreatedAt: createdAt,
			UpdatedAt: createdAt.Add(time.Duration(rand.Intn(86400)) * time.Second),
			Data: map[string]interface{}{
				"phone":      fmt.Sprintf("1%d", 10000000000+rand.Int63n(9999999999)),
				"email":      fmt.Sprintf("user%d@example.com", i),
				"name":       fmt.Sprintf("用户%d", i),
				"product_id": fmt.Sprintf("PROD%05d", rand.Intn(1000)),
				"quantity":   rand.Intn(10) + 1,
			},
		}

		r.data[id] = record
		r.maxID = id
	}

	logger.Info("mock polardb data generated", zap.Int("count", totalRecords))
}

func (r *MockPolarDBRepository) CountColdData(ctx context.Context, tableName string, coldDate time.Time) (int64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var count int64
	for _, rec := range r.data {
		if rec.CreatedAt.Before(coldDate) {
			count++
		}
	}

	logger.Debug("cold data count",
		zap.String("table", tableName),
		zap.Time("cold_date", coldDate),
		zap.Int64("count", count),
	)

	return count, nil
}

func (r *MockPolarDBRepository) GetMinMaxID(ctx context.Context, tableName string, coldDate time.Time) (int64, int64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var minID, maxID int64
	first := true

	for _, rec := range r.data {
		if rec.CreatedAt.Before(coldDate) {
			if first {
				minID = rec.ID
				maxID = rec.ID
				first = false
			}
			if rec.ID < minID {
				minID = rec.ID
			}
			if rec.ID > maxID {
				maxID = rec.ID
			}
		}
	}

	if first {
		return 0, 0, fmt.Errorf("no cold data found")
	}

	return minID, maxID, nil
}

func (r *MockPolarDBRepository) FetchShard(ctx context.Context, tableName string, startID, endID int64, coldDate time.Time, batchSize int) (repository.DataRecordIterator, error) {
	logger.Debug("fetch shard started",
		zap.Int64("start_id", startID),
		zap.Int64("end_id", endID),
		zap.Int("batch_size", batchSize),
	)

	iterID := uuid.New().String()
	atomic.AddInt64(&r.activeIters, 1)

	r.leakDetectMu.Lock()
	r.leakDetectMap[iterID] = fmt.Sprintf("shard[%d-%d] created at %s",
		startID, endID, time.Now().Format(time.RFC3339))
	r.leakDetectMu.Unlock()

	return &mockDataIterator{
		repo:         r,
		iterID:       iterID,
		startID:      startID,
		endID:        endID,
		coldDate:     coldDate,
		current:      startID - 1,
		closed:       false,
		lockHeld:     false,
		batchSize:    batchSize,
	}, nil
}

func (r *MockPolarDBRepository) DeleteShard(ctx context.Context, tableName string, startID, endID int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var count int64
	for id := startID; id <= endID; id++ {
		if _, exists := r.data[id]; exists {
			delete(r.data, id)
			count++
		}
	}

	logger.Info("shard deleted",
		zap.Int64("start_id", startID),
		zap.Int64("end_id", endID),
		zap.Int64("deleted_count", count),
	)

	return nil
}

func (r *MockPolarDBRepository) Close() error {
	logger.Info("mock polardb connection closed")
	return nil
}

type mockDataIterator struct {
	repo      *MockPolarDBRepository
	iterID    string
	startID   int64
	endID     int64
	coldDate  time.Time
	current   int64
	closed    bool
	lockHeld  bool
	batchSize int
}

func (it *mockDataIterator) safeRLock() {
	it.repo.mu.RLock()
	it.lockHeld = true
}

func (it *mockDataIterator) safeRUnlock() {
	if it.lockHeld {
		it.repo.mu.RUnlock()
		it.lockHeld = false
	}
}

func (it *mockDataIterator) Next() (*model.DataRecord, error) {
	it.safeRLock()
	defer it.safeRUnlock()

	defer func() {
		if r := recover(); r != nil {
			logger.Error("iterator Next() panic recovered",
				zap.String("iter_id", it.iterID),
				zap.Any("panic", r),
			)
			it.safeRUnlock()
		}
	}()

	for it.current < it.endID {
		it.current++
		rec, exists := it.repo.data[it.current]
		if exists && rec.CreatedAt.Before(it.coldDate) {
			cloned := *rec
			clonedData := make(map[string]interface{}, len(rec.Data))
			for k, v := range rec.Data {
				clonedData[k] = v
			}
			cloned.Data = clonedData
			return &cloned, nil
		}
	}

	return nil, fmt.Errorf("no more records")
}

func (it *mockDataIterator) HasNext() bool {
	it.safeRLock()
	defer it.safeRUnlock()

	defer func() {
		if r := recover(); r != nil {
			logger.Error("iterator HasNext() panic recovered",
				zap.String("iter_id", it.iterID),
				zap.Any("panic", r),
			)
			it.safeRUnlock()
		}
	}()

	for id := it.current + 1; id <= it.endID; id++ {
		rec, exists := it.repo.data[id]
		if exists && rec.CreatedAt.Before(it.coldDate) {
			return true
		}
	}
	return false
}

func (it *mockDataIterator) Close() error {
	if it.closed {
		return nil
	}

	it.closed = true

	it.safeRUnlock()

	atomic.AddInt64(&it.repo.activeIters, -1)
	atomic.AddInt64(&it.repo.closedIters, 1)

	it.repo.leakDetectMu.Lock()
	delete(it.repo.leakDetectMap, it.iterID)
	it.repo.leakDetectMu.Unlock()

	logger.Debug("iterator closed safely",
		zap.String("iter_id", it.iterID),
		zap.Int64("active_iters", atomic.LoadInt64(&it.repo.activeIters)),
	)

	return nil
}

func (r *MockPolarDBRepository) GetActiveIteratorCount() int64 {
	return atomic.LoadInt64(&r.activeIters)
}

func (r *MockPolarDBRepository) GetLeakedIterators() map[string]string {
	r.leakDetectMu.Lock()
	defer r.leakDetectMu.Unlock()

	result := make(map[string]string, len(r.leakDetectMap))
	for k, v := range r.leakDetectMap {
		result[k] = v
	}
	return result
}

func (r *MockPolarDBRepository) ForceCleanupLeakedIterators() int {
	r.leakDetectMu.Lock()
	defer r.leakDetectMu.Unlock()

	count := len(r.leakDetectMap)
	if count > 0 {
		logger.Warn("force cleaning up leaked iterators",
			zap.Int("leaked_count", count),
		)

		atomic.AddInt64(&r.activeIters, -int64(count))
		atomic.AddInt64(&r.closedIters, int64(count))

		r.leakDetectMap = make(map[string]string)
	}

	return count
}
