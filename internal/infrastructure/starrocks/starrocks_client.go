package starrocks

import (
	"context"
	"fmt"
	"sync"

	"datalake-archive-scheduler/internal/domain/model"
	"datalake-archive-scheduler/internal/domain/repository"
	"datalake-archive-scheduler/internal/infrastructure/config"
	"datalake-archive-scheduler/pkg/logger"

	"go.uber.org/zap"
)

type MockStarRocksRepository struct {
	config        *config.StarRocksConfig
	mu            sync.RWMutex
	tables        map[string]*TableInfo
	records       map[string]map[int64]map[string]interface{}
	loadedRecords int64
}

type TableInfo struct {
	Name        string
	Columns     []string
	Partitioned bool
	Partitions  map[string]int64
	TotalRows   int64
}

func NewMockStarRocksRepository(cfg *config.StarRocksConfig) repository.StarRocksRepository {
	repo := &MockStarRocksRepository{
		config:  cfg,
		tables:  make(map[string]*TableInfo),
		records: make(map[string]map[int64]map[string]interface{}),
	}
	repo.initDefaultTable()
	return repo
}

func (r *MockStarRocksRepository) initDefaultTable() {
	tableName := r.config.Database + "." + "unicorn_pro_history"
	r.tables[tableName] = &TableInfo{
		Name:        tableName,
		Columns:     []string{"id", "user_id", "order_id", "amount", "currency", "status", "data", "dt", "created_at", "updated_at"},
		Partitioned: true,
		Partitions:  make(map[string]int64),
		TotalRows:   0,
	}
	r.records[tableName] = make(map[int64]map[string]interface{})
	logger.Info("starrocks default table initialized", zap.String("table", tableName))
}

func (r *MockStarRocksRepository) LoadDataFromOSS(ctx context.Context, tableName string, ossPath string, partition string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	fullTableName := r.config.Database + "." + tableName
	table, exists := r.tables[fullTableName]
	if !exists {
		return fmt.Errorf("table not found: %s", fullTableName)
	}

	loadedCount := int64(1000 + int(partition[3])%100)
	table.Partitions[partition] += loadedCount
	table.TotalRows += loadedCount
	r.loadedRecords += loadedCount

	logger.Info("starrocks load data from oss success",
		zap.String("table", fullTableName),
		zap.String("oss_path", ossPath),
		zap.String("partition", partition),
		zap.Int64("loaded_rows", loadedCount),
	)

	return nil
}

func (r *MockStarRocksRepository) ImportStream(ctx context.Context, tableName string, dataChannel <-chan []byte, format string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	fullTableName := r.config.Database + "." + tableName
	table, exists := r.tables[fullTableName]
	if !exists {
		return fmt.Errorf("table not found: %s", fullTableName)
	}

	var count int64
	for range dataChannel {
		count++
	}

	table.TotalRows += count
	r.loadedRecords += count

	logger.Info("starrocks stream import success",
		zap.String("table", fullTableName),
		zap.String("format", format),
		zap.Int64("rows", count),
	)

	return nil
}

func (r *MockStarRocksRepository) ExecuteSQL(ctx context.Context, sql string) error {
	logger.Debug("starrocks execute sql", zap.String("sql", sql))
	return nil
}

func (r *MockStarRocksRepository) TableExists(ctx context.Context, tableName string) (bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	fullTableName := r.config.Database + "." + tableName
	_, exists := r.tables[fullTableName]
	return exists, nil
}

func (r *MockStarRocksRepository) GetRecordCount(ctx context.Context, tableName string, partition string) (int64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	fullTableName := r.config.Database + "." + tableName
	table, exists := r.tables[fullTableName]
	if !exists {
		return 0, fmt.Errorf("table not found: %s", fullTableName)
	}

	if partition != "" {
		return table.Partitions[partition], nil
	}

	return table.TotalRows, nil
}

func (r *MockStarRocksRepository) GetTotalLoadedRecords() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.loadedRecords
}

func (r *MockStarRocksRepository) GetRecordByID(ctx context.Context, tableName string, id int64) (map[string]interface{}, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	fullTableName := r.config.Database + "." + tableName
	tableRecords, exists := r.records[fullTableName]
	if !exists {
		return nil, false
	}

	rec, found := tableRecords[id]
	if !found {
		return nil, false
	}

	result := make(map[string]interface{}, len(rec))
	for k, v := range rec {
		result[k] = v
	}
	return result, true
}

func (r *MockStarRocksRepository) GetRecordsByIDRange(ctx context.Context, tableName string, startID, endID int64) ([]map[string]interface{}, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	fullTableName := r.config.Database + "." + tableName
	tableRecords, exists := r.records[fullTableName]
	if !exists {
		return nil, fmt.Errorf("table not found: %s", fullTableName)
	}

	var result []map[string]interface{}
	for id := startID; id <= endID; id++ {
		if rec, found := tableRecords[id]; found {
			recCopy := make(map[string]interface{}, len(rec))
			for k, v := range rec {
				recCopy[k] = v
			}
			result = append(result, recCopy)
		}
	}

	return result, nil
}

func (r *MockStarRocksRepository) InsertRecord(tableName string, id int64, record map[string]interface{}) {
	r.mu.Lock()
	defer r.mu.Unlock()

	fullTableName := r.config.Database + "." + tableName
	if _, exists := r.records[fullTableName]; !exists {
		r.records[fullTableName] = make(map[int64]map[string]interface{})
	}

	recCopy := make(map[string]interface{}, len(record))
	for k, v := range record {
		recCopy[k] = v
	}
	r.records[fullTableName][id] = recCopy
	r.loadedRecords++
}

func (r *MockStarRocksRepository) InsertRecords(tableName string, records []*model.DataRecord) {
	r.mu.Lock()
	defer r.mu.Unlock()

	fullTableName := r.config.Database + "." + tableName
	if _, exists := r.records[fullTableName]; !exists {
		r.records[fullTableName] = make(map[int64]map[string]interface{})
	}

	for _, rec := range records {
		r.records[fullTableName][rec.ID] = map[string]interface{}{
			"id":         rec.ID,
			"user_id":    rec.UserID,
			"order_id":   rec.OrderID,
			"amount":     rec.Amount,
			"currency":   rec.Currency,
			"status":     rec.Status,
			"created_at": rec.CreatedAt,
			"updated_at": rec.UpdatedAt,
		}
	}
	r.loadedRecords += int64(len(records))
}

func (r *MockStarRocksRepository) SimulateDataLoss(tableName string, ids []int64) int {
	r.mu.Lock()
	defer r.mu.Unlock()

	fullTableName := r.config.Database + "." + tableName
	tableRecords, exists := r.records[fullTableName]
	if !exists {
		return 0
	}

	count := 0
	for _, id := range ids {
		if _, found := tableRecords[id]; found {
			delete(tableRecords, id)
			count++
		}
	}

	return count
}

func (r *MockStarRocksRepository) SimulateDataMismatch(tableName string, ids []int64) int {
	r.mu.Lock()
	defer r.mu.Unlock()

	fullTableName := r.config.Database + "." + tableName
	tableRecords, exists := r.records[fullTableName]
	if !exists {
		return 0
	}

	count := 0
	for _, id := range ids {
		if rec, found := tableRecords[id]; found {
			if amount, ok := rec["amount"].(float64); ok {
				rec["amount"] = amount * 1.1
				count++
			}
		}
	}

	return count
}

func (r *MockStarRocksRepository) Close() error {
	logger.Info("mock starrocks connection closed",
		zap.Int64("total_loaded", r.loadedRecords),
	)
	return nil
}
