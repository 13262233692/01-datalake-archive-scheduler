package starrocks

import (
	"context"
	"fmt"
	"sync"

	"datalake-archive-scheduler/internal/domain/repository"
	"datalake-archive-scheduler/internal/infrastructure/config"
	"datalake-archive-scheduler/pkg/logger"

	"go.uber.org/zap"
)

type MockStarRocksRepository struct {
	config        *config.StarRocksConfig
	mu            sync.RWMutex
	tables        map[string]*TableInfo
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
		config: cfg,
		tables: make(map[string]*TableInfo),
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

func (r *MockStarRocksRepository) Close() error {
	logger.Info("mock starrocks connection closed",
		zap.Int64("total_loaded", r.loadedRecords),
	)
	return nil
}
