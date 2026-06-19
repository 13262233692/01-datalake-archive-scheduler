package hive

import (
	"context"
	"fmt"
	"sync"
	"time"

	"datalake-archive-scheduler/internal/domain/model"
	"datalake-archive-scheduler/internal/domain/repository"
	"datalake-archive-scheduler/internal/infrastructure/config"
	"datalake-archive-scheduler/pkg/logger"

	"go.uber.org/zap"
)

type MockHiveMetastoreRepository struct {
	config     *config.HiveConfig
	mu         sync.RWMutex
	tables     map[string]*model.PartitionInfo
	partitions map[string]map[string]*model.PartitionInfo
}

func NewMockHiveMetastoreRepository(cfg *config.HiveConfig) repository.HiveMetastoreRepository {
	return &MockHiveMetastoreRepository{
		config:     cfg,
		tables:     make(map[string]*model.PartitionInfo),
		partitions: make(map[string]map[string]*model.PartitionInfo),
	}
}

func (r *MockHiveMetastoreRepository) GetTable(ctx context.Context, database, tableName string) (*model.PartitionInfo, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	key := database + "." + tableName
	table, exists := r.tables[key]
	if !exists {
		return nil, fmt.Errorf("table not found: %s.%s", database, tableName)
	}

	logger.Debug("hive get table",
		zap.String("database", database),
		zap.String("table", tableName),
	)

	return table, nil
}

func (r *MockHiveMetastoreRepository) AddPartition(ctx context.Context, partition *model.PartitionInfo) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	tableKey := partition.Database + "." + partition.Table
	if _, exists := r.partitions[tableKey]; !exists {
		r.partitions[tableKey] = make(map[string]*model.PartitionInfo)
	}

	r.partitions[tableKey][partition.PartitionName] = partition

	logger.Info("hive add partition success",
		zap.String("database", partition.Database),
		zap.String("table", partition.Table),
		zap.String("partition", partition.PartitionName),
		zap.String("location", partition.Location),
	)

	return nil
}

func (r *MockHiveMetastoreRepository) DropPartition(ctx context.Context, database, tableName, partitionName string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	tableKey := database + "." + tableName
	if partitions, exists := r.partitions[tableKey]; exists {
		delete(partitions, partitionName)
	}

	logger.Info("hive drop partition",
		zap.String("database", database),
		zap.String("table", tableName),
		zap.String("partition", partitionName),
	)

	return nil
}

func (r *MockHiveMetastoreRepository) GetPartition(ctx context.Context, database, tableName, partitionName string) (*model.PartitionInfo, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	tableKey := database + "." + tableName
	partitions, exists := r.partitions[tableKey]
	if !exists {
		return nil, fmt.Errorf("table not found: %s.%s", database, tableName)
	}

	partition, exists := partitions[partitionName]
	if !exists {
		return nil, fmt.Errorf("partition not found: %s", partitionName)
	}

	logger.Debug("hive get partition",
		zap.String("database", database),
		zap.String("table", tableName),
		zap.String("partition", partitionName),
	)

	return partition, nil
}

func (r *MockHiveMetastoreRepository) PartitionExists(ctx context.Context, database, tableName, partitionName string) (bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	tableKey := database + "." + tableName
	partitions, exists := r.partitions[tableKey]
	if !exists {
		return false, nil
	}

	_, exists = partitions[partitionName]
	return exists, nil
}

func (r *MockHiveMetastoreRepository) AlterPartition(ctx context.Context, partition *model.PartitionInfo) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	tableKey := partition.Database + "." + partition.Table
	if _, exists := r.partitions[tableKey]; !exists {
		return fmt.Errorf("table not found: %s.%s", partition.Database, partition.Table)
	}

	r.partitions[tableKey][partition.PartitionName] = partition

	logger.Info("hive alter partition",
		zap.String("database", partition.Database),
		zap.String("table", partition.Table),
		zap.String("partition", partition.PartitionName),
	)

	return nil
}

func (r *MockHiveMetastoreRepository) RegisterTable(database, tableName string, columns []model.ColumnInfo) {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := database + "." + tableName
	r.tables[key] = &model.PartitionInfo{
		Database: database,
		Table:    tableName,
		Columns:  columns,
	}
	r.partitions[key] = make(map[string]*model.PartitionInfo)

	logger.Info("hive table registered",
		zap.String("database", database),
		zap.String("table", tableName),
		zap.Int("columns", len(columns)),
	)
}

func (r *MockHiveMetastoreRepository) GetPartitionCount(database, tableName string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	tableKey := database + "." + tableName
	if partitions, exists := r.partitions[tableKey]; exists {
		return len(partitions)
	}
	return 0
}

func (r *MockHiveMetastoreRepository) Close() error {
	logger.Info("mock hive metastore connection closed")
	return nil
}

func GeneratePartitionName(day time.Time) string {
	return "dt=" + day.Format("2006-01-02")
}
