package repository

import (
	"context"

	"datalake-archive-scheduler/internal/domain/model"
)

type HiveMetastoreRepository interface {
	GetTable(ctx context.Context, database, tableName string) (*model.PartitionInfo, error)
	AddPartition(ctx context.Context, partition *model.PartitionInfo) error
	DropPartition(ctx context.Context, database, tableName, partitionName string) error
	GetPartition(ctx context.Context, database, tableName, partitionName string) (*model.PartitionInfo, error)
	PartitionExists(ctx context.Context, database, tableName, partitionName string) (bool, error)
	AlterPartition(ctx context.Context, partition *model.PartitionInfo) error
	Close() error
}
