package repository

import (
	"context"
	"time"

	"datalake-archive-scheduler/internal/domain/model"
)

type SourceDBRepository interface {
	CountColdData(ctx context.Context, tableName string, coldDate time.Time) (int64, error)
	GetMinMaxID(ctx context.Context, tableName string, coldDate time.Time) (int64, int64, error)
	FetchShard(ctx context.Context, tableName string, startID, endID int64, coldDate time.Time, batchSize int) (DataRecordIterator, error)
	DeleteShard(ctx context.Context, tableName string, startID, endID int64) error
	Close() error
}

type DataRecordIterator interface {
	Next() (*model.DataRecord, error)
	HasNext() bool
	Close() error
}

type DataChannel <-chan *model.DataRecord
