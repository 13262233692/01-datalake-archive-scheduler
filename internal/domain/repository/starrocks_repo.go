package repository

import (
	"context"
)

type StarRocksRepository interface {
	LoadDataFromOSS(ctx context.Context, tableName string, ossPath string, partition string) error
	ImportStream(ctx context.Context, tableName string, dataChannel <-chan []byte, format string) error
	ExecuteSQL(ctx context.Context, sql string) error
	TableExists(ctx context.Context, tableName string) (bool, error)
	GetRecordCount(ctx context.Context, tableName string, partition string) (int64, error)
	GetRecordByID(ctx context.Context, tableName string, id int64) (map[string]interface{}, bool)
	GetRecordsByIDRange(ctx context.Context, tableName string, startID, endID int64) ([]map[string]interface{}, error)
	Close() error
}
