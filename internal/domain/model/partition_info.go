package model

import "time"

type PartitionType string

const (
	PartitionTypeYear  PartitionType = "YEAR"
	PartitionTypeMonth PartitionType = "MONTH"
	PartitionTypeDay   PartitionType = "DAY"
)

type PartitionInfo struct {
	Table         string
	Database      string
	PartitionName string
	PartitionType PartitionType
	PartitionValue time.Time
	Location      string
	Columns       []ColumnInfo
	SerdeInfo     SerdeInfo
}

type ColumnInfo struct {
	Name    string
	Type    string
	Comment string
}

type SerdeInfo struct {
	SerializationLib string
	InputFormat      string
	OutputFormat     string
}

func NewDayPartition(database, table string, day time.Time, location string) *PartitionInfo {
	return &PartitionInfo{
		Database:       database,
		Table:          table,
		PartitionName:  "dt=" + day.Format("2006-01-02"),
		PartitionType:  PartitionTypeDay,
		PartitionValue: day,
		Location:       location,
		SerdeInfo: SerdeInfo{
			SerializationLib: "org.apache.hadoop.hive.serde2.parquet.ParquetSerDe",
			InputFormat:      "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
			OutputFormat:     "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
		},
	}
}
