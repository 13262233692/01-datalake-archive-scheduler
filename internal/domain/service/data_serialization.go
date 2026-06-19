package service

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"time"

	"datalake-archive-scheduler/internal/domain/model"
)

type DataFormat string

const (
	FormatJSON    DataFormat = "json"
	FormatCSV     DataFormat = "csv"
	FormatParquet DataFormat = "parquet"
)

type DataSerializationService interface {
	SerializeRecords(records []*model.DataRecord, format DataFormat) ([]byte, error)
	SerializeStream(records <-chan *model.DataRecord, format DataFormat, w io.Writer) error
	SerializeLine(record *model.DataRecord) []byte
}

type JSONSerializationService struct{}

func NewJSONSerializationService() *JSONSerializationService {
	return &JSONSerializationService{}
}

func (s *JSONSerializationService) SerializeRecords(records []*model.DataRecord, format DataFormat) ([]byte, error) {
	switch format {
	case FormatJSON:
		return json.Marshal(records)
	case FormatCSV:
		return s.serializeCSV(records)
	default:
		return nil, fmt.Errorf("unsupported format: %s", format)
	}
}

func (s *JSONSerializationService) serializeCSV(records []*model.DataRecord) ([]byte, error) {
	buf := &bufferWriter{}
	writer := csv.NewWriter(buf)

	header := []string{"id", "user_id", "order_id", "amount", "currency", "status", "data", "created_at", "updated_at"}
	if err := writer.Write(header); err != nil {
		return nil, err
	}

	for _, rec := range records {
		dataJSON, _ := json.Marshal(rec.Data)
		row := []string{
			strconv.FormatInt(rec.ID, 10),
			rec.UserID,
			rec.OrderID,
			strconv.FormatFloat(rec.Amount, 'f', 2, 64),
			rec.Currency,
			rec.Status,
			string(dataJSON),
			rec.CreatedAt.Format(time.RFC3339),
			rec.UpdatedAt.Format(time.RFC3339),
		}
		if err := writer.Write(row); err != nil {
			return nil, err
		}
	}

	writer.Flush()
	return buf.Bytes(), nil
}

func (s *JSONSerializationService) SerializeStream(records <-chan *model.DataRecord, format DataFormat, w io.Writer) error {
	encoder := json.NewEncoder(w)
	for rec := range records {
		if err := encoder.Encode(rec); err != nil {
			return err
		}
	}
	return nil
}

func (s *JSONSerializationService) SerializeLine(record *model.DataRecord) []byte {
	line, _ := json.Marshal(record)
	return append(line, '\n')
}

type bufferWriter struct {
	data []byte
}

func (b *bufferWriter) Write(p []byte) (int, error) {
	b.data = append(b.data, p...)
	return len(p), nil
}

func (b *bufferWriter) Bytes() []byte {
	return b.data
}
