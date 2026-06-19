package service

import (
	"fmt"
	"strings"

	"datalake-archive-scheduler/internal/domain/model"
)

type DataCleaningService interface {
	Clean(record *model.DataRecord) (*model.DataRecord, error)
	CleanBatch(records []*model.DataRecord) ([]*model.DataRecord, error)
}

type DefaultCleaningService struct {
	requiredFields []string
}

func NewDataCleaningService() *DefaultCleaningService {
	return &DefaultCleaningService{
		requiredFields: []string{"order_id", "user_id", "amount"},
	}
}

func (s *DefaultCleaningService) Clean(record *model.DataRecord) (*model.DataRecord, error) {
	if record == nil {
		return nil, fmt.Errorf("record is nil")
	}

	record.OrderID = strings.TrimSpace(record.OrderID)
	record.UserID = strings.TrimSpace(record.UserID)
	record.Status = strings.TrimSpace(strings.ToUpper(record.Status))
	record.Currency = strings.TrimSpace(strings.ToUpper(record.Currency))

	if record.OrderID == "" {
		return nil, fmt.Errorf("missing required field: order_id")
	}
	if record.UserID == "" {
		return nil, fmt.Errorf("missing required field: user_id")
	}
	if record.Amount < 0 {
		return nil, fmt.Errorf("invalid amount: %f", record.Amount)
	}
	if record.Data == nil {
		record.Data = make(map[string]interface{})
	}

	s.normalizeData(record)

	return record, nil
}

func (s *DefaultCleaningService) CleanBatch(records []*model.DataRecord) ([]*model.DataRecord, error) {
	cleaned := make([]*model.DataRecord, 0, len(records))
	var errs []string

	for _, rec := range records {
		cleanedRec, err := s.Clean(rec)
		if err != nil {
			errs = append(errs, fmt.Sprintf("record %d: %v", rec.ID, err))
			continue
		}
		cleaned = append(cleaned, cleanedRec)
	}

	if len(errs) > 0 {
		return cleaned, fmt.Errorf("cleaning errors: %s", strings.Join(errs, "; "))
	}

	return cleaned, nil
}

func (s *DefaultCleaningService) normalizeData(record *model.DataRecord) {
	for key, value := range record.Data {
		if strVal, ok := value.(string); ok {
			record.Data[key] = strings.TrimSpace(strVal)
		}
	}
}
