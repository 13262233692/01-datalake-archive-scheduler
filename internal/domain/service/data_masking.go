package service

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"datalake-archive-scheduler/internal/domain/model"
)

type DataMaskingService interface {
	Mask(record *model.DataRecord) *model.DataRecord
	MaskBatch(records []*model.DataRecord) []*model.DataRecord
}

type DefaultMaskingService struct {
	sensitiveFields map[string]bool
	salt            string
}

func NewDataMaskingService(salt string) *DefaultMaskingService {
	return &DefaultMaskingService{
		sensitiveFields: map[string]bool{
			"phone":       true,
			"id_card":     true,
			"email":       true,
			"bank_card":   true,
			"address":     true,
			"name":        true,
			"real_name":   true,
		},
		salt: salt,
	}
}

func (s *DefaultMaskingService) Mask(record *model.DataRecord) *model.DataRecord {
	if record == nil {
		return nil
	}

	if record.UserID != "" {
		record.UserID = s.hashValue(record.UserID)
	}

	for key := range record.Data {
		if s.sensitiveFields[strings.ToLower(key)] {
			if strVal, ok := record.Data[key].(string); ok {
				record.Data[key] = s.maskString(strVal)
			}
		}
	}

	record.IsSensitive = true
	return record
}

func (s *DefaultMaskingService) MaskBatch(records []*model.DataRecord) []*model.DataRecord {
	for _, rec := range records {
		s.Mask(rec)
	}
	return records
}

func (s *DefaultMaskingService) hashValue(value string) string {
	h := sha256.New()
	h.Write([]byte(value + s.salt))
	return hex.EncodeToString(h.Sum(nil))[:32]
}

func (s *DefaultMaskingService) maskString(value string) string {
	if len(value) <= 4 {
		return "****"
	}
	if len(value) <= 8 {
		return value[:2] + "****" + value[len(value)-2:]
	}
	return value[:3] + strings.Repeat("*", len(value)-6) + value[len(value)-3:]
}
