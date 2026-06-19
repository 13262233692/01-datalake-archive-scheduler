package model

import "time"

type DataRecord struct {
	ID          int64                  `json:"id"`
	UserID      string                 `json:"user_id"`
	OrderID     string                 `json:"order_id"`
	Amount      float64                `json:"amount"`
	Currency    string                 `json:"currency"`
	Status      string                 `json:"status"`
	Data        map[string]interface{} `json:"data"`
	CreatedAt   time.Time              `json:"created_at"`
	UpdatedAt   time.Time              `json:"updated_at"`
	IsSensitive bool                   `json:"-"`
}

type DataRecordSlice []*DataRecord

func (d DataRecordSlice) Len() int {
	return len(d)
}

func (d DataRecordSlice) EstimateSize() int64 {
	var size int64
	for _, rec := range d {
		size += int64(len(rec.OrderID) + len(rec.UserID) + len(rec.Status))
		size += 32
		for k, v := range rec.Data {
			size += int64(len(k) + 8)
			switch val := v.(type) {
			case string:
				size += int64(len(val))
			default:
				size += 16
			}
		}
	}
	return size
}
