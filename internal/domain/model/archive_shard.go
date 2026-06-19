package model

import "time"

type ShardStatus string

const (
	ShardStatusPending    ShardStatus = "PENDING"
	ShardStatusRunning    ShardStatus = "RUNNING"
	ShardStatusCompleted  ShardStatus = "COMPLETED"
	ShardStatusFailed     ShardStatus = "FAILED"
	ShardStatusCompensated ShardStatus = "COMPENSATED"
)

type ArchiveShard struct {
	ID             string
	JobID          string
	ShardIndex     int
	StartID        int64
	EndID          int64
	Status         ShardStatus
	RecordCount    int64
	OSSPath        string
	RetryCount     int
	MaxRetryCount  int
	StartTime      *time.Time
	EndTime        *time.Time
	ErrorMessage   string
	CompensationID string
}

func (s *ArchiveShard) Start() {
	s.Status = ShardStatusRunning
	now := time.Now()
	s.StartTime = &now
}

func (s *ArchiveShard) Complete(recordCount int64, ossPath string) {
	s.Status = ShardStatusCompleted
	s.RecordCount = recordCount
	s.OSSPath = ossPath
	now := time.Now()
	s.EndTime = &now
}

func (s *ArchiveShard) Fail(errMsg string) {
	s.Status = ShardStatusFailed
	s.ErrorMessage = errMsg
	s.RetryCount++
	now := time.Now()
	s.EndTime = &now
}

func (s *ArchiveShard) CanRetry() bool {
	return s.RetryCount < s.MaxRetryCount
}

func (s *ArchiveShard) MarkCompensated(compensationID string) {
	s.Status = ShardStatusCompensated
	s.CompensationID = compensationID
}
