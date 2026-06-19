package service

import (
	"context"
	"fmt"
	"testing"
	"time"

	"datalake-archive-scheduler/internal/domain/model"
	"datalake-archive-scheduler/internal/infrastructure/config"
	"datalake-archive-scheduler/internal/infrastructure/persistance"
	"datalake-archive-scheduler/internal/infrastructure/starrocks"
	"datalake-archive-scheduler/pkg/crypto/merkletree"
)

func TestMerkleTree_Basic(t *testing.T) {
	hashes := []string{"a1", "b2", "c3", "d4"}

	tree := merkletree.NewMerkleTree(hashes)

	if tree.GetRootHash() == "" {
		t.Fatal("root hash should not be empty")
	}
	if tree.GetLeafCount() != 4 {
		t.Fatalf("expected 4 leaves, got %d", tree.GetLeafCount())
	}

	tree2 := merkletree.NewMerkleTree(hashes)
	if tree.GetRootHash() != tree2.GetRootHash() {
		t.Fatal("same data should produce same root hash")
	}
}

func TestMerkleTree_DifferentData(t *testing.T) {
	hashes1 := []string{"a1", "b2", "c3", "d4"}
	hashes2 := []string{"a1", "b2", "c3", "d5"}

	tree1 := merkletree.NewMerkleTree(hashes1)
	tree2 := merkletree.NewMerkleTree(hashes2)

	if tree1.GetRootHash() == tree2.GetRootHash() {
		t.Fatal("different data should produce different root hash")
	}
}

func TestRecordHash_Deterministic(t *testing.T) {
	fields := map[string]interface{}{
		"amount":     100.50,
		"created_at": time.Date(2023, 6, 15, 10, 30, 0, 0, time.UTC),
		"order_id":   "ORD-001",
	}

	hash1 := merkletree.RecordHash(123, fields)
	hash2 := merkletree.RecordHash(123, fields)

	if hash1 == "" {
		t.Fatal("hash should not be empty")
	}
	if hash1 != hash2 {
		t.Fatal("same data should produce same hash")
	}
}

func TestRecordHash_DifferentAmount(t *testing.T) {
	fields1 := map[string]interface{}{"amount": 100.0}
	fields2 := map[string]interface{}{"amount": 101.0}

	hash1 := merkletree.RecordHash(1, fields1)
	hash2 := merkletree.RecordHash(1, fields2)

	if hash1 == hash2 {
		t.Fatal("different amount should produce different hash")
	}
}

func TestRecordHash_DifferentID(t *testing.T) {
	fields := map[string]interface{}{"amount": 100.0}

	hash1 := merkletree.RecordHash(1, fields)
	hash2 := merkletree.RecordHash(2, fields)

	if hash1 == hash2 {
		t.Fatal("different ID should produce different hash")
	}
}

func TestAuditService_Integration(t *testing.T) {
	auditCfg := AuditConfig{
		Enabled:                true,
		SampleRate:             0.5,
		BatchSize:              10,
		Concurrency:            1,
		QueueCapacity:          1000,
		WebhookURL:             "",
		WebhookTimeout:         10 * time.Second,
		MaxRetries:             3,
		AlertThresholdMiss:     0.05,
		AlertThresholdMismatch: 0.05,
	}

	sourceDB := persistance.NewMockPolarDBRepository("order_detail")
	srCfg := &config.StarRocksConfig{Database: "test"}
	targetDB := starrocks.NewMockStarRocksRepository(srCfg)

	auditSvc := NewAuditService(auditCfg, sourceDB, targetDB)
	auditSvc.Start()
	defer auditSvc.Stop()

	if !auditSvc.IsRunning() {
		t.Fatal("audit service should be running")
	}
}

func TestAuditService_SubmitAndProcess(t *testing.T) {
	auditCfg := AuditConfig{
		Enabled:                true,
		SampleRate:             1.0,
		BatchSize:              100,
		Concurrency:            2,
		QueueCapacity:          100000,
		WebhookURL:             "",
		WebhookTimeout:         10 * time.Second,
		MaxRetries:             3,
		AlertThresholdMiss:     0.05,
		AlertThresholdMismatch: 0.05,
	}

	sourceDB := persistance.NewMockPolarDBRepository("order_detail")
	srCfg := &config.StarRocksConfig{Database: "test"}
	targetDB := starrocks.NewMockStarRocksRepository(srCfg)

	srMock := targetDB.(*starrocks.MockStarRocksRepository)

	coldDate := time.Now().AddDate(-3, 0, 0)
	minID, maxID, err := sourceDB.GetMinMaxID(context.Background(), "order_detail", coldDate)
	if err != nil {
		t.Fatalf("failed to get min max id: %v", err)
	}
	t.Logf("Source DB: min=%d, max=%d", minID, maxID)

	var records []*model.DataRecord
	iter, err := sourceDB.FetchShard(context.Background(), "order_detail", minID, maxID, coldDate, 1000)
	if err != nil {
		t.Fatalf("failed to fetch shard: %v", err)
	}

	for iter.HasNext() {
		rec, err := iter.Next()
		if err == nil {
			records = append(records, rec)
		}
	}
	iter.Close()

	t.Logf("Loaded %d records from source", len(records))

	srMock.InsertRecords("unicorn_pro_history", records)
	t.Log("Inserted records into target mock")

	auditSvc := NewAuditService(auditCfg, sourceDB, targetDB)
	auditSvc.Start()
	defer auditSvc.Stop()

	auditID, err := auditSvc.SubmitAudit("test-job-001", "order_detail", "unicorn_pro_history", int64(len(records)), coldDate)
	if err != nil {
		t.Fatalf("failed to submit audit: %v", err)
	}
	if auditID == "" {
		t.Fatal("audit ID should not be empty")
	}
	t.Logf("Audit job submitted: %s", auditID)

	time.Sleep(5 * time.Second)

	job, ok := auditSvc.GetAuditJob(auditID)
	if !ok {
		t.Fatal("audit job not found")
	}

	t.Logf("Audit status: %s", job.Status)
	t.Logf("Sampled records: %d", job.SampledRecords)
	t.Logf("Match count: %d", job.MatchCount)
	t.Logf("Mismatch count: %d", job.MismatchCount)
	t.Logf("Missing count: %d", job.MissingCount)
	t.Logf("Source root hash: %s", job.SourceRootHash)
	t.Logf("Target root hash: %s", job.TargetRootHash)

	if job.Status == model.AuditStatusPassed {
		t.Log("Audit passed - all records match")
		if job.MismatchCount != 0 {
			t.Errorf("expected 0 mismatches, got %d", job.MismatchCount)
		}
		if job.MissingCount != 0 {
			t.Errorf("expected 0 missing, got %d", job.MissingCount)
		}
	}
}

func TestAuditService_DataMismatchDetection(t *testing.T) {
	auditCfg := AuditConfig{
		Enabled:                true,
		SampleRate:             1.0,
		BatchSize:              100,
		Concurrency:            1,
		QueueCapacity:          100000,
		WebhookURL:             "",
		WebhookTimeout:         10 * time.Second,
		MaxRetries:             3,
		AlertThresholdMiss:     0.01,
		AlertThresholdMismatch: 0.01,
	}

	sourceDB := persistance.NewMockPolarDBRepository("order_detail")
	srCfg := &config.StarRocksConfig{Database: "test"}
	targetDB := starrocks.NewMockStarRocksRepository(srCfg)
	srMock := targetDB.(*starrocks.MockStarRocksRepository)

	coldDate := time.Now().AddDate(-3, 0, 0)
	minID, maxID, err := sourceDB.GetMinMaxID(context.Background(), "order_detail", coldDate)
	if err != nil {
		t.Fatalf("failed to get min max id: %v", err)
	}

	var records []*model.DataRecord
	iter, err := sourceDB.FetchShard(context.Background(), "order_detail", minID, maxID, coldDate, 1000)
	if err != nil {
		t.Fatalf("failed to fetch shard: %v", err)
	}

	for iter.HasNext() {
		rec, err := iter.Next()
		if err == nil {
			records = append(records, rec)
		}
	}
	iter.Close()

	srMock.InsertRecords("unicorn_pro_history", records)

	mismatchIDs := []int64{minID, minID + 1, minID + 2, minID + 4, minID + 5}
	mismatchCount := srMock.SimulateDataMismatch("unicorn_pro_history", mismatchIDs)
	t.Logf("Simulated %d mismatches", mismatchCount)

	missingIDs := []int64{minID + 10, minID + 11, minID + 12}
	missingCount := srMock.SimulateDataLoss("unicorn_pro_history", missingIDs)
	t.Logf("Simulated %d missing records", missingCount)

	auditSvc := NewAuditService(auditCfg, sourceDB, targetDB)
	auditSvc.Start()
	defer auditSvc.Stop()

	auditID, err := auditSvc.SubmitAudit("test-job-mismatch", "order_detail", "unicorn_pro_history", int64(len(records)), coldDate)
	if err != nil {
		t.Fatalf("failed to submit audit: %v", err)
	}

	time.Sleep(8 * time.Second)

	job, ok := auditSvc.GetAuditJob(auditID)
	if !ok {
		t.Fatal("audit job not found")
	}

	t.Logf("Audit status: %s", job.Status)
	t.Logf("Alert level: %s", job.AlertLevel)
	t.Logf("Mismatch count: %d", job.MismatchCount)
	t.Logf("Missing count: %d", job.MissingCount)
	t.Logf("Match count: %d", job.MatchCount)

	if job.MismatchCount <= 0 {
		t.Error("should detect mismatches")
	}
	if job.MissingCount <= 0 {
		t.Error("should detect missing records")
	}

	batches, ok := auditSvc.GetAuditBatches(auditID)
	if !ok {
		t.Fatal("audit batches not found")
	}
	t.Logf("Total batches: %d", len(batches))
}

func TestAuditService_Stats(t *testing.T) {
	auditCfg := AuditConfig{
		Enabled:                true,
		SampleRate:             1.0,
		BatchSize:              100,
		Concurrency:            2,
		QueueCapacity:          100000,
		AlertThresholdMiss:     0.05,
		AlertThresholdMismatch: 0.05,
	}

	sourceDB := persistance.NewMockPolarDBRepository("order_detail")
	srCfg := &config.StarRocksConfig{Database: "test"}
	targetDB := starrocks.NewMockStarRocksRepository(srCfg)
	srMock := targetDB.(*starrocks.MockStarRocksRepository)

	coldDate := time.Now().AddDate(-3, 0, 0)
	minID, maxID, _ := sourceDB.GetMinMaxID(context.Background(), "order_detail", coldDate)

	var records []*model.DataRecord
	iter, _ := sourceDB.FetchShard(context.Background(), "order_detail", minID, maxID, coldDate, 1000)
	for iter.HasNext() {
		rec, err := iter.Next()
		if err == nil {
			records = append(records, rec)
		}
	}
	iter.Close()

	srMock.InsertRecords("unicorn_pro_history", records)

	auditSvc := NewAuditService(auditCfg, sourceDB, targetDB)
	auditSvc.Start()
	defer auditSvc.Stop()

	for i := 0; i < 3; i++ {
		_, _ = auditSvc.SubmitAudit(fmt.Sprintf("test-job-%d", i), "order_detail", "unicorn_pro_history", int64(len(records)), coldDate)
	}

	time.Sleep(8 * time.Second)

	stats := auditSvc.GetStats()
	t.Logf("Total audits: %d", stats.TotalAudits)
	t.Logf("Passed audits: %d", stats.PassedAudits)
	t.Logf("Failed audits: %d", stats.FailedAudits)
	t.Logf("Alert audits: %d", stats.AlertAudits)
	t.Logf("Total sampled: %d", stats.TotalSampled)

	if stats.TotalAudits < 3 {
		t.Errorf("expected at least 3 audits, got %d", stats.TotalAudits)
	}

	queueStats := auditSvc.GetQueueStats()
	t.Logf("Queue size: %v", queueStats["queue_size"])
}
