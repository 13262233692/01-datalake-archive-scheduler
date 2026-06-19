package service

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"datalake-archive-scheduler/pkg/logger"

	"go.uber.org/zap"
)

type WebhookSender struct {
	url        string
	timeout    time.Duration
	maxRetries int
	client     *http.Client
	alertCount int64
}

func NewWebhookSender(url string, timeout time.Duration, maxRetries int) *WebhookSender {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	if maxRetries <= 0 {
		maxRetries = 3
	}

	return &WebhookSender{
		url:        url,
		timeout:    timeout,
		maxRetries: maxRetries,
		client: &http.Client{
			Timeout: timeout,
		},
	}
}

func (ws *WebhookSender) Send(payload interface{}) error {
	if ws.url == "" {
		logger.Warn("webhook URL not configured, skipping alert")
		return nil
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal webhook payload failed: %w", err)
	}

	var lastErr error
	for i := 0; i < ws.maxRetries; i++ {
		if err := ws.sendOnce(body); err != nil {
			lastErr = err
			logger.Warn("webhook send failed, retrying",
				zap.Int("attempt", i+1),
				zap.Error(err),
			)
			time.Sleep(time.Duration(i+1) * 500 * time.Millisecond)
			continue
		}
		return nil
	}

	return fmt.Errorf("webhook send failed after %d retries: %w", ws.maxRetries, lastErr)
}

func (ws *WebhookSender) sendOnce(body []byte) error {
	req, err := http.NewRequest("POST", ws.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request failed: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Alert-Type", "data-integrity")
	req.Header.Set("X-Alert-Level", "critical")

	resp, err := ws.client.Do(req)
	if err != nil {
		return fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("webhook returned status %d", resp.StatusCode)
	}

	return nil
}

func (ws *WebhookSender) SendCriticalAlert(auditJobID, archiveJobID, tableName string, mismatchCount, missingCount, sampledRecords int, sourceRoot, targetRoot string) error {
	alert := map[string]interface{}{
		"level":            "CRITICAL",
		"type":             "DATA_INTEGRITY_VIOLATION",
		"audit_job_id":     auditJobID,
		"archive_job_id":   archiveJobID,
		"table_name":       tableName,
		"source_root_hash": sourceRoot,
		"target_root_hash": targetRoot,
		"mismatch_count":   mismatchCount,
		"missing_count":    missingCount,
		"sampled_records":  sampledRecords,
		"timestamp":        time.Now().Format(time.RFC3339Nano),
		"severity":         "P0",
		"description": fmt.Sprintf(
			"Critical data integrity violation detected on table %s. %d mismatches and %d missing records found in %d sampled records. Source hash: %s, Target hash: %s",
			tableName, mismatchCount, missingCount, sampledRecords, sourceRoot, targetRoot,
		),
		"action_required":   "IMMEDIATE_INVESTIGATION",
		"blocking_action":   "ARCHIVE_PIPELINE_SHOULD_BE_HALTED",
	}

	return ws.Send(alert)
}

func (ws *WebhookSender) URL() string {
	return ws.url
}
