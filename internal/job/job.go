package job

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// Status represents the current state of a job
type Status string

const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
	StatusRetrying  Status = "retrying"
)

// Job represents a unit of work to be processed
type Job struct {
	ID         string          `json:"id"`
	Type       string          `json:"type"`
	Payload    json.RawMessage `json:"payload"`
	Status     Status          `json:"status"`
	MaxRetries int             `json:"max_retries"`
	Retries    int             `json:"retries"`
	CreatedAt  time.Time       `json:"created_at"`
	UpdatedAt  time.Time       `json:"updated_at"`
	RunAt      time.Time       `json:"run_at,omitempty"`
	Error      string          `json:"error,omitempty"`
}

// Handler defines the interface for job handlers
type Handler interface {
	// ProcessJob processes a job and returns an error if it fails
	ProcessJob(ctx context.Context, j *Job) error
}

// NewJob creates a new job with the given type and payload
func NewJob(jobType string, payload interface{}) (*Job, error) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload: %w", err)
	}

	now := time.Now()
	return &Job{
		ID:        fmt.Sprintf("%d", now.UnixNano()), // Simple ID for now, should use UUID in production
		Type:      jobType,
		Payload:   payloadBytes,
		Status:    StatusPending,
		CreatedAt: now,
		UpdatedAt: now,
	}, nil
}

// WithMaxRetries sets the maximum number of retries for a job
func (j *Job) WithMaxRetries(retries int) *Job {
	j.MaxRetries = retries
	return j
}

// WithRunAt schedules a job to run at a specific time
func (j *Job) WithRunAt(runAt time.Time) *Job {
	j.RunAt = runAt
	return j
}

// ShouldRetry determines if a job should be retried based on its max retries
func (j *Job) ShouldRetry() bool {
	return j.Retries < j.MaxRetries
}

// GetPayload unmarshals the job payload into the provided target
func (j *Job) GetPayload(target interface{}) error {
	return json.Unmarshal(j.Payload, target)
}
