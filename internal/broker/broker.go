package broker

import (
	"context"
	"fmt"
	"time"

	"github.com/yeohbraddy/go-job-queue/internal/job"
	"github.com/yeohbraddy/go-job-queue/internal/queue"
	"github.com/yeohbraddy/go-job-queue/internal/storage"
	"github.com/yeohbraddy/go-job-queue/internal/worker"
)

// Broker coordinates the job queue system components
type Broker struct {
	queue   queue.Queue
	storage storage.Storage
	worker  *worker.Worker
}

// NewBroker creates a new broker
func NewBroker(q queue.Queue, s storage.Storage, w *worker.Worker) *Broker {
	return &Broker{
		queue:   q,
		storage: s,
		worker:  w,
	}
}

// Start starts the broker and worker
func (b *Broker) Start(ctx context.Context) {
	b.worker.Start(ctx)
}

// Stop stops the broker and worker
func (b *Broker) Stop() {
	b.worker.Stop()
}

// EnqueueJob enqueues a job
func (b *Broker) EnqueueJob(ctx context.Context, j *job.Job) error {
	// Save job to storage first
	if err := b.storage.SaveJob(ctx, j); err != nil {
		return fmt.Errorf("failed to save job: %w", err)
	}

	// Then enqueue it
	if err := b.queue.Enqueue(ctx, j); err != nil {
		return fmt.Errorf("failed to enqueue job: %w", err)
	}

	return nil
}

// ScheduleJob schedules a job to run at a later time
func (b *Broker) ScheduleJob(ctx context.Context, j *job.Job, runAt time.Time) error {
	// Save job to storage first
	if err := b.storage.SaveJob(ctx, j); err != nil {
		return fmt.Errorf("failed to save job: %w", err)
	}

	// Then schedule it
	if err := b.queue.ScheduleJob(ctx, j, runAt); err != nil {
		return fmt.Errorf("failed to schedule job: %w", err)
	}

	return nil
}

// RegisterHandler registers a job handler
func (b *Broker) RegisterHandler(jobType string, handler job.Handler) {
	b.worker.RegisterHandler(jobType, handler)
}

// GetJob retrieves a job by ID
func (b *Broker) GetJob(ctx context.Context, id string) (*job.Job, error) {
	return b.storage.GetJob(ctx, id)
}

// ListJobs lists jobs with optional filters
func (b *Broker) ListJobs(ctx context.Context, status job.Status, limit, offset int) ([]*job.Job, error) {
	return b.storage.ListJobs(ctx, status, limit, offset)
}

// SetBackoffFunc sets a custom backoff function for the worker
func (b *Broker) SetBackoffFunc(fn func(retries int) time.Duration) {
	b.worker.SetBackoffFunc(fn)
}
