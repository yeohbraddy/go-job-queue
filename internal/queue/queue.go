package queue

import (
	"context"
	"time"

	"github.com/yeohbraddy/go-job-queue/internal/job"
)

// Queue defines the interface for a job queue
type Queue interface {
	// Enqueue adds a job to the queue
	Enqueue(ctx context.Context, j *job.Job) error

	// Dequeue removes and returns a job from the queue
	// If no job is available, it returns nil without error
	Dequeue(ctx context.Context) (*job.Job, error)

	// Size returns the current number of jobs in the queue
	Size(ctx context.Context) (int, error)

	// ScheduleJob adds a job to be executed at a later time
	ScheduleJob(ctx context.Context, j *job.Job, runAt time.Time) error

	// RequeueJob puts a job back in the queue, usually after a failed attempt
	RequeueJob(ctx context.Context, j *job.Job, backoff time.Duration) error
}

// InMemoryQueue is a simple in-memory implementation of Queue
// In a real distributed system, this would be replaced with a more robust
// solution like Redis, RabbitMQ, or a database
type InMemoryQueue struct {
	jobs      []*job.Job
	scheduled []*job.Job
}

// NewInMemoryQueue creates a new in-memory queue
func NewInMemoryQueue() *InMemoryQueue {
	return &InMemoryQueue{
		jobs:      make([]*job.Job, 0),
		scheduled: make([]*job.Job, 0),
	}
}

// Enqueue adds a job to the queue
func (q *InMemoryQueue) Enqueue(ctx context.Context, j *job.Job) error {
	q.jobs = append(q.jobs, j)
	return nil
}

// Dequeue removes and returns a job from the queue
func (q *InMemoryQueue) Dequeue(ctx context.Context) (*job.Job, error) {
	// First check for any scheduled jobs that are ready to run
	now := time.Now()
	for i, j := range q.scheduled {
		if j.RunAt.Before(now) || j.RunAt.Equal(now) {
			// Remove the job from scheduled
			q.scheduled = append(q.scheduled[:i], q.scheduled[i+1:]...)
			return j, nil
		}
	}

	// Then check regular queue
	if len(q.jobs) == 0 {
		return nil, nil
	}

	j := q.jobs[0]
	q.jobs = q.jobs[1:]
	return j, nil
}

// Size returns the current number of jobs in the queue
func (q *InMemoryQueue) Size(ctx context.Context) (int, error) {
	return len(q.jobs) + len(q.scheduled), nil
}

// ScheduleJob adds a job to be executed at a later time
func (q *InMemoryQueue) ScheduleJob(ctx context.Context, j *job.Job, runAt time.Time) error {
	j.RunAt = runAt
	q.scheduled = append(q.scheduled, j)

	// In a real implementation, you might want to sort the scheduled jobs by RunAt
	// for efficiency, or use a priority queue

	return nil
}

// RequeueJob puts a job back in the queue with backoff
func (q *InMemoryQueue) RequeueJob(ctx context.Context, j *job.Job, backoff time.Duration) error {
	j.Status = job.StatusRetrying
	j.Retries++
	j.UpdatedAt = time.Now()

	return q.ScheduleJob(ctx, j, time.Now().Add(backoff))
}
