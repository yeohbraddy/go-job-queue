package worker

import (
	"context"
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	"github.com/yeohbraddy/go-job-queue/internal/job"
	"github.com/yeohbraddy/go-job-queue/internal/queue"
)

// Worker is responsible for processing jobs from the queue
type Worker struct {
	queue       queue.Queue
	handlers    map[string]job.Handler
	concurrency int
	backoffFunc func(retries int) time.Duration
	wg          sync.WaitGroup
	stopCh      chan struct{}
	mu          sync.RWMutex
}

// NewWorker creates a new worker
func NewWorker(q queue.Queue, concurrency int) *Worker {
	return &Worker{
		queue:       q,
		handlers:    make(map[string]job.Handler),
		concurrency: concurrency,
		stopCh:      make(chan struct{}),
		// Default exponential backoff: 2^retries seconds
		backoffFunc: func(retries int) time.Duration {
			return time.Duration(math.Pow(2, float64(retries))) * time.Second
		},
	}
}

// RegisterHandler registers a handler for a job type
func (w *Worker) RegisterHandler(jobType string, handler job.Handler) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.handlers[jobType] = handler
}

// SetBackoffFunc sets a custom backoff function
func (w *Worker) SetBackoffFunc(fn func(retries int) time.Duration) {
	w.backoffFunc = fn
}

// Start starts the worker pool
func (w *Worker) Start(ctx context.Context) {
	for i := 0; i < w.concurrency; i++ {
		w.wg.Add(1)
		go w.processJobs(ctx)
	}
}

// Stop stops the worker pool gracefully
func (w *Worker) Stop() {
	close(w.stopCh)
	w.wg.Wait()
}

// processJobs is the main worker loop
func (w *Worker) processJobs(ctx context.Context) {
	defer w.wg.Done()

	for {
		select {
		case <-w.stopCh:
			return
		case <-ctx.Done():
			return
		default:
			// Continue processing
		}

		// Poll for a job
		j, err := w.queue.Dequeue(ctx)
		if err != nil {
			log.Printf("Error dequeuing job: %v", err)
			time.Sleep(1 * time.Second) // Wait a bit before retrying
			continue
		}

		// No job available
		if j == nil {
			time.Sleep(100 * time.Millisecond) // Avoid busy-waiting
			continue
		}

		// Process the job
		w.processJob(ctx, j)
	}
}

// processJob processes a single job
func (w *Worker) processJob(ctx context.Context, j *job.Job) {
	w.mu.RLock()
	handler, exists := w.handlers[j.Type]
	w.mu.RUnlock()

	if !exists {
		log.Printf("No handler registered for job type: %s", j.Type)
		j.Status = job.StatusFailed
		j.Error = fmt.Sprintf("no handler registered for job type: %s", j.Type)
		return
	}

	// Update job status
	j.Status = job.StatusRunning
	j.UpdatedAt = time.Now()

	// Execute the job
	err := handler.ProcessJob(ctx, j)
	if err != nil {
		log.Printf("Error processing job %s: %v", j.ID, err)
		j.Error = err.Error()

		// Retry logic
		if j.ShouldRetry() {
			backoff := w.backoffFunc(j.Retries)
			log.Printf("Retrying job %s in %v", j.ID, backoff)
			if err := w.queue.RequeueJob(ctx, j, backoff); err != nil {
				log.Printf("Error requeueing job %s: %v", j.ID, err)
			}
			return
		}

		j.Status = job.StatusFailed
		j.UpdatedAt = time.Now()
		return
	}

	// Job completed successfully
	j.Status = job.StatusCompleted
	j.UpdatedAt = time.Now()
	log.Printf("Job completed: %s", j.ID)
}
