package storage

import (
	"context"
	"sync"

	"github.com/yeohbraddy/go-job-queue/internal/job"
)

// Storage defines the interface for job persistence
type Storage interface {
	// SaveJob saves a job to storage
	SaveJob(ctx context.Context, j *job.Job) error

	// GetJob retrieves a job by ID
	GetJob(ctx context.Context, id string) (*job.Job, error)

	// UpdateJob updates a job in storage
	UpdateJob(ctx context.Context, j *job.Job) error

	// DeleteJob deletes a job from storage
	DeleteJob(ctx context.Context, id string) error

	// ListJobs retrieves a list of jobs with optional filters
	ListJobs(ctx context.Context, status job.Status, limit, offset int) ([]*job.Job, error)
}

// InMemoryStorage is a simple in-memory implementation of Storage
// In a real distributed system, this would be replaced with a database
type InMemoryStorage struct {
	jobs map[string]*job.Job
	mu   sync.RWMutex
}

// NewInMemoryStorage creates a new in-memory storage
func NewInMemoryStorage() *InMemoryStorage {
	return &InMemoryStorage{
		jobs: make(map[string]*job.Job),
	}
}

// SaveJob saves a job to storage
func (s *InMemoryStorage) SaveJob(ctx context.Context, j *job.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.jobs[j.ID] = j
	return nil
}

// GetJob retrieves a job by ID
func (s *InMemoryStorage) GetJob(ctx context.Context, id string) (*job.Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	j, exists := s.jobs[id]
	if !exists {
		return nil, nil // Job not found
	}

	return j, nil
}

// UpdateJob updates a job in storage
func (s *InMemoryStorage) UpdateJob(ctx context.Context, j *job.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.jobs[j.ID] = j
	return nil
}

// DeleteJob deletes a job from storage
func (s *InMemoryStorage) DeleteJob(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.jobs, id)
	return nil
}

// ListJobs retrieves a list of jobs with optional filters
func (s *InMemoryStorage) ListJobs(ctx context.Context, status job.Status, limit, offset int) ([]*job.Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*job.Job
	count := 0
	skip := offset

	for _, j := range s.jobs {
		if status != "" && j.Status != status {
			continue
		}

		if skip > 0 {
			skip--
			continue
		}

		result = append(result, j)
		count++

		if limit > 0 && count >= limit {
			break
		}
	}

	return result, nil
}
