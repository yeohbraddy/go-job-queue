package storage

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-redis/redis/v8"
	"github.com/yeohbraddy/go-job-queue/internal/job"
)

// RedisStorage is a Redis-backed implementation of Storage
type RedisStorage struct {
	client      *redis.Client
	jobsKey     string                         // Key for the sorted set storing job IDs by CreatedAt timestamp
	jobKeyFunc  func(id string) string         // Function to generate the key for storing a specific job's data (e.g., "prefix:job:<id>")
	jobsSetFunc func(status job.Status) string // Function to generate the key for the set storing job IDs by status (e.g., "prefix:jobs:pending")
}

// NewRedisStorage creates a new Redis-backed storage
func NewRedisStorage(client *redis.Client, keyPrefix string) *RedisStorage {
	return &RedisStorage{
		client:  client,
		jobsKey: fmt.Sprintf("%s:jobs", keyPrefix), // Example: "jobqueue:jobs"
		jobKeyFunc: func(id string) string {
			return fmt.Sprintf("%s:job:%s", keyPrefix, id) // Example: "jobqueue:job:12345"
		},
		jobsSetFunc: func(status job.Status) string {
			return fmt.Sprintf("%s:jobs:%s", keyPrefix, status) // Example: "jobqueue:jobs:pending"
		},
	}
}

// SaveJob saves a job to Redis
func (s *RedisStorage) SaveJob(ctx context.Context, j *job.Job) error {
	// 1. Marshal the job `j` into JSON bytes. Handle errors.
	jsonBytes, err := json.Marshal(j)
	if err != nil {
		return fmt.Errorf("failed to marshal job %w", err)
	}

	// 2. Generate the job key using `s.jobKeyFunc(j.ID)`.
	jobKey := s.jobKeyFunc(j.ID)

	// 3. Use `s.client.Set` to store the JSON bytes under the job key. Use 0 for expiration (no expiration). Handle errors.
	err = s.client.Set(ctx, jobKey, jsonBytes, 0).Err()
	if err != nil {
		return fmt.Errorf("failed to save job: %w", err)
	}

	// 4. Generate the status set key using `s.jobsSetFunc(j.Status)`.
	statusSetKey := s.jobsSetFunc(j.Status)

	// 5. Use `s.client.SAdd` to add the `j.ID` to the status set. Handle errors.
	err = s.client.SAdd(ctx, statusSetKey, j.ID).Err()
	if err != nil {
		return fmt.Errorf("failed to add job to status set: %w", err)
	}

	// 6. Generate the score using `float64(j.CreatedAt.UnixNano())`.
	score := float64(j.CreatedAt.UnixNano())

	// 7. Use `s.client.ZAdd` to add the `j.ID` to the main sorted set `s.jobsKey` with the calculated score. Handle errors.
	err = s.client.ZAdd(ctx, s.jobsKey, &redis.Z{
		Score:  score,
		Member: j.ID,
	}).Err()
	if err != nil {
		return fmt.Errorf("failed to add job to main sorted set: %w", err)
	}

	// 8. Return nil on success.
	return nil
}

// GetJob retrieves a job from Redis by ID
func (s *RedisStorage) GetJob(ctx context.Context, id string) (*job.Job, error) {
	// 1. Generate the job key using `s.jobKeyFunc(id)`.
	jobKey := s.jobKeyFunc(id)

	// 2. Use `s.client.Get` to retrieve the data associated with the job key.
	jobBytes, err := s.client.Get(ctx, jobKey).Bytes()

	// 3. Handle errors:
	//    - If the error is `redis.Nil`, the job wasn't found. Return `nil, nil`.
	//    - For other errors, return `nil, fmt.Errorf(...)`.
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get job: %w", err)
	}

	// 4. Create a variable of type `job.Job`.
	var j job.Job

	// 5. Unmarshal the JSON bytes into the job variable. Handle errors (`fmt.Errorf`).
	err = json.Unmarshal(jobBytes, &j)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal job: %w", err)
	}

	// 6. Return the pointer to the job variable (`&j`) and nil error on success.
	return &j, nil
}

// UpdateJob updates a job in Redis
func (s *RedisStorage) UpdateJob(ctx context.Context, j *job.Job) error {
	// 1. Get the current state of the job using `s.GetJob(ctx, j.ID)`. Handle errors (including not found).
	// 2. Check if the status has changed (`currentJob.Status != j.Status`).
	// 3. If status changed:
	//    a. Start a Redis pipeline: `pipe := s.client.Pipeline()`.
	//    b. Generate the old status set key using `s.jobsSetFunc(currentJob.Status)`.
	//    c. Use `pipe.SRem` to remove `j.ID` from the old status set.
	//    d. Generate the new status set key using `s.jobsSetFunc(j.Status)`.
	//    e. Use `pipe.SAdd` to add `j.ID` to the new status set.
	//    f. Execute the pipeline: `pipe.Exec(ctx)`. Handle errors.
	// 4. Update the job's `UpdatedAt` timestamp: `j.UpdatedAt = time.Now()`.
	// 5. Marshal the updated job `j` into JSON bytes. Handle errors.
	// 6. Generate the job key using `s.jobKeyFunc(j.ID)`.
	// 7. Use `s.client.Set` to store the updated JSON bytes under the job key (overwrite). Handle errors.
	// 8. Return nil on success.

	// --- Your implementation here ---
	return fmt.Errorf("UpdateJob not implemented")
}

// DeleteJob deletes a job from Redis
func (s *RedisStorage) DeleteJob(ctx context.Context, id string) error {
	// 1. Get the current job using `s.GetJob(ctx, id)` to know its status.
	// 2. Handle errors. If the job is not found (`currentJob == nil`), it's already gone, so return `nil`.
	// 3. Start a Redis pipeline: `pipe := s.client.Pipeline()`.
	// 4. Generate the status set key using `s.jobsSetFunc(currentJob.Status)`.
	// 5. Use `pipe.SRem` to remove the `id` from the status set.
	// 6. Use `pipe.ZRem` to remove the `id` from the main sorted set `s.jobsKey`.
	// 7. Generate the job key using `s.jobKeyFunc(id)`.
	// 8. Use `pipe.Del` to delete the job data itself.
	// 9. Execute the pipeline: `pipe.Exec(ctx)`. Handle errors.
	// 10. Return nil on success.

	// --- Your implementation here ---
	return fmt.Errorf("DeleteJob not implemented")
}

// ListJobs retrieves a list of jobs with optional filters
func (s *RedisStorage) ListJobs(ctx context.Context, status job.Status, limit, offset int) ([]*job.Job, error) {
	var jobIDs []string
	var err error

	// 1. Determine how to get the initial list of job IDs based on whether `status` is provided.
	if status != "" {
		// a. Get jobs by status:
		//    i. Generate the status set key using `s.jobsSetFunc(status)`.
		//    ii. Use `s.client.SMembers` to get all members (job IDs) from the set. Handle errors.
		//    iii. Assign the result to `jobIDs`.
	} else {
		// b. Get all jobs sorted by time (use ZRevRange for newest first):
		//    i. Use `s.client.ZRevRange` on `s.jobsKey`. The range is `[offset, offset+limit-1]`. Handle errors.
		//    ii. Assign the resulting slice of IDs to `jobIDs`.
	}

	// 2. If we retrieved by status (in step 1a), we need to manually apply limit and offset to the `jobIDs` slice.
	//    - Check bounds (`offset >= len(jobIDs)`).
	//    - Calculate `end` index, ensuring it doesn't exceed `len(jobIDs)`.
	//    - Slice `jobIDs` appropriately: `jobIDs = jobIDs[offset:end]`. Handle the case where `offset >= end`.
	//    (Note: If we retrieved using ZRevRange in step 1b, Redis already handled limit/offset).

	// 3. If `jobIDs` is empty after filtering/fetching, return an empty slice `[]*job.Job{}` and nil error.

	// 4. Fetch the actual job data for the filtered `jobIDs`:
	//    a. Create an empty slice `result := []*job.Job{}`.
	//    b. Start a Redis pipeline: `pipe := s.client.Pipeline()`.
	//    c. Create a map to store the pipeline commands: `cmds := make(map[string]*redis.StringCmd)`.
	//    d. Loop through `jobIDs`:
	//       i. Generate the job key using `s.jobKeyFunc(id)`.
	//       ii. Add a `pipe.Get(ctx, jobKey)` command to the pipeline and store the resulting command object in the map: `cmds[id] = pipe.Get(...)`.
	//    e. Execute the pipeline: `pipe.Exec(ctx)`. Handle errors (ignore `redis.Nil` for individual gets, but handle other pipeline errors).
	//    f. Loop through the `cmds` map (or iterate through `jobIDs` again and look up in `cmds`):
	//       i. Get the result bytes for the command: `cmd.Bytes()`.
	//       ii. If there's no error getting the bytes:
	//          - Create a `job.Job` variable.
	//          - Unmarshal the bytes into the job variable. Handle errors (`fmt.Errorf`).
	//          - Append the pointer to the job variable (`&j`) to the `result` slice.
	//       iii. (Handle potential errors where a job existed in the list/set but was deleted before the `Get` executed - `cmd.Err() == redis.Nil`).

	// 5. Return the `result` slice and nil error.

	// --- Your implementation here ---
	return nil, fmt.Errorf("ListJobs not implemented")
}
