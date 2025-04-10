package storage

import (
	"context"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yeohbraddy/go-job-queue/internal/job"
)

// setupRedisTest sets up a test environment with a Redis client
// and a RedisStorage instance, with a unique key prefix for each test
func setupRedisTest(t *testing.T) (*redis.Client, *RedisStorage, context.Context) {
	t.Helper()

	// Connect to Redis - update these connection details as needed
	// In a CI environment, you'd use environment variables
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379", // Default Redis address
		Password: "",               // No password
		DB:       0,                // Default DB
	})

	// Use a unique prefix for each test to avoid conflicts
	keyPrefix := "test:" + time.Now().Format("20060102150405.000000000")
	storage := NewRedisStorage(client, keyPrefix)
	ctx := context.Background()

	// Check if Redis is available
	_, err := client.Ping(ctx).Result()
	require.NoError(t, err, "Redis server must be available")

	// Setup cleanup
	t.Cleanup(func() {
		// Get all keys with our test prefix
		keys, err := client.Keys(ctx, keyPrefix+"*").Result()
		if err == nil && len(keys) > 0 {
			client.Del(ctx, keys...)
		}
		client.Close()
	})

	return client, storage, ctx
}

// createTestJob creates a test job with the specified ID and status
func createTestJob(id string, status job.Status) *job.Job {
	now := time.Now()
	return &job.Job{
		ID:         id,
		Type:       "test",
		Payload:    []byte(`{"test":"data"}`),
		Status:     status,
		MaxRetries: 3,
		Retries:    0,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
}

// TestSaveJob tests the SaveJob method
func TestSaveJob(t *testing.T) {
	_, storage, ctx := setupRedisTest(t)

	// Create a test job
	j := createTestJob("job1", job.StatusPending)

	// Save the job
	err := storage.SaveJob(ctx, j)
	require.NoError(t, err, "SaveJob should not return an error")

	// Verify the job was saved correctly
	savedJob, err := storage.GetJob(ctx, j.ID)
	require.NoError(t, err, "GetJob should not return an error")
	require.NotNil(t, savedJob, "GetJob should return the saved job")
	assert.Equal(t, j.ID, savedJob.ID, "Job IDs should match")
	assert.Equal(t, j.Type, savedJob.Type, "Job types should match")
	assert.Equal(t, j.Status, savedJob.Status, "Job statuses should match")
}

// TestGetJob tests the GetJob method
func TestGetJob(t *testing.T) {
	_, storage, ctx := setupRedisTest(t)

	t.Run("existing job", func(t *testing.T) {
		// Create and save a test job
		j := createTestJob("job2", job.StatusPending)
		err := storage.SaveJob(ctx, j)
		require.NoError(t, err, "SaveJob should not return an error")

		// Get the job
		savedJob, err := storage.GetJob(ctx, j.ID)
		require.NoError(t, err, "GetJob should not return an error")
		require.NotNil(t, savedJob, "GetJob should return the saved job")
		assert.Equal(t, j.ID, savedJob.ID, "Job IDs should match")
	})

	t.Run("non-existent job", func(t *testing.T) {
		// Get a non-existent job
		savedJob, err := storage.GetJob(ctx, "non-existent-job")
		require.NoError(t, err, "GetJob should not return an error for non-existent jobs")
		assert.Nil(t, savedJob, "GetJob should return nil for non-existent jobs")
	})
}

// TestUpdateJob tests the UpdateJob method
func TestUpdateJob(t *testing.T) {
	_, storage, ctx := setupRedisTest(t)

	t.Run("update existing job", func(t *testing.T) {
		// Create and save a test job
		j := createTestJob("job3", job.StatusPending)
		err := storage.SaveJob(ctx, j)
		require.NoError(t, err, "SaveJob should not return an error")

		// Update the job
		j.Status = job.StatusRunning
		j.Retries = 1
		err = storage.UpdateJob(ctx, j)
		require.NoError(t, err, "UpdateJob should not return an error")

		// Verify the job was updated
		updatedJob, err := storage.GetJob(ctx, j.ID)
		require.NoError(t, err, "GetJob should not return an error")
		require.NotNil(t, updatedJob, "GetJob should return the updated job")
		assert.Equal(t, j.Status, updatedJob.Status, "Job statuses should match")
		assert.Equal(t, j.Retries, updatedJob.Retries, "Job retries should match")
	})

	t.Run("update non-existent job", func(t *testing.T) {
		// Try to update a non-existent job
		j := createTestJob("non-existent-job", job.StatusPending)
		err := storage.UpdateJob(ctx, j)
		require.Error(t, err, "UpdateJob should return an error for non-existent jobs")
		assert.Contains(t, err.Error(), "not found", "Error should indicate job not found")
	})
}

// TestDeleteJob tests the DeleteJob method
func TestDeleteJob(t *testing.T) {
	_, storage, ctx := setupRedisTest(t)

	t.Run("delete existing job", func(t *testing.T) {
		// Create and save a test job
		j := createTestJob("job4", job.StatusPending)
		err := storage.SaveJob(ctx, j)
		require.NoError(t, err, "SaveJob should not return an error")

		// Delete the job
		err = storage.DeleteJob(ctx, j.ID)
		require.NoError(t, err, "DeleteJob should not return an error")

		// Verify the job was deleted
		deletedJob, err := storage.GetJob(ctx, j.ID)
		require.NoError(t, err, "GetJob should not return an error after deletion")
		assert.Nil(t, deletedJob, "GetJob should return nil for deleted jobs")
	})

	t.Run("delete non-existent job", func(t *testing.T) {
		// Try to delete a non-existent job
		err := storage.DeleteJob(ctx, "non-existent-job")
		require.NoError(t, err, "DeleteJob should not return an error for non-existent jobs")
	})
}

// TestListJobs tests the ListJobs method
func TestListJobs(t *testing.T) {
	_, storage, ctx := setupRedisTest(t)

	// Create and save several test jobs with different statuses
	jobs := []*job.Job{
		createTestJob("job5", job.StatusPending),
		createTestJob("job6", job.StatusRunning),
		createTestJob("job7", job.StatusCompleted),
		createTestJob("job8", job.StatusPending),
		createTestJob("job9", job.StatusFailed),
	}

	for _, j := range jobs {
		err := storage.SaveJob(ctx, j)
		require.NoError(t, err, "SaveJob should not return an error")
	}

	t.Run("list all jobs", func(t *testing.T) {
		// List all jobs (no status filter)
		listedJobs, err := storage.ListJobs(ctx, "", 10, 0)
		require.NoError(t, err, "ListJobs should not return an error")
		assert.Len(t, listedJobs, 5, "ListJobs should return all jobs")
	})

	t.Run("list jobs by status", func(t *testing.T) {
		// List pending jobs
		pendingJobs, err := storage.ListJobs(ctx, job.StatusPending, 10, 0)
		require.NoError(t, err, "ListJobs should not return an error")
		assert.Len(t, pendingJobs, 2, "ListJobs should return pending jobs")

		// Verify all returned jobs have the correct status
		for _, j := range pendingJobs {
			assert.Equal(t, job.StatusPending, j.Status, "All listed jobs should have the specified status")
		}
	})

	t.Run("pagination", func(t *testing.T) {
		// List with limit and offset
		limitedJobs, err := storage.ListJobs(ctx, "", 2, 0)
		require.NoError(t, err, "ListJobs should not return an error")
		assert.Len(t, limitedJobs, 2, "ListJobs should return only the specified number of jobs")

		// Test offset
		offsetJobs, err := storage.ListJobs(ctx, "", 2, 2)
		require.NoError(t, err, "ListJobs should not return an error")
		assert.Len(t, offsetJobs, 2, "ListJobs should return only the specified number of jobs with offset")
		assert.NotEqual(t, limitedJobs[0].ID, offsetJobs[0].ID, "Jobs returned with offset should be different")
	})

	t.Run("empty result", func(t *testing.T) {
		// Test getting a status with no matching jobs
		nonExistentJobs, err := storage.ListJobs(ctx, "non-existent-status", 10, 0)
		require.NoError(t, err, "ListJobs should not return an error for non-existent status")
		assert.Empty(t, nonExistentJobs, "ListJobs should return an empty slice for non-existent status")
		assert.NotNil(t, nonExistentJobs, "ListJobs should return an empty slice, not nil")
	})

	t.Run("race condition handling", func(t *testing.T) {
		// Create a job, then delete it right after listing to simulate race condition
		raceJob := createTestJob("race-job", job.StatusPending)
		err := storage.SaveJob(ctx, raceJob)
		require.NoError(t, err)

		// Now, simulate a race condition by deleting the job
		// before we try to get its data in ListJobs
		err = storage.DeleteJob(ctx, raceJob.ID)
		require.NoError(t, err)

		// Use low-level Redis commands to add just the ID to the sorted set
		// but don't create the actual job data
		client := redis.NewClient(&redis.Options{
			Addr: "localhost:6379",
		})
		defer client.Close()

		err = client.ZAdd(ctx, storage.jobsKey, &redis.Z{
			Score:  float64(time.Now().UnixNano()),
			Member: "phantom-job",
		}).Err()
		require.NoError(t, err)

		// List jobs, which should handle the race condition gracefully
		listedJobs, err := storage.ListJobs(ctx, "", 10, 0)
		require.NoError(t, err, "ListJobs should not return an error even with race conditions")

		// The result should still contain valid jobs
		assert.NotEmpty(t, listedJobs, "ListJobs should return the remaining valid jobs")
	})
}
