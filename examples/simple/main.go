package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/yeohbraddy/go-job-queue/internal/broker"
	"github.com/yeohbraddy/go-job-queue/internal/job"
	"github.com/yeohbraddy/go-job-queue/internal/queue"
	"github.com/yeohbraddy/go-job-queue/internal/storage"
	"github.com/yeohbraddy/go-job-queue/internal/worker"
)

// EmailPayload represents the payload for an email job
type EmailPayload struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

// EmailJobHandler handles email jobs
type EmailJobHandler struct{}

// ProcessJob implements the job.Handler interface
func (h *EmailJobHandler) ProcessJob(ctx context.Context, j *job.Job) error {
	var payload EmailPayload
	if err := j.GetPayload(&payload); err != nil {
		return fmt.Errorf("failed to get payload: %w", err)
	}

	// In a real application, this would actually send an email
	log.Printf("Sending email to %s with subject '%s'", payload.To, payload.Subject)

	// Simulate work
	time.Sleep(1 * time.Second)

	return nil
}

// SMSPayload represents the payload for an SMS job
type SMSPayload struct {
	To      string `json:"to"`
	Message string `json:"message"`
}

// SMSJobHandler handles SMS jobs
type SMSJobHandler struct{}

// ProcessJob implements the job.Handler interface
func (h *SMSJobHandler) ProcessJob(ctx context.Context, j *job.Job) error {
	var payload SMSPayload
	if err := j.GetPayload(&payload); err != nil {
		return fmt.Errorf("failed to get payload: %w", err)
	}

	// In a real application, this would actually send an SMS
	log.Printf("Sending SMS to %s: %s", payload.To, payload.Message)

	// Simulate work
	time.Sleep(500 * time.Millisecond)

	// Simulate failure and retry (for demo purposes)
	if j.Retries < 1 {
		return fmt.Errorf("simulated SMS failure (will retry)")
	}

	return nil
}

func main() {
	// Set up context
	ctx := context.Background()

	// Set up queue, storage, and worker
	q := queue.NewInMemoryQueue()
	s := storage.NewInMemoryStorage()
	w := worker.NewWorker(q, 2) // 2 concurrent workers

	// Create broker
	b := broker.NewBroker(q, s, w)

	// Register handlers
	b.RegisterHandler("email", &EmailJobHandler{})
	b.RegisterHandler("sms", &SMSJobHandler{})

	// Start the broker
	b.Start(ctx)
	defer b.Stop()

	// Create an email job
	emailJob, err := job.NewJob("email", EmailPayload{
		To:      "user@example.com",
		Subject: "Hello from Go Job Queue",
		Body:    "This is a test email from our job queue system.",
	})
	if err != nil {
		log.Fatalf("Failed to create email job: %v", err)
	}
	emailJob.WithMaxRetries(3)

	// Enqueue the email job
	if err := b.EnqueueJob(ctx, emailJob); err != nil {
		log.Fatalf("Failed to enqueue email job: %v", err)
	}
	log.Printf("Enqueued email job with ID: %s", emailJob.ID)

	// Create an SMS job
	smsJob, err := job.NewJob("sms", SMSPayload{
		To:      "+1234567890",
		Message: "Hello from Go Job Queue!",
	})
	if err != nil {
		log.Fatalf("Failed to create SMS job: %v", err)
	}
	smsJob.WithMaxRetries(3)

	// Enqueue the SMS job
	if err := b.EnqueueJob(ctx, smsJob); err != nil {
		log.Fatalf("Failed to enqueue SMS job: %v", err)
	}
	log.Printf("Enqueued SMS job with ID: %s", smsJob.ID)

	// Create a delayed job
	delayedJob, err := job.NewJob("email", EmailPayload{
		To:      "delayed@example.com",
		Subject: "Delayed Email",
		Body:    "This email was scheduled to be sent 5 seconds after enqueueing.",
	})
	if err != nil {
		log.Fatalf("Failed to create delayed job: %v", err)
	}
	delayedJob.WithMaxRetries(3)

	// Schedule the delayed job
	if err := b.ScheduleJob(ctx, delayedJob, time.Now().Add(5*time.Second)); err != nil {
		log.Fatalf("Failed to schedule delayed job: %v", err)
	}
	log.Printf("Scheduled delayed job with ID: %s", delayedJob.ID)

	// Wait for jobs to complete
	time.Sleep(10 * time.Second)

	// List completed jobs
	completedJobs, err := b.ListJobs(ctx, job.StatusCompleted, 10, 0)
	if err != nil {
		log.Fatalf("Failed to list completed jobs: %v", err)
	}

	log.Printf("Completed jobs (%d):", len(completedJobs))
	for _, j := range completedJobs {
		jobJSON, _ := json.MarshalIndent(j, "", "  ")
		log.Printf("%s", jobJSON)
	}
}
