package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/yeohbraddy/go-job-queue/internal/broker"
	"github.com/yeohbraddy/go-job-queue/internal/queue"
	"github.com/yeohbraddy/go-job-queue/internal/storage"
	"github.com/yeohbraddy/go-job-queue/internal/worker"
)

func main() {
	// Parse command line flags
	concurrencyFlag := flag.Int("concurrency", 4, "Number of concurrent worker goroutines")
	flag.Parse()

	// Set up context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up queue, storage, and worker
	q := queue.NewInMemoryQueue()
	s := storage.NewInMemoryStorage()
	w := worker.NewWorker(q, *concurrencyFlag)

	// Create broker
	b := broker.NewBroker(q, s, w)

	// Register handlers
	// Here you would register your job handlers
	// Example: b.RegisterHandler("email", &EmailJobHandler{})

	// Start the broker
	log.Println("Starting worker with concurrency:", *concurrencyFlag)
	b.Start(ctx)

	// Wait for interrupt signal to gracefully shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	<-sigCh
	log.Println("Shutting down worker...")

	// Stop the broker
	b.Stop()
	log.Println("Worker stopped")
}
