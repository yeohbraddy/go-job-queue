# Go Job Queue - Distributed Job Queue System

A distributed job queue system written in Go, inspired by Sidekiq and applying concepts from "Designing Data-Intensive Applications".

## Project Overview

This project implements a distributed job queue system with the following features:

- Job creation with typed payloads
- Job scheduling with delayed execution
- Automatic retries with exponential backoff
- In-memory implementations of queue and storage (extensible to persistent storage)
- Concurrent job processing with worker pools
- Job status tracking

## Components

- **Job**: Represents a unit of work to be processed.
- **Queue**: Interface for enqueueing and dequeuing jobs.
- **Worker**: Processes jobs from the queue with configurable concurrency.
- **Storage**: Persists jobs and their states.
- **Broker**: Coordinates between the queue, storage, and workers.

## Getting Started

### Running the Example

```bash
go run examples/simple/main.go
```

This will:
1. Create and start a job queue system
2. Register handlers for email and SMS jobs
3. Enqueue several jobs including a delayed job
4. Process the jobs concurrently
5. Display the completed jobs

### Running the Worker

```bash
go run cmd/worker/main.go --concurrency=4
```

## Key Concepts

### Durability and Reliability

- Jobs are persisted in storage before being enqueued
- Failed jobs are automatically retried with exponential backoff
- Jobs can be scheduled for future execution

### Eventual Consistency

- The system is designed to be eventually consistent
- Even if a worker fails, jobs will eventually be processed

### Distributed Systems Features

- Concurrent job processing
- Atomic job operations
- Failure handling with retries
- Backoff strategies for retry

## Extension Points

The system can be extended in several ways:

1. **Persistent Storage**: Replace `InMemoryStorage` with a database implementation
2. **Distributed Queue**: Replace `InMemoryQueue` with Redis, RabbitMQ, etc.
3. **Custom Backoff**: Implement custom backoff strategies
4. **Job Monitoring**: Add monitoring and metrics collection

## Learning Resources

- [Designing Data-Intensive Applications](https://dataintensive.net/) - Core concepts applied in this project
- [Sidekiq Documentation](https://github.com/sidekiq/sidekiq/wiki) - Inspiration for the job queue design
- [Go Concurrency Patterns](https://blog.golang.org/pipelines) - Patterns used for worker pools

## Go Concepts Used

- **Interfaces**: For queue, storage, and job handlers
- **Goroutines**: For concurrent job processing
- **Channels**: For worker communication and shutdown
- **Context**: For cancellation and timeouts
- **Mutexes**: For thread-safe queue and storage operations
- **Error Handling**: Proper error propagation and handling

## Future Enhancements

- **Middleware**: Add support for middleware for job processing
- **Priority Queues**: Support for job priorities
- **Dead Letter Queue**: Store failed jobs for later inspection
- **Web UI**: Admin interface for monitoring and managing jobs
- **Distributed Locking**: Ensure only one worker processes a job 