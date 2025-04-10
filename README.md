# Go Job Queue

A distributed job queue system written in Go, inspired by Sidekiq. This project implements a Redis-backed job processing system with a focus on reliability and distributed operations.

## Features

- Redis-backed persistent job storage
- Job status tracking (pending, in-progress, completed, failed)
- Efficient job retrieval and management
- Distributed worker system
- Graceful handling of race conditions and concurrency
- Context-based cancellation and timeout management

## Architecture

The project is structured into several key components:

- **Job**: Defines the job data structure and related operations
- **Queue**: Manages job queues and dispatching
- **Worker**: Processes jobs from the queue
- **Storage**: Handles persistence (Redis implementation)
- **Broker**: Coordinates job distribution between components

## Installation

```bash
# Clone the repository
git clone https://github.com/yeohbraddy/go-job-queue.git
cd go-job-queue

# Install dependencies
go mod download
```

## Requirements

- Go 1.24 or higher
- Running Redis server

## Usage

```go
// Example code coming soon
```

## Development Status

This project is currently under active development as a learning exercise for Go programming and distributed systems concepts from "Designing Data-Intensive Applications".

## License

MIT 