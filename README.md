# Concurrent Sharded Key-Value Cache

A high-performance, in-memory thread-safe key-value store built in Go.

## Engineering Highlights

- **Lock Stripping Architecture:** Replaced a single global `sync.Mutex` with a matrix of 16 independent cache shards to minimize CPU lock contention under high write/read volumes.
- **Fash Hashing:** Use non-blocking FNV-1a 32-bit checksumming to distribute keys uniformly across buckets.
- **Read-Write Optimization:** Implements `sync.RWMutex` to support multiple simultaneous readers while isolating write operations.

## How to Run

```bash
go run main.go
```
