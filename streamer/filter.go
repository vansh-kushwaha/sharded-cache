package streamer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
)

const (
	ChunkSize       = 32 * 1024       // 32KB slabs
	MaxMonsterLimit = 1 * 1024 * 1024 // 1MB Hard Circuit breaker limit
)

var ErrPoisonPillDetected = errors.New("circuit breake tripped: payload exceeded 1MB without a newline delimiter")

type RentedChunk struct {
	Data       *[]byte
	Size       int
	SequenceId uint64
}

type StreamManager struct {
	pool sync.Pool
}

func NewStreamManager() *StreamManager {
	return &StreamManager{
		pool: sync.Pool{
			New: func() any {
				slab := make([]byte, ChunkSize)
				return &slab
			},
		},
	}
}

func (sm *StreamManager) Stream(ctx context.Context, file io.Reader, outChan chan<- RentedChunk) error {
	defer close(outChan)

	leftover := make([]byte, 0, ChunkSize)
	var currentSeqId uint64 = 1
	for {

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if len(leftover) >= MaxMonsterLimit {
			return fmt.Errorf("%w: accumulated %d bytes", ErrPoisonPillDetected, len(leftover))
		}

		neededSize := len(leftover) + ChunkSize

		var workBuf []byte
		var rentedSlabPtr *[]byte

		if neededSize > ChunkSize {
			workBuf = make([]byte, neededSize)
		} else {
			rentedSlabPtr = sm.pool.Get().(*[]byte)
			workBuf = *rentedSlabPtr
		}

		copy(workBuf, leftover)
		readOffset := len(leftover)

		// 2. Read from disk to fill the REST of the box
		n, err := file.Read(workBuf[readOffset:])
		totalBytes := readOffset + n

		if totalBytes == 0 {
			if rentedSlabPtr != nil {
				sm.pool.Put(rentedSlabPtr)
			}
			break
		}
		// 3. Find the last safe '\n' in the box so we don't chop a word in half
		lastNewlineIdx := bytes.LastIndexByte(workBuf[readOffset:totalBytes], '\n')

		if lastNewlineIdx == -1 {
			// Extreme edge case: A single line of text is larger than 32KB!
			leftover = append(leftover, workBuf[readOffset:totalBytes]...)
			if rentedSlabPtr != nil {
				sm.pool.Put(rentedSlabPtr)
			}
			if err == io.EOF {
				// as this streaming is finished we can safely send leftover without need to copy in new memory
				sm.dispatchTail(ctx, outChan, leftover, currentSeqId)
				break
			}
			continue
		}

		// We found lastNewLineIdx, since we have pass slice from readOffset while finding new line we have to add readOffset
		lastNewlineIdx += readOffset

		safeLen := lastNewlineIdx + 1
		var dispatchPtr *[]byte

		if safeLen > ChunkSize {
			bigSlab := make([]byte, safeLen)
			dispatchPtr = &bigSlab
		} else {
			dispatchPtr = sm.pool.Get().(*[]byte)
		}

		dispatchBuf := (*dispatchPtr) //[:safeLen]
		copy(dispatchBuf, workBuf[:safeLen])

		select {
		case <-ctx.Done():
			if rentedSlabPtr != nil {
				sm.pool.Put(rentedSlabPtr)
			}
			if dispatchPtr != nil && safeLen <= ChunkSize {
				sm.pool.Put(dispatchPtr)
			}
			return ctx.Err()
		case outChan <- RentedChunk{
			Data:       dispatchPtr,
			Size:       safeLen,
			SequenceId: currentSeqId,
		}:
		}

		currentSeqId++

		leftover = leftover[:0]
		leftover = append(leftover, workBuf[safeLen:totalBytes]...)

		if rentedSlabPtr != nil {
			sm.pool.Put(rentedSlabPtr)
		}

		if err == io.EOF {
			sm.dispatchTail(ctx, outChan, leftover, currentSeqId)
			break
		}
	}

	return nil
}

func (sm *StreamManager) ReturnToShelf(chunk RentedChunk) {

	if cap(*chunk.Data) == ChunkSize {
		sm.pool.Put(chunk.Data)
	}
}

func (sm *StreamManager) dispatchTail(ctx context.Context, outChan chan<- RentedChunk, tail []byte, sequenceId uint64) {
	if len(tail) == 0 {
		return
	}

	select {
	case <-ctx.Done():
		return
	case outChan <- RentedChunk{
		Data:       &tail,
		Size:       len(tail),
		SequenceId: sequenceId,
	}:
	}

}

// ============================================================================
// DIAGNOSTIC PROOF (Run this to verify the memory physics)
// ============================================================================

func GenerateDummyLog(filename string, lines int) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	payload := []byte("2026-06-23T08:00:00Z [INFO] User logged in normally from IP 192.168.1.1\n2026-06-23T08:00:01Z [FATAL] Database connection lost in region us-east-1!\n")

	for i := 0; i < lines/2; i++ {
		_, _ = f.Write(payload)
	}
	return nil
}

func RunMemoryAudit() {
	const testFile = "audit_temp.log"
	fmt.Println("1. Generating 15MB Mock Log file...")
	_ = GenerateDummyLog(testFile, 150_000)
	defer os.Remove(testFile)

	file, _ := os.Open(testFile)
	defer file.Close()

	manager := NewStreamManager()
	streamPipe := make(chan RentedChunk, 10)

	// Force a strict GC baseline before we open the intake
	runtime.GC()
	var m1, m2 runtime.MemStats
	runtime.ReadMemStats(&m1)

	fmt.Println("2. Commencing High-Speed Pooled Stream...")

	go func() {
		_ = manager.Stream(context.Background(), file, streamPipe)
	}()

	var totalBytesProcessed int
	var chunksLeased int

	// Act as the Worker Fleet: Drink the chunks and instantly return them
	for chunk := range streamPipe {
		totalBytesProcessed += chunk.Size
		chunksLeased++

		// 🚰 THE CRITICAL STEP: Return the slab to the bowling rack!
		manager.ReturnToShelf(chunk)
	}

	runtime.ReadMemStats(&m2)

	fmt.Printf("\n🏁 STREAM COMPLETE\n")
	fmt.Printf("├─ Chunks Processed : %d slabs\n", chunksLeased)
	fmt.Printf("├─ Total Volume     : %d Megabytes\n", totalBytesProcessed/1024/1024)
	fmt.Printf("└─ Heap Allocations : %d individual RAM allocations\n", m2.Mallocs-m1.Mallocs)
}
