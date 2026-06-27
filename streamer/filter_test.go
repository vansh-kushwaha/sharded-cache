package streamer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func TestStreamManager_EmptyFile(t *testing.T) {
	// A file ending with no newline character
	payload := ""
	runStreamTest(t, payload)
}
func TestStreamManager_DirtyEnd(t *testing.T) {
	// A file ending with no newline character
	payload := "Line 1: INFO\nLine 2: FATAL\nLine 3: UNTERMINATED"
	runStreamTest(t, payload)
}
func TestStreamManager_Standard(t *testing.T) {
	payload := "Line 1: INFO\nLine 2: FATAL\nLine 3: DEBUG\n"
	runStreamTest(t, payload)
}

func TestStreamManager_WordSplitRescue(t *testing.T) {
	const filename = "test_split.log"

	// We craft a payload that is exactly ChunkSize + 10 bytes.
	// This forces the reader to chop a line right in the middle of execution.
	part1 := strings.Repeat("A", ChunkSize-5) + "CRITI"
	part2 := "CAL_ERROR\n"
	fullPayload := part1 + part2

	_ = os.WriteFile(filename, []byte(fullPayload), 0644)
	defer os.Remove(filename)

	file, _ := os.Open(filename)
	defer file.Close()

	sm := NewStreamManager()

	outChan := make(chan RentedChunk, 5)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = sm.Stream(ctx, file, outChan)
	}()

	var received strings.Builder
	for chunk := range outChan {
		received.Write((*chunk.Data)[:chunk.Size])
		sm.ReturnToShelf(chunk)
	}

	if received.String() != fullPayload {
		t.Fatalf("Word split rescue failed!, Data was corrupted during carry-over.")
	}
}

func runStreamTest(t *testing.T, payload string) {
	t.Helper()
	filename := fmt.Sprintf("test_%d.log", time.Now().UnixNano())
	_ = os.WriteFile(filename, ([]byte)(payload), 0664)
	defer os.Remove(filename)

	file, err := os.Open(filename)
	if err != nil {
		t.Fatalf("Failed to open test file: %v", err)
	}
	defer file.Close()

	sm := NewStreamManager()
	outChan := make(chan RentedChunk, 10)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = sm.Stream(ctx, file, outChan)
	}()

	var received strings.Builder
	for chunk := range outChan {
		// Verify safety: always use the chunk's reported size
		dataSlice := (*chunk.Data)[:chunk.Size]
		received.Write(dataSlice)

		// Recycle the memory box
		sm.ReturnToShelf(chunk)
	}

	if received.String() != payload {
		t.Fatalf("Payload mismatch!\nExpected: %q\nGot:      %q", payload, received.String())
	}
}
func TestStreamManager_CircuitBreakerPoisonPill(t *testing.T) {
	const filename = "test_poison.log"

	poisonPyaload := strings.Repeat("X", MaxMonsterLimit+100_000)
	_ = os.WriteFile(filename, []byte(poisonPyaload), 0644)
	defer os.Remove(filename)

	file, _ := os.Open(filename)
	defer file.Close()

	sm := NewStreamManager()
	outChan := make(chan RentedChunk, 5)

	err := sm.Stream(context.Background(), file, outChan)

	if !errors.Is(err, ErrPoisonPillDetected) {
		t.Fatalf("Expected ErrPoisonPillDetected, got %v", err)
	}
}

func TestStreamManager_AtomicEOF(t *testing.T) {
	// A small payload that will be read entirely in one go
	payload := "AtomicRead\n"
	reader := strings.NewReader(payload)

	sm := NewStreamManager()
	outChan := make(chan RentedChunk, 2)

	// This will force the internal Read() to return n=10, err=io.EOF
	err := sm.Stream(context.Background(), reader, outChan)
	if err != nil {
		t.Fatalf("Stream failed: %v", err)
	}

	// Verify we got the data
	chunk := <-outChan
	data := string((*chunk.Data)[:chunk.Size])
	if data != payload {
		t.Fatalf("Expected %q, got %q", payload, data)
	}

	// Verify the channel closed (pipeline finished)
	_, ok := <-outChan
	if ok {
		t.Fatal("Channel should have been closed")
	}
}

func TestStreamManager_Sequencing(t *testing.T) {
	// Create a payload that is large enough to span multiple chunks.
	// ChunkSize is 32KB, so 100KB guarantees at least 3 chunks.
	lines := 3000
	var sb strings.Builder
	for i := 1; i <= lines; i++ {
		sb.WriteString(fmt.Sprintf("Line %d: This is dummy log line with number %d\n", i, i))
	}
	payload := sb.String()

	filename := "test_sequencing.log"
	_ = os.WriteFile(filename, []byte(payload), 0644)
	defer os.Remove(filename)

	file, err := os.Open(filename)
	if err != nil {
		t.Fatalf("Failed to open test file: %v", err)
	}
	defer file.Close()

	sm := NewStreamManager()
	outChan := make(chan RentedChunk, 10)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = sm.Stream(ctx, file, outChan)
	}()

	var expectedSeqId uint64 = 1
	var chunkCount int
	for chunk := range outChan {
		chunkCount++
		if chunk.SequenceId != expectedSeqId {
			t.Errorf("Chunk %d: Expected SequenceId %d, got %d", chunkCount, expectedSeqId, chunk.SequenceId)
		}
		expectedSeqId++
		sm.ReturnToShelf(chunk)
	}

	if chunkCount <= 1 {
		t.Fatalf("Expected multiple chunks to test sequencing, but got only %d chunk(s)", chunkCount)
	}
}
