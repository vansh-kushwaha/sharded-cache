package pool

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestWorkerPool_BasicExecution(t *testing.T) {
	workFn := func(task Task[int]) Result[int] {
		return Result[int]{TaskID: task.ID, Output: task.Data * 2}
	}

	p := NewPool(2, 10, workFn)

	go func() {
		for i := 1; i <= 5; i++ {
			_ = p.Submit(Task[int]{ID: fmt.Sprintf("t-%d", i), Data: i})
		}
		p.Shutdown()
	}()

	count := 0

	for res := range p.Results() {
		count++
		t.Logf("Proccessed %s -> %d", res.TaskID, res.Output)
	}

	if count != 5 {
		t.Fatalf("Expected 5 Results, got %d", count)
	}

}

func TestWorkerPool_LoadShedding(t *testing.T) {
	work := func(task Task[int]) Result[int] {
		time.Sleep(50 * time.Millisecond)
		return Result[int]{
			Output: task.Data,
		}
	}

	p := NewPool(1, 1, work)

	_ = p.Submit(Task[int]{Data: 100})
	_ = p.Submit(Task[int]{Data: 200})
	err := p.TrySubmit(Task[int]{Data: 300})

	if err != ErrQueueFull {
		t.Fatalf("Expected ErrQueueFull, got %v", err)
	}

	res1 := <-p.Results()

	t.Logf("Successfully rescued: %d", res1.Output)

	p.Shutdown()
}

func TestWorkerPool_GracefulShutdownGuarantee(t *testing.T) {
	var completedJobs int32

	work := func(task Task[int]) Result[int] {
		time.Sleep(20 * time.Microsecond)
		atomic.AddInt32(&completedJobs, 1)
		return Result[int]{}
	}

	p := NewPool(2, 10, work)
	for i := 0; i < 10; i++ {
		_ = p.Submit(Task[int]{Data: i})
	}

	p.Shutdown()

	if atomic.LoadInt32(&completedJobs) != 10 {
		t.Fatalf("Shutdown leaked! Expected 10 completed jobs, got %d", completedJobs)
	}
}
