package pool

import (
	"errors"
	"sync"
)

var ErrQueueFull = errors.New("worker pool job queue is completely full")
var ErrPoolClosed = errors.New("cannot submit task: worker pool has been shut down")

type Task[T any] struct {
	ID   string
	Data T
}

type Result[R any] struct {
	TaskID string
	Output R
	Err    error
}

type Pool[T any, R any] struct {
	workerCount uint32
	bufferSize  uint32

	jobs    chan Task[T]
	results chan Result[R]

	stateMu  sync.RWMutex
	isClosed bool

	wg        sync.WaitGroup
	closeOnce sync.Once
	workFn    func(Task[T]) Result[R]
}

func NewPool[T any, R any](workers uint32, buffer uint32, workFn func(Task[T]) Result[R]) *Pool[T, R] {
	p := &Pool[T, R]{
		workerCount: workers,
		bufferSize:  buffer,
		jobs:        make(chan Task[T], buffer),
		results:     make(chan Result[R], buffer),
		workFn:      workFn,
	}

	for i := uint32(0); i < workers; i++ {
		p.wg.Add(1)
		go p.startWorkerLifecycle()
	}

	return p
}

func (p *Pool[T, R]) startWorkerLifecycle() {
	defer p.wg.Done()
	for job := range p.jobs {
		res := p.workFn(job)
		p.results <- res
	}
}

func (p *Pool[T, R]) Submit(task Task[T]) error {
	p.stateMu.RLock()
	defer p.stateMu.RUnlock()

	if p.isClosed {
		return ErrPoolClosed
	}

	p.jobs <- task
	return nil
}

func (p *Pool[T, R]) TrySubmit(task Task[T]) error {
	p.stateMu.RLock()
	defer p.stateMu.RUnlock()

	if p.isClosed {
		return ErrPoolClosed
	}

	select {
	case p.jobs <- task:
		return nil
	default:
		return ErrQueueFull
	}
}

func (p *Pool[T, R]) Results() <-chan Result[R] {
	return p.results
}

func (p *Pool[T, R]) Shutdown() {
	p.stateMu.Lock()
	if p.isClosed {
		p.stateMu.Unlock()
		return
	}
	p.isClosed = true
	close(p.jobs)
	p.stateMu.Unlock()
	p.wg.Wait()

	p.closeOnce.Do(func() {
		close(p.results)
	})
}
