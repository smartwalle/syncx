package syncx

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrRoutineClosed    = errors.New("routine is closed")
	ErrRoutineQueueFull = errors.New("routine queue is full")
	ErrRoutineBadTask   = errors.New("routine task is nil")
)

type Routine struct {
	tasks chan func()

	mu   sync.Mutex
	cond *sync.Cond

	runnerWg    sync.WaitGroup
	runnerCount int

	submitters int
	pending    int

	closed    chan struct{}
	closeOnce sync.Once

	maxConcurrency int
	idleTimeout    time.Duration

	panicHandler atomic.Value
}

type PanicHandler func(any)

func NewRoutine(maxConcurrency int, queueCapacity int) *Routine {
	if maxConcurrency < 1 {
		maxConcurrency = 1
	}
	if queueCapacity < 0 {
		queueCapacity = 0
	}

	var routine = &Routine{
		tasks:          make(chan func(), queueCapacity),
		closed:         make(chan struct{}),
		maxConcurrency: maxConcurrency,
		idleTimeout:    time.Second * 5,
	}
	routine.cond = sync.NewCond(&routine.mu)

	return routine
}

func (r *Routine) Go(ctx context.Context, fn func()) error {
	if fn == nil {
		return ErrRoutineBadTask
	}
	if !r.beginSubmit() {
		return ErrRoutineClosed
	}
	defer r.endSubmit()

	select {
	case r.tasks <- fn:
		return nil
	case <-ctx.Done():
		r.doneTask()
		return ctx.Err()
	case <-r.closed:
		r.doneTask()
		return ErrRoutineClosed
	}
}

func (r *Routine) TryGo(ctx context.Context, fn func()) error {
	if fn == nil {
		return ErrRoutineBadTask
	}
	if !r.beginSubmit() {
		return ErrRoutineClosed
	}
	defer r.endSubmit()

	select {
	case <-ctx.Done():
		r.doneTask()
		return ctx.Err()
	default:
	}

	select {
	case r.tasks <- fn:
		return nil
	case <-ctx.Done():
		r.doneTask()
		return ctx.Err()
	case <-r.closed:
		r.doneTask()
		return ErrRoutineClosed
	default:
		r.doneTask()
		return ErrRoutineQueueFull
	}
}

func (r *Routine) Close() {
	r.closeOnce.Do(func() {
		r.mu.Lock()
		close(r.closed)
		for r.submitters > 0 {
			r.cond.Wait()
		}
		close(r.tasks)
		r.mu.Unlock()

		r.runnerWg.Wait()
	})
}

func (r *Routine) Wait() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for r.pending > 0 {
		r.cond.Wait()
	}
}

func (r *Routine) OnPanic(handler PanicHandler) {
	if handler == nil {
		return
	}
	r.panicHandler.Store(handler)
}

func (r *Routine) Closed() bool {
	select {
	case <-r.closed:
		return true
	default:
		return false
	}
}

func (r *Routine) beginSubmit() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	select {
	case <-r.closed:
		return false
	default:
	}
	r.submitters++
	r.pending++

	if r.runnerCount < r.maxConcurrency {
		r.runnerCount++
		r.runnerWg.Add(1)
		go r.runnerLoop()
	}

	return true
}

func (r *Routine) endSubmit() {
	r.mu.Lock()
	r.submitters--
	if r.submitters == 0 {
		r.cond.Broadcast()
	}
	r.mu.Unlock()
}

func (r *Routine) doneTask() {
	r.mu.Lock()
	r.pending--
	if r.pending == 0 {
		r.cond.Broadcast()
	}
	r.mu.Unlock()
}

func (r *Routine) runnerLoop() {
	var stopped = false
	defer func() {
		if !stopped {
			r.finishRunner()
		}
		r.runnerWg.Done()
	}()

	var timer = time.NewTimer(r.idleTimeout)
	defer timer.Stop()

	for {
		select {
		case fn, ok := <-r.tasks:
			if !ok {
				return
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			r.run(fn)
			timer.Reset(r.idleTimeout)
		case <-timer.C:
			select {
			case fn, ok := <-r.tasks:
				if !ok {
					return
				}
				r.run(fn)
				timer.Reset(r.idleTimeout)
			default:
				r.mu.Lock()
				if r.submitters > 0 {
					r.mu.Unlock()
					timer.Reset(r.idleTimeout)
					continue
				}
				r.runnerCount--
				stopped = true
				r.mu.Unlock()
				return
			}
		}
	}
}

func (r *Routine) finishRunner() {
	r.mu.Lock()
	r.runnerCount--
	r.mu.Unlock()
}

func (r *Routine) run(fn func()) {
	defer func() {
		if x := recover(); x != nil {
			r.handlePanic(x)
		}
		r.doneTask()
	}()
	fn()
}

func (r *Routine) handlePanic(x any) {
	var handler, _ = r.panicHandler.Load().(PanicHandler)
	if handler == nil {
		return
	}

	defer func() {
		_ = recover()
	}()
	handler(x)
}
