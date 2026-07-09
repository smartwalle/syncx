package syncx

import (
	"context"
	"errors"
	"sync"
)

var (
	ErrEmitterClosed = errors.New("emitter is closed")
)

type Emitter[T any] struct {
	mu             sync.RWMutex
	subscribers    map[uint64]chan<- T
	nextSubscriber uint64
	closed         bool
}

func NewEmitter[T any]() *Emitter[T] {
	return &Emitter[T]{
		subscribers: make(map[uint64]chan<- T),
	}
}

func (e *Emitter[T]) Emit(ctx context.Context, payload T) error {
	if ctx == nil {
		ctx = context.Background()
	}
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.closed {
		return ErrEmitterClosed
	}

	for _, subscriber := range e.subscribers {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case subscriber <- payload:
		default:
		}
	}
	return nil
}

func (e *Emitter[T]) Listen(bufferSize int) (message <-chan T, cancel func()) {
	if bufferSize < 0 {
		bufferSize = 0
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		var closeChan = make(chan T)
		close(closeChan)
		return closeChan, func() {}
	}

	e.nextSubscriber++
	var id = e.nextSubscriber
	var subscriber = make(chan T, bufferSize)
	e.subscribers[id] = subscriber

	message = subscriber
	cancel = func() {
		e.removeSubscriber(id)
	}

	return message, cancel
}

func (e *Emitter[T]) removeSubscriber(id uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if subscriber, ok := e.subscribers[id]; ok {
		delete(e.subscribers, id)
		close(subscriber)
	}
}

func (e *Emitter[T]) Closed() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.closed
}

func (e *Emitter[T]) Close() {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return
	}
	e.closed = true

	for _, subscriber := range e.subscribers {
		close(subscriber)
	}
	e.subscribers = nil
}
