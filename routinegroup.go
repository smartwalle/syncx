package syncx

import (
	"context"
	"sync"
)

type RoutineGroup struct {
	rootCtx    context.Context
	rootCancel context.CancelCauseFunc

	routine *Routine

	errOnce sync.Once
	err     error
}

func NewRoutineGroup(ctx context.Context, maxConcurrency int, queueCapacity int) *RoutineGroup {
	ctx, cancel := context.WithCancelCause(ctx)
	if maxConcurrency < 1 {
		maxConcurrency = 1
	}
	if queueCapacity < 0 {
		queueCapacity = 0
	}
	return &RoutineGroup{
		rootCtx:    ctx,
		rootCancel: cancel,
		routine:    NewRoutine(maxConcurrency, queueCapacity),
	}
}

func (g *RoutineGroup) Wait() error {
	if g.routine != nil {
		g.routine.Wait()
		g.routine.Close()
	}

	if g.rootCancel != nil {
		g.rootCancel(g.err)
	}
	return g.err
}

func (g *RoutineGroup) OnPanic(handler PanicHandler) {
	g.routine.OnPanic(handler)
}

func (g *RoutineGroup) Go(fn func(context.Context) error) {
	select {
	case <-g.rootCtx.Done():
		return
	default:
	}

	_ = g.routine.Go(g.makeTask(fn, true))
}

func (g *RoutineGroup) Run(fn func(ctx context.Context) error) {
	select {
	case <-g.rootCtx.Done():
		return
	default:
	}

	_ = g.routine.Go(g.makeTask(fn, false))
}

func (g *RoutineGroup) TryGo(fn func(context.Context) error) bool {
	select {
	case <-g.rootCtx.Done():
		return false
	default:
	}

	if g.routine.TryGo(g.makeTask(fn, true)) != nil {
		return false
	}
	return true
}

func (g *RoutineGroup) makeTask(fn func(context.Context) error, cancelOnError bool) func() {
	return func() {
		if err := fn(g.rootCtx); err != nil {
			g.errOnce.Do(func() {
				g.err = err
				if cancelOnError && g.rootCancel != nil {
					g.rootCancel(g.err)
				}
			})
		}
	}
}
