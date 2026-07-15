package syncx

import (
	"context"
	"sync"
)

type RoutineGroup struct {
	rootCtx    context.Context
	rootCancel context.CancelCauseFunc

	wg sync.WaitGroup

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
	g.wg.Wait()
	if g.routine != nil {
		g.routine.Close()
	}
	cause := context.Cause(g.rootCtx)
	if g.rootCancel != nil {
		g.rootCancel(g.err)
	}
	if g.err != nil {
		return g.err
	}
	return cause
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

	g.wg.Add(1)
	if g.routine.Go(g.rootCtx, g.makeTask(fn, true)) != nil {
		g.wg.Done()
	}
}

func (g *RoutineGroup) Run(fn func(ctx context.Context) error) {
	select {
	case <-g.rootCtx.Done():
		return
	default:
	}

	g.wg.Add(1)
	if g.routine.Go(g.rootCtx, g.makeTask(fn, false)) != nil {
		g.wg.Done()
	}
}

func (g *RoutineGroup) TryGo(fn func(context.Context) error) bool {
	select {
	case <-g.rootCtx.Done():
		return false
	default:
	}

	g.wg.Add(1)
	if g.routine.TryGo(g.rootCtx, g.makeTask(fn, true)) != nil {
		g.wg.Done()
		return false
	}
	return true
}

func (g *RoutineGroup) makeTask(fn func(context.Context) error, cancelOnError bool) func() {
	return func() {
		defer g.wg.Done()
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
