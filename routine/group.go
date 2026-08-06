package routine

import (
	"context"
	"sync"
)

type Group struct {
	ctx    context.Context
	cancel context.CancelCauseFunc

	pool *Pool
	wg   sync.WaitGroup

	errOnce sync.Once
	err     error
}

func (p *Pool) Group(ctx context.Context) *Group {
	ctx, cancel := context.WithCancelCause(ctx)
	return &Group{
		ctx:    ctx,
		cancel: cancel,
		pool:   p,
	}
}

func (g *Group) Wait() error {
	g.wg.Wait()
	if g.cancel != nil {
		g.cancel(g.err)
	}
	return g.err
}

func (g *Group) Go(fn func(context.Context) error) {
	g.submit(fn, true)
}

func (g *Group) Run(fn func(context.Context) error) {
	g.submit(fn, false)
}

func (g *Group) submit(fn func(context.Context) error, cancelOnError bool) {
	if fn == nil {
		return
	}

	select {
	case <-g.ctx.Done():
		return
	default:
	}

	g.wg.Add(1)
	var task = func() {
		defer g.wg.Done()

		if err := fn(g.ctx); err != nil {
			g.setError(err, cancelOnError)
		}
	}

	var err = g.pool.Go(task)
	if err == nil {
		return
	}
	g.setError(err, cancelOnError)
	g.wg.Done()
}

func (g *Group) setError(err error, cancel bool) {
	g.errOnce.Do(func() {
		g.err = err
		if cancel && g.cancel != nil {
			g.cancel(err)
		}
	})
}
