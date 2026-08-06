package routine

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrRoutineClosed = errors.New("routine is closed")

	ErrRoutineQueueFull = errors.New("routine queue is full")

	ErrRoutineBadTask = errors.New("routine task is nil")
)

type Option func(*Pool)

type PanicHandler func(any)

func WithMaxWorkers(n int32) Option {
	return func(p *Pool) {
		p.maxWorkers = n
	}
}

func WithQueueCapacity(n int32) Option {
	return func(p *Pool) {
		p.taskQueueCapacity = n
	}
}

func WithIdleTimeout(timeout time.Duration) Option {
	return func(p *Pool) {
		p.idleTimeout = timeout
	}
}

type Pool struct {
	taskQueue chan func()

	// submitState 的最高位表示关闭状态，其余位表示已通过关闭边界、但尚未结束
	// 通道操作的提交者数量。一次原子加法即可完成提交者准入。
	submitState atomic.Uint64

	closed              chan struct{}
	submissionsDone     chan struct{}
	submissionsDoneOnce sync.Once

	closeOnce sync.Once
	workerWG  sync.WaitGroup

	maxWorkers        int32
	taskQueueCapacity int32
	idleTimeout       time.Duration
	workerCount       atomic.Int32
	cleanupDone       chan struct{}
	panicHandler      atomic.Value
}

const (
	poolClosedState    uint64 = 1 << 63
	defaultIdleTimeout        = 5 * time.Second
)

func New(options ...Option) *Pool {
	p := &Pool{
		maxWorkers:      1,
		idleTimeout:     defaultIdleTimeout,
		closed:          make(chan struct{}),
		submissionsDone: make(chan struct{}),
		cleanupDone:     make(chan struct{}),
	}
	for _, option := range options {
		if option != nil {
			option(p)
		}
	}
	if p.maxWorkers < 1 {
		p.maxWorkers = 1
	}
	if p.taskQueueCapacity < 0 {
		p.taskQueueCapacity = 0
	}
	if p.idleTimeout <= 0 {
		p.idleTimeout = defaultIdleTimeout
	}

	p.taskQueue = make(chan func(), int(p.taskQueueCapacity))
	go p.cleanupIdleWorkers()
	return p
}

func (p *Pool) Go(fn func()) error {
	if fn == nil {
		return ErrRoutineBadTask
	}
	if !p.beginSubmit() {
		return ErrRoutineClosed
	}
	defer p.endSubmit()
	p.startWorker()

	// 无缓冲队列没有可用的排队空间，提交者必须等待 worker 直接接手任务。
	// 等待期间同时监听关闭信号：Close 会先关闭 closed，再等待已准入的
	// 提交者退出；若不监听该信号，提交者与 Close 可能相互等待。
	if p.taskQueueCapacity == 0 {
		select {
		case p.taskQueue <- fn:
			return nil
		case <-p.closed:
			return ErrRoutineClosed
		}
	}

	// 有缓冲队列分为两个阶段提交。先仅尝试写入任务通道；队列尚有空位时，
	// 无需读取 closed，也让已通过 beginSubmit 的任务优先进入队列。
	select {
	case p.taskQueue <- fn:
		return nil
	default:
	}

	// 队列已满时才等待空间或关闭。Close 不会立即关闭 taskQueue，而是先关闭
	// closed 并等待提交者退出；这里必须监听 closed，才能让阻塞的提交者
	// 返回，避免 Close 与提交者相互等待。
	select {
	case p.taskQueue <- fn:
		return nil
	case <-p.closed:
		return ErrRoutineClosed
	}
}

func (p *Pool) TryGo(fn func()) error {
	if fn == nil {
		return ErrRoutineBadTask
	}
	if !p.beginSubmit() {
		return ErrRoutineClosed
	}
	defer p.endSubmit()
	p.startWorker()

	// 无缓冲队列只能直接交给空闲 worker；任务、关闭和队列满一次判定。
	if p.taskQueueCapacity == 0 {
		select {
		case p.taskQueue <- fn:
			return nil
		case <-p.closed:
			return ErrRoutineClosed
		default:
			return ErrRoutineQueueFull
		}
	}

	// 有缓冲队列先进行一次无阻塞写入。这里不等待队列腾出空间，保证 TryGo
	// 不会因 worker 正在执行任务而阻塞。
	select {
	case p.taskQueue <- fn:
		return nil
	default:
	}

	// 首次写入失败后，只检查关闭状态，不再等待或重试写入。即使队列随后
	// 出现空位，本次调用也按“无法立即提交”返回队列已满；若 Close 已开始，
	// 则优先返回关闭错误。
	select {
	case <-p.closed:
		return ErrRoutineClosed
	default:
		return ErrRoutineQueueFull
	}
}

// Close 拒绝后续提交，并等待已接收任务和 worker 结束。
// 不要在提交到同一个 Pool 的任务中调用 Close，否则会等待当前任务退出而死锁。
func (p *Pool) Close() {
	p.closeOnce.Do(func() {
		state := p.markClosed()
		close(p.closed)
		if state == poolClosedState {
			p.closeSubmissionsDone()
		}

		// 已准入的提交者要么写入任务，要么因关闭而退出；之后再关闭任务通道。
		<-p.submissionsDone
		<-p.cleanupDone
		close(p.taskQueue)
		p.workerWG.Wait()
	})
}

func (p *Pool) Closed() bool {
	return p.submitState.Load()&poolClosedState != 0
}

// OnPanic 设置任务 panic 的处理器。未设置时保留原始 panic 行为。
func (p *Pool) OnPanic(handler PanicHandler) {
	if handler != nil {
		p.panicHandler.Store(handler)
	}
}

func (p *Pool) worker() {
	defer p.finishWorker()

	for {
		fn, ok := <-p.taskQueue
		if !ok || fn == nil {
			return
		}
		p.run(fn)
	}
}

func (p *Pool) startWorker() {
	for {
		count := p.workerCount.Load()
		if count >= p.maxWorkers {
			return
		}
		if p.workerCount.CompareAndSwap(count, count+1) {
			p.workerWG.Add(1)
			go p.worker()
			return
		}
	}
}

func (p *Pool) scaleForQueuedWork() {
	if len(p.taskQueue) > 0 || (!p.Closed() && p.submitting() > 0) {
		p.startWorker()
	}
}

func (p *Pool) finishWorker() {
	defer p.workerWG.Done()
	p.workerCount.Add(-1)
	p.scaleForQueuedWork()
}

func (p *Pool) run(fn func()) {
	defer func() {
		if x := recover(); x != nil {
			p.handlePanic(x)
		}
	}()
	fn()
}

func (p *Pool) handlePanic(x any) {
	handler, _ := p.panicHandler.Load().(PanicHandler)
	if handler == nil {
		panic(x)
	}

	defer func() {
		_ = recover()
	}()
	handler(x)
}

func (p *Pool) cleanupIdleWorkers() {
	defer close(p.cleanupDone)

	ticker := time.NewTicker(p.idleTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-p.closed:
			return
		case <-ticker.C:
		}

		workerCount := p.workerCount.Load()
		if workerCount == 0 || len(p.taskQueue) != 0 {
			continue
		}
	retire:
		for i := int32(0); i < workerCount; i++ {
			select {
			case p.taskQueue <- nil:
			default:
				break retire
			}
		}
	}
}

func (p *Pool) beginSubmit() bool {
	state := p.submitState.Add(1)
	if state&poolClosedState != 0 {
		// Close 可能已关闭任务通道，因此当前调用不能再触碰 taskQueue。
		p.endSubmit()
		return false
	}
	return true
}

func (p *Pool) endSubmit() {
	state := p.submitState.Add(^uint64(0))
	if state == poolClosedState {
		p.closeSubmissionsDone()
	}
}

func (p *Pool) submitting() uint64 {
	return p.submitState.Load() &^ poolClosedState
}

func (p *Pool) markClosed() uint64 {
	return p.submitState.Add(poolClosedState)
}

func (p *Pool) closeSubmissionsDone() {
	p.submissionsDoneOnce.Do(func() {
		close(p.submissionsDone)
	})
}
