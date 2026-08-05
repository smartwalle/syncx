package syncx

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrRoutineClosed Routine 已关闭，不能再接受任务。
	ErrRoutineClosed = errors.New("routine is closed")

	// ErrRoutineQueueFull TryGo 无法立即将任务写入队列。
	ErrRoutineQueueFull = errors.New("routine queue is full")

	// ErrRoutineBadTask 提交的任务为 nil。
	ErrRoutineBadTask = errors.New("routine task is nil")
)

// Routine 是限制并发执行数量的任务池。
type Routine struct {
	tasks chan func()

	// state 统一记录每次任务提交和完成时变化的状态。
	// 将其合并为一个字可使 Close 拒绝新提交，同时继续跟踪已进入发送阶段的提交。
	state atomic.Uint64

	waitMu   sync.Mutex
	waitCond *sync.Cond

	runnerWg    sync.WaitGroup
	runnerCount atomic.Int64

	closed         chan struct{}
	tasksClosed    chan struct{}
	closeOnce      sync.Once
	tasksCloseOnce sync.Once

	maxConcurrency int
	idleTimeout    time.Duration

	panicHandler atomic.Value
}

type PanicHandler func(any)

const (
	// state 从低到高依次保存未完成任务数、正在提交的调用数和关闭标记。
	// 未完成任务数包含已准入但尚未写入 tasks 的任务，使 Close 不会遗漏这类任务。
	routinePendingBits            = 31
	routinePendingMask            = (uint64(1) << routinePendingBits) - 1
	routineSubmittingShift        = routinePendingBits
	routineSubmittingMask         = routinePendingMask << routineSubmittingShift
	routineSubmittingStep         = uint64(1) << routineSubmittingShift
	routineClosedState     uint64 = 1 << 63
)

// NewRoutine 创建最大并发数为 maxConcurrency、队列容量为 queueCapacity 的 Routine。
// maxConcurrency 小于 1 时按 1 处理；queueCapacity 小于 0 时按 0 处理。
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
		tasksClosed:    make(chan struct{}),
		maxConcurrency: maxConcurrency,
		idleTimeout:    time.Second * 5,
	}
	routine.waitCond = sync.NewCond(&routine.waitMu)

	return routine
}

// Go 提交任务；队列已满时会等待任务可写入、ctx 取消或 Routine 关闭。
func (r *Routine) Go(ctx context.Context, fn func()) error {
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
	}
}

// TryGo 尝试立即提交任务；队列暂时无法写入时返回 ErrRoutineQueueFull。
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

// Close 关闭 Routine，拒绝后续提交，并等待已接受的任务结束。
// 不要在提交到同一个 Routine 的任务中调用 Close。
func (r *Routine) Close() {
	r.closeOnce.Do(func() {
		state := r.markClosed()
		close(r.closed)
		r.closeTasksWhenReady(state)

		<-r.tasksClosed
		r.runnerWg.Wait()
	})
}

// Wait 阻塞直到当前尚未完成的任务执行完成。
// 与新的 Go 或 TryGo 并发调用时，不保证固定的任务边界。
// 不要在提交到同一个 Routine 的任务中调用 Wait。
func (r *Routine) Wait() {
	if r.pending() == 0 {
		return
	}

	r.waitMu.Lock()
	defer r.waitMu.Unlock()

	for r.pending() > 0 {
		r.waitCond.Wait()
	}
}

// OnPanic 设置任务 panic 的处理器。
func (r *Routine) OnPanic(handler PanicHandler) {
	if handler == nil {
		return
	}
	r.panicHandler.Store(handler)
}

// Closed 返回 Routine 是否已关闭。
func (r *Routine) Closed() bool {
	return r.state.Load()&routineClosedState != 0
}

// markClosed 设置关闭标记。成功的 CAS 是 Close 与提交者之间的准入边界。
func (r *Routine) markClosed() uint64 {
	for {
		state := r.state.Load()
		if state&routineClosedState != 0 {
			return state
		}
		next := state | routineClosedState
		if r.state.CompareAndSwap(state, next) {
			return next
		}
	}
}

// closeTasksWhenReady 仅在关闭后且所有提交者离开发送阶段时关闭 tasks。
// 这样可避免发送方与 close(tasks) 并发导致 panic，同时保留已入队任务的 drain 行为。
func (r *Routine) closeTasksWhenReady(state uint64) {
	if state&routineClosedState == 0 || state&routineSubmittingMask != 0 {
		return
	}

	r.tasksCloseOnce.Do(func() {
		close(r.tasks)
		close(r.tasksClosed)
	})
}

func (r *Routine) pending() uint64 {
	return r.state.Load() & routinePendingMask
}

func (r *Routine) submitters() uint64 {
	return (r.state.Load() & routineSubmittingMask) >> routineSubmittingShift
}

func (r *Routine) tasksAreClosed() bool {
	select {
	case <-r.tasksClosed:
		return true
	default:
		return false
	}
}

// startRunner 通过 CAS 占用一个 worker 配额，避免并发提交超过最大并发数。
func (r *Routine) startRunner() {
	for {
		runnerCount := r.runnerCount.Load()
		if runnerCount >= int64(r.maxConcurrency) {
			return
		}
		if r.runnerCount.CompareAndSwap(runnerCount, runnerCount+1) {
			r.runnerWg.Add(1)
			go r.runnerLoop()
			return
		}
	}
}

// beginSubmit 先将任务计入未完成和提交中，再启动 worker。
// 即使 Close 随后发生，也会等待这次已准入提交完成发送或取消处理。
func (r *Routine) beginSubmit() bool {
	for {
		state := r.state.Load()
		if state&routineClosedState != 0 {
			return false
		}
		if state&routinePendingMask == routinePendingMask || state&routineSubmittingMask == routineSubmittingMask {
			panic("routine task counter overflow")
		}

		next := state + routineSubmittingStep + 1
		if r.state.CompareAndSwap(state, next) {
			r.startRunner()
			return true
		}
	}
}

// endSubmit 标记发送阶段结束；最后一个提交者离开时可能触发任务通道关闭。
func (r *Routine) endSubmit() {
	for {
		state := r.state.Load()
		next := state - routineSubmittingStep
		if r.state.CompareAndSwap(state, next) {
			r.closeTasksWhenReady(next)
			return
		}
	}
}

// doneTask 标记任务不再需要等待。只有任务数归零时才获取通知锁唤醒 Wait，
// 避免每次任务完成都竞争互斥锁。
func (r *Routine) doneTask() {
	for {
		state := r.state.Load()
		next := state - 1
		if r.state.CompareAndSwap(state, next) {
			if next&routinePendingMask == 0 {
				r.waitMu.Lock()
				r.waitCond.Broadcast()
				r.waitMu.Unlock()
			}
			return
		}
	}
}

// runnerLoop 持续执行队列任务，空闲超时后回收 worker。
// 超时后会再次检查队列；若仍有提交者处于发送阶段则继续等待，避免其失去可用 worker。
func (r *Routine) runnerLoop() {
	var idleTimer *time.Timer
	defer func() {
		if idleTimer != nil {
			idleTimer.Stop()
		}
		r.finishRunner()
		r.runnerWg.Done()
	}()

	for {
		select {
		case fn, ok := <-r.tasks:
			if !ok {
				return
			}
			r.run(fn)
			continue
		default:
		}

		if idleTimer == nil {
			idleTimer = time.NewTimer(r.idleTimeout)
		} else {
			idleTimer.Reset(r.idleTimeout)
		}
		select {
		case fn, ok := <-r.tasks:
			if !idleTimer.Stop() {
				select {
				case <-idleTimer.C:
				default:
				}
			}
			if !ok {
				return
			}
			r.run(fn)
		case <-idleTimer.C:
			select {
			case fn, ok := <-r.tasks:
				if !ok {
					return
				}
				r.run(fn)
			default:
				if r.submitters() > 0 {
					continue
				}
				return
			}
		}
	}
}

// finishRunner 释放 worker 配额。提交者可能恰好在 worker 退出时进入发送阶段，
// 此时尝试补充 worker，避免队列中任务无人消费。
func (r *Routine) finishRunner() {
	r.runnerCount.Add(-1)
	if !r.tasksAreClosed() && r.submitters() > 0 {
		r.startRunner()
	}
}

// run 执行任务，并确保正常返回或 panic 时都会完成任务记账。
func (r *Routine) run(fn func()) {
	defer r.doneTask()
	defer func() {
		if x := recover(); x != nil {
			r.handlePanic(x)
		}
	}()
	fn()
}

// handlePanic 调用已注册的 panic 处理器；处理器本身的 panic 会被忽略。
// 未注册处理器时保留原始 panic 行为。
func (r *Routine) handlePanic(x any) {
	var handler, _ = r.panicHandler.Load().(PanicHandler)
	if handler == nil {
		panic(x)
	}

	defer func() {
		_ = recover()
	}()
	handler(x)
}
