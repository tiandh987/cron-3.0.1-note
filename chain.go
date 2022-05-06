package cron

import (
	"fmt"
	"runtime"
	"sync"
	"time"
)

// JobWrapper decorates the given Job with some behavior.
// JobWrapper 用一些行为装饰给定的 Job。
type JobWrapper func(Job) Job

// Chain is a sequence of JobWrappers that decorates submitted jobs with
// cross-cutting behaviors like logging or synchronization.
//
// Chain 是一个 JobWrapper 序列，它使用诸如日志记录或同步等 横切行为 来装饰提交的作业。
type Chain struct {
	wrappers []JobWrapper
}

// NewChain returns a Chain consisting of the given JobWrappers.
// NewChain 返回一个由给定 JobWrappers 组成的 Chain
func NewChain(c ...JobWrapper) Chain {
	return Chain{c}
}

// Then decorates the given job with all JobWrappers in the chain.
//
// This:
//     NewChain(m1, m2, m3).Then(job)
// is equivalent to:
//     m1(m2(m3(job)))
//
// Then 用 chain 中的所有 JobWrapper 装饰给定的作业。
//
// This：
// 		NewChain(m1, m2, m3).Then(job)
// 相当于：
// 		m1(m2(m3(job)))
func (c Chain) Then(j Job) Job {
	for i := range c.wrappers {
		j = c.wrappers[len(c.wrappers)-i-1](j)
	}
	return j
}

// Recover panics in wrapped jobs and log them with the provided logger.
// Recover 包装作业中的 panics 并使用提供的记录器记录它们。
func Recover(logger Logger) JobWrapper {
	return func(j Job) Job {
		return FuncJob(func() {
			defer func() {
				if r := recover(); r != nil {
					const size = 64 << 10
					buf := make([]byte, size)
					buf = buf[:runtime.Stack(buf, false)]
					err, ok := r.(error)
					if !ok {
						err = fmt.Errorf("%v", r)
					}
					logger.Error(err, "panic", "stack", "...\n"+string(buf))
				}
			}()
			j.Run()
		})
	}
}

// DelayIfStillRunning serializes jobs, delaying subsequent runs until the
// previous one is complete. Jobs running after a delay of more than a minute
// have the delay logged at Info.
// DelayIfStillRunning 序列化作业，延迟后续运行，直到前一个完成。 延迟超过一分钟后运行的作业会在 Info 中记录延迟。
func DelayIfStillRunning(logger Logger) JobWrapper {
	return func(j Job) Job {
		var mu sync.Mutex
		return FuncJob(func() {
			start := time.Now()
			mu.Lock()
			defer mu.Unlock()
			if dur := time.Since(start); dur > time.Minute {
				logger.Info("delay", "duration", dur)
			}
			j.Run()
		})
	}
}

// SkipIfStillRunning skips an invocation of the Job if a previous invocation is
// still running. It logs skips to the given logger at Info level.
// 如果先前的调用仍在运行，SkipIfStillRunning 将跳过 Job 的调用。 它使用给定的 logger 记录 skip 到 Info 级别。
func SkipIfStillRunning(logger Logger) JobWrapper {
	return func(j Job) Job {
		var ch = make(chan struct{}, 1)
		ch <- struct{}{}
		return FuncJob(func() {
			select {
			case v := <-ch:
				j.Run()
				ch <- v
			default:
				logger.Info("skip")
			}
		})
	}
}
