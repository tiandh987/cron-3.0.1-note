package cron

import (
	"context"
	"sort"
	"sync"
	"time"
)

// Cron keeps track of any number of entries, invoking the associated func as
// specified by the schedule. It may be started, stopped, and the entries may
// be inspected while running.
// Cron 跟踪任意数量的 entries，调用 调度指定的关联函数。 它可以启动、停止，并且可以在运行时检查条目。
type Cron struct {
	// 存放添加到 Cron 的所有 Entry
	entries   []*Entry
	chain     Chain
	stop      chan struct{}

	// 添加 Entry 时，如果 running 字段为 true，则将要添加的 Entry 放到这个 chan 中。
	add       chan *Entry
	remove    chan EntryID
	snapshot  chan chan []Entry

	// 默认值: false, 通过 runningMu 锁进行保护
	// 在调用 Start() 函数后, 会将 running 置为 true
	running   bool

	// 日志记录, 默认值: DefaultLogger
	logger    Logger

	runningMu sync.Mutex

	// 时区, 默认值 time.Local
	location  *time.Location

	// 默认: standardParser
	parser    ScheduleParser
	nextID    EntryID

	// 每启动一个 goroutine 运行 Job 就加 1
	// 在调用 Stop() 函数停止调度时,会等待正在运行的 goroutine 运行结束
	jobWaiter sync.WaitGroup
}

// ScheduleParser is an interface for schedule spec parsers that return a Schedule
// ScheduleParser 是一个解析调度 spec 的接口, 返回 Schedule
type ScheduleParser interface {
	Parse(spec string) (Schedule, error)
}

// Job is an interface for submitted cron jobs.
// Job 是提交的 cron 作业的接口。
type Job interface {
	Run()
}

// Schedule describes a job's duty cycle.
// Schedule 描述了作业的工作周期。
type Schedule interface {
	// Next returns the next activation time, later than the given time.
	// Next is invoked initially, and then each time the job is run.
	//
	// Next 返回下一个激活时间，晚于给定时间。
	// Next 最初被调用，然后在每次作业运行时调用。
	Next(time.Time) time.Time
}

// EntryID identifies an entry within a Cron instance
// EntryID 标识 Cron 实例中的条目
type EntryID int

// Entry consists of a schedule and the func to execute on that schedule.
type Entry struct {
	// ID is the cron-assigned ID of this entry, which may be used to look up a
	// snapshot or remove it.
	// ID 是 cron 分配给该 entyr 的 ID，可用于查找快照或删除它。
	ID EntryID

	// Schedule on which this job should be run.
	Schedule Schedule

	// Next time the job will run, or the zero time if Cron has not been
	// started or this entry's schedule is unsatisfiable
	// 下次作业将运行时间，或者如果 Cron 尚未启动或此条目的计划无法满足，则为零时间
	Next time.Time

	// Prev is the last time this job was run, or the zero time if never.
	// Prev 是上次运行此作业的时间，如果从未运行，则为零时间。
	Prev time.Time

	// WrappedJob is the thing to run when the Schedule is activated.
	// WrappedJob 是 Schedule 被激活时运行的东西。
	//
	// 被 Chain 包装后的 Job
	WrappedJob Job

	// Job is the thing that was submitted to cron.
	// It is kept around so that user code that needs to get at the job later,
	// e.g. via Entries() can do so.
	// Job 是提交给 cron 的东西。
	// 它被保留，以便稍后需要完成工作的用户代码，
	// 例如 通过 Entries() 可以这样做。
	//
	// 原始传入的Job
	Job Job
}

// Valid returns true if this is not the zero entry.
func (e Entry) Valid() bool { return e.ID != 0 }

// byTime is a wrapper for sorting the entry array by time
// (with zero time at the end).
//
// byTime 是一个包装器，用于按时间对条目数组进行排序
//（最后时间为零）。
type byTime []*Entry

func (s byTime) Len() int      { return len(s) }
func (s byTime) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s byTime) Less(i, j int) bool {
	// Two zero times should return false.
	// Otherwise, zero is "greater" than any other time.
	// (To sort it at the end of the list.)
	if s[i].Next.IsZero() {
		return false
	}
	if s[j].Next.IsZero() {
		return true
	}
	return s[i].Next.Before(s[j].Next)
}

// New returns a new Cron job runner, modified by the given options.
//
// Available Settings
//
//   Time Zone
//     Description: The time zone in which schedules are interpreted
//     Default:     time.Local
//
//   Parser
//     Description: Parser converts cron spec strings into cron.Schedules.
//     Default:     Accepts this spec: https://en.wikipedia.org/wiki/Cron
//
//   Chain
//     Description: Wrap submitted jobs to customize behavior.
//     Default:     A chain that recovers panics and logs them to stderr.
//
// See "cron.With*" to modify the default behavior.
//
// New 返回一个新的 Cron 作业运行器，由给定选项修改。
// 可用设置为：
//     Time Zone
//		   说明:   解释时间表的时区
//		   默认值: time.Local
// 	   Parser
//         说明：Parser 将 cron spec 字符串转换为 cron.Schedules。
//         默认值：接受此规范：https://en.wikipedia.org/wiki/Cron
//
//     Chain
// 		   说明：包装提交的作业以自定义行为。
func New(opts ...Option) *Cron {
	c := &Cron{
		entries:   nil,
		chain:     NewChain(),
		add:       make(chan *Entry),
		stop:      make(chan struct{}),
		snapshot:  make(chan chan []Entry),
		remove:    make(chan EntryID),
		running:   false,
		runningMu: sync.Mutex{},
		logger:    DefaultLogger,
		location:  time.Local,
		parser:    standardParser,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// FuncJob is a wrapper that turns a func() into a cron.Job
// FuncJob 是一个将 func() 转换为 cron.Job 的包装器
type FuncJob func()

func (f FuncJob) Run() { f() }

// AddFunc adds a func to the Cron to be run on the given schedule.
// The spec is parsed using the time zone of this Cron instance as the default.
// An opaque ID is returned that can be used to later remove it.
//
// AddFunc 将 func 添加到 Cron 以按照给定的 schedule 运行。
// 使用此 Cron 实例的时区作为默认值解析 spec。
// 返回一个 opaque ID，以后可以使用它来移除它。
func (c *Cron) AddFunc(spec string, cmd func()) (EntryID, error) {
	// FuncJob(cmd) 类型转换
	return c.AddJob(spec, FuncJob(cmd))
}

// AddJob adds a Job to the Cron to be run on the given schedule.
// The spec is parsed using the time zone of this Cron instance as the default.
// An opaque ID is returned that can be used to later remove it.
//
// AddJob 将 Job 添加到 Cron 以按照给定的 schedule 运行。
// 使用此 Cron 实例的时区作为默认值解析 spec。
// 返回一个 opaque ID，以后可以使用它来移除它。
func (c *Cron) AddJob(spec string, cmd Job) (EntryID, error) {
	schedule, err := c.parser.Parse(spec)
	if err != nil {
		return 0, err
	}
	return c.Schedule(schedule, cmd), nil
}

// Schedule adds a Job to the Cron to be run on the given schedule.
// The job is wrapped with the configured Chain.
//
// Schedule 将 Job 添加到 Cron 以按给定的 Schedule 运行。
func (c *Cron) Schedule(schedule Schedule, cmd Job) EntryID {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	c.nextID++
	entry := &Entry{
		ID:         c.nextID,
		Schedule:   schedule,
		WrappedJob: c.chain.Then(cmd),
		Job:        cmd,
	}
	if !c.running {
		c.entries = append(c.entries, entry)
	} else {
		c.add <- entry
	}
	return entry.ID
}

// Entries returns a snapshot of the cron entries.
func (c *Cron) Entries() []Entry {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
		replyChan := make(chan []Entry, 1)
		c.snapshot <- replyChan
		return <-replyChan
	}
	return c.entrySnapshot()
}

// Location gets the time zone location
func (c *Cron) Location() *time.Location {
	return c.location
}

// Entry returns a snapshot of the given entry, or nil if it couldn't be found.
func (c *Cron) Entry(id EntryID) Entry {
	for _, entry := range c.Entries() {
		if id == entry.ID {
			return entry
		}
	}
	return Entry{}
}

// Remove an entry from being run in the future.
func (c *Cron) Remove(id EntryID) {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
		c.remove <- id
	} else {
		c.removeEntry(id)
	}
}

// Start the cron scheduler in its own goroutine, or no-op if already started.
// 在它自己的 goroutine 中启动 cron 调度程序，如果已经启动则无操作。
func (c *Cron) Start() {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
		return
	}
	c.running = true
	go c.run()
}

// Run the cron scheduler, or no-op if already running.
func (c *Cron) Run() {
	c.runningMu.Lock()
	if c.running {
		c.runningMu.Unlock()
		return
	}
	c.running = true
	c.runningMu.Unlock()
	c.run()
}

// run the scheduler.. this is private just due to the need to synchronize
// access to the 'running' state variable.
//
// 运行调度程序.. 这是私有的，只是因为需要同步对 “running” 状态变量的访问。
func (c *Cron) run() {
	c.logger.Info("start")

	// Figure out the next activation times for each entry.
	// 计算每个 entry 的下一个激活时间。
	now := c.now()
	for _, entry := range c.entries {
		entry.Next = entry.Schedule.Next(now)
		c.logger.Info("schedule", "now", now, "entry", entry.ID, "next", entry.Next)
	}

	for {
		// Determine the next entry to run.
		// 按 Next 时间对 entries 进行排序, 决定下一个要运行的 entry.
		sort.Sort(byTime(c.entries))

		var timer *time.Timer
		if len(c.entries) == 0 || c.entries[0].Next.IsZero() {
			// If there are no entries yet, just sleep - it still handles new entries
			// and stop requests.
			timer = time.NewTimer(100000 * time.Hour)
		} else {
			// 定时器 = entry 将要执行时间 - now
			timer = time.NewTimer(c.entries[0].Next.Sub(now))
		}

		for {
			select {
			case now = <-timer.C:
				// 返回所在时区的时间
				now = now.In(c.location)
				c.logger.Info("wake", "now", now)

				// Run every entry whose next time was less than now
				for _, e := range c.entries {
					if e.Next.After(now) || e.Next.IsZero() {
						break
					}
					c.startJob(e.WrappedJob) // 启动一个 goroutine 运行 Job
					e.Prev = e.Next // 更新上次执行时间
					e.Next = e.Schedule.Next(now) // 更新下次执行时间
					c.logger.Info("run", "now", now, "entry", e.ID, "next", e.Next)
				}

			// 有新增加的 entry,可以动态添加
			case newEntry := <-c.add:
				timer.Stop()
				now = c.now()
				newEntry.Next = newEntry.Schedule.Next(now)
				c.entries = append(c.entries, newEntry)
				c.logger.Info("added", "now", now, "entry", newEntry.ID, "next", newEntry.Next)

			case replyChan := <-c.snapshot:
				replyChan <- c.entrySnapshot()
				continue

			// 停止定时任务调度, 不再启动 goroutine 运行 job
			case <-c.stop:
				timer.Stop()
				c.logger.Info("stop")
				return

			//	有删除的 entry,可以动态删除
			case id := <-c.remove:
				timer.Stop()
				now = c.now()
				c.removeEntry(id)
				c.logger.Info("removed", "entry", id)
			}

			break
		}
	}
}

// startJob runs the given job in a new goroutine.
// startJob 在新的 goroutine 中运行给定的作业。
func (c *Cron) startJob(j Job) {
	c.jobWaiter.Add(1)
	go func() {
		defer c.jobWaiter.Done()
		j.Run()
	}()
}

// now returns current time in c location
func (c *Cron) now() time.Time {
	return time.Now().In(c.location)
}

// Stop stops the cron scheduler if it is running; otherwise it does nothing.
// A context is returned so the caller can wait for running jobs to complete.
func (c *Cron) Stop() context.Context {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
		c.stop <- struct{}{}
		c.running = false
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		c.jobWaiter.Wait()
		cancel()
	}()
	return ctx
}

// entrySnapshot returns a copy of the current cron entry list.
func (c *Cron) entrySnapshot() []Entry {
	var entries = make([]Entry, len(c.entries))
	for i, e := range c.entries {
		entries[i] = *e
	}
	return entries
}

func (c *Cron) removeEntry(id EntryID) {
	var entries []*Entry
	for _, e := range c.entries {
		if e.ID != id {
			entries = append(entries, e)
		}
	}
	c.entries = entries
}
