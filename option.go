package cron

import (
	"time"
)

// Option represents a modification to the default behavior of a Cron.
// Option 表示对 Cron 的默认行为的修改。
type Option func(*Cron)

// WithLocation overrides the timezone of the cron instance.
// WithLocation 覆盖 cron 实例的时区。
func WithLocation(loc *time.Location) Option {
	return func(c *Cron) {
		c.location = loc
	}
}

// WithSeconds overrides the parser used for interpreting job schedules to
// include a seconds field as the first one.
// WithSeconds 会覆盖用于解释作业计划的 parser，以包含秒字段作为第一个字段。
func WithSeconds() Option {
	return WithParser(NewParser(
		Second | Minute | Hour | Dom | Month | Dow | Descriptor,
	))
}

// WithParser overrides the parser used for interpreting job schedules.
// WithParser 覆盖用于解释作业计划的 parser。
func WithParser(p ScheduleParser) Option {
	return func(c *Cron) {
		c.parser = p
	}
}

// WithChain specifies Job wrappers to apply to all jobs added to this cron.
// Refer to the Chain* functions in this package for provided wrappers.
//
// WithChain 指定作业包装器, 应用于添加到此 cron 的所有作业。
// 有关提供的包装器，请参阅此包中的 Chain* 函数。
func WithChain(wrappers ...JobWrapper) Option {
	return func(c *Cron) {
		c.chain = NewChain(wrappers...)
	}
}

// WithLogger uses the provided logger.
// WithLogger 使用提供的 logger。
func WithLogger(logger Logger) Option {
	return func(c *Cron) {
		c.logger = logger
	}
}
