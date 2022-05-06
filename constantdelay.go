package cron

import "time"

// ConstantDelaySchedule represents a simple recurring duty cycle, e.g. "Every 5 minutes".
// It does not support jobs more frequent than once a second.
//
// ConstantDelaySchedule 表示一个简单的循环占空比，例如 “每 5 分钟”。
// 它不支持比每秒一次更频繁的作业。
type ConstantDelaySchedule struct {
	Delay time.Duration
}

// Every returns a crontab Schedule that activates once every duration.
// Delays of less than a second are not supported (will round up to 1 second).
// Any fields less than a Second are truncated.
//
// Every 返回一个 crontab Schedule，该计划在每个持续时间激活一次。
// 不支持小于一秒的延迟（将四舍五入到 1 秒）。
// 任何小于秒的字段都会被截断。
func Every(duration time.Duration) ConstantDelaySchedule {
	if duration < time.Second {
		duration = time.Second
	}
	return ConstantDelaySchedule{
		Delay: duration - time.Duration(duration.Nanoseconds())%time.Second,
	}
}

// Next returns the next time this should be run.
// This rounds so that the next activation time will be on the second.
//
// Next 返回下一次应该运行的时间。
// 这轮，以便下一个激活时间将在秒。
func (schedule ConstantDelaySchedule) Next(t time.Time) time.Time {
	return t.Add(schedule.Delay - time.Duration(t.Nanosecond())*time.Nanosecond)
}
