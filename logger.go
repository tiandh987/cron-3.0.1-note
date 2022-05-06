package cron

import (
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"
)

// DefaultLogger is used by Cron if none is specified.
// 如果未指定，则 Cron 使用 DefaultLogger。
var DefaultLogger Logger = PrintfLogger(log.New(os.Stdout, "cron: ", log.LstdFlags))

// DiscardLogger can be used by callers to discard all log messages.
// DiscardLogger 可以被调用者用来丢弃所有的日志消息。
var DiscardLogger Logger = PrintfLogger(log.New(ioutil.Discard, "", 0))

// Logger is the interface used in this package for logging, so that any backend
// can be plugged in. It is a subset of the github.com/go-logr/logr interface.
// Logger 是这个包中用来记录日志的接口，这样任何后端都可以插入。它是 github.com/go-logr/logr 接口的子集。
type Logger interface {
	// Info logs routine messages about cron's operation.
	// Info 记录有关 cron 操作的例行消息。
	Info(msg string, keysAndValues ...interface{})
	// Error logs an error condition.
	// Error 记录错误情况。
	Error(err error, msg string, keysAndValues ...interface{})
}

// PrintfLogger wraps a Printf-based logger (such as the standard library "log")
// into an implementation of the Logger interface which logs errors only.
// PrintfLogger 将基于 Printf 的 logger（例如标准库“log”）包装到 "仅记录错误" 的 Logger 接口的实现中。
func PrintfLogger(l interface{ Printf(string, ...interface{}) }) Logger {
	return printfLogger{l, false}
}

// VerbosePrintfLogger wraps a Printf-based logger (such as the standard library
// "log") into an implementation of the Logger interface which logs everything.
// VerbosePrintfLogger 将基于 Printf 的 logger（例如标准库“log”）包装到 "记录所有内容" 的 Logger 接口的实现中。
func VerbosePrintfLogger(l interface{ Printf(string, ...interface{}) }) Logger {
	return printfLogger{l, true}
}

type printfLogger struct {
	logger  interface{ Printf(string, ...interface{}) }
	logInfo bool
}

func (pl printfLogger) Info(msg string, keysAndValues ...interface{}) {
	if pl.logInfo {
		keysAndValues = formatTimes(keysAndValues)
		pl.logger.Printf(
			formatString(len(keysAndValues)),
			append([]interface{}{msg}, keysAndValues...)...)
	}
}

func (pl printfLogger) Error(err error, msg string, keysAndValues ...interface{}) {
	keysAndValues = formatTimes(keysAndValues)
	pl.logger.Printf(
		formatString(len(keysAndValues)+2),
		append([]interface{}{msg, "error", err}, keysAndValues...)...)
}

// formatString returns a logfmt-like format string for the number of
// key/values.
// formatString 为键/值的数量返回一个类似于 logfmt 的格式字符串。
func formatString(numKeysAndValues int) string {
	var sb strings.Builder
	sb.WriteString("%s")
	if numKeysAndValues > 0 {
		sb.WriteString(", ")
	}
	for i := 0; i < numKeysAndValues/2; i++ {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("%v=%v")
	}
	return sb.String()
}

// formatTimes formats any time.Time values as RFC3339.
// formatTimes 将任何 time.Time 值格式化为 RFC3339。
func formatTimes(keysAndValues []interface{}) []interface{} {
	var formattedArgs []interface{}
	for _, arg := range keysAndValues {
		if t, ok := arg.(time.Time); ok {
			arg = t.Format(time.RFC3339)
		}
		formattedArgs = append(formattedArgs, arg)
	}
	return formattedArgs
}
