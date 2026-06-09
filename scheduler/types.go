package scheduler

import "time"

type ScheduleType string

const (
	ScheduleTypeInterval ScheduleType = "interval"
	ScheduleTypeCron     ScheduleType = "cron"
	ScheduleTypeOnce     ScheduleType = "once"
)

type JobConfig struct {
	Name string

	Type ScheduleType

	// interval mode
	Interval time.Duration

	// cron mode
	Cron string

	// once mode (delay after start)
	Delay time.Duration

	// optional
	RunImmediately bool
}

type JobFunc func()
