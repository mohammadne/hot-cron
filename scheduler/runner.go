package scheduler

import (
	"context"
	"sync"
	"time"

	"github.com/go-co-op/gocron/v2"
	"go.uber.org/zap"
)

type Runner struct {
	logger    *zap.Logger
	scheduler gocron.Scheduler
	guard     *guard
	jobs      map[string]JobFunc
}

type Registerer interface {
	Register(config JobConfig, fn JobFunc) error
}

func NewRunner(logger *zap.Logger) *Runner {
	s, err := gocron.NewScheduler()
	if err != nil {
		logger.Fatal("error creating gocron scheduler", zap.Error(err))
	}

	return &Runner{
		logger:    logger,
		scheduler: s,
		guard:     newGuard(),
		jobs:      make(map[string]JobFunc),
	}
}

func (r *Runner) Register(config JobConfig, fn JobFunc) error {
	if _, exists := r.jobs[config.Name]; exists {
		return nil // or return error if you want strict behavior
	}

	r.jobs[config.Name] = fn

	wrapped := func(jobName string) {
		if !r.guard.tryLock(config.Name) {
			r.logger.Warn("job skipped (already running)", zap.String("job-name", jobName))
			return
		}

		defer r.guard.unlock(jobName)

		r.logger.Info("job started", zap.String("job-name", jobName))
		fn()
		r.logger.Info("job finished", zap.String("job-name", jobName))
	}

	var job gocron.Job
	var err error

	switch config.Type {

	case ScheduleTypeInterval:
		job, err = r.scheduler.NewJob(
			gocron.DurationJob(config.Interval),
			gocron.NewTask(wrapped, config.Name),
		)
		// ...gocron.JobOption
		// job, err = r.scheduler.Every(config.Interval).Do(wrapped)

	case ScheduleTypeCron:
		job, err = r.scheduler.NewJob(
			gocron.CronJob(config.Cron, true),
			gocron.NewTask(wrapped, config.Name),
		)
		// job, err = r.scheduler.Cron(config.Cron).Do(wrapped)

	case ScheduleTypeOnce:
		job, err = r.scheduler.NewJob(
			gocron.OneTimeJob(gocron.OneTimeJobStartDateTime(time.Now().Add(config.Delay))),
			gocron.NewTask(wrapped, config.Name),
		)
		// job, err = r.scheduler.Every(1).LimitRunsTo(1).StartAt(time.Now()).Do(wrapped)

	default:
		return nil
	}

	if err != nil {
		return err
	}

	if config.RunImmediately && config.Type != ScheduleTypeOnce {
		go wrapped(config.Name)
	}

	_ = job
	return nil
}

func (r *Runner) StartSync(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	r.scheduler.Start()
	<-ctx.Done() // wait till signal for termination
	r.scheduler.Shutdown()
}
