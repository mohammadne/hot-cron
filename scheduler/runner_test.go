package scheduler_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/mohammadne/hot-cron/scheduler"
	"go.uber.org/zap"
)

func TestRunner(t *testing.T) {
	runner := scheduler.NewRunner(zap.NewNop())

	err := runner.Register(scheduler.JobConfig{
		Name:           "sync_users",
		Type:           scheduler.ScheduleTypeInterval,
		Interval:       2 * time.Second,
		RunImmediately: true,
	}, func() {
		time.Sleep(3 * time.Second)
		t.Log("sync_users executed")
	})
	if err != nil {
		t.Fatal(err)
	}

	// runner.Register(scheduler.JobConfig{
	// 	Name: "cleanup",
	// 	Type: scheduler.ScheduleCron,
	// 	Cron: "0 * * * * *", // every minute
	// }, func() {
	// 	t.Log("cleanup executed")
	// })

	// runner.Register(scheduler.JobConfig{
	// 	Name: "init_job",
	// 	Type: scheduler.ScheduleOnce,
	// }, func() {
	// 	t.Log("init job executed once")
	// })

	wg := sync.WaitGroup{}
	wg.Add(1)
	ctx, cf := context.WithTimeout(context.Background(), 5*time.Second)
	defer cf()

	runner.StartSync(ctx, &wg)

	wg.Wait()
}
