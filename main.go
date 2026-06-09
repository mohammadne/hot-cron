package main

import (
	"context"
	"os/signal"
	"sync"
	"syscall"

	"github.com/mohammadne/hot-cron/jobs"
	"github.com/mohammadne/hot-cron/scheduler"
	"github.com/mohammadne/hot-cron/services"
	"go.uber.org/zap"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger := zap.NewNop()

	scheduler := scheduler.NewRunner(logger)

	factorsService := services.NewFactors(logger)

	jobs.NewRetrieveFactors(ctx, logger, scheduler, factorsService)

	var wg sync.WaitGroup
	wg.Add(1)

	{ // servers
		go scheduler.StartSync(ctx, &wg)
	}

	<-ctx.Done()
	wg.Wait()

	logger.Info("Interruption signal received, gracefully shutdown the server")
}
