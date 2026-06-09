package jobs

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mohammadne/hot-cron/scheduler"
	"github.com/mohammadne/hot-cron/services"
	"go.uber.org/zap"
)

type RetrieveFactors struct {
	config *RetrieveFactorsConfig
	logger *zap.Logger

	factorsService *services.Factors

	//
	sourceID     atomic.Uint64
	lastFoundID  atomic.Uint64
	maxCheckedID atomic.Uint64
}

type RetrieveFactorsConfig struct {
	// Interval between jobs
	Interval time.Duration `required:"true"  split_words:"true" json:"interval"`

	// number of workers
	Concurrency int `required:"true"  split_words:"true" json:"concurrency"`

	//
	RetryCount int `required:"true"  split_words:"true" json:"retry_count"`

	//
	MaxConsecutiveNotFounds uint64 `required:"true"  split_words:"true" json:"max_consecutive_not_founds"`

	//
	Backoff time.Duration `required:"true"  split_words:"true" json:"backoff"`
}

func NewRetrieveFactors(ctx context.Context, logger *zap.Logger,
	r scheduler.Registerer, factorsService *services.Factors,
) {
	rf := &RetrieveFactors{
		config:         &RetrieveFactorsConfig{}, // pass from params
		logger:         logger.Named("jobs_retrieve_factors"),
		factorsService: factorsService,
	}

	startFrom := uint64(1) // or from DB / config

	rf.sourceID.Store(startFrom - 1)
	rf.lastFoundID.Store(startFrom)
	rf.maxCheckedID.Store(startFrom)

	err := r.Register(scheduler.JobConfig{
		Name:           "retrieve_factors",
		Type:           scheduler.ScheduleTypeInterval,
		Interval:       rf.config.Interval,
		RunImmediately: true,
	}, func() {
		var wg sync.WaitGroup

		runCtx, cancel := context.WithTimeout(ctx, rf.config.Interval-time.Second)
		defer cancel()

		for i := 0; i < rf.config.Concurrency; i++ {
			wg.Add(1)

			go func(workerID int) {
				defer wg.Done()

				for {
					select {
					case <-runCtx.Done():
						return
					default:
					}

					id := rf.sourceID.Add(1)

					if rf.process(runCtx, workerID, id) {
						cancel()
						return
					}

					// optional throttle to avoid tight loop
					select {
					case <-time.After(5 * time.Millisecond):
					case <-runCtx.Done():
						return
					}
				}
			}(i)
		}

		wg.Wait()
	})

	if err != nil {
		rf.logger.Fatal("error register retrieve-factors job", zap.Error(err))
	}
}

func (rf *RetrieveFactors) process(ctx context.Context, workerID int, id uint64) (stop bool) {
	var err error

	for attempt := 0; attempt < rf.config.RetryCount; attempt++ {
		found, err := rf.factorsService.RetrieveFactor(ctx, id, attempt+1 == rf.config.RetryCount)
		if err != nil {
			backoff := time.Duration(math.Pow(2, float64(attempt))) * rf.config.Backoff

			// cancel-aware backoff
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return true
			}
			continue
		}

		// SUCCESS
		if found {
			rf.updateLastFound(id)
			return false
		}

		// NOT FOUND
		rf.updateMaxChecked(id)

		// stop condition (heuristic but safe)
		return rf.maxCheckedID.Load()-rf.lastFoundID.Load() >= rf.config.MaxConsecutiveNotFounds
	}

	rf.logger.Error("failed after retries",
		zap.Int("worker", workerID),
		zap.Uint64("id", id),
		zap.Error(err))
	return false
}

// CAS-based max update
func (rf *RetrieveFactors) updateMaxChecked(id uint64) {
	for {
		current := rf.maxCheckedID.Load()
		if id <= current {
			return
		}
		if rf.maxCheckedID.CompareAndSwap(current, id) {
			return
		}
	}
}

// CAS-based max update
func (rf *RetrieveFactors) updateLastFound(id uint64) {
	for {
		current := rf.lastFoundID.Load()
		if id <= current {
			return
		}
		if rf.lastFoundID.CompareAndSwap(current, id) {
			return
		}
	}
}
