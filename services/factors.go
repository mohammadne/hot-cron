package services

import (
	"context"

	"go.uber.org/zap"
)

type Factors struct {
	logger *zap.Logger
}

func NewFactors(logger *zap.Logger) *Factors {
	return &Factors{
		logger: logger,
	}
}

// if resp.StatusCode == http.StatusNotFound {
// 	log.Printf("[worker %d] id=%d NOT FOUND", workerID, id)
// 	return
// }

// // retry on 5xx
// if resp.StatusCode >= 500 {
// 	err = errors.New("server error")
// 	rf.sleep(attempt)
// 	continue
// }

// other statuses → don't retry

func (f *Factors) RetrieveFactor(ctx context.Context, id uint64, lastTry bool) (found bool, err error) {
	// todo: if status 404, return no error

	// todo: if status ok 200
	// todo: if non sell-factor, return no error
	// todo: if sell-factor, store and return no error

	// todo: other statuses on lastTry, store report

	return true, nil
}

func (f *Factors) VerifyFactor(ctx context.Context, id uint64) {

}
