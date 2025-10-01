package workers

import (
	"context"
	"time"
)

type IntervalWorker struct {
	interval     time.Duration
	workerCancel context.CancelFunc
}

func NewIntervalWorker(interval time.Duration) *IntervalWorker {
	return &IntervalWorker{
		interval: interval,
	}
}

func (w *IntervalWorker) Start(fn func()) {
	ctx, cancel := context.WithCancel(context.Background())
	w.workerCancel = cancel

	go func(interval time.Duration, ctx context.Context) {
		for {
			fn() // TODO recover

			select {
			case <-ctx.Done():
				return
			case <-time.After(interval):
				// just wait
			}
		}
	}(w.interval, ctx)
}

func (w *IntervalWorker) Stop() {
	if w.workerCancel != nil {
		w.workerCancel()
	}
}
