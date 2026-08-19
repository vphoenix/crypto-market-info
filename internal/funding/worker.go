package funding

import (
	"container/heap"
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
)

type ActualProvider interface {
	ActualFundingRate(context.Context, model.Instrument, time.Time) (model.FundingRate, bool, error)
}

type confirmationKey struct {
	instrumentID uint32
	fundingMS    int64
}

type confirmationTask struct {
	instrument model.Instrument
	target     time.Time
	attempt    int
	due        time.Time
	index      int
}

type taskHeap []*confirmationTask

func (h taskHeap) Len() int           { return len(h) }
func (h taskHeap) Less(i, j int) bool { return h[i].due.Before(h[j].due) }
func (h taskHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index, h[j].index = i, j
}
func (h *taskHeap) Push(value any) {
	task := value.(*confirmationTask)
	task.index = len(*h)
	*h = append(*h, task)
}
func (h *taskHeap) Pop() any {
	old := *h
	task := old[len(old)-1]
	old[len(old)-1] = nil
	task.index = -1
	*h = old[:len(old)-1]
	return task
}

type ConfirmationWorker struct {
	Exchange      string
	Provider      ActualProvider
	Sink          Sink
	MinInterval   time.Duration
	RetryDelays   []time.Duration
	QueueCapacity int
	Logger        *slog.Logger

	scheduleOnce sync.Once
	scheduleMu   sync.Mutex
	schedule     chan confirmationTask
	known        map[confirmationKey]time.Time
}

func (w *ConfirmationWorker) Schedule(ctx context.Context, instrument model.Instrument, fundingTime time.Time) error {
	if instrument.ID == 0 || instrument.MarketType != model.MarketPerpetual {
		return fmt.Errorf("funding confirmation requires a registered perpetual instrument")
	}
	if fundingTime.IsZero() || !fundingTime.Equal(time.UnixMilli(fundingTime.UnixMilli()).UTC()) {
		return fmt.Errorf("funding confirmation target must have millisecond precision")
	}
	w.scheduleOnce.Do(func() {
		capacity := w.QueueCapacity
		if capacity <= 0 {
			capacity = 4096
		}
		w.schedule = make(chan confirmationTask, capacity)
	})
	task := confirmationTask{instrument: instrument, target: fundingTime.UTC()}
	key := confirmationKey{instrumentID: instrument.ID, fundingMS: task.target.UnixMilli()}
	w.scheduleMu.Lock()
	if _, exists := w.known[key]; exists {
		w.scheduleMu.Unlock()
		return nil
	}
	if w.known == nil {
		w.known = make(map[confirmationKey]time.Time)
	}
	for previousKey, previousTarget := range w.known {
		if previousTarget.Before(task.target.Add(-48 * time.Hour)) {
			delete(w.known, previousKey)
		}
	}
	w.known[key] = task.target
	w.scheduleMu.Unlock()
	select {
	case w.schedule <- task:
		return nil
	case <-ctx.Done():
		w.scheduleMu.Lock()
		delete(w.known, key)
		w.scheduleMu.Unlock()
		return ctx.Err()
	}
}

func (w *ConfirmationWorker) Run(ctx context.Context) error {
	if err := w.defaults(); err != nil {
		return err
	}
	active := make(map[confirmationKey]*confirmationTask)
	finished := make(map[confirmationKey]time.Time)
	queue := &taskHeap{}
	heap.Init(queue)
	var lastRequest time.Time
	for {
		var timer *time.Timer
		var timerC <-chan time.Time
		if queue.Len() > 0 {
			delay := time.Until((*queue)[0].due)
			if delay < 0 {
				delay = 0
			}
			timer = time.NewTimer(delay)
			timerC = timer.C
		}
		select {
		case <-ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			return nil
		case incoming := <-w.schedule:
			if timer != nil {
				timer.Stop()
			}
			key := confirmationKey{instrumentID: incoming.instrument.ID, fundingMS: incoming.target.UnixMilli()}
			if _, exists := active[key]; exists {
				continue
			}
			if _, done := finished[key]; done {
				continue
			}
			incoming.attempt, incoming.due = initialConfirmationAttempt(incoming.target, time.Now().UTC(), w.RetryDelays)
			copy := incoming
			active[key] = &copy
			heap.Push(queue, &copy)
			w.pruneFinished(finished, time.Now().UTC())
		case <-timerC:
			task := heap.Pop(queue).(*confirmationTask)
			key := confirmationKey{instrumentID: task.instrument.ID, fundingMS: task.target.UnixMilli()}
			if remaining := w.MinInterval - time.Since(lastRequest); !lastRequest.IsZero() && remaining > 0 {
				if !wait(ctx, remaining) {
					return nil
				}
			}
			lastRequest = time.Now()
			rate, found, err := w.Provider.ActualFundingRate(ctx, task.instrument, task.target)
			if ctx.Err() != nil {
				return nil
			}
			if err == nil && found {
				err = validateActual(rate, task.instrument, task.target)
			}
			if err == nil && found {
				err = w.Sink.UpsertFundingRate(ctx, rate)
			}
			if err == nil && found {
				delete(active, key)
				finished[key] = time.Now().UTC()
				continue
			}
			if err != nil {
				w.Logger.Error("actual funding confirmation failed", "exchange", w.Exchange, "instrument_id", task.instrument.ID, "funding_time", task.target, "attempt", task.attempt+1, "error", err)
			}
			task.attempt++
			if task.attempt >= len(w.RetryDelays) {
				delete(active, key)
				finished[key] = time.Now().UTC()
				continue
			}
			task.due = task.target.Add(w.RetryDelays[task.attempt])
			heap.Push(queue, task)
		}
	}
}

func initialConfirmationAttempt(target, now time.Time, delays []time.Duration) (int, time.Time) {
	attempt := 0
	due := target.Add(delays[0])
	for index, delay := range delays {
		planned := target.Add(delay)
		if planned.After(now) {
			break
		}
		attempt = index
		due = now
	}
	return attempt, due
}

func (w *ConfirmationWorker) defaults() error {
	if w.Provider == nil || w.Sink == nil || w.Exchange == "" {
		return fmt.Errorf("funding confirmation worker requires exchange, provider and sink")
	}
	if w.MinInterval <= 0 {
		w.MinInterval = time.Second
	}
	if len(w.RetryDelays) == 0 {
		w.RetryDelays = []time.Duration{2 * time.Minute, 5 * time.Minute, 15 * time.Minute, 60 * time.Minute}
	}
	for index, delay := range w.RetryDelays {
		if delay < 0 || (index > 0 && delay <= w.RetryDelays[index-1]) {
			return fmt.Errorf("funding confirmation retry delays must be non-negative and increasing")
		}
	}
	w.scheduleOnce.Do(func() {
		capacity := w.QueueCapacity
		if capacity <= 0 {
			capacity = 4096
		}
		w.schedule = make(chan confirmationTask, capacity)
	})
	if w.Logger == nil {
		w.Logger = slog.Default()
	}
	return nil
}

func (w *ConfirmationWorker) pruneFinished(finished map[confirmationKey]time.Time, now time.Time) {
	for key, completedAt := range finished {
		if completedAt.Before(now.Add(-48 * time.Hour)) {
			delete(finished, key)
		}
	}
}

func validateActual(rate model.FundingRate, instrument model.Instrument, target time.Time) error {
	if err := rate.Validate(); err != nil {
		return err
	}
	if !rate.IsActual || rate.InstrumentID != instrument.ID || rate.FundingTime.UnixMilli() != target.UnixMilli() {
		return fmt.Errorf("actual funding response does not match confirmation task")
	}
	wantHour := target.UTC().Truncate(time.Hour)
	if !rate.HourTime.Equal(wantHour) {
		return fmt.Errorf("actual funding hour_time does not match target")
	}
	return nil
}
