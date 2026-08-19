package sampler

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
)

type BookSource interface {
	Snapshot(int) (model.BookSnapshot, bool)
}
type MinuteSink interface {
	WriteMinute(context.Context, model.MinuteBatch) error
}

type Source struct {
	InstrumentID uint32
	Book         BookSource
}

type Engine struct {
	sources []Source
	sink    MinuteSink
	logger  *slog.Logger
	queue   chan model.MinuteBatch
	mu      sync.Mutex
	minute  time.Time
	buffers map[uint32]*MinuteBuffer
	started bool
}

func NewEngine(sources []Source, sink MinuteSink, queueCapacity int, logger *slog.Logger) (*Engine, error) {
	if sink == nil || len(sources) == 0 {
		return nil, fmt.Errorf("sampler requires sources and a minute sink")
	}
	if queueCapacity <= 0 {
		queueCapacity = len(sources) * 3
	}
	if logger == nil {
		logger = slog.Default()
	}
	seen := make(map[uint32]struct{}, len(sources))
	for _, source := range sources {
		if source.InstrumentID == 0 || source.Book == nil {
			return nil, fmt.Errorf("sampler source is invalid")
		}
		if _, ok := seen[source.InstrumentID]; ok {
			return nil, fmt.Errorf("duplicate sampler instrument %d", source.InstrumentID)
		}
		seen[source.InstrumentID] = struct{}{}
	}
	sources = append([]Source(nil), sources...)
	sort.Slice(sources, func(i, j int) bool { return sources[i].InstrumentID < sources[j].InstrumentID })
	return &Engine{sources: sources, sink: sink, logger: logger, queue: make(chan model.MinuteBatch, queueCapacity), buffers: make(map[uint32]*MinuteBuffer)}, nil
}

func (e *Engine) Run(ctx context.Context) error {
	e.mu.Lock()
	if e.started {
		e.mu.Unlock()
		return fmt.Errorf("sampler already started")
	}
	e.started = true
	e.mu.Unlock()
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for batch := range e.queue {
			writeCtx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			err := e.sink.WriteMinute(writeCtx, batch)
			cancel()
			if err != nil {
				e.logger.Error("minute batch dropped after writer retries", "instrument_id", batch.Minute.InstrumentID, "minute", batch.Minute.MinuteTime, "error", err)
			}
		}
	}()
	defer func() { close(e.queue); <-writerDone }()
	for {
		now := time.Now().UTC()
		next := now.Truncate(time.Second).Add(time.Second)
		timer := time.NewTimer(time.Until(next))
		select {
		case <-ctx.Done():
			timer.Stop()
			e.flushIfCompleted(time.Now().UTC())
			return nil
		case tick := <-timer.C:
			if err := e.SampleAt(tick.UTC().Truncate(time.Second)); err != nil {
				e.logger.Error("second sample failed", "second", tick.UTC(), "error", err)
			}
		}
	}
}

func (e *Engine) flushIfCompleted(now time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.minute.IsZero() && now.UTC().Truncate(time.Minute).After(e.minute) {
		e.enqueueCompletedLocked()
		e.minute = time.Time{}
		e.buffers = make(map[uint32]*MinuteBuffer)
	}
}

// SampleAt is public to permit deterministic clock-driven tests.
func (e *Engine) SampleAt(at time.Time) error {
	at = at.UTC().Truncate(time.Second)
	e.mu.Lock()
	defer e.mu.Unlock()
	minute := at.Truncate(time.Minute)
	if !e.minute.IsZero() && minute.Before(e.minute) {
		return fmt.Errorf("sample clock moved backwards")
	}
	if e.minute.IsZero() || minute.After(e.minute) {
		if !e.minute.IsZero() {
			e.enqueueCompletedLocked()
		}
		e.minute = minute
		e.buffers = make(map[uint32]*MinuteBuffer, len(e.sources))
		for _, source := range e.sources {
			buffer, err := NewMinuteBuffer(source.InstrumentID, minute)
			if err != nil {
				return err
			}
			e.buffers[source.InstrumentID] = buffer
		}
	}
	for _, source := range e.sources {
		snapshot, valid := source.Book.Snapshot(model.BookDepth)
		if err := e.buffers[source.InstrumentID].Sample(at, snapshot, valid); err != nil {
			return fmt.Errorf("instrument %d: %w", source.InstrumentID, err)
		}
	}
	return nil
}

func (e *Engine) enqueueCompletedLocked() {
	for _, source := range e.sources {
		batch, ok := e.buffers[source.InstrumentID].Batch()
		if !ok {
			continue
		}
		select {
		case e.queue <- batch:
		default:
			e.logger.Error("minute writer queue full; dropping minute", "instrument_id", batch.Minute.InstrumentID, "minute", batch.Minute.MinuteTime)
		}
	}
}
