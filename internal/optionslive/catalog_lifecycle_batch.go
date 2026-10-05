package optionslive

import (
	"context"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/options"
)

type lifecycleBatchSink interface {
	WriteOptionsLifecycles(context.Context, []options.LifecycleObservation) error
}

// A full universe can close and reopen in one burst. Count and bytes are both
// bounded; a refused state still has a pending barrier and active HTTP repair.
func (s *catalogSupervisor) evidenceQueueLimit() int {
	return 2*s.cfg.catalogDefaults().MaxBooks + 512
}

func (s *catalogSupervisor) persistLifecycleLoop() {
	for {
		var first *catalogJob
		select {
		case <-s.ctx.Done():
			return
		case first = <-s.lifeJobs:
		}
		batch := []*catalogJob{first}
		timer := time.NewTimer(5 * time.Millisecond)
	collect:
		for len(batch) < 128 {
			select {
			case j := <-s.lifeJobs:
				batch = append(batch, j)
			case <-timer.C:
				break collect
			case <-s.ctx.Done():
				timer.Stop()
				return
			}
		}
		timer.Stop()
		ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
		err := s.persistLifecycles(ctx, batch)
		cancel()
		for _, j := range batch {
			select {
			case s.persisted <- catalogResult{j, err}:
			case <-s.ctx.Done():
				return
			}
		}
	}
}

func (s *catalogSupervisor) persistLifecycles(ctx context.Context, jobs []*catalogJob) error {
	observations := make([]options.LifecycleObservation, len(jobs))
	for n, j := range jobs {
		if err := j.life.Validate(); err != nil {
			return err
		}
		if err := archiveCatalog(s.cfg.EvidenceDir, j.raw, j.life.PayloadHash); err != nil {
			return err
		}
		observations[n] = *j.life
	}
	if sink, ok := s.sink.(lifecycleBatchSink); ok {
		return sink.WriteOptionsLifecycles(ctx, observations)
	}
	for _, o := range observations {
		if err := s.sink.WriteOptionsLifecycle(ctx, o); err != nil {
			return err
		}
	}
	return nil
}
