package across

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
)

type probePriorityKey struct{}
type discoveryQuotaKey struct{}

func probeContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, probePriorityKey{}, true)
}

// SourceQuota is shared by all cooperating processes using the same RPC host.
// The small binary file contains pacing/cooldown timestamps, never URLs or keys.
// It is operational state, not chain evidence. HTTP launch times are archived.
type SourceQuota struct {
	Path     string
	Interval time.Duration
	Burst    int
}

func NewSourceQuota(dir, host string, interval time.Duration, burst int) (*SourceQuota, error) {
	if interval <= 0 || burst < 1 || burst > 10 {
		return nil, errors.New("invalid_source_quota")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	return &SourceQuota{filepath.Join(dir, Hex(ID(host))[2:]+".quota"), interval, burst}, nil
}
func sleepContext(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
func (q *SourceQuota) state(ctx context.Context, update func(*[4]int64) (time.Duration, error)) (time.Duration, error) {
	f, err := os.OpenFile(q.Path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	for {
		err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			break
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) {
			return 0, err
		}
		if err = sleepContext(ctx, 10*time.Millisecond); err != nil {
			return 0, err
		}
	}
	defer syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	if err = ctx.Err(); err != nil {
		return 0, err
	}
	var raw [32]byte
	n, err := f.ReadAt(raw[:], 0)
	if err != nil && err != io.EOF {
		return 0, err
	}
	if n != 0 && n != len(raw) {
		return 0, errors.New("source_quota_state_truncated")
	}
	var s [4]int64
	for i := range s {
		s[i] = int64(binary.LittleEndian.Uint64(raw[i*8:]))
	}
	original := s
	wait, err := update(&s)
	if err != nil {
		return 0, err
	}
	if n == len(raw) && s == original {
		return wait, nil
	}
	for i := range s {
		binary.LittleEndian.PutUint64(raw[i*8:], uint64(s[i]))
	}
	if _, err = f.WriteAt(raw[:], 0); err != nil {
		return 0, err
	}
	return wait, nil
}

// Cooldown reads the shared provider backoff without consuming admission or
// changing its timestamps. Discovery can yield to another chain immediately.
func (q *SourceQuota) Cooldown(ctx context.Context) (time.Duration, error) {
	return q.state(ctx, func(s *[4]int64) (time.Duration, error) {
		return max(time.Duration(s[1]-time.Now().UnixNano()), time.Duration(0)), nil
	})
}
func (q *SourceQuota) Before(ctx context.Context, members int) error {
	if members < 1 || members > q.Burst {
		return fmt.Errorf("source_quota_batch_exceeds_burst: %d", members)
	}
	urgent, _ := ctx.Value(probePriorityKey{}).(bool)
	discovery, _ := ctx.Value(discoveryQuotaKey{}).(bool)
	for {
		var providerCooldown int64
		wait, err := q.state(ctx, func(s *[4]int64) (time.Duration, error) {
			now := time.Now().UnixNano()
			providerCooldown = s[1]
			if discovery && providerCooldown > now {
				return 0, errors.New("rpc_source_cooldown")
			}
			// All batch sizes compete for the same next admission. Borrowing
			// credits for small requests lets a stream of probes permanently
			// starve the three-member header batches needed by history.
			ready := max(s[1], s[0])
			if urgent && s[2] > 0 && now >= s[2] {
				// A completed priority window must yield to background work.
				// Negative values encode that bounded yield deadline; a queued
				// probe cannot renew either deadline.
				s[2] = -(max(now, ready) + int64(q.Interval))
			}
			if urgent && s[2] < 0 {
				if ready >= -s[2] {
					// A newly extended provider cooldown must not consume the
					// whole background opportunity before quota is available.
					s[2] = -(ready + int64(q.Interval))
				}
				if now < -s[2] {
					ready = max(ready, -s[2])
				} else {
					s[2] = 0
				}
			}
			if !urgent && s[2] > 0 {
				ready = max(ready, s[2])
			}
			if ready > now {
				return time.Duration(ready - now), nil
			}
			s[0] = now + int64(time.Duration(members)*q.Interval)
			// Cover one bounded live probe sequence, without renewal by its
			// individual RPCs. Restored/background probes have no priority.
			if urgent && s[2] == 0 {
				lease := min(4*time.Second, 10*q.Interval)
				if deadline, ok := ctx.Deadline(); ok {
					lease = min(lease, max(time.Until(deadline), time.Duration(0)))
				}
				s[2] = now + int64(lease)
			} else if !urgent {
				s[2] = 0
			}
			return 0, nil
		})
		if err != nil {
			return err
		}
		if wait <= 0 {
			return nil
		}
		if deadline, ok := ctx.Deadline(); ok && !time.Unix(0, providerCooldown).Before(deadline) {
			return context.DeadlineExceeded
		}
		if err = sleepContext(ctx, min(wait, 100*time.Millisecond)); err != nil {
			return err
		}
	}
}
func (q *SourceQuota) After(_ int, err error) {
	// Local cancellation/queue deadlines do not impose provider backoff.
	if err != nil && !ethereum.IsRateLimited(err) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, _ = q.state(ctx, func(s *[4]int64) (time.Duration, error) {
		if err == nil {
			// A cheap header succeeding just after backoff does not prove that
			// an expensive log query has recovered. Preserve escalation until
			// the source has had a full minute without another rate limit.
			if time.Now().UnixNano() >= s[1]+int64(time.Minute) {
				s[3] = 0
			}
			return 0, nil
		}
		s[3] = min(s[3]+1, int64(4))
		pause := time.Duration(1<<uint(s[3]-1)) * 5 * time.Second
		var httpErr *ethereum.HTTPError
		if errors.As(err, &httpErr) {
			pause = max(pause, min(httpErr.RetryAfter, 5*time.Minute))
		}
		s[1] = max(s[1], time.Now().Add(pause).UnixNano())
		return 0, nil
	})
}
