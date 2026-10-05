package across

import (
	"context"
	"sync"

	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
)

const reportArchiveWorkers = 8
const reportArchiveChunkSize = 1000

type reportArchiveResult struct {
	evidence CaptureEvidence
	reason   string
}

// Only compact capture metadata is checked here. Source response bodies are
// no longer part of query correctness; LoadBatches verifies typed DB members.
type reportArchiveValidator struct{ archive ethereum.Archive }

func newReportArchiveValidator(archive ethereum.Archive) *reportArchiveValidator {
	return &reportArchiveValidator{archive: archive}
}
func (v *reportArchiveValidator) count() uint64            { return 0 }
func (v *reportArchiveValidator) unavailableCount() uint64 { return 0 }

func (v *reportArchiveValidator) check(ctx context.Context, captures []Capture, memberErrors map[string]error) ([]reportArchiveResult, error) {
	results := make([]reportArchiveResult, len(captures))
	tasks := make(chan int)
	var workers sync.WaitGroup
	for range min(reportArchiveWorkers, len(captures)) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := range tasks {
				if ctx.Err() != nil {
					continue
				}
				cap := captures[i]
				evidence, err := ReadCaptureEvidence(v.archive, cap)
				if err != nil {
					results[i].reason = "missing_or_invalid_capture_evidence"
					continue
				}
				// Membership has already been checked by ReadCaptureEvidence; keep
				// only report scope fields while the other workers finish this chunk.
				results[i].evidence = CaptureEvidence{CaptureID: evidence.CaptureID, FromTime: evidence.FromTime, ToTime: evidence.ToTime}

			}
		}()
	}
	for i, cap := range captures {
		if !cap.Canonical || !cap.Committed || memberErrors[cap.CaptureId] != nil {
			continue
		}
		select {
		case <-ctx.Done():
			close(tasks)
			workers.Wait()
			return nil, ctx.Err()
		case tasks <- i:
		}
	}
	close(tasks)
	workers.Wait()
	return results, ctx.Err()
}
