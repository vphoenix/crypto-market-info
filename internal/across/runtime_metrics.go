package across

import (
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
)

type runtimeTelemetry struct {
	At                           time.Time
	Written, FileSyncs, DirSyncs uint64
}

func runtimeRSS() uint64 {
	raw, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	parts := strings.Fields(string(raw))
	if len(parts) < 2 {
		return 0
	}
	pages, _ := strconv.ParseUint(parts[1], 10, 64)
	return pages * uint64(os.Getpagesize())
}
func earlierNonzero(a, b time.Time) time.Time {
	if a.IsZero() || !b.IsZero() && b.Before(a) {
		return b
	}
	return a
}

// Summaries come only from owned state/page headers and process counters. No
// database query or full page walk is permitted for ten-second telemetry.
func (o *runtimeOwner) logRuntimeMetrics(at time.Time) {
	if o.progress == nil {
		return
	}
	m := o.store.Metrics()
	window := at.Sub(o.telemetry.At)
	if o.telemetry.At.IsZero() {
		window = 0
	}
	var bytesPerSecond uint64
	if window > 0 {
		bytesPerSecond = (m.WrittenBytes - o.telemetry.Written) * uint64(time.Second) / uint64(window)
	}
	var oldest time.Time
	for _, c := range o.state.Active {
		oldest = earlierNonzero(oldest, c.StartedAt)
	}
	for _, c := range o.state.Partial {
		oldest = earlierNonzero(oldest, c.StartedAt)
	}
	for _, p := range o.state.Watch.Orders {
		oldest = earlierNonzero(oldest, p.Deposit.AvailableAt)
	}
	var cacheBytes, pageReads, pageWrites uint64
	for _, pages := range []*runtimeCapturePages{o.state.activePages, o.state.partialPages} {
		if pages != nil {
			p, q := pages.records.Metrics(), pages.order.Metrics()
			cacheBytes += uint64(p.ResidentBytes + q.ResidentBytes)
			pageReads += p.PagesRead + q.PagesRead
			pageWrites += p.PagesWritten + q.PagesWritten
			oldest = earlierNonzero(oldest, pages.records.Oldest())
		}
	}
	pending := 0
	if o.state.retryPages != nil {
		p, q := o.state.retryPages.Metrics(), o.state.skippedPages.Metrics()
		cacheBytes += uint64(p.ResidentBytes + q.ResidentBytes)
		pageReads += p.PagesRead + q.PagesRead
		pageWrites += p.PagesWritten + q.PagesWritten
		oldest = earlierNonzero(oldest, o.state.skippedPages.Oldest())
	}
	if o.state.receiptPages != nil {
		p := o.state.receiptPages.Metrics()
		q := o.state.receiptSchedule.Metrics()
		cacheBytes += uint64(p.ResidentBytes + q.ResidentBytes)
		pageReads += p.PagesRead + q.PagesRead
		pageWrites += p.PagesWritten + q.PagesWritten
		pending = int(o.state.receiptSchedule.Len())
		oldest = earlierNonzero(oldest, o.state.receiptPages.Oldest())
	} else {
		for _, r := range o.state.Receipts {
			if r.Pending {
				pending++
				oldest = earlierNonzero(oldest, r.FirstObserved)
			}
		}
	}
	var lines []string
	gaps := 0
	for chain, ch := range o.state.Chains {
		if !ch.Started {
			continue
		}
		raw := o.state.coverage(chain, false)
		decoded := o.state.coverage(chain, true)
		next := ch.Start
		for _, r := range raw {
			if r.from > next {
				gaps++
			}
			next = max(next, r.to+1)
		}
		if next <= ch.End {
			gaps++
		}
		decodedCursor := uint64(0)
		n := firstUncovered(ch.Start, ch.End, decoded)
		if n > 0 {
			decodedCursor = n - 1
		}
		lines = append(lines, fmt.Sprintf("%d:%d/%d", chain, o.state.Watch.Cursors[chain], decodedCursor))
	}
	sort.Strings(lines)
	stateBytes := runtimecheckpoint.EstimateTypedBytes(o.state)
	indexBytes := runtimecheckpoint.EstimateTypedBytes(o.state.receiptParents)
	var process runtime.MemStats
	runtime.ReadMemStats(&process)
	oldestAge := int64(0)
	if !oldest.IsZero() {
		oldestAge = max(int64(0), at.Sub(oldest).Milliseconds())
	}
	o.progress(fmt.Sprintf("runtime_state progress_raw_decoded=%s gaps=%d active=%d partial=%d receipts_pending=%d receipts_retained=%d orders=%d inflight=%d oldest_pending_age_ms=%d checkpoint_age_ms=%d checkpoint_bytes=%d uncheckpointed=%d working_set_estimate_bytes=%d total_hot_estimate_bytes=%d page_index_bytes=%d page_cache_bytes=%d recovery_metadata_bytes=%d verified_metadata_bytes=%d heap_bytes=%d rss_bytes=%d written_bytes=%d written_bytes_per_second=%d file_syncs=%d directory_syncs=%d file_syncs_window=%d directory_syncs_window=%d metrics_window_ms=%d recovery_bytes=%d recovery_peak_bytes=%d page_reads=%d page_writes=%d error=%v", strings.Join(lines, ","), gaps, o.state.captureCount(false), o.state.captureCount(true), pending, o.state.receiptCount(), len(o.state.Watch.Orders), len(o.inflight), oldestAge, at.Sub(m.CheckpointAt).Milliseconds(), m.CheckpointBytes, m.Uncheckpointed, stateBytes, o.hotBytes(), indexBytes, cacheBytes, m.HotBytes, m.VerifiedMetadataBytes, process.HeapAlloc, runtimeRSS(), m.WrittenBytes, bytesPerSecond, m.FileSyncs, m.DirectorySyncs, m.FileSyncs-o.telemetry.FileSyncs, m.DirectorySyncs-o.telemetry.DirSyncs, window.Milliseconds(), m.RecoveryBytes, m.PeakRecoveryBytes, pageReads, pageWrites, o.poison))
	o.telemetry = runtimeTelemetry{At: at, Written: m.WrittenBytes, FileSyncs: m.FileSyncs, DirSyncs: m.DirectorySyncs}
}
