package mysql

import (
	"sync"
	"time"
)

type metadataTotal struct {
	Database, Service, Operation string
	Attempts, Failures           uint64
	LockMS, BodyMS, CommitMS     float64
}

var metadataTotals = struct {
	sync.Mutex
	started time.Time
	rows    map[string]metadataTotal
}{rows: map[string]metadataTotal{}}

// Completed attempts include failures. Body/commit time is distinct from the
// generation-lock wait, so short but frequent metadata work remains visible.
func recordMetadataTotal(database, service, operation string, lock, body, commit time.Duration, failed bool) {
	now := time.Now()
	metadataTotals.Lock()
	if metadataTotals.started.IsZero() {
		metadataTotals.started = now
	}
	key := database + "\x00" + service + "\x00" + operation
	r := metadataTotals.rows[key]
	r.Database, r.Service, r.Operation = database, service, operation
	r.Attempts++
	if failed {
		r.Failures++
	}
	r.LockMS += float64(lock) / float64(time.Millisecond)
	r.BodyMS += float64(body) / float64(time.Millisecond)
	r.CommitMS += float64(commit) / float64(time.Millisecond)
	metadataTotals.rows[key] = r
	seconds := now.Sub(metadataTotals.started).Seconds()
	if seconds < 60 {
		metadataTotals.Unlock()
		return
	}
	finished := metadataTotals.rows
	metadataTotals.rows = map[string]metadataTotal{}
	metadataTotals.started = now
	metadataTotals.Unlock()
	rows := make([]metadataTotal, 0, len(finished))
	for _, r := range finished {
		rows = append(rows, r)
	}
	metadataDiagnostics.Info("storage metadata transaction totals", "interval_seconds", seconds, "operations", rows)
}
