package justlendkeeper

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

// IndexPage is small, typed pagination progress, not an API response archive.
// It lives separately so legacy frozen captures keep their original identity.
type IndexPage struct {
	CaptureId        uuid.UUID `ch:"capture_id"`
	CaptureStartedAt time.Time `ch:"capture_started_at"`
	ScanID           string    `ch:"scan_id"`
	FingerprintIn    string    `ch:"fingerprint_in"`
	FingerprintOut   string    `ch:"fingerprint_out"`
	RequestStartedAt time.Time `ch:"request_started_at"`
	AvailableAt      time.Time `ch:"available_at"`
	PayloadHash      string    `ch:"payload_hash"`
}

type IndexPageReader interface {
	KeeperIndexPages(context.Context, []Capture) (map[uuid.UUID]IndexPage, error)
}
type IndexPageWriter interface {
	WriteKeeperIndexPages(context.Context, []IndexPage) error
}

func validateIndexPage(cap Capture, p IndexPage) error {
	if cap.CaptureKind != "event_index" || cap.Status != "complete" || p.CaptureId != cap.CaptureId || !p.CaptureStartedAt.Equal(cap.CaptureStartedAt) || p.ScanID == "" || len(p.PayloadHash) != 32 || p.RequestStartedAt.IsZero() || p.AvailableAt.Before(p.RequestStartedAt) || p.AvailableAt.After(cap.AvailableAt) || cap.PaginationExhausted != (p.FingerprintOut == "") {
		return errors.New("invalid_index_page_progress")
	}
	return nil
}

func pageProgress(cap Capture, scanID, in, out string, requests []Evidence) (IndexPage, error) {
	for i := len(requests) - 1; i >= 0; i-- {
		ev := requests[i]
		if ev.Source != "trongrid" || ev.Error != "" || ev.ResponseHash == "" {
			continue
		}
		if cap.RequestedFrom == nil || cap.RequestedTo == nil || ev.Path != EventPath(cap.EventKind, *cap.RequestedFrom, *cap.RequestedTo, in) {
			return IndexPage{}, errors.New("index_page_request_mismatch")
		}
		h, err := BinaryHex(ev.ResponseHash, 32)
		if err != nil {
			return IndexPage{}, err
		}
		p := IndexPage{cap.CaptureId, cap.CaptureStartedAt, scanID, in, out, ev.Started, ev.Available, h}
		return p, validateIndexPage(cap, p)
	}
	return IndexPage{}, errors.New("index_page_source_hash_missing")
}

func (c *Collector) prepareIndexPage(o *Operation) error {
	if o.Batch.Capture.CaptureKind != "event_index" || o.Batch.Capture.Status != "complete" || o.Batch.IndexPage != nil {
		return nil
	}
	s := c.scan(o.ScanID)
	if s == nil {
		return errors.New("index_page_scan_missing")
	}
	p, err := pageProgress(o.Batch.Capture, o.ScanID, s.Fingerprint, o.NextFingerprint, o.Evidence)
	if err != nil {
		return err
	}
	o.Batch.IndexPage = &p
	return nil
}

// Explicit, local-only upgrade. Reads old summaries once; never re-fetches a
// historical API page or rewrites a capture, member, digest, budget or cursor.
func MigrateIndexPages(ctx context.Context, r ReportReader, a Archive, w IndexPageWriter) (int, error) {
	pr, ok := r.(IndexPageReader)
	if !ok {
		return 0, errors.New("index_page_reader_required")
	}
	caps, err := r.KeeperCaptures(ctx, time.Unix(0, 0).UTC(), time.Now().UTC().Add(time.Second))
	if err != nil {
		return 0, err
	}
	var selected []Capture
	for _, cap := range caps {
		if cap.Committed && cap.CaptureKind == "event_index" && cap.Status == "complete" {
			selected = append(selected, cap)
		}
	}
	count := 0
	for i := 0; i < len(selected); i += 256 {
		group := selected[i:min(i+256, len(selected))]
		existing, err := pr.KeeperIndexPages(ctx, group)
		if err != nil {
			return count, err
		}
		batches := map[uuid.UUID]Batch{}
		if bulk, ok := r.(BulkReportReader); ok {
			batches, err = bulk.KeeperBatches(ctx, group)
			if err != nil {
				return count, err
			}
		} else {
			for _, cap := range group {
				b, err := r.KeeperBatch(ctx, cap)
				if err != nil {
					return count, err
				}
				batches[cap.CaptureId] = b
			}
		}
		var pages []IndexPage
		for _, cap := range group {
			if p, ok := existing[cap.CaptureId]; ok {
				if err := validateIndexPage(cap, p); err != nil {
					return count, err
				}
				continue
			}
			m, err := readManifest(a, cap)
			if err != nil {
				return count, err
			}
			if m.Kind != "index_page" {
				return count, errors.New("legacy_index_page_kind_mismatch")
			}
			p, err := pageProgress(cap, m.ScanID, m.FingerprintIn, m.FingerprintOut, m.Requests)
			if err != nil {
				return count, err
			}
			b, ok := batches[cap.CaptureId]
			if !ok || !captureEqual(cap, b.Capture) {
				return count, errors.New("migration_batch_identity_mismatch")
			}
			b.IndexPage = &p
			if err := Validate(b); err != nil {
				return count, err
			}
			pages = append(pages, p)
		}
		if err := w.WriteKeeperIndexPages(ctx, pages); err != nil {
			return count, err
		}
		count += len(pages)
	}
	return count, nil
}
