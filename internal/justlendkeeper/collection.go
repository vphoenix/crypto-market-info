package justlendkeeper

import (
	"context"
	"errors"
	"strings"
	"time"
)

// A single background operation is restored from the committed source pages.
// Source pages stay in the database, rather than becoming an unbounded gob queue.
type CollectionReader interface {
	KeeperNextEnrichment(context.Context, string, time.Time) (*Capture, error)
	KeeperBatch(context.Context, Capture) (Batch, error)
	KeeperEvidenceFrontier(context.Context, string, string, time.Time, time.Time, func([]Capture) (bool, error)) (time.Time, error)
}

func realtimeScan(s Scan) bool { return strings.HasPrefix(s.ID, "watch:") }

func (c *Collector) indexRows(o *Operation, p Page, next string) error {
	o.Kind = "index_page"
	if s := c.scan(o.ScanID); s != nil && (strings.HasPrefix(s.ID, "history:") || strings.HasPrefix(s.ID, "rescan:")) {
		o.Batch.Capture.CaptureMode = "history"
	}
	o.Batch.Capture.CaptureKind = "event_index"
	o.Batch.Capture.CoverageScope = "indexed_energy_events"
	o.Batch.Capture.SolidHeight = nil
	o.Batch.Capture.SolidHash = nil
	o.Batch.Capture.Status = "complete"
	o.Batch.Capture.Reason = ""
	o.Batch.Events = nil
	o.Batch.Receipts = nil
	o.Batch.IndexedEvents = nil
	o.RawEvents = nil
	o.Receipts = nil
	o.Headers = nil
	o.Discoveries = p.Data
	o.ExcludedBoundary = p.ExcludedBoundary
	o.ExcludedResource = p.ExcludedResource
	o.Batch.Capture.PageCount = 1
	o.Batch.Capture.DiscoveredCandidates = uint32(len(p.Data))
	o.NextFingerprint = next
	o.Batch.Capture.PaginationExhausted = next == ""
	for _, raw := range p.Data {
		row, err := indexedRow(raw, o.Batch.Capture, raw.Ordinal)
		if err != nil {
			return err
		}
		o.Batch.IndexedEvents = append(o.Batch.IndexedEvents, row)
	}
	return nil
}

func (c *Collector) enableIndexing() error {
	legacy := !c.State.IndexingEnabled
	if !c.State.IndexingEnabled {
		c.State.EvidenceCursors = make(map[string]time.Time, len(c.State.EventCursors))
		for k, v := range c.State.EventCursors {
			c.State.EvidenceCursors[k] = v
		}
		c.State.IndexingEnabled = true
	}
	for i := range c.State.Ops {
		o := &c.State.Ops[i]
		if o.Kind != "page" || o.Frozen || o.Failed {
			continue
		}
		s := c.scan(o.ScanID)
		if s == nil {
			return errors.New("missing_legacy_scan")
		}
		// A new index window must begin at the first page. Preserve this old
		// attempt as a failure record, then re-read its complete source window.
		if s.Fingerprint != "" || s.Pages > 0 {
			o.Failed = true
			o.Batch.Capture.Status = "partial"
			o.Batch.Capture.Reason = "legacy_prefix_reindexed"
			o.Requests = nil
			continue
		}
		found := len(o.Discoveries) > 0 || o.Batch.Capture.PageCount > 0
		if found {
			if err := c.indexRows(o, Page{Data: o.Discoveries, ExcludedBoundary: o.ExcludedBoundary, ExcludedResource: o.ExcludedResource}, o.NextFingerprint); err != nil {
				return err
			}
			o.Requests = nil
		}
		if !found {
			o.Kind = "index_page"
			o.Batch.Capture.CaptureKind = "event_index"
			if strings.HasPrefix(s.ID, "history:") || strings.HasPrefix(s.ID, "rescan:") {
				o.Batch.Capture.CaptureMode = "history"
			}
			o.Batch.Capture.CoverageScope = "indexed_energy_events"
			o.Batch.Capture.SolidHeight = nil
			o.Batch.Capture.SolidHash = nil
			o.Requests = []Request{{Source: "trongrid", Path: EventPath(s.Kind, s.From, s.To, s.Fingerprint), Role: "page", Background: !realtimeScan(*s)}}
		}
	}
	if legacy {
		for i := range c.State.Scans {
			s := &c.State.Scans[i]
			if (s.Done && !s.Failed) || (s.Pages == 0 && s.Fingerprint == "") {
				continue
			}
			frozen := false
			for _, o := range c.State.Ops {
				if o.ScanID == s.ID && o.Frozen {
					frozen = true
					break
				}
			}
			if !frozen {
				s.Pages = 0
				s.Fingerprint = ""
			}
		}
	}
	return nil
}

func (c *Collector) restoreEnrichment(ctx context.Context) error {
	if c.Active("enrichment") || c.Now().Before(c.State.NextEnrichment) {
		return nil
	}
	r, ok := c.Store.(CollectionReader)
	if !ok {
		return nil
	}
	c.State.NextEnrichment = c.Now().Add(5 * time.Second)
	parent, err := r.KeeperNextEnrichment(ctx, c.Config.Hash(), c.Now().UTC())
	if err != nil {
		return err
	}
	if parent == nil {
		for _, kind := range []string{"RentResource", "ReturnResource", "Liquidate"} {
			if err := c.advanceEvidence(ctx, kind); err != nil {
				return err
			}
		}
		return nil
	}
	b, err := r.KeeperBatch(ctx, *parent)
	if err != nil {
		return err
	}
	if parent.ConfigHash != c.Config.Hash() || parent.CaptureKind != "event_index" || parent.Status != "complete" || parent.RequestedFrom == nil || parent.RequestedTo == nil {
		return errors.New("invalid_enrichment_parent")
	}
	if b.Capture.CaptureId != parent.CaptureId || !captureEqual(b.Capture, *parent) {
		return errors.New("enrichment_parent_identity_mismatch")
	}
	if err = Validate(b); err != nil {
		return err
	}
	var p Page
	for _, row := range b.IndexedEvents {
		raw, err := rawFromIndexed(row)
		if err != nil {
			return err
		}
		p.Data = append(p.Data, raw)
	}
	o := c.NewOperation("enrichment", parent.CaptureMode, "publicnode")
	if parent.CaptureMode == "backfill" {
		o.Batch.Capture.CaptureMode = "history"
	}
	o.Parent = parent
	o.Batch.Capture.ParentCaptureId = Ptr(parent.CaptureId)
	o.Batch.Capture.CaptureKind = "enrichment"
	o.Batch.Capture.EventKind = parent.EventKind
	o.Batch.Capture.RequestedFrom = parent.RequestedFrom
	o.Batch.Capture.RequestedTo = parent.RequestedTo
	o.Batch.Capture.CoverageScope = "returned_liquidations"
	if parent.EventKind != "Liquidate" {
		o.Batch.Capture.CoverageScope = "sampled_rental_events"
	}
	o.Discoveries = p.Data
	o.Batch.Capture.DiscoveredCandidates = uint32(len(p.Data))
	for _, raw := range p.Data {
		selected := parent.EventKind == "Liquidate"
		if !selected {
			o.Batch.Capture.CoverageScope = "sampled_rental_events"
			row, _ := EventRow(raw)
			for _, member := range c.State.Cohort {
				if member.Renter == row.Renter && member.Receiver == row.Receiver {
					selected = true
					break
				}
			}
		}
		if selected {
			o.RawEvents = append(o.RawEvents, raw)
		}
	}
	o.Batch.Capture.SelectedCandidates = uint32(len(o.RawEvents))
	if len(o.RawEvents) > 0 {
		o.Requests = []Request{headRequest(true, "solid", true)}
		o.AddHydration()
	}
	c.State.Ops = append(c.State.Ops, o)
	return c.Save()
}

func (c *Collector) evidenceProgress(ctx context.Context, o Operation) error {
	if !c.State.IndexingEnabled || o.Kind != "enrichment" || o.Failed || o.Parent == nil || o.Parent.CaptureMode == "history" || o.Parent.CaptureMode == "backfill" {
		return nil
	}
	return c.advanceEvidence(ctx, o.Parent.EventKind)
}

func (c *Collector) advanceEvidence(ctx context.Context, kind string) error {
	if !c.State.IndexingEnabled {
		return nil
	}
	r, ok := c.Store.(CollectionReader)
	if !ok {
		return nil
	}
	// A bounded number of complete windows per pass. No-child passes resume
	// this work too, so a large already-complete backlog cannot become stuck.
	for i := 0; i < 32; i++ {
		old := c.State.EvidenceCursors[kind]
		at, err := r.KeeperEvidenceFrontier(ctx, c.Config.Hash(), kind, old, c.State.EventCursors[kind], func(caps []Capture) (bool, error) { return c.indexWindowComplete(ctx, caps) })
		if err != nil {
			return err
		}
		if !at.After(old) {
			break
		}
		c.State.EvidenceCursors[kind] = at
	}
	return nil
}

func (c *Collector) indexWindowComplete(ctx context.Context, caps []Capture) (bool, error) {
	r, ok := c.Store.(IndexPageReader)
	if !ok {
		return false, errors.New("index_page_reader_required")
	}
	pages, err := r.KeeperIndexPages(ctx, caps)
	if err != nil {
		return false, err
	}
	byToken := map[string]IndexPage{}
	scanID := ""
	for _, cap := range caps {
		m, found := pages[cap.CaptureId]
		if !found {
			return false, errors.New("index_page_progress_missing_run_migrate_pages")
		}
		if err := validateIndexPage(cap, m); err != nil {
			return false, err
		}
		if m.ScanID == "" || (scanID != "" && m.ScanID != scanID) {
			return false, nil
		}
		scanID = m.ScanID
		if _, exists := byToken[m.FingerprintIn]; exists {
			return false, nil
		}
		byToken[m.FingerprintIn] = m
		if cap.PaginationExhausted != (m.FingerprintOut == "") {
			return false, nil
		}
	}
	token := ""
	seen := map[string]bool{}
	for len(seen) < len(caps) {
		m, ok := byToken[token]
		if !ok || seen[token] {
			return false, nil
		}
		seen[token] = true
		if m.FingerprintOut == "" {
			return len(seen) == len(caps), nil
		}
		token = m.FingerprintOut
	}
	return false, nil
}

func (c *Collector) operationPriority(o Operation) int {
	if o.Kind == "index_page" {
		if s := c.scan(o.ScanID); s != nil && realtimeScan(*s) {
			return 0
		}
	}
	if o.Kind == "probe" && o.probeAttempted() {
		return 1
	}
	if o.Kind == "index_page" {
		return 2
	}
	return 3
}
