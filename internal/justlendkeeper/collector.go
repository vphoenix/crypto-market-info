package justlendkeeper

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

type Store interface {
	WriteKeeperBatch(context.Context, Batch) error
}
type ReceiptLogs struct {
	Receipt TxReceipt
	Logs    []Log
}
type Operation struct {
	Discoveries []RawEvent

	Kind             string
	ScanID           string
	Requests         []Request
	Evidence         []Evidence
	Batch            Batch
	RawEvents        []RawEvent
	Headers          []Header
	Receipts         []ReceiptLogs
	NextFingerprint  string
	Frozen           bool
	Failed           bool
	NotBefore        time.Time
	Candidate        *Candidate
	IdentityAddress  string
	IdentityCode     string
	Scheduled        time.Time
	Caller           string
	ValidCallers     []string
	ExcludedBoundary uint32
	ExcludedResource uint32
	Parent           *Capture
}
type Collector struct {
	Config Config
	State  *State
	Path   string
	API    *Client
	Store  Store
	Now    func() time.Time
	Log    func(string)
	Sleep  func(context.Context, time.Duration) error
}

func NewCollector(cfg Config, s *State, path string, a Archive, store Store) *Collector {
	now := time.Now
	c := &Collector{Config: cfg, State: s, Path: path, Store: store, Now: now, Log: func(string) {}, Sleep: Sleep}
	l := &Limiter{State: s, Save: c.Save, Now: func() time.Time { return c.Now() }, Started: UTC(now()), Budget: cfg.DailyBudget}
	c.API = NewClient(cfg, a, l)
	return c
}
func Sleep(ctx context.Context, d time.Duration) error {
	if d < 0 {
		d = 0
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
func (c *Collector) Save() error { return SaveState(c.Path, *c.State) }
func (c *Collector) NewOperation(kind, mode, source string) Operation {
	now := UTC(c.Now())
	contract, _ := HexAddress(ContractHex)
	cap := Capture{CaptureId: uuid.New(), CaptureStartedAt: now, ConfigHash: c.Config.Hash(), Network: Network, ContractAddress: contract, CaptureKind: kind, CaptureMode: mode, SourceId: source, CoverageScope: "selected_tasks", Status: "complete"}
	return Operation{Kind: kind, Batch: Batch{Capture: cap}}
}
func (c *Collector) AddIdentity() {
	o := c.NewOperation("identity", "live", "publicnode")
	o.Batch.Capture.CaptureKind = "costs"
	o.Requests = append(o.Requests, headRequest(false, "identity_before", false))
	for _, caller := range c.Config.Callers {
		o.Requests = append(o.Requests, Post("publicnode", "/wallet/getaccount", "account", caller, map[string]any{"address": caller}, false))
	}
	o.Requests = append(o.Requests, Post("publicnode", "/wallet/triggerconstantcontract", "implementation", "", map[string]any{"owner_address": c.Config.Callers[0], "contract_address": ContractHex, "function_selector": "implementation()", "parameter": "", "call_value": 0}, false), Post("publicnode", "/wallet/getcontract", "implementation_code", "", map[string]any{"value": ImplementationHex}, false), headRequest(false, "identity_after", false))
	c.State.Ops = append(c.State.Ops, o)
}
func headRequest(solid bool, role string, bg bool) Request {
	path := "/wallet/getblock"
	if solid {
		path = "/walletsolidity/getblock"
	}
	return Post("publicnode", path, role, "", map[string]any{"detail": false}, bg)
}
func (c *Collector) AddPage(s Scan) {
	o := c.NewOperation("index_page", s.Mode, "trongrid")
	o.ScanID = s.ID
	o.Batch.Capture.CaptureKind = "event_index"
	o.Batch.Capture.EventKind = s.Kind
	o.Batch.Capture.RequestedFrom = Ptr(s.From)
	o.Batch.Capture.RequestedTo = Ptr(s.To)
	if strings.HasPrefix(s.ID, "history:") || strings.HasPrefix(s.ID, "rescan:") {
		o.Batch.Capture.CaptureMode = "history"
	}
	o.Batch.Capture.CoverageScope = "indexed_energy_events"
	o.Requests = []Request{{Source: "trongrid", Path: EventPath(s.Kind, s.From, s.To, s.Fingerprint), Role: "page", Background: !realtimeScan(s)}}
	c.State.Ops = append(c.State.Ops, o)
}
func (c *Collector) scan(id string) *Scan {
	for i := range c.State.Scans {
		if c.State.Scans[i].ID == id {
			return &c.State.Scans[i]
		}
	}
	return nil
}
func (o *Operation) AddHydration() {
	seenBlocks := map[uint64]bool{}
	seenTx := map[string]bool{}
	for _, ev := range o.RawEvents {
		if !seenBlocks[ev.BlockNumber] {
			o.Requests = append(o.Requests, Post("publicnode", "/wallet/getblock", "block", strconv.FormatUint(ev.BlockNumber, 10), map[string]any{"id_or_num": strconv.FormatUint(ev.BlockNumber, 10), "detail": true}, true))
			seenBlocks[ev.BlockNumber] = true
		}
		if !seenTx[ev.Transaction] {
			o.Requests = append(o.Requests, Post("publicnode", "/wallet/gettransactioninfobyid", "receipt", ev.Transaction, map[string]any{"value": ev.Transaction}, true), Post("publicnode", "/wallet/gettransactionbyid", "body", ev.Transaction, map[string]any{"value": ev.Transaction}, true))
			seenTx[ev.Transaction] = true
		}
	}
}
func (c *Collector) Process(o *Operation, r Request, ev Evidence, b []byte) error {
	switch r.Role {
	case "solid":
		h, e := ParseBlock(b, false)
		if e != nil {
			return e
		}
		if c.State.LastSolidNumber == h.Number && c.State.LastSolidHash != "" && c.State.LastSolidHash != h.Hash {
			return errors.New("solid_hash_conflict")
		}
		if h.Number >= c.State.LastSolidNumber {
			c.State.LastSolidNumber = h.Number
			c.State.LastSolidHash = h.Hash
		}
		o.Batch.Capture.SolidHeight = Ptr(h.Number)
		o.Batch.Capture.SolidHash = Ptr(h.Hash)
	case "page":
		s := c.scan(o.ScanID)
		if s == nil {
			return errors.New("missing_scan")
		}
		p, next, e := ParsePage(b, ev, c.Config, *s)
		if e != nil {
			return e
		}
		if o.Kind == "index_page" {
			return c.indexRows(o, p, next)
		}
		o.ExcludedBoundary = p.ExcludedBoundary
		o.Batch.Capture.PageCount = 1
		o.NextFingerprint = next
		o.Batch.Capture.PaginationExhausted = next == ""
		o.RawEvents = p.Data
		o.Discoveries = p.Data
		if s.Mode == "bootstrap" {
			o.Batch.Capture.DiscoveredCandidates = uint32(len(p.Data))
			break
		}
		if s.Kind != "Liquidate" {
			selected := []RawEvent{}
			for _, re := range p.Data {
				row, _ := EventRow(re)
				for _, can := range c.State.Cohort {
					if can.Renter == row.Renter && can.Receiver == row.Receiver {
						selected = append(selected, re)
						break
					}
				}
			}
			o.RawEvents = selected
		}
		o.AddHydration()
	case "block":
		h, e := ParseBlock(b, true)
		if e != nil {
			return e
		}
		n, _ := strconv.ParseUint(r.Key, 10, 64)
		if h.Number != n || o.Batch.Capture.SolidHeight == nil || h.Number > *o.Batch.Capture.SolidHeight {
			return errors.New("block_not_solid_or_height_mismatch")
		}
		if old, ok := c.State.SolidAnchors[h.Number]; ok && old != h.Hash {
			return errors.New("solid_hash_conflict")
		}
		c.State.SolidAnchors[h.Number] = h.Hash
		if len(c.State.SolidAnchors) > 4096 {
			oldest := h.Number
			for n := range c.State.SolidAnchors {
				if n < oldest {
					oldest = n
				}
			}
			delete(c.State.SolidAnchors, oldest)
		}
		o.Headers = append(o.Headers, h)
	case "receipt":
		rr, logs, e := ParseReceipt(b, ev)
		if e != nil {
			return e
		}
		if Hex(rr.TxId) != r.Key {
			return errors.New("receipt_transaction_mismatch")
		}
		rr.CaptureId = o.Batch.Capture.CaptureId
		rr.CaptureStartedAt = o.Batch.Capture.CaptureStartedAt
		o.Receipts = append(o.Receipts, ReceiptLogs{rr, logs})
	case "body":
		for i := range o.Receipts {
			if Hex(o.Receipts[i].Receipt.TxId) == r.Key {
				return ApplyBody(&o.Receipts[i].Receipt, b, ev)
			}
		}
		return errors.New("body_without_receipt")
	case "account":
		var a struct {
			Address string          `json:"address"`
			Type    json.RawMessage `json:"type"`
		}
		if e := Decode(b, &a); e != nil {
			return e
		}
		if a.Address != r.Key {
			return errors.New("caller_not_activated")
		}
		if len(a.Type) > 0 && string(a.Type) != "0" && string(a.Type) != "\"Normal\"" {
			return errors.New("caller_not_normal")
		}
		caller, _ := HexAddress(r.Key)
		o.ValidCallers = append(o.ValidCallers, caller)
	case "implementation":
		v, e := ParseConstant(b)
		if e != nil {
			return e
		}
		a, e := WordAddress(v)
		if e != nil {
			return e
		}
		o.IdentityAddress = a
	case "implementation_code":
		var m struct {
			Address  string `json:"contract_address"`
			Bytecode string `json:"bytecode"`
		}
		if e := Decode(b, &m); e != nil {
			return e
		}
		a, e := Address(m.Address)
		if e != nil || Hex(a) != ImplementationHex {
			return errors.New("implementation_code_address_mismatch")
		}
		code, e := hex.DecodeString(m.Bytecode)
		if e != nil || len(code) == 0 {
			return errors.New("missing_implementation_bytecode")
		}
		o.IdentityCode = Hash(code)
	case "identity_before", "identity_after", "probe_before", "probe_after", "cost_before", "cost_after":
		h, e := ParseBlock(b, false)
		if e != nil {
			return e
		}
		o.Headers = append(o.Headers, h)
	case "probe":
		p, e := ParseProbe(b, ev, *o.Candidate, o.Caller, o.Scheduled, c)
		if e != nil {
			return e
		}
		p.ProbeIndex = uint32(len(o.Batch.Probes))
		p.CaptureId = o.Batch.Capture.CaptureId
		p.CaptureStartedAt = o.Batch.Capture.CaptureStartedAt
		p.CohortId = c.State.CohortId
		o.Batch.Probes = append(o.Batch.Probes, p)
	case "quote":
		q, e := ParseQuote(b, ev)
		if e != nil {
			return e
		}
		q.ObservationIndex = uint32(len(o.Batch.Costs))
		q.CaptureId = o.Batch.Capture.CaptureId
		q.CaptureStartedAt = o.Batch.Capture.CaptureStartedAt
		o.Batch.Costs = append(o.Batch.Costs, q)
	case "parameters":
		v, e := ParseParameters(b, ev)
		if e != nil {
			return e
		}
		v.ObservationIndex = uint32(len(o.Batch.Costs))
		v.CaptureId = o.Batch.Capture.CaptureId
		v.CaptureStartedAt = o.Batch.Capture.CaptureStartedAt
		o.Batch.Costs = append(o.Batch.Costs, v)
	case "proxy":
		var m map[string]json.RawMessage
		if e := Decode(b, &m); e != nil {
			return e
		}
		addr, e := text(m, "contract_address")
		if e != nil || addr != ContractHex {
			return errors.New("proxy_address_mismatch")
		}
		u, e := ReqU(m, "consume_user_resource_percent")
		if e != nil || u > 100 {
			return errors.New("invalid_resource_percent")
		}
		v, e := ReqU(m, "origin_energy_limit")
		if e != nil {
			return e
		}
		if len(o.Batch.Costs) == 0 {
			return errors.New("missing_chain_parameters")
		}
		o.Batch.Costs[len(o.Batch.Costs)-1].ContractUserResourcePercent = Ptr(uint8(u))
		o.Batch.Costs[len(o.Batch.Costs)-1].ContractOriginEnergyLimit = Ptr(v)
	default:
		return errors.New("unknown_request_role")
	}
	return nil
}
func (c *Collector) Hydrate(o *Operation) error {
	used := map[string]map[int]bool{}
	sort.SliceStable(o.RawEvents, func(i, j int) bool {
		a, b := o.RawEvents[i], o.RawEvents[j]
		if a.Transaction == b.Transaction {
			return a.Index < b.Index
		}
		return a.BlockNumber < b.BlockNumber
	})
	seen := map[string]RentalEvent{}
	for _, ev := range o.RawEvents {
		row, e := EventRow(ev)
		if e != nil {
			return e
		}
		key := ev.Transaction + ":" + strconv.FormatUint(uint64(ev.Index), 10)
		if prev, ok := seen[key]; ok {
			if !EventEqual(prev, row) {
				return errors.New("provider_event_conflict")
			}
			continue
		}
		seen[key] = row
		var header *Header
		for i := range o.Headers {
			if o.Headers[i].Number == ev.BlockNumber {
				header = &o.Headers[i]
				break
			}
		}
		if header == nil || !header.Time.Equal(row.BlockTime) {
			return errors.New("event_block_anchor_missing")
		}
		idx := -1
		for i, tx := range header.Transactions {
			if tx == row.TxId {
				idx = i
				break
			}
		}
		if idx < 0 {
			return errors.New("transaction_not_in_block")
		}
		row.BlockHash = header.Hash
		row.TransactionIndex = Ptr(uint32(idx))
		row.CaptureId = o.Batch.Capture.CaptureId
		row.CaptureStartedAt = o.Batch.Capture.CaptureStartedAt
		var receipt *ReceiptLogs
		for i := range o.Receipts {
			if o.Receipts[i].Receipt.TxId == row.TxId {
				receipt = &o.Receipts[i]
				break
			}
		}
		if receipt == nil || receipt.Receipt.BlockNumber != header.Number || !receipt.Receipt.BlockTime.Equal(header.Time) || receipt.Receipt.ExecutionResult != "SUCCESS" {
			return errors.New("event_receipt_not_success_or_anchor_mismatch")
		}
		// A provider index is not a receipt log index. Identical logs split over
		// pages cannot be located uniquely from this page's content alone.
		matches, supplied := 0, 0
		suppliedKeys := map[uint32]bool{}
		for _, l := range receipt.Logs {
			d, er := DecodeLog(l)
			if er == nil && EventEqual(row, d) {
				matches++
			}
		}
		for _, source := range o.RawEvents {
			if source.Transaction == ev.Transaction && !suppliedKeys[source.Index] {
				d, er := EventRow(source)
				if er == nil && EventEqual(row, d) {
					supplied++
					suppliedKeys[source.Index] = true
				}
			}
		}
		if matches > supplied {
			return errors.New("identical_logs_split_page_ambiguous")
		}
		receipt.Receipt.BlockHash = header.Hash
		if used[ev.Transaction] == nil {
			used[ev.Transaction] = map[int]bool{}
		}
		found := -1
		for i, l := range receipt.Logs {
			if used[ev.Transaction][i] {
				continue
			}
			decoded, e := DecodeLog(l)
			if e == nil && EventEqual(row, decoded) {
				found = i
				break
			}
		}
		if found < 0 {
			return errors.New("event_receipt_log_mismatch")
		}
		used[ev.Transaction][found] = true
		row.ReceiptLogIndex = Ptr(uint32(found))
		row.PositionStatus = "receipt_verified"
		o.Batch.Events = append(o.Batch.Events, row)
	}
	for _, rr := range o.Receipts {
		if rr.Receipt.BlockHash != "" {
			o.Batch.Receipts = append(o.Batch.Receipts, rr.Receipt)
		}
	}
	return nil
}
func (c *Collector) Finish(ctx context.Context, o *Operation) error {
	if !o.Frozen {
		if !o.Failed && (o.Kind == "page" && o.Batch.Capture.CaptureMode != "bootstrap" || o.Kind == "enrichment") {
			if e := c.Hydrate(o); e != nil {
				o.Failed = true
				o.Batch.Capture.Status = "partial"
				o.Batch.Capture.Reason = e.Error()
				o.Batch.Events = nil
				o.Batch.Receipts = nil
			}
		}
		if !o.Failed && o.Kind == "seed" {
			if e := c.Hydrate(o); e != nil {
				o.Failed = true
				o.Batch.Capture.Status = "partial"
				o.Batch.Capture.Reason = e.Error()
				o.Batch.Events = nil
				o.Batch.Receipts = nil
			}
		}
		if !o.Failed && o.Kind == "identity" {
			if Hex(o.IdentityAddress) != ImplementationHex || Hex(o.IdentityCode) != ImplementationSHA || len(o.ValidCallers) != 2 {
				o.Failed = true
				o.Batch.Capture.Status = "partial"
				o.Batch.Capture.Reason = "identity_manifest_mismatch"
			}
		}
		if o.Kind == "probe" {
			for i := range o.Batch.Probes {
				p := &o.Batch.Probes[i]
				if len(o.Headers) > 0 {
					h := o.Headers[0]
					p.HeadBeforeNumber = Ptr(h.Number)
					p.HeadBeforeHash = Ptr(h.Hash)
					p.HeadBeforeTime = Ptr(h.Time)
				}
				if len(o.Headers) > 1 {
					h := o.Headers[len(o.Headers)-1]
					p.HeadAfterNumber = Ptr(h.Number)
					p.HeadAfterHash = Ptr(h.Hash)
					p.HeadAfterTime = Ptr(h.Time)
				}
				if len(o.Headers) < 2 && p.Status != "rpc_error" && p.Status != "timeout" && p.Status != "revert" && p.Status != "tvm_failure" {
					p.Status = "unknown"
					p.ErrorCode = "missing_head_range"
				}
			}
		}
		completedAt := UTC(c.Now())
		if o.Kind == "enrichment" {
			for i := range o.Batch.Events {
				o.Batch.Events[i].AvailableAt = completedAt
			}
			for i := range o.Batch.Receipts {
				o.Batch.Receipts[i].AvailableAt = completedAt
			}
		}
		for i := range o.Batch.Costs {
			q := &o.Batch.Costs[i]
			if q.ObservationKind == "chain_resource" && q.Status != "error" {
				q.AvailableAt = completedAt
				if len(o.Headers) > 0 {
					q.HeadBeforeNumber = Ptr(o.Headers[0].Number)
					q.HeadBeforeHash = Ptr(o.Headers[0].Hash)
				}
				if len(o.Headers) > 1 {
					q.HeadAfterNumber = Ptr(o.Headers[len(o.Headers)-1].Number)
					q.HeadAfterHash = Ptr(o.Headers[len(o.Headers)-1].Hash)
				}
				if len(o.Headers) < 2 || q.ContractUserResourcePercent == nil {
					q.Status = "partial"
					q.Reason = "incomplete_resource_context"
				}
			}
		}
		cap := &o.Batch.Capture
		if o.Kind == "page" && !o.Failed && !cap.PaginationExhausted {
			cap.Status = "partial"
			cap.Reason = "pagination_continues"
			if s := c.scan(o.ScanID); s != nil && s.MaxPages > 0 && s.Pages+1 >= s.MaxPages {
				cap.Reason = "page_budget_exhausted"
			}
		}
		cap.AvailableAt = completedAt
		cap.ExpectedTasks = uint32(len(o.Evidence) + len(o.Requests))
		cap.CompletedTasks = uint32(len(o.Evidence))
		if cap.ExpectedTasks < cap.CompletedTasks {
			cap.ExpectedTasks = cap.CompletedTasks
		}
		manifest := Manifest{Version: "keeper-v1", DigestEncoding: "jl-keeper-fact-v1", Capture: cap.CaptureId.String(), Kind: o.Kind, ScanID: o.ScanID, FingerprintOut: o.NextFingerprint, Requests: o.Evidence, CohortID: c.State.CohortId.String(), Selection: "stratified-15-15-20-fixed-hash-keeper-cohort-v1", Rotation: c.State.Rotation, Changes: c.State.CohortChanges, ExcludedBoundary: o.ExcludedBoundary}
		manifest.ExcludedResource = o.ExcludedResource
		if o.Parent != nil {
			manifest.ParentCapture = o.Parent.CaptureId.String()
			manifest.ParentManifestHash = Hex(o.Parent.EvidenceManifestHash)
		}
		if sc := c.scan(o.ScanID); sc != nil {
			manifest.FingerprintIn = sc.Fingerprint
			// The request path is the authoritative envelope for legacy attempts.
			// Only new aligned scans advertise the new query contract.
			if len(sc.ID) >= 9 && sc.ID[:9] == "watch:v2:" {
				lo, hi := eventQueryBounds(sc.From, sc.To)
				manifest.EventFilterRevision = "second-envelope-v1"
				manifest.QueryFrom, manifest.QueryTo = &lo, &hi
			}
		}
		for _, can := range c.State.Cohort {
			manifest.Cohort = append(manifest.Cohort, Snapshot(can))
		}
		if o.Candidate != nil {
			manifest.ProbeLifecycle = o.Candidate.Lifecycle
		}

		// Keep the summary hash for compatibility, without archiving its body.
		cap.EvidenceManifestHash = Hash(JSONEvidence(manifest))
		if e := c.prepareIndexPage(o); e != nil {
			return e
		}
		Seal(&o.Batch)
		o.Frozen = true
		if e := c.Save(); e != nil {
			return e
		}
	}
	// Old frozen batches retain their capture and member digests unchanged.
	// Only add the separate pagination record, recovered from saved progress.
	if e := c.prepareIndexPage(o); e != nil {
		return e
	}
	if e := c.Store.WriteKeeperBatch(ctx, o.Batch); e != nil {
		return e
	}
	if o.Kind == "identity" {
		c.State.IdentityAt = o.Batch.Capture.AvailableAt
		c.State.IdentityStatus = "unknown"
		if !o.Failed {
			c.State.Implementation = o.IdentityAddress
			c.State.CodeHash = o.IdentityCode
			c.State.CallersValid = o.ValidCallers
			c.State.IdentityStatus = "verified_manifest"
		} else {
			c.State.IdentityStatus = "changed"
		}
	}
	if o.Kind == "page" || o.Kind == "index_page" {
		s := c.scan(o.ScanID)
		if s != nil {
			if o.Failed {
				s.Failed = true
				s.Done = true
				s.Failures++
				delay := time.Minute << min(s.Failures-1, 5)
				s.RetryAt = UTC(c.Now().Add(min(delay, 30*time.Minute)))
				if o.Batch.Capture.Reason == "legacy_prefix_reindexed" {
					s.Fingerprint = ""
					s.Pages = 0
					s.Done = false
					s.Failed = false
					s.RetryAt = time.Time{}
				}
			} else {
				s.Failures = 0
				s.RetryAt = time.Time{}
				s.Pages++
				s.Fingerprint = o.NextFingerprint
				s.Done = s.Fingerprint == ""
				if s.Done && strings.HasPrefix(s.ID, "history:") {
					c.State.HistoryCursor = s.To
				}
				if s.Done && (s.Mode == "live" || (s.Mode == "catchup" && len(s.ID) > 6 && s.ID[:6] == "watch:")) {
					c.State.EventCursors[s.Kind] = s.To
				}
				if s.MaxPages > 0 && s.Pages >= s.MaxPages && !s.Done {
					s.Done = true
					s.Failed = true
				}
				if s.Mode == "bootstrap" {
					c.AddCandidates(o.Discoveries)
				}
				if o.Kind == "page" && c.State.IndexingEnabled {
					if s.Done && realtimeScan(*s) && !s.From.After(c.State.EvidenceCursors[s.Kind]) {
						c.State.EvidenceCursors[s.Kind] = s.To
					}
					if !s.Done {
						s.Fingerprint = ""
						s.Pages = 0
					}
				}
			}
		}
	}
	if (o.Kind == "page" || o.Kind == "index_page") && !o.Failed && o.Batch.Capture.CaptureMode != "bootstrap" && o.Batch.Capture.EventKind != "Liquidate" {
		c.AddCandidates(o.Discoveries)
	}
	if o.Kind == "seed" || o.Kind == "page" || o.Kind == "enrichment" {
		c.ApplyEvents(o.Batch.Events)
	}
	if e := c.evidenceProgress(ctx, *o); e != nil {
		return e
	}
	if o.Kind == "seed" && o.Failed && o.Candidate != nil {
		for i := range c.State.Cohort {
			can := &c.State.Cohort[i]
			if can.Renter == o.Candidate.Renter && can.Receiver == o.Candidate.Receiver {
				can.SeedNext = UTC(c.Now().Add(30 * time.Minute))
			}
		}
	}
	c.Log(fmt.Sprintf("capture=%s kind=%s status=%s indexed=%d events=%d receipts=%d probes=%d costs=%d reason=%s", o.Batch.Capture.CaptureId, o.Kind, o.Batch.Capture.Status, len(o.Batch.IndexedEvents), len(o.Batch.Events), len(o.Batch.Receipts), len(o.Batch.Probes), len(o.Batch.Costs), o.Batch.Capture.Reason))
	return nil
}
func (c *Collector) Step(ctx context.Context) (bool, time.Time, error) {
	now := c.Now()
	next := now.Add(time.Second)
	sort.SliceStable(c.State.Ops, func(i, j int) bool { return c.operationPriority(c.State.Ops[i]) < c.operationPriority(c.State.Ops[j]) })
	for i := 0; i < len(c.State.Ops); i++ {
		o := &c.State.Ops[i]
		if o.Frozen || len(o.Requests) == 0 {
			if now.Before(o.NotBefore) {
				if o.NotBefore.Before(next) {
					next = o.NotBefore
				}
				continue
			}
			if e := c.Finish(ctx, o); e != nil {
				o.NotBefore = now.Add(5 * time.Second)
				if se := c.Save(); se != nil {
					return false, next, se
				}
				return false, o.NotBefore, e
			}
			c.State.Ops = append(c.State.Ops[:i], c.State.Ops[i+1:]...)
			return true, next, c.Save()
		}
		r := o.Requests[0]
		if o.Kind == "probe" && !o.probeAttempted() && now.After(o.Scheduled.Add(30*time.Second)) {
			o.Failed = true
			o.Batch.Capture.Status = "skipped"
			o.Batch.Capture.Reason = "stale_probe_round"
			o.Requests = nil
			c.rotate(i)
			return true, next, c.Save()
		}
		ready := c.API.Limiter.Ready(r.Source, r.Background)
		if ready.Before(r.NotBefore) {
			ready = r.NotBefore
		}
		if ready.Before(o.NotBefore) {
			ready = o.NotBefore
		}
		if now.Before(ready) || c.State.Sources[r.Source].Blocked {
			if ready.Before(next) {
				next = ready
			}
			continue
		}
		ev, b, e := c.API.Send(ctx, r)
		if e != nil {
			return false, next, e
		}
		o.Evidence = append(o.Evidence, ev)
		if ev.Error != "" {
			if r.Role == "probe" {
				p := c.ErrorProbe(o, ev)
				o.Batch.Probes = append(o.Batch.Probes, p)
			}
			if r.Role == "quote" || r.Role == "parameters" {
				c.ErrorCost(o, r, ev)
			}
			r.Attempts++
			if (ev.Status >= 500 || ev.Status == 0) && r.Attempts < 3 {
				delay := 5 * time.Second
				if r.Attempts == 2 {
					delay = 15 * time.Second
				}
				r.NotBefore = c.Now().Add(delay)
				o.Requests[0] = r
				c.rotate(i)
				return true, next, c.Save()
			}
			o.Failed = true
			o.Batch.Capture.Status = "error"
			o.Batch.Capture.Reason = ev.Error
			o.Requests = nil
			c.rotate(i)
			return true, next, c.Save()
		}
		o.Requests = o.Requests[1:]
		if e = c.Process(o, r, ev, b); e != nil {
			if e.Error() == "solid_hash_conflict" {
				return false, next, e
			}
			o.Failed = true
			o.Batch.Capture.Status = "partial"
			o.Batch.Capture.Reason = e.Error()
			o.Requests = nil
		}
		c.rotate(i)
		return true, next, c.Save()
	}
	return false, next, nil
}

// Once a constant call was attempted, retain its observation and finish its
// after-head even when the admission deadline has passed (also for old state).
func (o Operation) probeAttempted() bool {
	if len(o.Batch.Probes) > 0 {
		return true
	}
	for _, ev := range o.Evidence {
		if ev.Path == "/wallet/triggerconstantcontract" {
			return true
		}
	}
	return false
}
func (c *Collector) AddCandidates(events []RawEvent) {
	for _, re := range events {
		r, e := EventRow(re)
		if e != nil {
			continue
		}
		found := -1
		for i, can := range c.State.Candidates {
			if can.Renter == r.Renter && can.Receiver == r.Receiver {
				found = i
				break
			}
		}
		can := Candidate{Renter: r.Renter, Receiver: r.Receiver, LastActivity: r.BlockTime, Origin: re}
		if found >= 0 {
			old := c.State.Candidates[found]
			if can.LastActivity.Equal(old.LastActivity) && (can.Origin.Transaction != old.Origin.Transaction || can.Origin.Index != old.Origin.Index) {
				c.State.Candidates[found].Ambiguous = true
			}
			if can.LastActivity.After(old.LastActivity) {
				c.State.Candidates[found] = can
			}
		} else {
			c.State.Candidates = append(c.State.Candidates, can)
		}
	}
	// This is a bounded replacement cache, not a population estimate. All source
	// pages remain in the evidence archive; selected cohort members live separately.
	if len(c.State.Candidates) > 1500 {
		sort.Slice(c.State.Candidates, func(i, j int) bool {
			a, b := c.State.Candidates[i], c.State.Candidates[j]
			return Hash([]byte("keeper-cohort-v1"+a.Renter+a.Receiver)) < Hash([]byte("keeper-cohort-v1"+b.Renter+b.Receiver))
		})
		limits := [3]int{500, 500, 500}
		kept := c.State.Candidates[:0]
		for _, can := range c.State.Candidates {
			a := age(can.LastActivity, c.Now())
			if limits[a] > 0 {
				kept = append(kept, can)
				limits[a]--
			}
		}
		c.State.Candidates = kept
	}
}
func (c *Collector) ApplyEvents(events []RentalEvent) {
	sort.Slice(events, func(i, j int) bool {
		a, b := events[i], events[j]
		if a.BlockNumber != b.BlockNumber {
			return a.BlockNumber < b.BlockNumber
		}
		if *a.TransactionIndex != *b.TransactionIndex {
			return *a.TransactionIndex < *b.TransactionIndex
		}
		return *a.ReceiptLogIndex < *b.ReceiptLogIndex
	})
	for _, r := range events {
		targets := []*Candidate{}
		for i := range c.State.Cohort {
			targets = append(targets, &c.State.Cohort[i])
		}
		for i := range c.State.Candidates {
			targets = append(targets, &c.State.Candidates[i])
		}
		for _, can := range targets {
			if can.Renter != r.Renter || can.Receiver != r.Receiver || r.BlockTime.Before(can.LastActivity) {
				continue
			}
			if can.Ambiguous && r.BlockNumber <= can.Origin.BlockNumber {
				continue
			}
			if can.LastNumber > r.BlockNumber || (can.LastNumber == r.BlockNumber && (can.LastTx > *r.TransactionIndex || (can.LastTx == *r.TransactionIndex && can.LastLog >= *r.ReceiptLogIndex))) {
				continue
			}
			can.Ambiguous = false
			can.LastNumber = r.BlockNumber
			can.LastTx = *r.TransactionIndex
			can.LastLog = *r.ReceiptLogIndex
			can.Verified = true
			can.LastActivity = r.BlockTime
			if r.EventKind == "rent" {
				if can.Closed || can.Lifecycle == "" {
					can.Lifecycle = Hex(r.TxId) + ":" + strconv.Itoa(int(*r.ReceiptLogIndex))
				}
				can.Closed = false
				can.Origin = RawFromRow(r)
			} else if r.EventKind == "liquidate" || (r.EventKind == "return" && r.AmountSun.Cmp(big.NewInt(0)) == 0) {
				can.Closed = true
			}
		}
	}
}

func (c *Collector) rotate(i int) {
	o := c.State.Ops[i]
	c.State.Ops = append(append(c.State.Ops[:i], c.State.Ops[i+1:]...), o)
}
