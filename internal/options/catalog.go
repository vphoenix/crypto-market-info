package options

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/model"
)

const CatalogSelection = "catalog_v2"

// Both the sampler and writer require the actual referenced catalog evidence
// to be fresh. Refreshing another observation cannot extend its lifetime.
const CatalogEvidenceMaxAge = 35 * time.Minute

func ValidMarketState(s string) bool {
	return slices.Contains([]string{"open", "closed", "settlement", "delivered", "inactive", "locked", "halted", "archivized"}, s)
}

// Preserve the historical paired-selection contract; catalog shards admit
// individually verified contracts without analysis-specific pairing.
func ValidateLiveSpecs(selection string, specs []ContractSpec) error {
	if selection != CatalogSelection {
		return ValidateSelection(specs)
	}
	if len(specs) == 0 || len(specs) > MaxLiveBooks {
		return fmt.Errorf("invalid catalog shard size")
	}
	seen := map[string]bool{}
	for _, s := range specs {
		if err := s.Validate(); err != nil {
			return err
		}
		if s.Instrument.MarketType == model.MarketOptionCombo || !ValidIndex(s.IndexID) || seen[s.Instrument.ExchangeSymbol] {
			return fmt.Errorf("unsupported/duplicate catalog member")
		}
		seen[s.Instrument.ExchangeSymbol] = true
	}
	return nil
}

type CatalogObservation struct {
	ID              uuid.UUID `ch:"observation_id"`
	Scope           string    `ch:"scope"`
	Kind            string    `ch:"source_kind"`
	URL             string    `ch:"source_url"`
	RequestedAt     time.Time `ch:"requested_at"`
	ObservedAt      time.Time `ch:"observed_at"`
	PayloadHash     string    `ch:"payload_hash"`
	Status          string    `ch:"status"`
	RawCount        uint32    `ch:"raw_count"`
	NativeIDs       []uint64  `ch:"native_ids"`
	Symbols         []string  `ch:"symbols"`
	InstrumentIDs   []uint32  `ch:"instrument_ids"`
	Definitions     []string  `ch:"definition_hashes"`
	Rules           []string  `ch:"rule_ids"`
	States          []string  `ch:"states"`
	Active          []bool    `ch:"active"`
	ExcludedSymbols []string  `ch:"excluded_symbols"`
	ExcludedReasons []string  `ch:"excluded_reasons"`
	RowHash         string    `ch:"row_hash" json:"-"`
}

func (o CatalogObservation) Hash() string { return catalogDigest("options-catalog-observation-v2", o) }
func (o CatalogObservation) Validate() error {
	if o.ID == uuid.Nil || o.Scope == "" || o.URL == "" || !microTime(o.RequestedAt) || !microTime(o.ObservedAt) || o.ObservedAt.Before(o.RequestedAt) || !slices.Contains([]string{"catalog", "creation", "instrument"}, o.Kind) {
		return fmt.Errorf("invalid catalog provenance")
	}
	n := len(o.Symbols)
	if n != len(o.NativeIDs) || n != len(o.InstrumentIDs) || n != len(o.Definitions) || n != len(o.Rules) || n != len(o.States) || n != len(o.Active) || len(o.ExcludedSymbols) != len(o.ExcludedReasons) {
		return fmt.Errorf("invalid catalog arrays")
	}
	if o.Status != "complete" {
		if !slices.Contains([]string{"request_error", "parse_error"}, o.Status) || n != 0 || o.RawCount != 0 || len(o.ExcludedSymbols) != 0 || (o.PayloadHash != "" && !model.ValidDigest(o.PayloadHash)) {
			return fmt.Errorf("failed catalog carries facts")
		}
		return nil
	}
	if !model.ValidDigest(o.PayloadHash) || uint64(n)+uint64(len(o.ExcludedSymbols)) != uint64(o.RawCount) {
		return fmt.Errorf("incomplete catalog evidence")
	}
	seen, native := map[string]bool{}, map[uint64]bool{}
	for i, name := range o.Symbols {
		if name == "" || seen[name] || o.NativeIDs[i] == 0 || native[o.NativeIDs[i]] || o.InstrumentIDs[i] == 0 || !model.ValidDigest(o.Definitions[i]) || !model.ValidDigest(o.Rules[i]) || !ValidMarketState(o.States[i]) {
			return fmt.Errorf("invalid catalog identity/rule")
		}
		seen[name], native[o.NativeIDs[i]] = true, true
	}
	for i, name := range o.ExcludedSymbols {
		if name == "" || seen[name] || o.ExcludedReasons[i] == "" {
			return fmt.Errorf("invalid exclusion")
		}
		seen[name] = true
	}
	return nil
}

type LifecycleObservation struct {
	ID            uuid.UUID  `ch:"observation_id"`
	Kind          string     `ch:"kind"`
	Channel       string     `ch:"source_channel"`
	Epoch         uuid.UUID  `ch:"epoch"`
	Sequence      uint64     `ch:"ingress_sequence"`
	SourceTime    *time.Time `ch:"source_time"`
	ReceivedAt    time.Time  `ch:"received_at"`
	PayloadHash   string     `ch:"payload_hash"`
	Symbol        string     `ch:"symbol"`
	State         string     `ch:"state"`
	IndexID       string     `ch:"index_id"`
	Locked        *bool      `ch:"locked"`
	Maintenance   *bool      `ch:"maintenance"`
	LockMode      string     `ch:"lock_mode"`
	LockedIndexes []string   `ch:"locked_indices"`
	RowHash       string     `ch:"row_hash" json:"-"`
}

func (o LifecycleObservation) Hash() string {
	return catalogDigest("options-lifecycle-observation-v2", o)
}
func (o LifecycleObservation) Validate() error {
	if o.ID == uuid.Nil || o.Epoch == uuid.Nil || o.Sequence == 0 || o.Channel == "" || !microTime(o.ReceivedAt) || !model.ValidDigest(o.PayloadHash) || (o.SourceTime != nil && (!microTime(*o.SourceTime) || o.SourceTime.After(o.ReceivedAt.Add(time.Second)))) {
		return fmt.Errorf("invalid lifecycle provenance")
	}
	switch o.Kind {
	case "state":
		if o.Symbol == "" || !ValidMarketState(o.State) || o.SourceTime == nil || o.LockMode != "" || o.Locked != nil || o.Maintenance != nil {
			return fmt.Errorf("invalid state event")
		}
	case "platform":
		if o.Maintenance == nil && (o.Locked == nil || (o.IndexID == "" || strings.ContainsAny(o.IndexID, " \t\n"))) {
			return fmt.Errorf("invalid platform event")
		}
	case "status":
		if !slices.Contains([]string{"true", "partial", "false"}, o.LockMode) || (o.LockMode == "partial" && len(o.LockedIndexes) == 0) {
			return fmt.Errorf("invalid lock baseline")
		}
		seen := map[string]bool{}
		for _, id := range o.LockedIndexes {
			if id == "" || seen[id] {
				return fmt.Errorf("invalid locked index list")
			}
			seen[id] = true
		}
		if o.LockMode == "false" && len(o.LockedIndexes) > 0 {
			return fmt.Errorf("contradictory lock baseline")
		}
	case "error":
	default:
		return fmt.Errorf("unknown lifecycle kind")
	}
	return nil
}

type CollectionPlan struct {
	ID              uuid.UUID   `ch:"plan_id"`
	SessionID       uuid.UUID   `ch:"session_id"`
	Revision        uint64      `ch:"revision"`
	PreviousID      uuid.UUID   `ch:"previous_plan_id"`
	PreviousHash    string      `ch:"previous_plan_hash"`
	ConfigHash      string      `ch:"config_hash"`
	CreatedAt       time.Time   `ch:"created_at"`
	EffectiveMinute time.Time   `ch:"effective_minute"`
	InstrumentIDs   []uint32    `ch:"instrument_ids"`
	Definitions     []string    `ch:"definition_hashes"`
	RunIDs          []uuid.UUID `ch:"run_ids"`
	ObservationIDs  []uuid.UUID `ch:"observation_ids"`
	PendingSymbols  []string    `ch:"pending_symbols"`
	PendingReasons  []string    `ch:"pending_reasons"`
	ExcludedSymbols []string    `ch:"excluded_symbols"`
	ExcludedReasons []string    `ch:"excluded_reasons"`
	RowHash         string      `ch:"row_hash" json:"-"`
}

func (p CollectionPlan) Hash() string { return catalogDigest("options-collection-plan-v2", p) }
func (p CollectionPlan) Validate() error {
	n := len(p.InstrumentIDs)
	if p.ID == uuid.Nil || p.SessionID == uuid.Nil || p.Revision == 0 || !model.ValidDigest(p.ConfigHash) || !microTime(p.CreatedAt) || !p.EffectiveMinute.Equal(p.EffectiveMinute.Truncate(time.Minute)) || !p.EffectiveMinute.After(p.CreatedAt) || n != len(p.Definitions) || n != len(p.RunIDs) || len(p.PendingSymbols) != len(p.PendingReasons) || len(p.ExcludedSymbols) != len(p.ExcludedReasons) {
		return fmt.Errorf("invalid collection plan")
	}
	if (p.PreviousID == uuid.Nil) != (p.PreviousHash == "") || (p.PreviousHash != "" && !model.ValidDigest(p.PreviousHash)) {
		return fmt.Errorf("invalid plan predecessor")
	}
	for i, id := range p.InstrumentIDs {
		if id == 0 || (i > 0 && id <= p.InstrumentIDs[i-1]) || p.RunIDs[i] == uuid.Nil || !model.ValidDigest(p.Definitions[i]) {
			return fmt.Errorf("duplicate/invalid plan owner")
		}
	}
	for i, name := range p.PendingSymbols {
		if name == "" || p.PendingReasons[i] == "" {
			return fmt.Errorf("invalid pending member")
		}
	}
	return nil
}
func (p CollectionPlan) ValidateSuccessor(previous *CollectionPlan) error {
	if err := p.Validate(); err != nil {
		return err
	}
	if previous == nil {
		if p.PreviousID != uuid.Nil {
			return fmt.Errorf("missing plan predecessor")
		}
		return nil
	}
	if p.PreviousID != previous.ID || p.PreviousHash != previous.Hash() || !p.EffectiveMinute.After(previous.EffectiveMinute) || (p.SessionID == previous.SessionID && p.Revision != previous.Revision+1) || (p.SessionID != previous.SessionID && p.Revision != 1) {
		return fmt.Errorf("collection plan fork/order conflict")
	}
	return nil
}
func (p CollectionPlan) ValidateRun(r LiveRun, minute time.Time) error {
	if minute.Before(p.EffectiveMinute) || r.Selection != CatalogSelection {
		return fmt.Errorf("plan not effective/catalog run required")
	}
	var members []uint32
	for n, id := range p.InstrumentIDs {
		if p.RunIDs[n] == r.ID {
			members = append(members, id)
			m := slices.IndexFunc(r.Members, func(m LiveMember) bool { return m.InstrumentID == id })
			if m < 0 || r.Members[m].DefinitionHash != p.Definitions[n] {
				return fmt.Errorf("plan definition mismatch")
			}
		}
	}
	if len(members) != len(r.Members) {
		return fmt.Errorf("plan/run membership mismatch")
	}
	return nil
}

type QualityEvidence struct {
	RunID              uuid.UUID    `ch:"run_id"`
	InstrumentID       uint32       `ch:"instrument_id"`
	MinuteTime         time.Time    `ch:"minute_time"`
	BatchID            string       `ch:"batch_id"`
	StateIDs           []*uuid.UUID `ch:"state_ids"`
	StateKinds         []uint8      `ch:"state_kinds"`
	RuleObservationIDs []*uuid.UUID `ch:"rule_observation_ids"`
	PlatformIDs        []*uuid.UUID `ch:"platform_ids"`
	MaintenanceIDs     []*uuid.UUID `ch:"maintenance_ids"`
	LockIDs            []*uuid.UUID `ch:"lock_ids"`
	LifecycleEpochs    []uuid.UUID  `ch:"lifecycle_epochs"`
	LifecycleConfirmed []*time.Time `ch:"lifecycle_confirmed"`
	RowHash            string       `ch:"row_hash" json:"-"`
}

func (e QualityEvidence) Hash() string {
	e.BatchID = ""
	return catalogDigest("options-quality-evidence-v2", e)
}
func (e QualityEvidence) Validate() error {
	if e.RunID == uuid.Nil || e.InstrumentID == 0 || !e.MinuteTime.Equal(e.MinuteTime.Truncate(time.Minute)) || len(e.StateIDs) != 60 || len(e.StateKinds) != 60 || len(e.RuleObservationIDs) != 60 || len(e.PlatformIDs) != 60 || len(e.MaintenanceIDs) != 60 || len(e.LockIDs) != 60 || len(e.LifecycleEpochs) != 60 || len(e.LifecycleConfirmed) != 60 {
		return fmt.Errorf("invalid quality evidence slots")
	}
	for s, k := range e.StateKinds {
		if k > 2 || (k == 0) != (e.StateIDs[s] == nil) {
			return fmt.Errorf("invalid state evidence kind")
		}
		for _, id := range []*uuid.UUID{e.StateIDs[s], e.RuleObservationIDs[s], e.PlatformIDs[s], e.MaintenanceIDs[s], e.LockIDs[s]} {
			if id != nil && *id == uuid.Nil {
				return fmt.Errorf("zero evidence identity")
			}
		}
	}
	return nil
}

type CatalogEnvelope struct {
	PlanID   uuid.UUID
	PlanHash string
	Live     LiveEnvelope
	Evidence []QualityEvidence
}

func (e CatalogEnvelope) ID() string {
	var hashes []string
	for _, q := range e.Evidence {
		hashes = append(hashes, q.Hash())
	}
	return catalogDigest("deribit-catalog-live-minute-v2", struct {
		PlanID           uuid.UUID
		PlanHash, LiveID string
		Evidence         []string
	}{e.PlanID, e.PlanHash, e.Live.ID(), hashes})
}
func (e CatalogEnvelope) Validate() error {
	if e.PlanID == uuid.Nil || !model.ValidDigest(e.PlanHash) {
		return fmt.Errorf("missing catalog plan")
	}
	if err := e.Live.Validate(); err != nil {
		return err
	}
	if len(e.Evidence) != len(e.Live.Books) {
		return fmt.Errorf("incomplete quality evidence")
	}
	for n, q := range e.Evidence {
		if err := q.Validate(); err != nil {
			return err
		}
		b := e.Live.Books[n]
		if q.RunID != e.Live.RunID || q.InstrumentID != b.InstrumentID || !q.MinuteTime.Equal(e.Live.MinuteTime) {
			return fmt.Errorf("evidence/book identity mismatch")
		}
		for s, v := range b.Quality {
			if v.MarketKnown && (q.StateIDs[s] == nil || q.RuleObservationIDs[s] == nil || q.PlatformIDs[s] == nil || q.StateKinds[s] != v.MarketStateBasis) {
				return fmt.Errorf("known state without complete evidence")
			}
		}
	}
	return nil
}

// Canonical V2 arrays are independent of driver nil/empty representation.
// This never changes any historical V1 hash.
func catalogDigest(domain string, v any) string {
	b, err := json.Marshal(canonicalCatalog(reflect.ValueOf(v)))
	if err != nil {
		panic(err)
	}
	return PayloadHash(append([]byte(domain+"\x00"), b...))
}
func canonicalCatalog(v reflect.Value) any {
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil
		}
		return canonicalCatalog(v.Elem())
	}
	if v.CanInterface() {
		switch x := v.Interface().(type) {
		case time.Time:
			return x.UTC().Format(time.RFC3339Nano)
		case uuid.UUID:
			return x.String()
		case decimal.Decimal:
			return x.String()
		}
	}
	switch v.Kind() {
	case reflect.Struct:
		m := map[string]any{}
		for i := 0; i < v.NumField(); i++ {
			f := v.Type().Field(i)
			if f.PkgPath == "" && f.Tag.Get("json") != "-" {
				m[f.Name] = canonicalCatalog(v.Field(i))
			}
		}
		return m
	case reflect.Slice, reflect.Array:
		a := make([]any, v.Len())
		for i := range a {
			a[i] = canonicalCatalog(v.Index(i))
		}
		return a
	default:
		return v.Interface()
	}
}
