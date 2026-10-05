package options

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/model"
)

const MaxLiveBooks = 32

type LiveMember struct {
	InstrumentID                    uint32
	Symbol, DefinitionHash, IndexID string
}

// LiveRun is one immutable, bounded selection. Members are sorted by ID.
type LiveRun struct {
	ID                        uuid.UUID
	StartedAt                 time.Time
	RESTURL, WSURL, Selection string
	Members                   []LiveMember
	Indexes                   []string
	References                []SelectionReference
}

type SelectionReference struct {
	Symbol                 string
	Bid, Ask               int64
	SourceTime, ReceivedAt time.Time
	PayloadHash            string
}

func (r LiveRun) Hash() string { b, _ := json.Marshal(r); return PayloadHash(b) }
func (r LiveRun) Validate() error {
	if r.ID == uuid.Nil || !microTime(r.StartedAt) || r.RESTURL == "" || r.WSURL == "" || (r.Selection != "explicit" && r.Selection != "near_atm_v1" && r.Selection != CatalogSelection) || len(r.Members) == 0 || len(r.Members) > MaxLiveBooks || len(r.Indexes) == 0 || len(r.Indexes) > 4 {
		return fmt.Errorf("invalid live run")
	}
	seen := map[string]bool{}
	indexes := map[string]bool{}
	for n, m := range r.Members {
		if m.InstrumentID == 0 || m.Symbol == "" || seen[m.Symbol] || !model.ValidDigest(m.DefinitionHash) || !ValidIndex(m.IndexID) || (n > 0 && r.Members[n-1].InstrumentID >= m.InstrumentID) {
			return fmt.Errorf("invalid live membership")
		}
		seen[m.Symbol] = true
		indexes[m.IndexID] = true
	}
	if len(indexes) != len(r.Indexes) || !slices.IsSorted(r.Indexes) {
		return fmt.Errorf("index membership mismatch")
	}
	for n, id := range r.Indexes {
		if !indexes[id] || (n > 0 && r.Indexes[n-1] == id) {
			return fmt.Errorf("invalid index membership")
		}
	}
	for _, ref := range r.References {
		if !seen[ref.Symbol] || ref.Bid <= 0 || ref.Ask <= ref.Bid || !microTime(ref.SourceTime) || !microTime(ref.ReceivedAt) || ref.ReceivedAt.After(r.StartedAt) || !model.ValidDigest(ref.PayloadHash) {
			return fmt.Errorf("invalid selection reference")
		}
	}
	return nil
}

func ValidIndex(s string) bool {
	return s == "btc_usd" || s == "eth_usd" || s == "btc_usdc" || s == "eth_usdc"
}
func microTime(t time.Time) bool { return !t.IsZero() && t.Equal(t.UTC().Truncate(time.Microsecond)) }

// A missing index has no carried-over value. Held values keep original times.
type IndexState uint8

const (
	IndexMissing IndexState = iota
	IndexObserved
	IndexHeld
	IndexDisconnected
	IndexInvalid
)

type IndexSample struct {
	Price                  *decimal.Decimal
	SourceTime, ReceivedAt time.Time
	Epoch                  uuid.UUID
	State                  IndexState
}
type IndexMinute struct {
	IndexID string
	Samples [60]IndexSample
}

func (m IndexMinute) Validate(minute time.Time) error {
	if !ValidIndex(m.IndexID) {
		return fmt.Errorf("invalid index identity")
	}
	for sec, s := range m.Samples {
		if s.State > IndexInvalid {
			return fmt.Errorf("invalid index state")
		}
		valid := s.State == IndexObserved || s.State == IndexHeld
		if valid {
			if s.Price == nil || !s.Price.IsPositive() || ValidateDecimal(*s.Price) != nil || !microTime(s.SourceTime) || !microTime(s.ReceivedAt) || s.ReceivedAt.After(minute.Add(time.Duration(sec)*time.Second)) || s.Epoch == uuid.Nil {
				return fmt.Errorf("invalid index provenance/value")
			}
		} else if s.Price != nil || !s.SourceTime.IsZero() || !s.ReceivedAt.IsZero() {
			return fmt.Errorf("invalid index carries stale value")
		}
	}
	return nil
}
func writeText(h hash.Hash, s string) {
	_ = binary.Write(h, binary.LittleEndian, uint64(len(s)))
	_, _ = h.Write([]byte(s))
}
func (m IndexMinute) Hash() string {
	h := sha256.New()
	writeText(h, "options-index-minute-v1")
	writeText(h, m.IndexID)
	for _, s := range m.Samples {
		_, _ = h.Write([]byte{byte(s.State)})
		_, _ = h.Write(s.Epoch[:])
		for _, t := range []time.Time{s.SourceTime, s.ReceivedAt} {
			writeText(h, t.UTC().Format(time.RFC3339Nano))
		}
		value := ""
		if s.Price != nil {
			value = s.Price.String()
		}
		writeText(h, value)
	}
	return hex.EncodeToString(h.Sum(nil))
}

type LiveEnvelope struct {
	RunID                  uuid.UUID
	RunHash                string
	MinuteTime, PreparedAt time.Time
	Books                  []model.DerivativeMinuteBatch
	Indexes                []IndexMinute
}

func (e LiveEnvelope) Validate() error {
	if e.RunID == uuid.Nil || !model.ValidDigest(e.RunHash) || !microTime(e.MinuteTime) || !e.MinuteTime.Equal(e.MinuteTime.Truncate(time.Minute)) || !microTime(e.PreparedAt) || e.PreparedAt.Before(e.MinuteTime.Add(59*time.Second)) || len(e.Books) > MaxLiveBooks || len(e.Indexes) == 0 || len(e.Indexes) > 4 {
		return fmt.Errorf("invalid live envelope")
	}
	if err := ValidateBookBatches(e.MinuteTime, e.Books); err != nil {
		return err
	}
	for _, b := range e.Books {
		for _, q := range b.Quality {
			if q.ReplayValid && (!q.MarketOpen || q.TradingRuleID == "") {
				return fmt.Errorf("live replay requires open market and published rule")
			}
		}
	}
	for n, m := range e.Indexes {
		if n > 0 && e.Indexes[n-1].IndexID >= m.IndexID {
			return fmt.Errorf("unsorted/duplicate indexes")
		}
		if err := m.Validate(e.MinuteTime); err != nil {
			return err
		}
	}
	return nil
}
func (e LiveEnvelope) ValidateRun(r LiveRun) error {
	if err := r.Validate(); err != nil {
		return err
	}
	if err := e.Validate(); err != nil {
		return err
	}
	if e.RunID != r.ID || e.RunHash != r.Hash() || !e.MinuteTime.After(r.StartedAt) || len(e.Books) != len(r.Members) || len(e.Indexes) != len(r.Indexes) {
		return fmt.Errorf("live envelope/run mismatch")
	}
	for n, b := range e.Books {
		if b.InstrumentID != r.Members[n].InstrumentID || b.Signed {
			return fmt.Errorf("live book membership mismatch")
		}
	}
	for n, m := range e.Indexes {
		if m.IndexID != r.Indexes[n] {
			return fmt.Errorf("live index membership mismatch")
		}
	}
	return nil
}
func (e LiveEnvelope) ID() string {
	h := sha256.New()
	writeText(h, "deribit-live-minute-v1")
	_, _ = h.Write(e.RunID[:])
	writeText(h, e.RunHash)
	writeText(h, e.MinuteTime.UTC().Format(time.RFC3339Nano))
	writeText(h, e.PreparedAt.UTC().Format(time.RFC3339Nano))
	for _, b := range e.Books {
		writeText(h, model.DerivativeBatchHash(b))
	}
	for _, m := range e.Indexes {
		writeText(h, m.Hash())
	}
	return hex.EncodeToString(h.Sum(nil))
}
func (e LiveEnvelope) Clone() LiveEnvelope {
	e.Books = append([]model.DerivativeMinuteBatch(nil), e.Books...)
	for n := range e.Books {
		e.Books[n] = model.CloneDerivativeBatch(e.Books[n])
	}
	e.Indexes = append([]IndexMinute(nil), e.Indexes...)
	for n := range e.Indexes {
		for s := range e.Indexes[n].Samples {
			if p := e.Indexes[n].Samples[s].Price; p != nil {
				v := *p
				e.Indexes[n].Samples[s].Price = &v
			}
		}
	}
	return e
}

type MetadataObservation struct {
	ScopeComplete                                             bool
	ScopeRawCount, ScopeAcceptedCount, ScopeExcludedCount     uint32
	AttemptID, RunID                                          uuid.UUID
	InstrumentID                                              uint32
	Symbol, Scope, SourceURL                                  string
	RequestedAt, ObservedAt                                   time.Time
	PayloadHash, Status, DefinitionHash, TradingRuleID, State string
	Active                                                    bool
}

func (o MetadataObservation) Validate() error {
	if o.AttemptID == uuid.Nil || o.RunID == uuid.Nil || o.InstrumentID == 0 || o.Symbol == "" || o.Scope == "" || o.SourceURL == "" || !microTime(o.RequestedAt) || !microTime(o.ObservedAt) || o.ObservedAt.Before(o.RequestedAt) {
		return fmt.Errorf("invalid metadata observation")
	}
	if o.PayloadHash != "" && !model.ValidDigest(o.PayloadHash) {
		return fmt.Errorf("invalid metadata hash")
	}
	if o.ScopeComplete {
		if uint64(o.ScopeAcceptedCount)+uint64(o.ScopeExcludedCount) != uint64(o.ScopeRawCount) || !model.ValidDigest(o.PayloadHash) {
			return fmt.Errorf("invalid scope counts/evidence")
		}
	} else if o.ScopeRawCount != 0 || o.ScopeAcceptedCount != 0 || o.ScopeExcludedCount != 0 {
		return fmt.Errorf("incomplete scope has counts")
	}
	switch o.Status {
	case "complete":
		if !o.ScopeComplete || o.ScopeAcceptedCount == 0 {
			return fmt.Errorf("member success without complete scope")
		}
		if !model.ValidDigest(o.PayloadHash) || !model.ValidDigest(o.DefinitionHash) || !model.ValidDigest(o.TradingRuleID) || !ValidMarketState(o.State) {
			return fmt.Errorf("incomplete metadata success")
		}
	case "request_error", "parse_error", "missing", "definition_changed":
		if o.ScopeComplete != (o.Status == "missing" || o.Status == "definition_changed") {
			return fmt.Errorf("metadata status disagrees with scope completeness")
		}
		if o.TradingRuleID != "" || o.Active || o.State != "" {
			return fmt.Errorf("failed metadata carries current facts")
		}
		if o.Status != "request_error" && !model.ValidDigest(o.PayloadHash) {
			return fmt.Errorf("missing metadata evidence")
		}
	default:
		return fmt.Errorf("unknown metadata status")
	}
	return nil
}
func (o MetadataObservation) Hash() string { b, _ := json.Marshal(o); return PayloadHash(b) }
