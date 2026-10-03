package lst

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"math/big"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

// CanonicalHash uses length-delimited bytes, never JSON string encoding: Ethereum
// hashes and addresses routinely contain invalid UTF-8. Nil and empty arrays are
// equivalent because ClickHouse Array has no nullable-array representation.
func CanonicalHash(v any) string {
	h := sha256.New()
	canonicalWrite(h, reflect.ValueOf(v))
	return string(h.Sum(nil))
}
func canonicalBytes(h hash.Hash, b []byte) {
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(len(b)))
	_, _ = h.Write(n[:])
	_, _ = h.Write(b)
}
func canonicalWrite(h hash.Hash, v reflect.Value) {
	if !v.IsValid() {
		canonicalBytes(h, []byte("nil"))
		return
	}
	if v.Kind() == reflect.Interface {
		canonicalWrite(h, v.Elem())
		return
	}
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			canonicalBytes(h, []byte("nil"))
			return
		}
		canonicalBytes(h, []byte("present"))
		canonicalWrite(h, v.Elem())
		return
	}
	if v.CanInterface() {
		switch x := v.Interface().(type) {
		case time.Time:
			canonicalBytes(h, []byte("time"))
			canonicalBytes(h, []byte(strconv.FormatInt(x.UnixMicro(), 10)))
			return
		case big.Int:
			canonicalBytes(h, []byte("integer"))
			canonicalBytes(h, []byte(x.String()))
			return
		case decimal.Decimal:
			canonicalBytes(h, []byte("decimal"))
			canonicalBytes(h, []byte(x.String()))
			return
		}
	}
	canonicalBytes(h, []byte(v.Kind().String()))
	switch v.Kind() {
	case reflect.String:
		canonicalBytes(h, []byte(v.String()))
	case reflect.Bool:
		if v.Bool() {
			canonicalBytes(h, []byte{1})
		} else {
			canonicalBytes(h, []byte{0})
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		canonicalBytes(h, []byte(strconv.FormatInt(v.Int(), 10)))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		canonicalBytes(h, []byte(strconv.FormatUint(v.Uint(), 10)))
	case reflect.Slice, reflect.Array:
		canonicalBytes(h, []byte(strconv.Itoa(v.Len())))
		for i := 0; i < v.Len(); i++ {
			canonicalWrite(h, v.Index(i))
		}
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			canonicalBytes(h, []byte(v.Type().Field(i).Name))
			canonicalWrite(h, v.Field(i))
		}
	default:
		panic("unsupported canonical LST value: " + v.Type().String())
	}
}

func rowHash(row any) string {
	v := reflect.ValueOf(row)
	copy := reflect.New(v.Type()).Elem()
	copy.Set(v)
	copy.FieldByName("RowHash").SetString("")
	return CanonicalHash(copy.Interface())
}

// CaptureIdentity hashes every frozen capture field. Only a finality revision may
// change Canonical/Finality/Revision; all source times and membership stay fixed.
func CaptureIdentity(c Capture) string {
	c.Canonical = false
	c.Finality = ""
	c.Revision = 0
	return CanonicalHash(c)
}

type member struct{ Table, Key, Hash string }

func batchRows(b Batch) []struct {
	table string
	rows  reflect.Value
} {
	return []struct {
		table string
		rows  reflect.Value
	}{
		{"protocol", reflect.ValueOf(b.Protocols)}, {"quote", reflect.ValueOf(b.Quotes)},
		{"request", reflect.ValueOf(b.Requests)}, {"finalization", reflect.ValueOf(b.Finalizations)},
		{"claim", reflect.ValueOf(b.Claims)}, {"funding", reflect.ValueOf(b.Funding)},
	}
}
func rowKey(row any) string {
	switch r := row.(type) {
	case ProtocolState:
		return "protocol"
	case Quote:
		return r.QuoteId
	case WithdrawalRequest:
		return CanonicalHash([]any{r.BlockNumber, r.TransactionHash, r.LogIndex})
	case WithdrawalFinalization:
		return CanonicalHash([]any{r.BlockNumber, r.TransactionHash, r.LogIndex})
	case WithdrawalClaim:
		return CanonicalHash([]any{r.BlockNumber, r.TransactionHash, r.LogIndex})
	case FundingSettlement:
		return CanonicalHash([]any{r.InstrumentId, r.FundingTime})
	default:
		panic("unknown LST row")
	}
}
func members(b Batch) ([]member, error) {
	out := []member{}
	seen := map[string]bool{}
	for _, group := range batchRows(b) {
		for i := 0; i < group.rows.Len(); i++ {
			row := group.rows.Index(i)
			key := rowKey(row.Interface())
			id := group.table + key
			if seen[id] {
				return nil, fmt.Errorf("duplicate_%s_member", group.table)
			}
			seen[id] = true
			out = append(out, member{group.table, key, row.FieldByName("RowHash").String()})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Table != out[j].Table {
			return out[i].Table < out[j].Table
		}
		return out[i].Key < out[j].Key
	})
	return out, nil
}

// Seal freezes counts and hashes. Caller must provide source/local times, source
// status, and expected row counts; sealing never promotes partial data to complete.
// A supplied EvidenceRootHash may include response evidence with no decoded rows
// (notably a successful empty log range), and is retained verbatim.
func (b *Batch) Seal() error {
	for _, group := range batchRows(*b) {
		for i := 0; i < group.rows.Len(); i++ {
			row := group.rows.Index(i)
			row.FieldByName("RowHash").SetString(rowHash(row.Interface()))
		}
	}
	c := &b.Capture
	c.ProtocolRows = uint32(len(b.Protocols))
	c.QuoteRows = uint32(len(b.Quotes))
	c.RequestRows = uint32(len(b.Requests))
	c.FinalizationRows = uint32(len(b.Finalizations))
	c.ClaimRows = uint32(len(b.Claims))
	c.FundingRows = uint32(len(b.Funding))
	m, e := members(*b)
	if e != nil {
		return e
	}
	c.FactDigest = CanonicalHash(m)
	if c.EvidenceRootHash == "" {
		unique := map[string]bool{}
		for _, group := range batchRows(*b) {
			for i := 0; i < group.rows.Len(); i++ {
				row := group.rows.Index(i)
				for j := 0; j < row.NumField(); j++ {
					name := row.Type().Field(j).Tag.Get("ch")
					v := row.Field(j)
					if strings.HasSuffix(name, "payload_hash") {
						if v.Kind() == reflect.Pointer {
							if v.IsNil() {
								continue
							}
							v = v.Elem()
						}
						unique[v.String()] = true
					} else if strings.HasSuffix(name, "payload_hashes") {
						for k := 0; k < v.Len(); k++ {
							unique[v.Index(k).String()] = true
						}
					}
				}
			}
		}
		hashes := []string{}
		for h := range unique {
			hashes = append(hashes, h)
		}
		sort.Strings(hashes)
		c.EvidenceRootHash = CanonicalHash(hashes)
	}
	c.Committed = true
	return Validate(*b)
}
func memberOf(s string, values ...string) bool {
	for _, v := range values {
		if s == v {
			return true
		}
	}
	return false
}
func goodHash(s string, n int) bool { return len(s) == n && (n == 4 || s != strings.Repeat("\x00", n)) }
func timeValid(t time.Time) bool {
	_, offset := t.Zone()
	return !t.IsZero() && offset == 0 && t.Nanosecond()%1000 == 0
}
func validateColumns(row any) error {
	v := reflect.ValueOf(row)
	for i := 0; i < v.NumField(); i++ {
		f := v.Type().Field(i)
		col := f.Tag.Get("lst")
		x := v.Field(i)
		if col == "" {
			continue
		}
		nullable := strings.HasPrefix(col, "Nullable(")
		if nullable {
			col = col[9 : len(col)-1]
			if x.IsNil() {
				continue
			}
			x = x.Elem()
		} else if x.Kind() == reflect.Pointer {
			if x.IsNil() {
				return fmt.Errorf("%s_required", f.Name)
			}
			x = x.Elem()
		}
		if strings.HasPrefix(col, "FixedString(") {
			n, _ := strconv.Atoi(col[12 : len(col)-1])
			if !goodHash(x.String(), n) {
				return fmt.Errorf("%s_invalid_bytes", f.Name)
			}
		} else if strings.HasPrefix(col, "Array(FixedString(") {
			n, _ := strconv.Atoi(col[18 : len(col)-2])
			for j := 0; j < x.Len(); j++ {
				if !goodHash(x.Index(j).String(), n) {
					return fmt.Errorf("%s_invalid_array_bytes", f.Name)
				}
			}
		} else if strings.HasPrefix(col, "DateTime64(") {
			if !timeValid(x.Interface().(time.Time)) {
				return fmt.Errorf("%s_invalid_utc_microsecond_time", f.Name)
			}
		} else if col == "UInt256" {
			n := x.Interface().(big.Int)
			if n.Sign() < 0 || n.BitLen() > 256 {
				return fmt.Errorf("%s_uint256_range", f.Name)
			}
		} else if strings.HasPrefix(col, "Decimal(") {
			n := x.Interface().(decimal.Decimal)
			if !n.Equal(n.Truncate(18)) || n.Abs().GreaterThanOrEqual(decimal.New(1, 20)) {
				return fmt.Errorf("%s_decimal38_18_range", f.Name)
			}
		}
	}
	return nil
}
func validateSourceTimes(requested, received, available *time.Time) error {
	if received != nil && (requested == nil || received.Before(*requested)) {
		return errors.New("source_receive_time_order")
	}
	if available != nil && (received == nil || available.Before(*received)) {
		return errors.New("source_available_time_order")
	}
	return nil
}

func Validate(b Batch) error {
	c := b.Capture
	if e := validateColumns(c); e != nil {
		return fmt.Errorf("capture: %w", e)
	}
	if c.CaptureId == uuid.Nil || c.SourceId == "" || !c.Committed {
		return errors.New("invalid_capture_identity_or_commit")
	}
	if !memberOf(c.CaptureKind, "market", "logs", "funding") || !memberOf(c.CaptureMode, "live", "backfill", "restart") || !memberOf(c.Status, "complete", "partial", "failed") || !memberOf(c.Finality, "head", "safe", "finalized", "orphaned", "not_applicable") {
		return errors.New("invalid_capture_enum")
	}
	if c.Finality == "orphaned" && c.Canonical {
		return errors.New("orphaned_canonical")
	}
	if c.AvailableAt.Before(c.StartedAt) || (c.ReceivedAt != nil && (c.ReceivedAt.Before(c.StartedAt) || c.AvailableAt.Before(*c.ReceivedAt))) {
		return errors.New("capture_time_order")
	}
	if c.ProtocolRows != uint32(len(b.Protocols)) || c.QuoteRows != uint32(len(b.Quotes)) || c.RequestRows != uint32(len(b.Requests)) || c.FinalizationRows != uint32(len(b.Finalizations)) || c.ClaimRows != uint32(len(b.Claims)) || c.FundingRows != uint32(len(b.Funding)) {
		return errors.New("capture_member_count_mismatch")
	}
	if c.ExpectedProtocolRows != c.ProtocolRows || c.ExpectedQuoteRows != c.QuoteRows {
		return errors.New("capture_expected_member_count_mismatch")
	}
	m, e := members(b)
	if e != nil {
		return e
	}
	if CanonicalHash(m) != c.FactDigest {
		return errors.New("capture_fact_digest_mismatch")
	}
	anchorFields := []bool{c.ChainId != nil, c.FromBlock != nil, c.ToBlock != nil, c.FromBlockHash != nil, c.ToBlockHash != nil, c.FromBlockTime != nil, c.ToBlockTime != nil}
	anchorCount := 0
	for _, set := range anchorFields {
		if set {
			anchorCount++
		}
	}
	if anchorCount != 0 && anchorCount != len(anchorFields) {
		return errors.New("partial_capture_chain_anchor")
	}
	if anchorCount > 0 && (*c.ChainId != 1 || *c.FromBlock > *c.ToBlock || c.ToBlockTime.Before(*c.FromBlockTime)) {
		return errors.New("invalid_capture_chain_anchor")
	}
	if c.CaptureKind == "market" {
		if len(b.Protocols) != 1 || len(b.Requests)+len(b.Finalizations)+len(b.Claims)+len(b.Funding) > 0 {
			return errors.New("market_membership")
		}
	}
	if c.CaptureKind == "logs" {
		if len(b.Protocols)+len(b.Quotes)+len(b.Funding) > 0 {
			return errors.New("logs_membership")
		}
		if c.Status == "complete" && (anchorCount == 0 || c.Finality != "finalized" || !c.Canonical) {
			return errors.New("logs_not_finalized")
		}
		if c.Status != "complete" && len(b.Requests)+len(b.Finalizations)+len(b.Claims) > 0 {
			return errors.New("incomplete_log_members")
		}
	}
	if c.CaptureKind == "funding" {
		if len(b.Protocols)+len(b.Quotes)+len(b.Requests)+len(b.Finalizations)+len(b.Claims) > 0 || c.Finality != "not_applicable" || anchorCount != 0 {
			return errors.New("funding_membership_or_anchor")
		}
		if c.Status == "complete" && (c.WindowFromAt == nil || c.WindowToAt == nil || c.WindowToAt.Before(*c.WindowFromAt)) {
			return errors.New("funding_window_required")
		}
	}
	for _, group := range batchRows(b) {
		for i := 0; i < group.rows.Len(); i++ {
			v := group.rows.Index(i)
			row := v.Interface()
			if e := validateColumns(row); e != nil {
				return fmt.Errorf("%s[%d]: %w", group.table, i, e)
			}
			if v.FieldByName("CaptureId").Interface().(uuid.UUID) != c.CaptureId {
				return errors.New("child_capture_mismatch")
			}
			if v.FieldByName("RowHash").String() != rowHash(row) {
				return fmt.Errorf("%s_row_hash_mismatch", group.table)
			}
			if v.FieldByName("AvailableAt").Interface().(time.Time).After(c.AvailableAt) {
				return errors.New("child_available_after_capture")
			}
			switch r := row.(type) {
			case ProtocolState:
				if !memberOf(r.StateStatus, "ok", "unknown", "failed") {
					return errors.New("invalid_protocol_status")
				}
				if r.ChainId != 1 {
					return errors.New("protocol_chain")
				}
				if e := validateSourceTimes(r.RequestedAt, r.ReceivedAt, r.SourceAvailableAt); e != nil {
					return e
				}
				if r.StateStatus == "ok" {
					if !r.IdentityOk || r.BlockNumber == nil || r.BlockHash == nil || r.BlockTime == nil || r.RequestedAt == nil || r.ReceivedAt == nil || r.SourceAvailableAt == nil || len(r.PayloadHashes) == 0 || anchorCount == 0 {
						return errors.New("successful_protocol_missing_evidence")
					}
					if *r.BlockNumber != *c.ToBlock || *r.BlockHash != *c.ToBlockHash || !r.BlockTime.Equal(*c.ToBlockTime) {
						return errors.New("protocol_capture_anchor_mismatch")
					}
				}
			case Quote:
				for _, status := range []string{r.BuyStatus, r.ConversionStatus, r.ExitStatus, r.HedgeStatus} {
					if !memberOf(status, "ok", "unknown", "failed") {
						return errors.New("invalid_quote_leg_status")
					}
				}
				if !memberOf(r.TimingStatus, "fresh", "stale", "late", "missed", "unknown") {
					return errors.New("invalid_quote_timing_status")
				}
				if !memberOf(r.QuoteRole, "entry", "followup") || r.RouteId == "" || r.HedgeInstrumentId == 0 {
					return errors.New("quote_identity")
				}
				if r.QuoteRole == "entry" && (r.PurchaseBudgetUsdtRaw == nil || r.PurchaseBudgetUsdtRaw.Sign() <= 0 || r.ReferenceQuoteId != nil) {
					return errors.New("entry_quote_identity")
				}
				if r.QuoteRole == "followup" && (r.PurchaseBudgetUsdtRaw != nil || r.ReferenceQuoteId == nil || r.TargetDelaySeconds == nil || r.PlannedForAt == nil || r.IsFollowupSeed) {
					return errors.New("followup_quote_identity")
				}
				for _, ts := range [][3]*time.Time{{r.ChainRequestedAt, r.ChainReceivedAt, r.ChainAvailableAt}, {r.HedgeDepthRequestedAt, r.HedgeDepthReceivedAt, r.HedgeDepthAvailableAt}, {r.MarkRequestedAt, r.MarkReceivedAt, r.MarkAvailableAt}} {
					if e := validateSourceTimes(ts[0], ts[1], ts[2]); e != nil {
						return e
					}
				}
				if r.BuyStatus == "ok" && (r.BuyLstOutRaw == nil || r.BuyWethOutWei == nil || r.ChainRequestedAt == nil || r.ChainReceivedAt == nil || r.ChainAvailableAt == nil || len(r.ChainPayloadHashes) == 0 || anchorCount == 0) {
					return errors.New("buy_quote_missing_evidence")
				}
				if r.ConversionStatus == "ok" && (r.RequestStethWei == nil || r.RequestSharesRaw == nil || r.RequestParts == nil || r.NominalRedeemEthWei == nil) {
					return errors.New("conversion_quote_missing_amounts")
				}
				if r.ExitStatus == "ok" && (r.EthExitInputWei == nil || r.EthExitUsdtOutRaw == nil || r.ChainRequestedAt == nil || r.ChainReceivedAt == nil || r.ChainAvailableAt == nil || len(r.ChainPayloadHashes) == 0 || anchorCount == 0) {
					return errors.New("exit_quote_missing_evidence")
				}
				if r.MarkPriceTickE8 != nil || r.IndexPriceTickE8 != nil || r.IndicatedFundingRate != nil || r.NextFundingTime != nil {
					if r.MarkSourceTime == nil || r.MarkPayloadHash == nil || r.MarkRequestedAt == nil || r.MarkReceivedAt == nil || r.MarkAvailableAt == nil {
						return errors.New("known_mark_missing_evidence")
					}
				}
				if r.HedgeStatus == "ok" && (r.HedgeQuantityLot == nil || r.HedgeEthWei == nil || r.UnhedgedEthResidualWei == nil || r.HedgeSellNotionalUsdtE8 == nil || r.HedgeBuyNotionalUsdtE8 == nil || r.HedgeDepthPayloadHash == nil || r.HedgeDepthRequestedAt == nil || r.HedgeDepthReceivedAt == nil || r.HedgeDepthAvailableAt == nil) {
					return errors.New("hedge_quote_missing_evidence")
				}
			case WithdrawalRequest:
				if e := validateEvent(c, r.ChainId, r.BlockNumber, r.BlockHash, r.BlockTime); e != nil {
					return e
				}
			case WithdrawalFinalization:
				if e := validateEvent(c, r.ChainId, r.BlockNumber, r.BlockHash, r.BlockTime); e != nil {
					return e
				}
				if r.FromRequestId.Sign() <= 0 || r.FromRequestId.Cmp(r.ToRequestId) > 0 {
					return errors.New("finalization_invalid_inclusive_range")
				}
			case WithdrawalClaim:
				if e := validateEvent(c, r.ChainId, r.BlockNumber, r.BlockHash, r.BlockTime); e != nil {
					return e
				}
			case FundingSettlement:
				if r.InstrumentId == 0 || r.SourceId != c.SourceId || r.ReceivedAt.Before(r.RequestedAt) || r.AvailableAt.Before(r.ReceivedAt) {
					return errors.New("funding_identity_or_time")
				}
				if c.WindowFromAt == nil || c.WindowToAt == nil || r.FundingTime.Before(*c.WindowFromAt) || r.FundingTime.After(*c.WindowToAt) {
					return errors.New("funding_outside_window")
				}
			}
		}
	}
	return nil
}
func validateEvent(c Capture, chain, number uint64, hash string, at time.Time) error {
	if chain != 1 || c.FromBlock == nil || c.ToBlock == nil || number < *c.FromBlock || number > *c.ToBlock {
		return errors.New("event_outside_capture")
	}
	if number == *c.FromBlock && hash != *c.FromBlockHash || number == *c.ToBlock && hash != *c.ToBlockHash {
		return errors.New("event_boundary_hash_mismatch")
	}
	if at.Before(*c.FromBlockTime) || at.After(*c.ToBlockTime) {
		return errors.New("event_outside_capture_time")
	}
	return nil
}
