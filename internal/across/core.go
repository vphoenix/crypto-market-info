package across

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
)

type Store interface {
	AcrossCaptures(context.Context, string) ([]Capture, error)
	AcrossBatch(context.Context, Capture) (Batch, error)
	WriteAcrossBatch(context.Context, Batch) error
	WriteAcrossRevision(context.Context, Capture) error
}

func Now() time.Time      { return time.Now().UTC().Truncate(time.Microsecond) }
func Ptr[T any](v T) *T   { return &v }
func Hex(s string) string { return "0x" + hex.EncodeToString([]byte(s)) }
func ID(v any) string     { h := sha256.Sum256(Encoding(v)); return string(h[:]) }

// Encoding retains binary strings and exact integers/decimals, unlike JSON rows.
func Encoding(v any) []byte {
	var b bytes.Buffer
	write := func(s string) { _ = binary.Write(&b, binary.BigEndian, uint64(len(s))); b.WriteString(s) }
	var walk func(reflect.Value)
	walk = func(x reflect.Value) {
		if x.Kind() == reflect.Interface {
			x = x.Elem()
		}
		if x.Kind() == reflect.Pointer {
			if x.IsNil() {
				b.WriteByte(0)
				return
			}
			b.WriteByte(1)
			walk(x.Elem())
			return
		}
		if x.Type() == reflect.TypeOf(time.Time{}) {
			write(x.Interface().(time.Time).UTC().Format(time.RFC3339Nano))
			return
		}
		if x.Type() == reflect.TypeOf(big.Int{}) {
			n := x.Interface().(big.Int)
			write(n.String())
			return
		}
		if x.Type() == reflect.TypeOf(decimal.Decimal{}) {
			write(x.Interface().(decimal.Decimal).StringFixed(18))
			return
		}
		switch x.Kind() {
		case reflect.Struct:
			for i := 0; i < x.NumField(); i++ {
				walk(x.Field(i))
			}
		case reflect.Slice, reflect.Array:
			_ = binary.Write(&b, binary.BigEndian, uint64(x.Len()))
			for i := 0; i < x.Len(); i++ {
				walk(x.Index(i))
			}
		case reflect.String:
			write(x.String())
		case reflect.Bool:
			if x.Bool() {
				b.WriteByte(1)
			} else {
				b.WriteByte(0)
			}
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			_ = binary.Write(&b, binary.BigEndian, x.Uint())
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			_ = binary.Write(&b, binary.BigEndian, x.Int())
		default:
			panic("unsupported canonical type " + x.Type().String())
		}
	}
	walk(reflect.ValueOf(v))
	return b.Bytes()
}
func batchTables(b Batch) []any {
	return []any{b.Deposits, b.Updates, b.Fills, b.Refunds, b.Receipts, b.Probes}
}
func Members(rows any) []string {
	v := reflect.ValueOf(rows)
	ids := make([]string, v.Len())
	for i := range ids {
		ids[i] = ID(v.Index(i).Interface())
	}
	sort.Strings(ids)
	return ids
}
func Seal(b *Batch) {
	c := &b.Capture
	c.TableIds = []uint8{}
	c.RowCounts = []uint32{}
	c.RowDigests = []string{}
	for i, t := range batchTables(*b) {
		m := Members(t)
		c.TableIds = append(c.TableIds, uint8(i+1))
		c.RowCounts = append(c.RowCounts, uint32(len(m)))
		c.RowDigests = append(c.RowDigests, ID(m))
	}
	c.Committed = true
	if c.Revision == 0 {
		c.Revision = 1
	}
}
func Validate(b Batch) error {
	c := b.Capture
	if len(c.ManifestHash) != 32 || len(c.CaptureId) != 32 || len(c.EvidenceHash) != 32 || c.ChainId == 0 || c.StartedAt.IsZero() || c.AvailableAt.Before(c.StartedAt) || !c.Committed || c.Revision == 0 {
		return errors.New("invalid_capture")
	}
	if c.CompletedTasks > c.ExpectedTasks || len(c.TableIds) != 6 || len(c.RowCounts) != 6 || len(c.RowDigests) != 6 {
		return errors.New("capture_member_arrays")
	}
	if (c.FromBlock == nil) != (c.ToBlock == nil) || (c.FromHash == nil) != (c.ToHash == nil) || (c.FromBlock == nil) != (c.FromHash == nil) {
		return errors.New("capture_partial_anchor")
	}
	if c.FromBlock != nil && (*c.FromBlock > *c.ToBlock || len(*c.FromHash) != 32 || len(*c.ToHash) != 32) {
		return errors.New("capture_invalid_anchor")
	}
	for i, t := range batchTables(b) {
		members := Members(t)
		if c.TableIds[i] != uint8(i+1) || c.RowCounts[i] != uint32(len(members)) || c.RowDigests[i] != ID(members) {
			return errors.New("capture_member_digest_mismatch")
		}
		seen := map[string]bool{}
		v := reflect.ValueOf(t)
		for j := 0; j < v.Len(); j++ {
			row := v.Index(j)
			if row.FieldByName("CaptureId").String() != c.CaptureId || row.FieldByName("ChainId").Uint() != c.ChainId {
				return errors.New("capture_row_identity_mismatch")
			}
			if seen[members[j]] {
				return errors.New("duplicate_capture_row")
			}
			seen[members[j]] = true
			if err := validateRow(row); err != nil {
				return err
			}
			n := row.FieldByName("BlockNumber")
			h := row.FieldByName("BlockHash")
			if n.Kind() == reflect.Pointer {
				if n.IsNil() {
					continue
				}
				n = n.Elem()
				if h.IsNil() {
					return errors.New("missing_row_block_hash")
				}
				h = h.Elem()
			}
			if c.FromBlock == nil || n.Uint() < *c.FromBlock || n.Uint() > *c.ToBlock {
				return errors.New("row_outside_capture_range")
			}
			if n.Uint() == *c.FromBlock && h.String() != *c.FromHash || n.Uint() == *c.ToBlock && h.String() != *c.ToHash {
				return errors.New("row_anchor_conflict")
			}
		}
	}
	for _, d := range b.Deposits {
		if d.InputAmountRaw.Sign() <= 0 {
			return errors.New("invalid_deposit_amount")
		}
	}
	for _, f := range b.Fills {
		if f.FillType > 2 {
			return errors.New("unknown_fill_type")
		}
	}
	for _, r := range b.Refunds {
		if len(r.RefundAddresses) != len(r.RefundAmountsRaw) {
			return errors.New("refund_array_mismatch")
		}
	}
	for _, r := range b.Receipts {
		if r.FeeComplete && r.TotalFeeWei == nil {
			return errors.New("complete_fee_missing")
		}
	}
	for _, p := range b.Probes {
		if p.FillStatus != nil && *p.FillStatus > 2 {
			return errors.New("unknown_fill_status")
		}
		if p.ProbeStatus == "ok" && (p.FillStatus == nil || p.PausedFills == nil || p.ContractTime == nil || p.BlockHash == nil) {
			return errors.New("incomplete_ok_probe")
		}
	}
	return nil
}
func validateRow(v reflect.Value) error {
	for i := 0; i < v.NumField(); i++ {
		if e := validateValue(v.Field(i), v.Type().Field(i).Tag.Get("dbtype")); e != nil {
			return fmt.Errorf("%s: %w", v.Type().Field(i).Name, e)
		}
	}
	return nil
}
func validateValue(v reflect.Value, typ string) error {
	if strings.HasPrefix(typ, "Nullable(") {
		if v.IsNil() {
			return nil
		}
		typ = typ[9 : len(typ)-1]
		if v.Type() != reflect.TypeOf((*big.Int)(nil)) {
			v = v.Elem()
		}
	}
	if strings.HasPrefix(typ, "Array(") {
		for i := 0; i < v.Len(); i++ {
			if e := validateValue(v.Index(i), typ[6:len(typ)-1]); e != nil {
				return e
			}
		}
		return nil
	}
	if strings.HasPrefix(typ, "FixedString(") {
		n, _ := strconv.Atoi(typ[12 : len(typ)-1])
		if v.Len() != n {
			return errors.New("binary_width")
		}
	}
	if typ == "UInt256" {
		if v.IsNil() {
			return errors.New("missing_uint256")
		}
		n := v.Interface().(*big.Int)
		if n.Sign() < 0 || n.BitLen() > 256 {
			return errors.New("uint256_range")
		}
	}
	if strings.HasPrefix(typ, "DateTime64") {
		t := v.Interface().(time.Time)
		if t.IsZero() || t.Nanosecond()%1000 != 0 {
			return errors.New("invalid_microsecond_time")
		}
	}
	if strings.HasPrefix(typ, "Decimal") {
		n := v.Interface().(decimal.Decimal)
		if !n.Equal(n.Truncate(18)) || n.Abs().GreaterThanOrEqual(decimal.New(1, 20)) {
			return errors.New("decimal_precision")
		}
	}
	return nil
}

// Evidence is JSON-safe: all binary hashes are hexadecimal text in this envelope.
type EvidenceHeader struct {
	ChainID uint64
	Number  uint64
	Hash    string
	Time    time.Time
}
type CaptureEvidence struct {
	Version                                             int
	CaptureID, ManifestHash, Kind, Mode, Source, Reason string
	FromBlock, ToBlock                                  *uint64
	FromTime, ToTime                                    *time.Time
	StartedAt, AvailableAt                              time.Time
	Headers                                             []EvidenceHeader
	RPCPayloads                                         []string
	TableIDs                                            []uint8
	RowCounts                                           []uint32
	RowDigests                                          []string
	RowMembers                                          [][]string
}

func ArchiveBatch(a ethereum.Archive, b *Batch, headers []EvidenceHeader, payloads []string) error {
	Seal(b)
	c := &b.Capture
	e := CaptureEvidence{Version: 1, CaptureID: Hex(c.CaptureId), ManifestHash: Hex(c.ManifestHash), Kind: c.CaptureKind, Mode: c.CaptureMode, Source: c.SourceId, Reason: c.Reason, FromBlock: c.FromBlock, ToBlock: c.ToBlock, StartedAt: c.StartedAt, AvailableAt: c.AvailableAt, Headers: headers, TableIDs: c.TableIds, RowCounts: c.RowCounts}
	for _, h := range headers {
		if c.FromBlock != nil && h.ChainID == c.ChainId && h.Number == *c.FromBlock {
			e.FromTime = Ptr(h.Time)
		}
		if c.ToBlock != nil && h.ChainID == c.ChainId && h.Number == *c.ToBlock {
			e.ToTime = Ptr(h.Time)
		}
	}
	for _, p := range payloads {
		e.RPCPayloads = append(e.RPCPayloads, Hex(p))
	}
	for i, t := range batchTables(*b) {
		e.RowDigests = append(e.RowDigests, Hex(c.RowDigests[i]))
		m := Members(t)
		for j := range m {
			m[j] = Hex(m[j])
		}
		e.RowMembers = append(e.RowMembers, m)
	}
	h, err := a.PutObject(e)
	if err != nil {
		return err
	}
	c.EvidenceHash = string(h[:])
	return Validate(*b)
}
func ReadCaptureEvidence(a ethereum.Archive, c Capture) (CaptureEvidence, error) {
	var e CaptureEvidence
	var h dex.Hash
	copy(h[:], c.EvidenceHash)
	raw, err := a.Get(h)
	if err != nil {
		return e, err
	}
	if err = json.Unmarshal(raw, &e); err != nil {
		return e, err
	}
	if e.Version != 1 || e.CaptureID != Hex(c.CaptureId) || e.ManifestHash != Hex(c.ManifestHash) || len(e.RowDigests) != 6 || len(e.RowCounts) != 6 || len(e.RowMembers) != 6 || len(e.TableIDs) != 6 || len(c.RowDigests) != 6 || len(c.RowCounts) != 6 {
		return e, errors.New("capture_evidence_identity")
	}
	for i, d := range c.RowDigests {
		if e.TableIDs[i] != uint8(i+1) || e.RowDigests[i] != Hex(d) || e.RowCounts[i] != c.RowCounts[i] || len(e.RowMembers[i]) != int(c.RowCounts[i]) {
			return e, errors.New("capture_evidence_members")
		}
		members := make([]string, len(e.RowMembers[i]))
		for j, m := range e.RowMembers[i] {
			raw, err := hex.DecodeString(strings.TrimPrefix(m, "0x"))
			if err != nil || len(raw) != 32 {
				return e, errors.New("invalid_evidence_member")
			}
			members[j] = string(raw)
		}
		if ID(members) != d {
			return e, errors.New("evidence_member_digest_mismatch")
		}
	}
	return e, nil
}
