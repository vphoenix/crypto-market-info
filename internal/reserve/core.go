package reserve

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"sort"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/dex"
)

func Hash(v dex.Hash) string       { return string(v[:]) }
func Address(v dex.Address) string { return string(v[:]) }
func Hex(s string) string          { return "0x" + hex.EncodeToString([]byte(s)) }
func ID(v any) string              { h := sha256.Sum256(Encoding(v)); return string(h[:]) }

// Encoding retains arbitrary binary FixedString bytes. JSON encoding a Go string
// replaces invalid UTF-8 and is therefore unsuitable for row/member identities.
func Encoding(v any) []byte {
	var b bytes.Buffer
	var walk func(reflect.Value)
	write := func(s string) { _ = binary.Write(&b, binary.BigEndian, uint64(len(s))); b.WriteString(s) }
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
			panic("unsupported canonical type: " + x.Type().String())
		}
	}
	walk(reflect.ValueOf(v))
	return b.Bytes()
}
func Ptr[T any](v T) *T        { return &v }
func Uint(v int64) *big.Int    { return big.NewInt(v) }
func Copy(v *big.Int) *big.Int { return dex.Copy(v) }
func Uint256(v *big.Int) bool  { return dex.ValidUint(v, 256) }
func MulCeil(a, b, d *big.Int) (*big.Int, error) {
	if !Uint256(a) || !Uint256(b) || !Uint256(d) || d.Sign() == 0 {
		return nil, errors.New("invalid_fee_operand")
	}
	n := new(big.Int).Mul(a, b)
	if n.BitLen() > 256 {
		return nil, errors.New("solidity_fee_multiplication_overflow")
	}
	n.Add(n, new(big.Int).Sub(d, Uint(1)))
	if n.BitLen() > 256 {
		return nil, errors.New("solidity_fee_addition_overflow")
	}
	return n.Div(n, d), nil
}
func MintFee(gross, fee, numerator, denominator, floor *big.Int) (*big.Int, error) {
	t, e := MulCeil(gross, fee, Uint(1_000_000_000_000_000_000))
	if e != nil {
		return nil, e
	}
	d, e := MulCeil(t, numerator, denominator)
	if e != nil {
		return nil, e
	}
	f := Copy(floor)
	if !Uint256(f) {
		return nil, errors.New("missing_fee_floor")
	}
	if f.Cmp(Uint(300_000_000_000_000)) < 0 {
		f = Uint(300_000_000_000_000)
	}
	m, e := MulCeil(gross, f, Uint(1_000_000_000_000_000_000))
	if e != nil {
		return nil, e
	}
	if d.Cmp(t) > 0 {
		t = d
	}
	if m.Cmp(t) > 0 {
		t = m
	}
	if t.Cmp(gross) > 0 {
		return nil, errors.New("fee_exceeds_shares")
	}
	return t, nil
}

type Batch struct {
	Capture  Capture
	States   []State
	Quotes   []Quote
	Logs     []dex.Log
	Receipts []dex.Receipt
}

func StateMembers(in []State) string {
	v := append([]State{}, in...)
	sort.Slice(v, func(i, j int) bool { return v[i].Folio < v[j].Folio })
	return ID(v)
}
func QuoteMembers(in []Quote) string {
	v := append([]Quote{}, in...)
	sort.Slice(v, func(i, j int) bool { return v[i].QuoteId < v[j].QuoteId })
	return ID(v)
}
func LogMembers(in []dex.Log) string {
	v := append([]dex.Log{}, in...)
	sort.Slice(v, func(i, j int) bool {
		if v[i].Number != v[j].Number {
			return v[i].Number < v[j].Number
		}
		return v[i].Index < v[j].Index
	})
	return ID(v)
}
func LogFact(l dex.Log) string {
	return ID(struct {
		Chain, Number  uint64
		Block, Tx      dex.Hash
		Index, TxIndex uint32
		Emitter        dex.Address
		Topics         []dex.Hash
		Data           []byte
		Removed        bool
	}{l.ChainID, l.Number, l.Hash, l.TxHash, l.Index, l.TxIndex, l.Emitter, l.Topics, l.Data, l.Removed})
}
func ReceiptMembers(in []dex.Receipt) string {
	type member struct {
		Chain                        uint64
		Block, Tx, Receipt, Calldata dex.Hash
	}
	v := []member{}
	for _, r := range in {
		v = append(v, member{r.ChainID, r.Hash, r.TxHash, r.ReceiptHash, r.CalldataHash})
	}
	sort.Slice(v, func(i, j int) bool {
		if v[i].Block != v[j].Block {
			return v[i].Block.String() < v[j].Block.String()
		}
		return v[i].Tx.String() < v[j].Tx.String()
	})
	return ID(v)
}
func (b *Batch) Seal() {
	c := &b.Capture
	c.ReceiptRefs = []ReceiptRef{}
	for _, r := range b.Receipts {
		c.ReceiptRefs = append(c.ReceiptRefs, ReceiptRef{Hash(r.Hash), Hash(r.TxHash), Hash(r.ReceiptHash), Hash(r.CalldataHash)})
	}
	sort.Slice(c.ReceiptRefs, func(i, j int) bool {
		if c.ReceiptRefs[i].BlockHash != c.ReceiptRefs[j].BlockHash {
			return c.ReceiptRefs[i].BlockHash < c.ReceiptRefs[j].BlockHash
		}
		return c.ReceiptRefs[i].TxHash < c.ReceiptRefs[j].TxHash
	})
	c.ActualStates = uint32(len(b.States))
	c.ActualQuotes = uint32(len(b.Quotes))
	c.LogCount = uint32(len(b.Logs))
	c.ActualReceipts = uint32(len(b.Receipts))
	c.StateMembers = StateMembers(b.States)
	c.QuoteMembers = QuoteMembers(b.Quotes)
	c.LogMembers = LogMembers(b.Logs)
	c.ReceiptMembers = ReceiptMembers(b.Receipts)
	c.AvailableAt = dex.Now()
	c.Revision = uint64(c.AvailableAt.UnixMicro())
	c.Committed = true
}
func Validate(b Batch) error {
	c := b.Capture
	if c.ChainId != 1 || len(c.ManifestHash) != 32 || len(c.CaptureId) != 32 || len(c.BatchId) != 32 || len(c.PayloadHash) != 32 || len(c.PlanHash) != 32 || len(c.FromHash) != 32 || len(c.ToHash) != 32 || !c.Committed || c.FromBlock > c.ToBlock || c.AvailableAt.IsZero() || c.FromTime.IsZero() || c.ToTime.IsZero() {
		return errors.New("invalid_capture")
	}
	if c.ExpectedStates < c.ActualStates || c.ExpectedQuotes < c.ActualQuotes || c.ExpectedReceipts < c.ActualReceipts {
		return errors.New("invalid_expected_member_count")
	}
	if c.ActualStates != uint32(len(b.States)) || c.ActualQuotes != uint32(len(b.Quotes)) || c.LogCount != uint32(len(b.Logs)) || c.ActualReceipts != uint32(len(b.Receipts)) || c.StateMembers != StateMembers(b.States) || c.QuoteMembers != QuoteMembers(b.Quotes) || c.LogMembers != LogMembers(b.Logs) || c.ReceiptMembers != ReceiptMembers(b.Receipts) {
		return errors.New("member_digest_mismatch")
	}
	var walk func(reflect.Value) error
	walk = func(x reflect.Value) error {
		if x.Kind() == reflect.Pointer {
			if x.IsNil() {
				return nil
			}
			if x.Type() == reflect.TypeOf((*big.Int)(nil)) {
				if !Uint256(x.Interface().(*big.Int)) {
					return errors.New("uint256_overflow")
				}
				return nil
			}
			return walk(x.Elem())
		}
		switch x.Kind() {
		case reflect.Struct:
			if x.Type() == reflect.TypeOf(time.Time{}) {
				return nil
			}
			for i := 0; i < x.NumField(); i++ {
				if e := walk(x.Field(i)); e != nil {
					return e
				}
			}
		case reflect.Slice:
			for i := 0; i < x.Len(); i++ {
				if e := walk(x.Index(i)); e != nil {
					return e
				}
			}
		}
		return nil
	}
	for _, s := range b.States {
		if len(s.Folio) != 20 || len(s.PayloadHash) != 32 || s.AvailableAt.IsZero() {
			return errors.New("invalid_state_identity")
		}
		if s.CaptureId != c.CaptureId || s.BatchId != c.BatchId || s.ManifestHash != c.ManifestHash || s.BlockHash != c.ToHash || s.BlockNumber != c.ToBlock || s.ChainId != 1 {
			return errors.New("state_anchor_mismatch")
		}
		if e := walk(reflect.ValueOf(s)); e != nil {
			return e
		}
		if s.StateComplete && (!s.IdentityOk || len(s.Basket) == 0 || s.TotalSupplyRaw == nil || s.MintFeeD18 == nil || s.DaoFeeDenominator == nil) {
			return errors.New("complete_state_missing_fields")
		}
		for _, a := range s.Basket {
			if len(a.Token) != 20 {
				return errors.New("invalid_basket_token")
			}
		}
	}
	for _, q := range b.Quotes {
		if len(q.Folio) != 20 || len(q.TokenIn) != 20 || len(q.TokenOut) != 20 || len(q.BudgetToken) != 20 || len(q.RouteId) != 32 || len(q.QuoteId) != 32 || len(q.PayloadHash) != 32 || q.RequestedBudgetRaw == nil || q.AvailableAt.IsZero() {
			return errors.New("invalid_quote_identity")
		}
		if q.CaptureId != c.CaptureId || q.BatchId != c.BatchId || q.ManifestHash != c.ManifestHash || q.BlockHash != c.ToHash || q.BlockNumber != c.ToBlock || q.ChainId != 1 {
			return errors.New("quote_anchor_mismatch")
		}
		if e := walk(reflect.ValueOf(q)); e != nil {
			return e
		}
		if q.Quality != "incomplete" {
			if q.AmountInRaw == nil || q.AmountOutRaw == nil || q.ExpectedLegs != q.SuccessfulLegs || q.WithinBudget == nil || !*q.WithinBudget {
				return fmt.Errorf("invalid_complete_quote")
			}
			if q.Quality == "quoted_complete" && len(q.SharedPools) > 0 {
				return errors.New("shared_pool_quote")
			}
		}
		for _, l := range q.DexLegs {
			if len(l.TokenIn) != 20 || len(l.TokenOut) != 20 || len(l.PayloadHash) != 32 || l.RequestedRaw == nil || l.AvailableAt.IsZero() {
				return errors.New("invalid_leg_identity")
			}
			if len(l.PathTokens) > 0 && (len(l.PathTokens) != len(l.PathPools)+1 || len(l.PoolFees) != len(l.PathPools)) {
				return errors.New("invalid_leg_path")
			}
		}
	}
	refs := []ReceiptRef{}
	for _, r := range b.Receipts {
		refs = append(refs, ReceiptRef{Hash(r.Hash), Hash(r.TxHash), Hash(r.ReceiptHash), Hash(r.CalldataHash)})
	}
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].BlockHash != refs[j].BlockHash {
			return refs[i].BlockHash < refs[j].BlockHash
		}
		return refs[i].TxHash < refs[j].TxHash
	})
	if ID(refs) != ID(c.ReceiptRefs) {
		return errors.New("receipt_reference_members_mismatch")
	}
	for _, l := range b.Logs {
		if l.Manifest.String() != Hex(c.ManifestHash) || Hash(l.Batch) != c.BatchId || l.Number < c.FromBlock || l.Number > c.ToBlock || l.Removed {
			return errors.New("log_anchor_mismatch")
		}
	}
	return nil
}
