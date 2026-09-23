// Package dex contains the typed observations of the Ethereum arbitrage experiment.
// All amounts are unsigned token atoms; nil means unknown, never zero.
package dex

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
	"time"
)

type Hash [32]byte
type Address [20]byte

func (h Hash) String() string                   { return "0x" + hex.EncodeToString(h[:]) }
func (a Address) String() string                { return "0x" + hex.EncodeToString(a[:]) }
func (h Hash) MarshalText() ([]byte, error)     { return []byte(h.String()), nil }
func (a Address) MarshalText() ([]byte, error)  { return []byte(a.String()), nil }
func (h *Hash) UnmarshalText(b []byte) error    { return decodeFixed(string(b), h[:]) }
func (a *Address) UnmarshalText(b []byte) error { return decodeFixed(string(b), a[:]) }
func decodeFixed(s string, b []byte) error {
	if len(s) != 2+len(b)*2 || s[:2] != "0x" {
		return fmt.Errorf("invalid hex width")
	}
	n, e := hex.Decode(b, []byte(s[2:]))
	if e != nil || n != len(b) {
		return fmt.Errorf("invalid hex")
	}
	return nil
}
func ParseHash(s string) (h Hash, err error)       { err = decodeFixed(s, h[:]); return }
func ParseAddress(s string) (a Address, err error) { err = decodeFixed(s, a[:]); return }
func MustAddress(s string) Address {
	a, e := ParseAddress(s)
	if e != nil {
		panic(e)
	}
	return a
}
func Digest(b []byte) Hash { return sha256.Sum256(b) }
func ObjectHash(v any) Hash {
	b, e := json.Marshal(v)
	if e != nil {
		panic(e)
	}
	return Digest(b)
}
func Copy(n *big.Int) *big.Int {
	if n == nil {
		return nil
	}
	return new(big.Int).Set(n)
}
func Zero() *big.Int                      { return new(big.Int) }
func ValidUint(n *big.Int, bits int) bool { return n != nil && n.Sign() >= 0 && n.BitLen() <= bits }
func Now() time.Time                      { return time.Now().UTC().Truncate(time.Microsecond) }

type Anchor struct {
	ChainID  uint64
	Number   uint64
	Hash     Hash
	Manifest Hash
	Batch    Hash
	Payload  Hash
	Time     time.Time
}
type Block struct {
	Anchor
	Parent                                               Hash
	BaseFee                                              *big.Int
	Miner                                                Address
	ReceivedAt, AvailableAt                              time.Time
	Capture                                              string // live or backfill: latter never contributes live opportunity counts.
	Canonical                                            bool
	Finality                                             string
	Revision                                             uint64
	LogCoverage, ReceiptCoverage, QuoteCoverage          string
	ExpectedQuotes, ActualQuotes, LogCount, ReceiptCount uint32
	QuoteMembers, LogMembers, ReceiptMembers             Hash
	Committed                                            bool
}
type Sky struct {
	Anchor
	Module                                          string
	Tin, Tout, Buf, DAICash, USDCCash, Allowance    *big.Int
	VatLive, DAIJoinLive, DAIJoinWard, USDSJoinWard bool
	IdentityOK, Complete                            bool
	Reason                                          string
}
type Quote struct {
	Anchor
	ID                                  Hash
	Role, Route, Mode                   string
	TokenIn, TokenOut                   Address
	Requested, AmountIn, AmountOut      *big.Int
	DustDAI, DustUSDS                   *big.Int
	V3InToken, V3OutToken               Address
	V3In, V3Out, SqrtAfter, GasEstimate *big.Int
	TicksCrossed                        uint32
	Status, Reason                      string
	AvailableAt                         time.Time
}

func (q *Quote) SetID() {
	q.ID = ObjectHash(struct {
		Batch             Hash
		Role, Route, Mode string
		In, Out           Address
		Amount            string
	}{q.Batch, q.Role, q.Route, q.Mode, q.TokenIn, q.TokenOut, q.Requested.String()})
}

type Log struct {
	Anchor
	TxHash         Hash
	TxIndex, Index uint32
	Emitter        Address
	Topics         []Hash
	Data           []byte
	Event          string
	Removed        bool
}
type Receipt struct {
	Anchor
	TxHash       Hash
	TxIndex      uint32
	From, To     Address
	HasTo        bool
	Type         uint8
	Value        *big.Int
	Selector     []byte
	CalldataHash Hash
	Status       uint8
	GasUsed      uint64
	GasPrice     *big.Int
	LogCount     uint32
	ReceiptHash  Hash
	AvailableAt  time.Time
}
type Batch struct {
	Block    Block
	Sky      *Sky
	Quotes   []Quote
	Logs     []Log
	Receipts []Receipt
}

func (b *Batch) Seal() {
	b.Block.ActualQuotes = uint32(len(b.Quotes))
	b.Block.LogCount = uint32(len(b.Logs))
	b.Block.ReceiptCount = uint32(len(b.Receipts))
	b.Block.QuoteMembers = QuoteDigest(b.Quotes)
	b.Block.LogMembers = LogDigest(b.Logs)
	b.Block.ReceiptMembers = ReceiptDigest(b.Receipts)
	b.Block.AvailableAt = Now()
	b.Block.Revision = uint64(b.Block.AvailableAt.UnixMicro())
	b.Block.Committed = true
}

type SwapRequest struct {
	TokenIn, TokenOut Address
	Fee               uint32
	Amount            *big.Int
	ExactOutput       bool
}
type SwapResult struct {
	Amount, SqrtAfter, Gas *big.Int
	Ticks                  uint32
	Payload                Hash
	At                     time.Time
	Err                    error
}

func QuoteDigest(in []Quote) Hash {
	v := append([]Quote{}, in...)
	sort.Slice(v, func(i, j int) bool { return v[i].ID.String() < v[j].ID.String() })
	return ObjectHash(v)
}
func LogDigest(in []Log) Hash {
	v := append([]Log{}, in...)
	sort.Slice(v, func(i, j int) bool { return v[i].Index < v[j].Index })
	return ObjectHash(v)
}
func ReceiptDigest(in []Receipt) Hash {
	v := append([]Receipt{}, in...)
	sort.Slice(v, func(i, j int) bool { return v[i].TxHash.String() < v[j].TxHash.String() })
	return ObjectHash(v)
}
