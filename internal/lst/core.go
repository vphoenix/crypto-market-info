package lst

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"time"
)

func Now() time.Time      { return time.Now().UTC().Truncate(time.Microsecond) }
func Ptr[T any](v T) *T   { return &v }
func Hex(v string) string { return "0x" + hex.EncodeToString([]byte(v)) }
func ParseHex(s string, n int) (string, error) {
	if len(s) != 2+n*2 || len(s) < 2 || s[:2] != "0x" {
		return "", errors.New("hex_width")
	}
	b, e := hex.DecodeString(s[2:])
	return string(b), e
}
func HashBytes(b []byte) string { h := sha256.Sum256(b); return string(h[:]) }
func Uint256(n *big.Int) bool   { return n != nil && n.Sign() >= 0 && n.BitLen() <= 256 }
func clone(n *big.Int) *big.Int {
	if n == nil {
		return nil
	}
	return new(big.Int).Set(n)
}
func exactJSON(b []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(b))
	if e := d.Decode(v); e != nil {
		return e
	}
	if d.Decode(new(any)) != io.EOF {
		return errors.New("trailing_json")
	}
	return nil
}
func quantity(s string) (*big.Int, error) {
	if len(s) < 3 || s[:2] != "0x" || len(s) > 3 && s[2] == '0' {
		return nil, errors.New("rpc_quantity")
	}
	n, ok := new(big.Int).SetString(s[2:], 16)
	if !ok || !Uint256(n) {
		return nil, errors.New("rpc_quantity")
	}
	return n, nil
}
func q64(s string) (uint64, error) {
	n, e := quantity(s)
	if e != nil {
		return 0, e
	}
	if !n.IsUint64() {
		return 0, errors.New("rpc_quantity_overflow")
	}
	return n.Uint64(), nil
}
func requestParts(n, min, max *big.Int) (uint32, error) {
	if !Uint256(n) || !Uint256(min) || !Uint256(max) || min.Sign() == 0 || max.Cmp(min) < 0 || n.Cmp(min) < 0 {
		return 0, errors.New("withdrawal_amount_invalid")
	}
	parts := new(big.Int).Quo(new(big.Int).Add(n, new(big.Int).Sub(max, big.NewInt(1))), max)
	if !parts.IsUint64() || parts.Uint64() > 1<<32-1 {
		return 0, errors.New("withdrawal_parts_overflow")
	}
	if new(big.Int).Mul(parts, min).Cmp(n) > 0 {
		return 0, errors.New("withdrawal_split_below_minimum")
	}
	return uint32(parts.Uint64()), nil
}
