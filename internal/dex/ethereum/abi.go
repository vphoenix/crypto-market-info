package ethereum

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"math/big"
	"strings"
)

func Bytes(s string) ([]byte, error) {
	if !strings.HasPrefix(s, "0x") || len(s)%2 != 0 {
		return nil, errors.New("invalid_hex_bytes")
	}
	return hex.DecodeString(s[2:])
}
func HexResult(r Result) ([]byte, error) {
	if r.Err != nil {
		return nil, r.Err
	}
	var s string
	if json.Unmarshal(r.Raw, &s) != nil {
		return nil, errors.New("invalid_hex_result")
	}
	return Bytes(s)
}
func Quantity(s string, bits int) (*big.Int, error) {
	if !strings.HasPrefix(s, "0x") || len(s) < 3 || (len(s) > 3 && s[2] == '0') {
		return nil, errors.New("invalid_quantity")
	}
	n, ok := new(big.Int).SetString(s[2:], 16)
	if !ok || !dex.ValidUint(n, bits) {
		return nil, errors.New("quantity_overflow")
	}
	return n, nil
}
func ABIWord(r Result, bits int) (*big.Int, error) {
	b, e := HexResult(r)
	if e != nil {
		return nil, e
	}
	if len(b) != 32 {
		return nil, errors.New("abi_word_length")
	}
	n := new(big.Int).SetBytes(b)
	if !dex.ValidUint(n, bits) {
		return nil, errors.New("abi_word_overflow")
	}
	return n, nil
}
func word(n *big.Int) string           { return hex.EncodeToString(n.FillBytes(make([]byte, 32))) }
func addressWord(a dex.Address) string { return strings.Repeat("0", 24) + hex.EncodeToString(a[:]) }
func data(m Manifest, sig string, args ...dex.Address) string {
	out, ok := m.Selectors[sig]
	if !ok {
		panic("missing ABI selector: " + sig)
	}
	for _, a := range args {
		out += addressWord(a)
	}
	return out
}
