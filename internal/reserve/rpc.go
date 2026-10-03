package reserve

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
)

//go:embed abi.json
var abiJSON string
var ABI = func() abi.ABI {
	a, e := abi.JSON(strings.NewReader(abiJSON))
	if e != nil {
		panic(e)
	}
	return a
}()

type Reader struct {
	RPC      *ethereum.Client
	Manifest Manifest
	Used     int
	Members  []dex.Hash
	Limit    int
	At       time.Time
}

func SourceError(rpc *ethereum.Client, v ethereum.Result) string {
	if v.Err == nil {
		return ""
	}
	reason := v.Err.Error()
	if reason != "rpc_method_error" {
		return reason
	}
	raw, e := rpc.Archive.Get(v.Payload)
	if e != nil {
		return reason
	}
	var envelope struct{ Response []byte }
	if json.Unmarshal(raw, &envelope) != nil {
		return reason
	}
	var replies []struct {
		Error *struct {
			Code    int
			Message string
		}
	}
	if json.Unmarshal(envelope.Response, &replies) != nil {
		return reason
	}
	errorsSeen := 0
	allReverts := true
	for _, reply := range replies {
		if reply.Error == nil {
			continue
		}
		errorsSeen++
		msg := strings.ToLower(reply.Error.Message)
		if strings.Contains(msg, "archive") && strings.Contains(msg, "token") {
			return "rpc_archive_unavailable"
		}
		if reply.Error.Code != 3 || !strings.Contains(msg, "revert") {
			allReverts = false
		}
	}
	if errorsSeen > 0 && allReverts {
		return "contract_revert"
	}
	return reason
}

func NewReader(rpc *ethereum.Client, m Manifest) *Reader {
	return &Reader{RPC: rpc, Manifest: m, Limit: 300}
}
func (r *Reader) Batch(ctx context.Context, calls []ethereum.Call) []ethereum.Result {
	out := make([]ethereum.Result, len(calls))
	n := min(len(calls), max(0, r.Limit-r.Used))
	r.Used += n
	if n > 0 {
		copy(out, r.RPC.Batch(ctx, calls[:n]))
	}
	for i := n; i < len(out); i++ {
		out[i] = ethereum.Result{Err: errors.New("rpc_member_budget_exhausted"), At: dex.Now()}
	}
	for _, v := range out {
		if v.Payload != (dex.Hash{}) {
			r.Members = append(r.Members, v.Payload)
		}
		if v.At.After(r.At) {
			r.At = v.At
		}
	}
	return out
}
func Call(to dex.Address, h dex.Hash, name string, args ...any) ethereum.Call {
	b, e := ABI.Pack(name, args...)
	if e != nil {
		panic(e)
	}
	return ethereum.EthCall(to, "0x"+common.Bytes2Hex(b), h)
}
func Decode(v ethereum.Result, name string) ([]any, error) {
	if v.Err != nil {
		return nil, v.Err
	}
	b, e := ethereum.HexResult(v)
	if e != nil {
		return nil, e
	}
	out, e := ABI.Unpack(name, b)
	if e != nil {
		return nil, fmt.Errorf("abi_%s: %w", name, e)
	}
	encoded, e := ABI.Methods[name].Outputs.Pack(out...)
	if e != nil || string(encoded) != string(b) {
		return nil, errors.New("noncanonical_abi_response")
	}
	return out, nil
}
func (r *Reader) Read(ctx context.Context, h dex.Hash, to dex.Address, name string, args ...any) ([]any, error) {
	v := r.Batch(ctx, []ethereum.Call{Call(to, h, name, args...)})[0]
	return Decode(v, name)
}
func Eth(a dex.Address) common.Address  { return common.Address(a) }
func Addr(a common.Address) dex.Address { return dex.Address(a) }
func CodeHash(v ethereum.Result) (dex.Hash, error) {
	b, e := ethereum.HexResult(v)
	if e != nil {
		return dex.Hash{}, e
	}
	if len(b) == 0 {
		return dex.Hash{}, errors.New("empty_code")
	}
	return dex.Hash(crypto.Keccak256Hash(b)), nil
}
func (r *Reader) Proof(v any) (string, error) {
	h, e := r.RPC.Archive.PutObject(struct {
		Data    any
		Members []dex.Hash
		At      time.Time
	}{v, r.Members, dex.Now()})
	return Hash(h), e
}
func (r *Reader) Preflight(ctx context.Context, b dex.Block) error {
	chain := r.Batch(ctx, []ethereum.Call{{Method: "eth_chainId", Params: []any{}}, {Method: "eth_getCode", Params: []any{r.Manifest.Implementation.String(), ethereum.BlockRef(b.Hash)}}, {Method: "eth_getCode", Params: []any{r.Manifest.Quoter.String(), ethereum.BlockRef(b.Hash)}}})
	if chain[0].Err != nil {
		return chain[0].Err
	}
	var id string
	if json.Unmarshal(chain[0].Raw, &id) != nil || id != "0x1" {
		return errors.New("wrong_chain")
	}
	i, e := CodeHash(chain[1])
	if e != nil {
		return e
	}
	q, e := CodeHash(chain[2])
	if e != nil {
		return e
	}
	if i != r.Manifest.ImplementationCodeHash || q != r.Manifest.QuoterCodeHash {
		return errors.New("code_hash_mismatch")
	}
	for _, p := range r.Manifest.Paths {
		for j, pool := range p.Pools {
			v, e := r.Read(ctx, b.Hash, r.Manifest.Factory, "getPool", Eth(p.Tokens[j]), Eth(p.Tokens[j+1]), new(big.Int).SetUint64(uint64(p.Fees[j])))
			if e != nil {
				return e
			}
			if Addr(v[0].(common.Address)) != pool {
				return errors.New("pool_factory_mismatch")
			}
			calls := []ethereum.Call{Call(pool, b.Hash, "token0"), Call(pool, b.Hash, "token1"), Call(pool, b.Hash, "fee")}
			vals := r.Batch(ctx, calls)
			v0, e := Decode(vals[0], "token0")
			if e != nil {
				return e
			}
			v1, e := Decode(vals[1], "token1")
			if e != nil {
				return e
			}
			vf, e := Decode(vals[2], "fee")
			if e != nil {
				return e
			}
			a, z := Addr(v0[0].(common.Address)), Addr(v1[0].(common.Address))
			x, y := p.Tokens[j], p.Tokens[j+1]
			if !((a == x && z == y) || (a == y && z == x)) || vf[0].(*big.Int).Uint64() != uint64(p.Fees[j]) {
				return errors.New("pool_identity_mismatch")
			}
		}
	}
	return nil
}
