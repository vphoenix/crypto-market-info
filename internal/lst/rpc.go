package lst

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/crypto"
)

type Block struct {
	Number           uint64
	Hash, ParentHash string
	Time             time.Time
	BaseFee          *big.Int
	Response         Response
}
type RPC struct {
	Transport *Transport
	URL       string
	sequence  atomic.Uint64
}
type RPCError struct {
	Code    int
	Message string
}

func (e *RPCError) Error() string { return fmt.Sprintf("rpc_error_%d", e.Code) }
func (r *RPC) Call(ctx context.Context, method string, params any, class string) (json.RawMessage, Response, error) {
	switch method {
	case "eth_chainId", "eth_getBlockByNumber", "eth_getBlockByHash", "eth_getCode", "eth_call", "eth_getLogs", "eth_getTransactionReceipt", "eth_getTransactionByHash":
	default:
		return nil, Response{}, errors.New("rpc_method_not_readonly_whitelist")
	}
	id := r.sequence.Add(1)
	body, _ := json.Marshal(struct {
		JSONRPC string `json:"jsonrpc"`
		Id      uint64 `json:"id"`
		Method  string `json:"method"`
		Params  any    `json:"params"`
	}{"2.0", id, method, params})
	res, e := r.Transport.Do(ctx, "rpc", "POST", r.URL, body, class)
	if res.PayloadHash != "" {
		h, he := ParseHex(res.PayloadHash, 32)
		if he != nil {
			return nil, res, he
		}
		res.PayloadHash = h
	}
	// Some providers send JSON-RPC errors with HTTP 400. Only inspect a
	// complete, archived response for this exact transport failure. Rate limits,
	// bans and evidence/network failures retain their transport semantics.
	transportErr := e
	inspectHTTPError := e != nil && e.Error() == "source_http_400" && res.HTTPStatus == 400 && len(res.PayloadHash) == 32
	if e != nil && !inspectHTTPError {
		return nil, res, e
	}
	var env struct {
		JSONRPC string          `json:"jsonrpc"`
		Id      uint64          `json:"id"`
		Result  json.RawMessage `json:"result"`
		Error   *struct {
			Code    *int    `json:"code"`
			Message *string `json:"message"`
		} `json:"error"`
	}
	if e = exactJSON(res.Raw, &env); e != nil || env.JSONRPC != "2.0" || env.Id != id {
		if inspectHTTPError {
			return nil, res, transportErr
		}
		return nil, res, errors.New("rpc_envelope")
	}
	if env.Error != nil {
		if env.Error.Code == nil || env.Error.Message == nil || len(env.Result) != 0 {
			if inspectHTTPError {
				return nil, res, transportErr
			}
			return nil, res, errors.New("rpc_envelope")
		}
		rpcErr := &RPCError{*env.Error.Code, *env.Error.Message}
		if inspectHTTPError {
			return nil, res, errors.Join(transportErr, rpcErr)
		}
		return nil, res, rpcErr
	}
	if inspectHTTPError {
		// An HTTP failure cannot become a successful RPC result.
		return nil, res, transportErr
	}
	if len(env.Result) == 0 || bytes.Equal(bytes.TrimSpace(env.Result), []byte("null")) {
		return nil, res, errors.New("rpc_null")
	}
	return env.Result, res, nil
}
func (r *RPC) Header(ctx context.Context, tag string) (Block, error) {
	raw, res, e := r.Call(ctx, "eth_getBlockByNumber", []any{tag, false}, "normal")
	b := Block{Response: res}
	if e != nil {
		return b, e
	}
	var v struct{ Number, Hash, ParentHash, Timestamp, BaseFeePerGas string }
	if e = exactJSON(raw, &v); e != nil {
		return b, e
	}
	if b.Number, e = q64(v.Number); e != nil {
		return b, e
	}
	if b.Hash, e = ParseHex(v.Hash, 32); e != nil {
		return b, e
	}
	if b.ParentHash, e = ParseHex(v.ParentHash, 32); e != nil {
		return b, e
	}
	sec, e := q64(v.Timestamp)
	if e != nil || sec > 1<<63-1 {
		return b, errors.New("block_timestamp")
	}
	b.Time = time.Unix(int64(sec), 0).UTC()
	if v.BaseFeePerGas != "" {
		if b.BaseFee, e = quantity(v.BaseFeePerGas); e != nil {
			return b, e
		}
	}
	return b, nil
}
func (r *RPC) Code(ctx context.Context, address string, b Block) (string, Response, error) {
	raw, res, e := r.Call(ctx, "eth_getCode", []any{Hex(address), blockRef(b)}, "normal")
	if e != nil {
		return "", res, e
	}
	var s string
	if e = exactJSON(raw, &s); e != nil {
		return "", res, e
	}
	code, e := decodeBytes(s)
	if e != nil || len(code) == 0 {
		return "", res, errors.New("empty_contract_code")
	}
	return string(crypto.Keccak256(code)), res, nil
}
func blockRef(b Block) map[string]any {
	return map[string]any{"blockHash": Hex(b.Hash), "requireCanonical": true}
}
func decodeBytes(s string) ([]byte, error) {
	if !strings.HasPrefix(s, "0x") || len(s)%2 != 0 {
		return nil, errors.New("rpc_bytes")
	}
	return hex.DecodeString(s[2:])
}
func arguments(types []string) (abi.Arguments, error) {
	out := abi.Arguments{}
	for _, s := range types {
		t, e := abi.NewType(s, "", nil)
		if e != nil {
			return nil, e
		}
		out = append(out, abi.Argument{Type: t})
	}
	return out, nil
}
func (r *RPC) View(ctx context.Context, address string, b Block, signature string, outputs []string, class string, values ...any) ([]any, Response, error) {
	l := strings.IndexByte(signature, '(')
	if l < 1 || !strings.HasSuffix(signature, ")") {
		return nil, Response{}, errors.New("abi_signature")
	}
	types := []string{}
	if signature[l+1:len(signature)-1] != "" {
		types = strings.Split(signature[l+1:len(signature)-1], ",")
	}
	args, e := arguments(types)
	if e != nil {
		return nil, Response{}, e
	}
	encoded, e := args.Pack(values...)
	if e != nil {
		return nil, Response{}, e
	}
	data := append(crypto.Keccak256([]byte(signature))[:4], encoded...)
	raw, res, e := r.Call(ctx, "eth_call", []any{map[string]string{"to": Hex(address), "data": "0x" + hex.EncodeToString(data), "gas": "0x4c4b40"}, blockRef(b)}, class)
	if e != nil {
		return nil, res, e
	}
	var text string
	if e = exactJSON(raw, &text); e != nil {
		return nil, res, e
	}
	decoded, e := decodeBytes(text)
	if e != nil {
		return nil, res, e
	}
	outs, e := arguments(outputs)
	if e != nil {
		return nil, res, e
	}
	v, e := outs.Unpack(decoded)
	if e != nil {
		return nil, res, e
	}
	again, e := outs.Pack(v...)
	if e != nil || !bytes.Equal(again, decoded) {
		return nil, res, errors.New("noncanonical_abi_response")
	}
	return v, res, nil
}
func (r *RPC) Uint(ctx context.Context, address string, b Block, sig string, values ...any) (*big.Int, Response, error) {
	v, res, e := r.View(ctx, address, b, sig, []string{"uint256"}, "normal", values...)
	if e != nil {
		return nil, res, e
	}
	n, ok := v[0].(*big.Int)
	if !ok || !Uint256(n) {
		return nil, res, errors.New("abi_uint256")
	}
	return n, res, nil
}
func (r *RPC) Canonical(ctx context.Context, b Block) (bool, Response, error) {
	again, e := r.Header(ctx, fmt.Sprintf("0x%x", b.Number))
	return e == nil && again.Hash == b.Hash, again.Response, e
}
