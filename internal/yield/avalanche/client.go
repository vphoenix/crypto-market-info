package avalanche

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
	marketyield "github.com/vphoenix/crypto-market-info/internal/yield"
)

const (
	DefaultRPCURL = "https://api.avax.network/ext/bc/C/rpc"
	NativeAsset   = "eip155:43114:native:AVAX"
	Network       = "avalanche-c-mainnet"
)

type Client struct {
	Endpoint   string
	HTTPClient *http.Client
	Retry      exchange.HTTPRetryConfig
	Now        func() time.Time
}

func NewClient(endpoint string) *Client {
	if strings.TrimSpace(endpoint) == "" {
		endpoint = DefaultRPCURL
	}
	retry := exchange.DefaultHTTPRetryConfig()
	retry.Cooldown = exchange.NewRequestGate(time.Second)
	return &Client{Endpoint: endpoint, HTTPClient: &http.Client{Timeout: 20 * time.Second}, Retry: retry}
}

// Read is either one fixed eth_call or an eth_getBalance when Data is empty.
// Only zero/one-argument static ABI calls are needed for these three routes.
type Read struct {
	Address string
	Data    string
}

type Snapshot struct {
	BlockHeight uint64
	BlockHash   string
	BlockTime   time.Time
	CollectedAt time.Time
	Results     []string
	PayloadHash string
}

type rpcRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      int    `json:"id"`
	Method  string `json:"method"`
	Params  []any  `json:"params"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      *int            `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   json.RawMessage `json:"error"`
}

type rpcBatch struct {
	Results  []json.RawMessage
	Request  []byte
	Response []byte
}

func (c *Client) Read(ctx context.Context, reads []Read) (Snapshot, error) {
	if c == nil || strings.TrimSpace(c.Endpoint) == "" || len(reads) == 0 || len(reads) > 40 {
		return Snapshot{}, fmt.Errorf("Avalanche RPC invalid read configuration")
	}
	for _, read := range reads {
		if _, err := fixedHex(read.Address, 20); err != nil {
			return Snapshot{}, fmt.Errorf("Avalanche RPC invalid target address")
		}
		if read.Data != "" {
			if len(read.Data) != 10 && len(read.Data) != 74 {
				return Snapshot{}, fmt.Errorf("Avalanche RPC invalid call data length")
			}
			if _, err := fixedHex(read.Data, (len(read.Data)-2)/2); err != nil {
				return Snapshot{}, fmt.Errorf("Avalanche RPC invalid call data")
			}
		}
	}
	anchor, err := c.batch(ctx, "anchor", []rpcRequest{
		{JSONRPC: "2.0", ID: 1, Method: "eth_chainId", Params: []any{}},
		{JSONRPC: "2.0", ID: 2, Method: "eth_getBlockByNumber", Params: []any{"finalized", false}},
	})
	if err != nil {
		return Snapshot{}, err
	}
	var chainText string
	if json.Unmarshal(anchor.Results[0], &chainText) != nil {
		return Snapshot{}, fmt.Errorf("Avalanche anchor invalid chain quantity")
	}
	chain, err := Quantity(chainText)
	if err != nil || chain.Cmp(big.NewInt(43114)) != 0 {
		return Snapshot{}, fmt.Errorf("Avalanche anchor is not C-chain mainnet")
	}
	var block struct {
		Number    string `json:"number"`
		Hash      string `json:"hash"`
		Timestamp string `json:"timestamp"`
	}
	if json.Unmarshal(anchor.Results[1], &block) != nil {
		return Snapshot{}, fmt.Errorf("Avalanche anchor invalid block")
	}
	height, heightErr := Quantity(block.Number)
	stamp, stampErr := Quantity(block.Timestamp)
	_, hashErr := fixedHex(block.Hash, 32)
	if heightErr != nil || stampErr != nil || hashErr != nil || !height.IsUint64() || !stamp.IsUint64() || stamp.Sign() <= 0 || stamp.Uint64() > math.MaxInt64 {
		return Snapshot{}, fmt.Errorf("Avalanche anchor invalid height, timestamp or hash")
	}
	snapshot := Snapshot{BlockHeight: height.Uint64(), BlockHash: strings.ToLower(block.Hash), BlockTime: time.Unix(int64(stamp.Uint64()), 0).UTC()}
	anchorParam := struct {
		BlockHash        string `json:"blockHash"`
		RequireCanonical bool   `json:"requireCanonical"`
	}{BlockHash: snapshot.BlockHash, RequireCanonical: true}
	requests := make([]rpcRequest, 0, len(reads))
	for i, read := range reads {
		request := rpcRequest{JSONRPC: "2.0", ID: i + 1}
		if read.Data == "" {
			request.Method, request.Params = "eth_getBalance", []any{strings.ToLower(read.Address), anchorParam}
		} else {
			call := struct {
				To   string `json:"to"`
				Data string `json:"data"`
			}{To: strings.ToLower(read.Address), Data: strings.ToLower(read.Data)}
			request.Method, request.Params = "eth_call", []any{call, anchorParam}
		}
		requests = append(requests, request)
	}
	state, err := c.batch(ctx, "state", requests)
	if err != nil {
		return Snapshot{}, err
	}
	snapshot.Results = make([]string, len(reads))
	for i := range reads {
		if json.Unmarshal(state.Results[i], &snapshot.Results[i]) != nil {
			return Snapshot{}, fmt.Errorf("Avalanche state invalid result type")
		}
	}
	now := c.Now
	if now == nil {
		now = time.Now
	}
	snapshot.CollectedAt = now().UTC().Truncate(time.Millisecond)
	if snapshot.BlockTime.Before(snapshot.CollectedAt.Add(-10*time.Minute)) || snapshot.BlockTime.After(snapshot.CollectedAt.Add(time.Minute)) {
		return Snapshot{}, fmt.Errorf("Avalanche finalized block is stale or in the future")
	}
	snapshot.PayloadHash = marketyield.HashPayloads(
		marketyield.Payload{Name: "anchor-request", Body: anchor.Request}, marketyield.Payload{Name: "anchor-response", Body: anchor.Response},
		marketyield.Payload{Name: "state-request", Body: state.Request}, marketyield.Payload{Name: "state-response", Body: state.Response})
	return snapshot, nil
}

func (c *Client) batch(ctx context.Context, stage string, requests []rpcRequest) (rpcBatch, error) {
	body, err := json.Marshal(requests)
	if err != nil {
		return rpcBatch{}, fmt.Errorf("Avalanche %s invalid request", stage)
	}
	response, err := exchange.PostJSON(ctx, c.HTTPClient, c.Endpoint, body, c.Retry)
	if err != nil {
		// The common HTTP helper includes URL and provider text in its errors.
		// Never wrap it: RPC paths, query strings and userinfo can carry keys.
		if ctx.Err() != nil {
			return rpcBatch{}, fmt.Errorf("Avalanche %s request: %w", stage, ctx.Err())
		}
		return rpcBatch{}, fmt.Errorf("Avalanche %s RPC request failed", stage)
	}
	var decoded []rpcResponse
	if marketyield.DecodeJSON(response, &decoded) != nil || len(decoded) != len(requests) {
		return rpcBatch{}, fmt.Errorf("Avalanche %s invalid or incomplete batch", stage)
	}
	results := make([]json.RawMessage, len(requests))
	for _, item := range decoded {
		if item.JSONRPC != "2.0" || item.ID == nil || *item.ID < 1 || *item.ID > len(requests) || results[*item.ID-1] != nil {
			return rpcBatch{}, fmt.Errorf("Avalanche %s invalid version or response IDs", stage)
		}
		if len(item.Error) > 0 && string(item.Error) != "null" {
			var rpcError struct {
				Code *int64 `json:"code"`
			}
			if json.Unmarshal(item.Error, &rpcError) == nil && rpcError.Code != nil {
				return rpcBatch{}, fmt.Errorf("Avalanche %s RPC error code %d", stage, *rpcError.Code)
			}
			return rpcBatch{}, fmt.Errorf("Avalanche %s RPC error", stage)
		}
		if len(item.Result) == 0 || string(item.Result) == "null" {
			return rpcBatch{}, fmt.Errorf("Avalanche %s missing result", stage)
		}
		results[*item.ID-1] = item.Result
	}
	return rpcBatch{Results: results, Request: body, Response: response}, nil
}

func fixedHex(raw string, size int) ([]byte, error) {
	if len(raw) != 2+size*2 || !strings.HasPrefix(raw, "0x") {
		return nil, fmt.Errorf("invalid fixed-width hex")
	}
	value, err := hex.DecodeString(raw[2:])
	if err != nil {
		return nil, fmt.Errorf("invalid hex digits")
	}
	return value, nil
}

// Quantity is RPC's minimal hex integer, not a padded ABI word.
func Quantity(raw string) (*big.Int, error) {
	if !strings.HasPrefix(raw, "0x") || len(raw) < 3 || len(raw) > 66 || (len(raw) > 3 && raw[2] == '0') {
		return nil, fmt.Errorf("invalid RPC quantity")
	}
	for _, digit := range raw[2:] {
		if !((digit >= '0' && digit <= '9') || (digit >= 'a' && digit <= 'f') || (digit >= 'A' && digit <= 'F')) {
			return nil, fmt.Errorf("invalid RPC quantity digits")
		}
	}
	value, ok := new(big.Int).SetString(raw[2:], 16)
	if !ok || value.BitLen() > 256 {
		return nil, fmt.Errorf("invalid uint256 quantity")
	}
	return value, nil
}

func Uint256(raw string) (*big.Int, error) {
	word, err := fixedHex(raw, 32)
	if err != nil {
		return nil, err
	}
	return new(big.Int).SetBytes(word), nil
}

func Uint64(raw string) (uint64, error) {
	value, err := Uint256(raw)
	if err != nil || !value.IsUint64() {
		return 0, fmt.Errorf("ABI integer is not uint64")
	}
	return value.Uint64(), nil
}

func Bool(raw string) (bool, error) {
	word, err := fixedHex(raw, 32)
	if err != nil {
		return false, err
	}
	return wordBool(word)
}

func wordBool(word []byte) (bool, error) {
	value := new(big.Int).SetBytes(word)
	if value.Cmp(big.NewInt(1)) > 0 {
		return false, fmt.Errorf("ABI bool must be zero or one")
	}
	return value.Sign() != 0, nil
}

func Address(raw string) (string, error) {
	word, err := fixedHex(raw, 32)
	if err != nil {
		return "", err
	}
	if new(big.Int).SetBytes(word[:12]).Sign() != 0 {
		return "", fmt.Errorf("ABI address has nonzero high bytes")
	}
	return "0x" + hex.EncodeToString(word[12:]), nil
}

// Markets is BENQI's fixed (bool, uint256, bool) tuple. Validate all three
// words even though only the first word, isListed, is used here.
func Markets(raw string) (bool, error) {
	words, err := fixedHex(raw, 96)
	if err != nil {
		return false, err
	}
	listed, err := wordBool(words[:32])
	if err != nil {
		return false, err
	}
	if _, err = wordBool(words[64:]); err != nil {
		return false, err
	}
	return listed, nil
}

func Scaled(value *big.Int, exponent int32) (decimal.Decimal, error) {
	if value == nil || value.Sign() < 0 {
		return decimal.Zero, fmt.Errorf("negative or missing amount")
	}
	result := decimal.NewFromBigInt(value, exponent)
	if result.GreaterThanOrEqual(decimal.New(1, 20)) {
		return decimal.Zero, fmt.Errorf("amount exceeds Decimal(38,18)")
	}
	return result.Truncate(18), nil
}

func PositiveScaled(value *big.Int, exponent int32) (decimal.Decimal, error) {
	result, err := Scaled(value, exponent)
	if err != nil || !result.IsPositive() {
		return decimal.Zero, fmt.Errorf("exchange ratio is zero or outside Decimal(38,18)")
	}
	return result, nil
}
