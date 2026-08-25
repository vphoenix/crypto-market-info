package solana

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/exchange"
	marketyield "github.com/vphoenix/crypto-market-info/internal/yield"
)

const (
	MainnetRPCURL    = "https://api.mainnet.solana.com"
	StakePoolProgram = "SPoo1Ku8WFXoNDMHPsrGSTSG1Y47rzgn41SLUNakuHy"
	SPLTokenProgram  = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
	BSOLPoolAddress  = "stk9ApL5HeVAwPLr3TLhDXdZS8ptVu7zp6ov8HFDuMi"
	BSOLMintAddress  = "bSo13r4TkiE4KumL71LsHTPpL2euBYLFx6h9HP3piy1"
	JitoPoolAddress  = "Jito4APyf642JPZPx3hGc6WWJ8zPKtRbRs4P815Awbb"
	JitoMintAddress  = "J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn"
	MarinadeProgram  = "MarBmsSgKXdrN1egZf5sqe1TMai9K1rChYNDJgjq7aD"
	MarinadeState    = "8szGkuLTAux9XMgZ2vtY39jVSowEcpBfFfD8hXSEqdGC"
	MSOLMintAddress  = "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So"
	SOLAsset         = "solana:mainnet:native:SOL"
	BSOLAsset        = "solana:mainnet:spl:" + BSOLMintAddress
	JitoSOLAsset     = "solana:mainnet:spl:" + JitoMintAddress
	MSOLAsset        = "solana:mainnet:spl:" + MSOLMintAddress
)

type Client struct {
	BaseURL    string
	HTTPClient *http.Client
	Retry      exchange.HTTPRetryConfig
}

func NewClient(baseURL string) *Client {
	if strings.TrimSpace(baseURL) == "" {
		baseURL = MainnetRPCURL
	}
	retry := exchange.DefaultHTTPRetryConfig()
	// bSOL, JitoSOL, and mSOL share one client and start concurrently. A small
	// gate keeps their low-frequency public RPC calls serialized.
	retry.Cooldown = exchange.NewRequestGate(250 * time.Millisecond)
	return &Client{BaseURL: strings.TrimRight(baseURL, "/"), HTTPClient: &http.Client{Timeout: 20 * time.Second}, Retry: retry}
}

type rpcError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

type rpcEnvelope struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.Number     `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *rpcError       `json:"error"`
}

func (c *Client) call(ctx context.Context, method string, params any, target any) ([]byte, error) {
	if c == nil || strings.TrimSpace(c.BaseURL) == "" {
		return nil, fmt.Errorf("solana rpc client is not configured")
	}
	request, err := json.Marshal(struct {
		JSONRPC string `json:"jsonrpc"`
		ID      int    `json:"id"`
		Method  string `json:"method"`
		Params  any    `json:"params"`
	}{JSONRPC: "2.0", ID: 1, Method: method, Params: params})
	if err != nil {
		return nil, err
	}
	payload, err := exchange.PostJSON(ctx, c.HTTPClient, c.BaseURL, request, c.Retry)
	if err != nil {
		return nil, err
	}
	var envelope rpcEnvelope
	if err = marketyield.DecodeJSON(payload, &envelope); err != nil {
		return nil, fmt.Errorf("%s response: %w", method, err)
	}
	if envelope.JSONRPC != "2.0" || envelope.ID.String() != "1" {
		return nil, fmt.Errorf("%s response has unexpected envelope", method)
	}
	if envelope.Error != nil {
		return nil, fmt.Errorf("%s rpc error %d: %s", method, envelope.Error.Code, envelope.Error.Message)
	}
	if len(envelope.Result) == 0 || string(envelope.Result) == "null" {
		return nil, fmt.Errorf("%s returned null result", method)
	}
	if err = marketyield.DecodeJSON(envelope.Result, target); err != nil {
		return nil, fmt.Errorf("%s result: %w", method, err)
	}
	return payload, nil
}

type accountResult struct {
	Context struct {
		Slot json.Number `json:"slot"`
	} `json:"context"`
	Value *struct {
		Data       []json.RawMessage `json:"data"`
		Executable bool              `json:"executable"`
		Owner      string            `json:"owner"`
	} `json:"value"`
}

type Account struct {
	Slot       uint64
	Owner      string
	Data       []byte
	Executable bool
	Payload    []byte
}

func (c *Client) AccountInfo(ctx context.Context, address string) (Account, error) {
	if _, err := DecodePubkey(address); err != nil {
		return Account{}, fmt.Errorf("invalid account address: %w", err)
	}
	var result accountResult
	payload, err := c.call(ctx, "getAccountInfo", []any{address, map[string]any{"encoding": "base64", "commitment": "finalized"}}, &result)
	if err != nil {
		return Account{}, err
	}
	if result.Value == nil || len(result.Value.Data) != 2 || result.Value.Executable {
		return Account{}, fmt.Errorf("account %s is missing or invalid", address)
	}
	var encoded, encoding string
	if err = json.Unmarshal(result.Value.Data[0], &encoded); err != nil {
		return Account{}, fmt.Errorf("account %s data: %w", address, err)
	}
	if err = json.Unmarshal(result.Value.Data[1], &encoding); err != nil || encoding != "base64" {
		return Account{}, fmt.Errorf("account %s has unexpected data encoding", address)
	}
	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return Account{}, fmt.Errorf("account %s base64: %w", address, err)
	}
	slot, err := parseUint(result.Context.Slot, "context slot")
	if err != nil {
		return Account{}, err
	}
	return Account{Slot: slot, Owner: result.Value.Owner, Data: data, Executable: result.Value.Executable, Payload: payload}, nil
}

func (c *Client) ValidateMSOLIdentity(ctx context.Context) error {
	state, err := c.AccountInfo(ctx, MarinadeState)
	if err != nil {
		return err
	}
	if state.Owner != MarinadeProgram {
		return fmt.Errorf("mSOL state owner %q does not match Marinade program", state.Owner)
	}
	mint, err := c.AccountInfo(ctx, MSOLMintAddress)
	if err != nil {
		return err
	}
	if mint.Owner != SPLTokenProgram || len(mint.Data) < 82 || mint.Data[44] != 9 || mint.Data[45] != 1 {
		return fmt.Errorf("mSOL mint identity is invalid")
	}
	return nil
}

func DecodePubkey(value string) ([32]byte, error) {
	var out [32]byte
	if value == "" || len(value) > 44 {
		return out, fmt.Errorf("invalid base58 pubkey length")
	}
	number := new(big.Int)
	base := big.NewInt(58)
	for _, character := range value {
		index := strings.IndexRune("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz", character)
		if index < 0 {
			return out, fmt.Errorf("invalid base58 character %q", character)
		}
		number.Mul(number, base)
		number.Add(number, big.NewInt(int64(index)))
	}
	decoded := number.Bytes()
	leading := 0
	for leading < len(value) && value[leading] == '1' {
		leading++
	}
	if leading+len(decoded) != len(out) {
		return out, fmt.Errorf("pubkey does not decode to 32 bytes")
	}
	copy(out[leading:], decoded)
	return out, nil
}

func parseUint(value json.Number, name string) (uint64, error) {
	raw := value.String()
	if raw == "" || strings.ContainsAny(raw, ".eE+-") {
		return 0, fmt.Errorf("%s is not an unsigned integer", name)
	}
	parsed, ok := new(big.Int).SetString(raw, 10)
	if !ok || parsed.Sign() < 0 || !parsed.IsUint64() {
		return 0, fmt.Errorf("%s is not an unsigned 64-bit integer", name)
	}
	return parsed.Uint64(), nil
}
