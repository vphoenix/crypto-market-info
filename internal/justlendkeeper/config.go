package justlendkeeper

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"os"
	"time"
)

const Network = "tron-mainnet"
const ContractBase58 = "TU2MJ5Veik1LRAgjeSzEdvmDYx7mefJZvd"
const ContractHex = "41c60a6f5c81431c97ed01b61698b6853557f3afd4"
const ImplementationHex = "418cb94f0ccb41cc8de909e00b01edef6a717ade8c"
const ImplementationSHA = "c66f4e441f88f9bc0b2cf31be8417c5ea220596cfc49dd9abfebf85cf92c3782"

type Config struct {
	NodeRPCURL  string   `json:"node_rpc_url"`
	EventAPIURL string   `json:"event_api_url"`
	QuoteAPIURL string   `json:"quote_api_url"`
	Callers     []string `json:"callers"`
	DailyBudget uint32   `json:"daily_budget"`
	// Positive reward classification remains gated until the successful ABI is verified.
}

func DefaultConfig() Config {
	return Config{"https://tron-rpc.publicnode.com", "https://api.trongrid.io", "https://api.binance.com", []string{"41020c064bab81eb2ef520f1cc2a26f5f1867e29b3", "4166fff827c2d1d2804881e8e74dd465186126f801"}, 40000}
}
func LoadConfig(path string) (Config, error) {
	c := DefaultConfig()
	b, e := os.ReadFile(path)
	if e != nil {
		return c, e
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if e = d.Decode(&c); e != nil {
		return c, e
	}
	if e = d.Decode(new(any)); e != io.EOF {
		return c, errors.New("trailing_config_data")
	}
	return c, c.Validate()
}
func (c Config) Validate() error {
	if c.DailyBudget == 0 || c.DailyBudget > 40000 {
		return errors.New("daily_budget_must_be_1_to_40000")
	}
	for _, s := range []string{c.NodeRPCURL, c.EventAPIURL, c.QuoteAPIURL} {
		u, e := url.Parse(s)
		if e != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
			return errors.New("public_https_origin_required")
		}
	}
	if len(c.Callers) != 2 || c.Callers[0] == c.Callers[1] {
		return errors.New("two_distinct_public_callers_required")
	}
	for _, s := range c.Callers {
		if _, e := HexAddress(s); e != nil {
			return e
		}
	}
	return nil
}
func (c Config) Hash() string { b, _ := json.Marshal(c); return Hash(b) }
func (c Config) Base(source string) string {
	switch source {
	case "publicnode":
		return c.NodeRPCURL
	case "trongrid":
		return c.EventAPIURL
	case "binance":
		return c.QuoteAPIURL
	}
	return ""
}
func HexAddress(s string) (string, error) {
	b, e := hex.DecodeString(s)
	if e != nil || len(b) != 21 || b[0] != 0x41 {
		return "", errors.New("invalid_tron_hex_address")
	}
	return string(b), nil
}
func Address(s string) (string, error) {
	if len(s) == 42 && s[:2] == "0x" {
		return HexAddress("41" + s[2:])
	}
	if len(s) == 42 {
		return HexAddress(s)
	}
	if len(s) == 40 {
		return HexAddress("41" + s)
	}
	return "", errors.New("invalid_address")
}
func UTC(t time.Time) time.Time { return t.UTC().Truncate(time.Microsecond) }
func Ptr[T any](v T) *T         { return &v }
func BinaryHex(s string, n int) (string, error) {
	b, e := hex.DecodeString(s)
	if e != nil || len(b) != n {
		return "", errors.New("invalid_hex_length")
	}
	return string(b), nil
}
