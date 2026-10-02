package across

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/ethereum/go-ethereum/crypto"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
)

const ImplementationSlot = "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc"
const CollectorVersion = "across-mvp-1"

type Implementation struct {
	Address     string `json:"address"`
	CodeHash    string `json:"code_hash"`
	ABIRevision string `json:"abi_revision"`
	Evidence    string `json:"evidence"`
}
type ChainConfig struct {
	ChainID         uint64           `json:"chain_id"`
	SpokePool       string           `json:"spoke_pool"`
	USDC            string           `json:"usdc"`
	RPCEnv          string           `json:"rpc_env"`
	DefaultRPC      string           `json:"default_rpc"`
	Implementations []Implementation `json:"implementations"`
}
type Manifest struct {
	Raw          []byte        `json:"-"`
	Hash         string        `json:"-"`
	Chains       []ChainConfig `json:"chains"`
	PollMillis   uint32        `json:"poll_millis"`
	MaxLogBlocks uint64        `json:"max_log_blocks"`
	MaxLogs      uint32        `json:"max_logs"`
	FreshMillis  uint32        `json:"fresh_millis"`
	FutureMillis uint32        `json:"future_millis"`
	MaxPending   uint32        `json:"max_pending"`
	BinanceURL   string        `json:"binance_url"`
}

func ParseHex(s string, n int) (string, error) {
	if !strings.HasPrefix(s, "0x") || len(s) != 2+n*2 {
		return "", errors.New("invalid_hex_width")
	}
	b, e := hex.DecodeString(s[2:])
	if e != nil {
		return "", errors.New("invalid_hex")
	}
	return string(b), nil
}
func LoadManifest(file string) (Manifest, error) {
	var m Manifest
	b, e := os.ReadFile(file)
	if e != nil {
		return m, e
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if e = d.Decode(&m); e != nil {
		return m, e
	}
	if d.Decode(new(any)) != io.EOF {
		return m, errors.New("trailing_manifest_data")
	}
	m.Raw = b
	for i := range m.Chains {
		c := &m.Chains[i]
		c.SpokePool, e = ParseHex(c.SpokePool, 20)
		if e != nil {
			return m, e
		}
		c.USDC, e = ParseHex(c.USDC, 20)
		if e != nil {
			return m, e
		}
		for j := range c.Implementations {
			v := &c.Implementations[j]
			v.Address, e = ParseHex(v.Address, 20)
			if e != nil {
				return m, e
			}
			v.CodeHash, e = ParseHex(v.CodeHash, 32)
			if e != nil {
				return m, e
			}
		}
	}
	h := sha256.Sum256(append(append(append([]byte{}, b...), []byte(abiJSON)...), []byte(CollectorVersion)...))
	m.Hash = string(h[:])
	if e = m.Validate(); e != nil {
		return m, e
	}
	return m, m.verifyEvidence(filepath.Dir(filepath.Dir(file)))
}
func (m Manifest) Validate() error {
	if len(m.Chains) != 2 || m.PollMillis < 100 || m.MaxLogBlocks == 0 || m.MaxLogBlocks > 512 || m.MaxLogs == 0 || m.FreshMillis == 0 || m.FreshMillis > 60000 || m.FutureMillis > 10000 || m.MaxPending == 0 || m.MaxPending > 1000 || m.BinanceURL != "https://api.binance.com" {
		return errors.New("invalid_across_manifest")
	}
	want := map[uint64][2]string{8453: {"0x09aea4b2242abc8bb4bb78d537a67a245a7bec64", "0x833589fcd6edb6e08f4c7c32d4f71b54bdA02913"}, 42161: {"0xe35e9842fceaca96570b734083f4a58e8f7c5f2a", "0xaf88d065e77c8cc2239327c5edb3a432268e5831"}}
	seen := map[uint64]bool{}
	for _, c := range m.Chains {
		w, ok := want[c.ChainID]
		if !ok || seen[c.ChainID] || !strings.EqualFold(Hex(c.SpokePool), w[0]) || !strings.EqualFold(Hex(c.USDC), w[1]) || c.RPCEnv == "" || !strings.HasPrefix(c.DefaultRPC, "https://") {
			return errors.New("invalid_chain_identity")
		}
		seen[c.ChainID] = true
		seenImpl := map[string]bool{}
		for _, v := range c.Implementations {
			if len(v.Address) != 20 || len(v.CodeHash) != 32 || v.ABIRevision != ABIRevision || v.Evidence == "" || seenImpl[v.Address] {
				return errors.New("invalid_implementation_allowlist")
			}
			seenImpl[v.Address] = true
		}
	}
	return nil
}
func (m Manifest) Chain(id uint64) (ChainConfig, bool) {
	for _, c := range m.Chains {
		if c.ChainID == id {
			return c, true
		}
	}
	return ChainConfig{}, false
}
func (c ChainConfig) Endpoint() string {
	if s := os.Getenv(c.RPCEnv); s != "" {
		return s
	}
	return c.DefaultRPC
}

// Verify the allowlist against the archived explorer response, including its
// SHA-256, bytecode, and the exact event layouts used by this decoder.
func (m Manifest) verifyEvidence(root string) error {
	var embedded []map[string]any
	if json.Unmarshal([]byte(abiJSON), &embedded) != nil {
		return errors.New("invalid_embedded_abi")
	}
	for _, c := range m.Chains {
		for _, v := range c.Implementations {
			parts := strings.Split(v.Evidence, "#sha256=")
			if len(parts) != 2 || len(parts[1]) != 64 || filepath.IsAbs(parts[0]) || strings.HasPrefix(filepath.Clean(parts[0]), "..") {
				return errors.New("invalid_implementation_evidence")
			}
			raw, e := os.ReadFile(filepath.Join(root, parts[0]))
			if e != nil {
				return errors.New("implementation_evidence_missing")
			}
			hash := sha256.Sum256(raw)
			if hex.EncodeToString(hash[:]) != parts[1] {
				return errors.New("implementation_evidence_hash_mismatch")
			}
			var proof struct {
				Verified bool             `json:"is_verified"`
				Code     string           `json:"deployed_bytecode"`
				ABI      []map[string]any `json:"abi"`
			}
			if json.Unmarshal(raw, &proof) != nil || !proof.Verified {
				return errors.New("implementation_source_unverified")
			}
			code, e := ParseHex(proof.Code, (len(proof.Code)-2)/2)
			if e != nil || len(code) == 0 || string(crypto.Keccak256([]byte(code))) != v.CodeHash {
				return errors.New("implementation_evidence_code_mismatch")
			}
			for _, entry := range embedded {
				name, _ := entry["name"].(string)
				kind, _ := entry["type"].(string)
				if kind != "event" && name != "fillStatuses" && name != "pausedFills" && name != "getCurrentTime" && name != "chainId" && name != "getV3RelayHash" {
					continue
				}
				found := false
				for _, actual := range proof.ABI {
					if actual["name"] == name && actual["type"] == kind {
						found = true
						if !reflect.DeepEqual(entry, actual) {
							return errors.New("implementation_evidence_abi_mismatch")
						}
						break
					}
				}
				// Auxiliary administrative events differ between chain implementations.
				required := kind != "event" || name == "FundsDeposited" || name == "FilledRelay" || name == "RequestedSpeedUpDeposit" || name == "ExecutedRelayerRefundRoot" || name == "ClaimedRelayerRefund" || name == "V3FundsDeposited" || name == "FilledV3Relay" || name == "RequestedSpeedUpV3Deposit"
				if !found && required {
					return errors.New("implementation_evidence_abi_missing")
				}
			}
		}
	}
	return nil
}
