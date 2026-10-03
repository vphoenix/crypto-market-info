package reserve

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"io"
	"math/big"
	"os"
)

const Implementation = "0xb6b35b2c7032E00BAa2535Ba480d461321B7E0A6"

// Bump whenever decoding, sizing, permission or sampling semantics change.
const CollectorVersion = "reserve-r5-mvp-1"

type ManifestIdentity struct {
	CollectorVersion string
	SourceFileHash   dex.Hash
	ABIHash          dex.Hash
}

func (m Manifest) Identity() ManifestIdentity {
	return ManifestIdentity{CollectorVersion, dex.Digest(m.Raw), dex.Digest([]byte(abiJSON))}
}

const ImplementationSlot = "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc"

var USDC = dex.MustAddress("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")
var USDT = dex.MustAddress("0xdac17f958d2ee523a2206206994597c13d831ec7")
var WETH = dex.MustAddress("0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2")

type Path struct {
	Tokens []dex.Address `json:"tokens"`
	Pools  []dex.Address `json:"pools"`
	Fees   []uint32      `json:"fees"`
}
type Folio struct {
	Address         dex.Address `json:"address"`
	Name            string      `json:"name"`
	DeploymentBlock uint64      `json:"deployment_block"`
	ShareDecimals   uint8       `json:"share_decimals"`
}
type AssetIdentity struct {
	Address           dex.Address `json:"address"`
	Decimals          uint8       `json:"decimals"`
	TransferSemantics string      `json:"transfer_semantics"`
}
type Manifest struct {
	Raw                    []byte          `json:"-"`
	Assets                 []AssetIdentity `json:"assets"`
	ChainID                uint64          `json:"chain_id"`
	SourceCommit           string          `json:"source_commit"`
	Implementation         dex.Address     `json:"implementation"`
	ImplementationCodeHash dex.Hash        `json:"implementation_code_hash"`
	Quoter                 dex.Address     `json:"quoter"`
	QuoterCodeHash         dex.Hash        `json:"quoter_code_hash"`
	Factory                dex.Address     `json:"factory"`
	Folios                 []Folio         `json:"folios"`
	Paths                  []Path          `json:"paths"` // forward paths always begin at USDC
	Budgets                []string        `json:"budgets_usdc_raw"`
	Hash                   dex.Hash        `json:"-"`
}

func LoadManifest(file string) (Manifest, error) {
	b, e := os.ReadFile(file)
	if e != nil {
		return Manifest{}, e
	}
	var m Manifest
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if e = d.Decode(&m); e != nil {
		return m, e
	}
	var extra any
	if e = d.Decode(&extra); e != io.EOF {
		return m, errors.New("trailing_manifest_data")
	}
	m.Raw = append([]byte{}, b...)
	m.Hash = dex.ObjectHash(m.Identity())
	return m, m.Validate()
}
func (m Manifest) Validate() error {
	if m.ChainID != 1 || m.SourceCommit != "241f0d244c45a311a4cd2af2c383a4f3bbe56265" || m.Implementation != dex.MustAddress(Implementation) || m.ImplementationCodeHash == (dex.Hash{}) || m.QuoterCodeHash == (dex.Hash{}) || len(m.Folios) == 0 || len(m.Folios) > 3 || len(m.Budgets) == 0 || len(m.Budgets) > 3 {
		return errors.New("invalid_reserve_manifest")
	}
	seen := map[dex.Address]bool{}
	for _, f := range m.Folios {
		if f.Address == (dex.Address{}) || seen[f.Address] {
			return errors.New("duplicate_folio")
		}
		seen[f.Address] = true
	}
	counts := map[dex.Address]int{}
	known := map[dex.Address]bool{}
	for _, a := range m.Assets {
		if known[a.Address] || a.Address == (dex.Address{}) || a.TransferSemantics == "" {
			return errors.New("invalid_asset_whitelist")
		}
		known[a.Address] = true
	}
	for _, p := range m.Paths {
		for _, a := range p.Tokens {
			if !known[a] {
				return errors.New("path_token_not_whitelisted")
			}
		}
		if len(p.Tokens) < 2 || len(p.Tokens) > 3 || len(p.Pools) != len(p.Tokens)-1 || len(p.Fees) != len(p.Pools) || p.Tokens[0] != USDC {
			return errors.New("invalid_v3_path")
		}
		counts[p.Tokens[len(p.Tokens)-1]]++
		if counts[p.Tokens[len(p.Tokens)-1]] > 2 {
			return errors.New("too_many_paths")
		}
		for i, a := range p.Pools {
			if a == (dex.Address{}) || p.Tokens[i] == p.Tokens[i+1] || p.Fees[i] == 0 || p.Fees[i] > 1_000_000 {
				return errors.New("invalid_pool")
			}
		}
	}
	for _, s := range m.Budgets {
		n, ok := new(big.Int).SetString(s, 10)
		if !ok || !Uint256(n) || n.Sign() <= 0 {
			return errors.New("invalid_budget")
		}
	}
	return nil
}
