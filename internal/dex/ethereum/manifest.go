package ethereum

import (
	_ "embed"
	"encoding/json"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/dexarb"
	"math/big"
)

//go:embed manifest.json
var ManifestJSON []byte

type Pool struct {
	ID      string      `json:"id"`
	Address dex.Address `json:"address"`
	Token0  string      `json:"token0"`
	Token1  string      `json:"token1"`
	Fee     uint32      `json:"fee"`
}
type CodePin struct {
	Name    string      `json:"name"`
	Address dex.Address `json:"address"`
	Hash    dex.Hash    `json:"sha256"`
}
type StoragePin struct {
	Address  dex.Address `json:"address"`
	Slot     string      `json:"slot"`
	Expected string      `json:"expected"`
}
type Manifest struct {
	Version   string                 `json:"version"`
	ChainID   uint64                 `json:"chain_id"`
	Addresses map[string]dex.Address `json:"addresses"`
	Pools     []Pool                 `json:"pools"`
	Codes     []CodePin              `json:"code_pins"`
	Storage   []StoragePin           `json:"storage_pins"`
	Decimals  map[string]uint8       `json:"decimals"`
	Amounts   []string               `json:"amounts_usdc"`
	Selectors map[string]string      `json:"selectors"`
	Events    map[string]string      `json:"events"`
}

func DefaultManifest() Manifest {
	var m Manifest
	if e := json.Unmarshal(ManifestJSON, &m); e != nil {
		panic(e)
	}
	return m
}
func ManifestHash() dex.Hash { return dex.Digest(ManifestJSON) }
func (m Manifest) Routes() []dexarb.Route {
	a := m.Addresses
	return []dexarb.Route{
		{ID: "dai_usdc_100_v3_first", Fee: 100, In: a["USDC"], Out: a["DAI"], PostBuy: true},
		{ID: "dai_usdc_100_psm_first", Fee: 100, In: a["DAI"], Out: a["USDC"], PreSell: true},
		{ID: "dai_usdc_500_v3_first", Fee: 500, In: a["USDC"], Out: a["DAI"], PostBuy: true},
		{ID: "dai_usdc_500_psm_first", Fee: 500, In: a["DAI"], Out: a["USDC"], PreSell: true},
		{ID: "usdc_usds_3000_v3_first", Fee: 3000, In: a["USDC"], Out: a["USDS"], PostBuy: true, PostWrapper: true},
		{ID: "usdc_usds_3000_psm_first", Fee: 3000, In: a["USDS"], Out: a["USDC"], PreSell: true, PreWrapper: true},
		{ID: "dai_usds_3000_dai_first", Fee: 3000, In: a["DAI"], Out: a["USDS"], PreSell: true, PostBuy: true, PostWrapper: true},
		{ID: "dai_usds_3000_usds_first", Fee: 3000, In: a["USDS"], Out: a["DAI"], PreSell: true, PreWrapper: true, PostBuy: true},
	}
}
func (m Manifest) Sizes() []*big.Int {
	out := make([]*big.Int, len(m.Amounts))
	for i, s := range m.Amounts {
		n, ok := new(big.Int).SetString(s, 10)
		if !ok {
			panic("invalid manifest amount")
		}
		out[i] = n.Mul(n, big.NewInt(1_000_000))
	}
	return out
}
func (m Manifest) LogAddresses() []string {
	out := []string{}
	for _, p := range m.Pools {
		out = append(out, p.Address.String())
	}
	for _, n := range []string{"psm", "converter", "wrapper", "daiJoin", "usdsJoin", "vat"} {
		out = append(out, m.Addresses[n].String())
	}
	return out
}
