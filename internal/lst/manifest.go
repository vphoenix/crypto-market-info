package lst

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"os"
	"sort"

	"github.com/ethereum/go-ethereum/common"
)

const CollectorVersion = "lst-mvp-11"

type Manifest struct {
	ChainId          uint64            `json:"chain_id"`
	AbiVersion       string            `json:"abi_version"`
	DeploymentSource string            `json:"deployment_source"`
	Addresses        map[string]string `json:"addresses"`
	CodeHashes       map[string]string `json:"code_hashes"`
	Pools            map[string]string `json:"pools"`
	BudgetsUsdtRaw   []string          `json:"budgets_usdt_raw"`
	MaxLogBlocks     uint64            `json:"max_log_blocks"`
	MaxLogs          uint32            `json:"max_logs"`
	Raw              []byte            `json:"-"`
	Hash             string            `json:"-"`
}

func LoadManifest(path string) (Manifest, error) {
	b, e := os.ReadFile(path)
	if e != nil {
		return Manifest{}, e
	}
	return ParseManifest(b)
}
func ParseManifest(b []byte) (Manifest, error) {
	var m Manifest
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if e := d.Decode(&m); e != nil {
		return m, e
	}
	if d.Decode(new(any)) != io.EOF {
		return m, errors.New("manifest_trailing_data")
	}
	if m.ChainId != 1 || m.AbiVersion != "lido-v4.0.1" || m.DeploymentSource == "" || m.MaxLogBlocks == 0 || m.MaxLogBlocks > 512 || m.MaxLogs < 1 || m.MaxLogs > 10000 {
		return m, errors.New("manifest_scope")
	}
	for _, k := range []string{"steth", "wsteth", "queue", "weth", "usdt", "curve", "factory", "quoter", "steth_impl", "queue_impl"} {
		v := m.Addresses[k]
		if !common.IsHexAddress(v) || common.HexToAddress(v) == (common.Address{}) {
			return m, errors.New("manifest_address_" + k)
		}
	}
	if len(m.BudgetsUsdtRaw) != 4 {
		return m, errors.New("manifest_four_budgets")
	}
	want := []string{"10000000000", "50000000000", "100000000000", "250000000000"}
	for i, v := range m.BudgetsUsdtRaw {
		if v != want[i] {
			return m, errors.New("manifest_fixed_budgets")
		}
	}
	for _, v := range m.CodeHashes {
		if _, e := ParseHex(v, 32); e != nil {
			return m, e
		}
	}
	for _, v := range m.Pools {
		if !common.IsHexAddress(v) || common.HexToAddress(v) == (common.Address{}) {
			return m, errors.New("manifest_pool")
		}
	}
	m.Raw = append([]byte(nil), b...)
	m.Hash = HashBytes(b)
	return m, nil
}
func (m Manifest) Address(k string) string {
	return string(common.HexToAddress(m.Addresses[k]).Bytes())
}
func (m Manifest) Budget(i int) *big.Int {
	n, _ := new(big.Int).SetString(m.BudgetsUsdtRaw[i], 10)
	return n
}
func (m Manifest) CodeRoles() []string {
	out := make([]string, 0, len(m.Addresses))
	for k := range m.Addresses {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
func (m Manifest) Pinned() bool {
	for _, k := range m.CodeRoles() {
		if len(m.CodeHashes[k]) != 66 {
			return false
		}
	}
	return len(m.Pools["usdt_weth_500"]) == 42 && len(m.Pools["weth_wsteth_100"]) == 42
}
