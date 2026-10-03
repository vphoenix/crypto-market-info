// One-time public deployment/path whitelist construction. Not a runtime source.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
	"github.com/vphoenix/crypto-market-info/internal/reserve"
	"os"
	"time"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	rpc, e := ethereum.NewClient("https://ethereum-rpc.publicnode.com", "var/reserve/evidence")
	must(e)
	b, e := rpc.Header(ctx, "latest")
	must(e)
	m := reserve.Manifest{ChainID: 1, SourceCommit: "241f0d244c45a311a4cd2af2c383a4f3bbe56265", Implementation: dex.MustAddress(reserve.Implementation), Quoter: dex.MustAddress("0x61ffe014ba17989e743c5f6cb21bf9697530b21e"), Factory: dex.MustAddress("0x1f98431c8ad98523631ae4a59f267346ea31f984"), Folios: []reserve.Folio{{Address: dex.MustAddress("0x188d12eb13a5eadd0867074ce8354b1ad6f4790b"), Name: "DFX"}}, Budgets: []string{"5000000000", "20000000000", "50000000000"}}
	rr := rpc.Batch(ctx, []ethereum.Call{{Method: "eth_getCode", Params: []any{m.Implementation.String(), ethereum.BlockRef(b.Hash)}}, {Method: "eth_getCode", Params: []any{m.Quoter.String(), ethereum.BlockRef(b.Hash)}}})
	m.ImplementationCodeHash, e = reserve.CodeHash(rr[0])
	must(e)
	m.QuoterCodeHash, e = reserve.CodeHash(rr[1])
	must(e)
	paths, e := os.ReadFile("research/2026-10-02-reserve-implementation/reviewer-preflight/dfx-suggested-paths.json")
	must(e)
	must(json.Unmarshal(paths, &m.Paths))
	m.Paths = append(m.Paths, reserve.Path{Tokens: []dex.Address{reserve.USDC, reserve.USDT}, Pools: []dex.Address{dex.MustAddress("0x3416cf6c708da44db2624d63ea0aaef7113527c6")}, Fees: []uint32{100}})
	for i := range m.Folios {
		m.Folios[i].ShareDecimals = 18
	}
	seen := map[dex.Address]bool{}
	for _, p := range m.Paths {
		for _, a := range p.Tokens {
			if seen[a] {
				continue
			}
			seen[a] = true
			d := uint8(18)
			if a == reserve.USDC || a == reserve.USDT {
				d = 6
			}
			m.Assets = append(m.Assets, reserve.AssetIdentity{Address: a, Decimals: d, TransferSemantics: "standard_erc20_assumed_pending_simulation"})
		}
	}
	must(m.Validate())
	raw, e := json.MarshalIndent(m, "", "  ")
	must(e)
	must(os.WriteFile("config/reserve-ethereum.json", append(raw, '\n'), 0644))
	fmt.Println("manifest pinned", b.Number, b.Hash.String(), "paths", len(m.Paths))
}
func must(e error) {
	if e != nil {
		panic(e)
	}
}
