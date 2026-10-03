package reserve

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"math/big"
)

func quoteBase(s State, kind string, budget *big.Int, route string) Quote {
	return Quote{ChainId: 1, ManifestHash: s.ManifestHash, CaptureId: s.CaptureId, BatchId: s.BatchId, BlockNumber: s.BlockNumber, BlockHash: s.BlockHash, BlockTime: s.BlockTime, Folio: s.Folio, RouteId: ID(route), RouteKind: kind, BudgetToken: Address(USDC), RequestedBudgetRaw: Copy(budget), TokenIn: Address(USDC), TokenOut: Address(USDC), BasketAmounts: []Amount{}, DexLegs: []Leg{}, SharedPools: []string{}}
}
func stateHash(s State) dex.Hash       { var h dex.Hash; copy(h[:], s.BlockHash); return h }
func folioAddress(s State) dex.Address { var a dex.Address; copy(a[:], s.Folio); return a }
func basketLegs(amounts []Amount, entry bool) []Leg {
	out := []Leg{}
	for _, a := range amounts {
		if a.AmountRaw.Sign() == 0 {
			continue
		}
		side, mode, in, to := "exit", "exact_input", a.Token, Address(USDC)
		if entry {
			side, mode, in, to = "entry", "exact_output", Address(USDC), a.Token
		}
		out = append(out, NewLeg(uint16(len(out)), side, mode, in, to, a.AmountRaw))
	}
	return out
}

func ethRaw(s string) common.Address { var a common.Address; copy(a[:], s); return a }
