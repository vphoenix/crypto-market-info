package reserve

import (
	"math/big"
	"time"
)

type SimObservation struct {
	Finality             string    `json:"finality"`
	CaptureMode          string    `json:"capture_mode"`
	QuoteId              string    `json:"quote_id"`
	Block                uint64    `json:"block"`
	BlockHash            string    `json:"block_hash"`
	Kind                 string    `json:"kind"`
	Status               string    `json:"status"`
	Reason               string    `json:"reason,omitempty"`
	AvailableAt          time.Time `json:"available_at"`
	BudgetUSDC           string    `json:"budget_usdc"`
	SyntheticFundingUSDC string    `json:"synthetic_funding_usdc"`
	AmountInUSDC         *string   `json:"amount_in_usdc,omitempty"`
	AmountOutUSDC        *string   `json:"amount_out_usdc,omitempty"`
	GrossUSDC            *string   `json:"gross_usdc,omitempty"`
	GasInternal          *uint64   `json:"gas_internal,omitempty"`
	WithinBudget         *bool     `json:"within_budget,omitempty"`
	PayloadHash          string    `json:"payload_hash"`
}

func SimulationObservation(s Simulation) SimObservation {
	v := SimObservation{QuoteId: Hex(s.QuoteId), Block: s.BlockNumber, BlockHash: Hex(s.BlockHash), Kind: s.RouteKind, Status: s.Status, Reason: s.Reason, AvailableAt: s.AvailableAt, BudgetUSDC: decimal6(s.BudgetRaw), SyntheticFundingUSDC: decimal6(s.FundedRaw), PayloadHash: Hex(s.PayloadHash), GasInternal: s.GasInternal, WithinBudget: s.WithinBudget}
	if s.Status == "joint_call_simulated" {
		v.AmountInUSDC = Ptr(decimal6(s.AmountInRaw))
		v.AmountOutUSDC = Ptr(decimal6(s.AmountOutRaw))
		v.GrossUSDC = Ptr(decimal6(new(big.Int).Sub(s.AmountOutRaw, s.AmountInRaw)))
	}
	return v
}
