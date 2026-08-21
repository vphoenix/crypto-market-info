package tron

import (
	"fmt"

	"github.com/shopspring/decimal"
)

const TheoreticalBlocksPerDay int64 = 28792

func APR(rank int, votes, totalVotes decimal.Decimal, brokerage int64, witnessPaySun, voterPaySun int64) (decimal.Decimal, error) {
	if rank < 1 || rank > 127 || !votes.IsPositive() || !totalVotes.IsPositive() || votes.GreaterThan(totalVotes) {
		return decimal.Decimal{}, fmt.Errorf("invalid rank or vote totals")
	}
	if brokerage < 0 || brokerage > 100 || witnessPaySun < 0 || voterPaySun < 0 {
		return decimal.Decimal{}, fmt.Errorf("invalid brokerage or reward parameter")
	}
	b := decimal.NewFromInt(TheoreticalBlocksPerDay)
	oneMinusCommission := decimal.NewFromInt(100 - brokerage).Div(decimal.NewFromInt(100))
	voterPay := decimal.NewFromInt(voterPaySun).Div(decimal.NewFromInt(1_000_000))
	voteDaily := b.Mul(voterPay).Div(totalVotes).Mul(oneMinusCommission)
	blockDaily := decimal.Zero
	if rank <= 27 {
		witnessPay := decimal.NewFromInt(witnessPaySun).Div(decimal.NewFromInt(1_000_000))
		blockDaily = b.Mul(witnessPay).Div(decimal.NewFromInt(27)).Div(votes).Mul(oneMinusCommission)
	}
	return voteDaily.Add(blockDaily).Mul(decimal.NewFromInt(365)), nil
}
