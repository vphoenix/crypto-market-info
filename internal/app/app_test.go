package app

import (
	"testing"

	"github.com/vphoenix/crypto-market-info/internal/model"
)

func TestSelectSymbolsIsExactOrderedAndRejectsMissing(t *testing.T) {
	available := []model.Instrument{{ExchangeSymbol: "A"}, {ExchangeSymbol: "B"}}
	selected, err := selectSymbols("X", available, []string{"B", "A"})
	if err != nil {
		t.Fatal(err)
	}
	if selected[0].ExchangeSymbol != "B" || selected[1].ExchangeSymbol != "A" {
		t.Fatalf("selected=%+v", selected)
	}
	if _, err = selectSymbols("X", available, []string{"a"}); err == nil {
		t.Fatal("case-folded symbol was accepted")
	}
}
