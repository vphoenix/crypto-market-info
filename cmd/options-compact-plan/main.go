// options-compact-plan prints reviewed migration SQL; it does not open a
// database or modify a service. Copy/verification/swap are explicit operations.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/vphoenix/crypto-market-info/internal/storage/clickhouse"
)

func main() {
	database := flag.String("database", "crypto_market_info", "existing database")
	flag.Parse()
	plan, err := clickhouse.OptionsCompactMigrations(*database)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(plan); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
