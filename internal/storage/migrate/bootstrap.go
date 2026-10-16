package migrate

import (
	"context"
	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/storage/mysql"
)

// BootstrapTables is the legacy conservative preload allowlist, also used to
// validate previously sealed snapshots. It is not the minimum startup closure:
// main-service publications, provenance and DEX quotes need record selection
// from actual recovery calls. Do not require this entire list before cutover.
func BootstrapTables(family string) ([]string, error) {
	catalog, e := mysql.Mappings()
	if e != nil {
		return nil, e
	}
	excluded := map[string]bool{"order_book_minute": true, "order_book_second_delta": true, "cex_book_minute_commit": true, "derivative_book_minute": true, "derivative_book_second_delta": true, "derivative_book_quality_minute": true, "derivative_book_foundation_commit": true, "options_live_minute_commit": true, "options_catalog_live_minute_commit": true, "options_catalog_quality_evidence_minute": true, "options_index_minute": true}
	names := []string{}
	for _, t := range catalog.Tables {
		if t.Database == family && (family != "crypto_market_info" || !excluded[t.Name]) {
			names = append(names, t.Name)
		}
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("unknown bootstrap family")
	}
	return names, nil
}
func Bootstrap(ctx context.Context, c Config, progress func(mysql.MigrationResult)) error {
	if c.Table != "" {
		return fmt.Errorf("bootstrap uses its complete dependency closure; omit --table")
	}
	names, e := BootstrapTables(c.Family)
	if e != nil {
		return e
	}
	// Every table range is planned before copying; all members are present before
	// production recovery readers validate the committed markers.
	for _, name := range names {
		part := c
		part.Table = name
		if e = Plan(ctx, part); e != nil {
			return fmt.Errorf("plan dependency %s: %w", name, e)
		}
	}
	for _, name := range names {
		part := c
		part.Table = name
		if e = Copy(ctx, part, progress); e != nil {
			return fmt.Errorf("copy dependency %s: %w", name, e)
		}
	}
	for _, name := range names {
		part := c
		part.Table = name
		if e = Verify(ctx, part); e != nil {
			return fmt.Errorf("verify dependency %s: %w", name, e)
		}
	}
	return nil
}
