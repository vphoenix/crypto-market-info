package mysql

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"time"
)

type schemaConnector struct {
	driver.Connector
	query      string
	hashes     map[string][32]byte
	invalidate func()
}

func schemaCheckedConnector(inner driver.Connector, cfg Config) (driver.Connector, error) {
	if !identifier.MatchString(cfg.Database) {
		return nil, fmt.Errorf("schema validation requires a destination")
	}
	family := cfg.Family
	if family == "" {
		family = cfg.Database
	}
	hashes := map[string][32]byte{}
	names := []string{}
	for _, name := range cfg.SchemaTables {
		table, e := Mapping(family, name)
		if e != nil {
			return nil, e
		}
		if _, ok := hashes[name]; ok {
			continue
		}
		hashes[name] = sha256.Sum256([]byte(table.DDL))
		names = append(names, "'"+name+"'")
	}
	query := "SELECT table_name,version,ddl_hash FROM cmi_storage.schema_version WHERE database_name='" + cfg.Database + "' AND table_name IN (" + strings.Join(names, ",") + ")"
	return &schemaConnector{inner, query, hashes, cfg.SchemaInvalidated}, nil
}
func (c *schemaConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, e := c.Connector.Connect(ctx)
	if e != nil {
		return nil, e
	}
	if e = c.validate(ctx, conn); e != nil {
		conn.Close()
		if c.invalidate != nil {
			c.invalidate()
		}
		return nil, e
	}
	return conn, nil
}
func driverBytes(v driver.Value) []byte {
	if raw, ok := v.([]byte); ok {
		return raw
	}
	return []byte(fmt.Sprint(v))
}
func (c *schemaConnector) validate(ctx context.Context, conn driver.Conn) error {
	query, ok := conn.(driver.QueryerContext)
	if !ok {
		return fmt.Errorf("driver cannot validate physical schema connection")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	rows, e := query.QueryContext(ctx, c.query, nil)
	if e != nil {
		return e
	}
	defer rows.Close()
	seen := map[string]bool{}
	values := make([]driver.Value, 3)
	for {
		e = rows.Next(values)
		if e == io.EOF {
			break
		}
		if e != nil {
			return e
		}
		name := string(driverBytes(values[0]))
		want, exists := c.hashes[name]
		if !exists || seen[name] || string(driverBytes(values[1])) != "1" || !bytes.Equal(driverBytes(values[2]), want[:]) {
			return fmt.Errorf("physical connection schema %s requires administrative migration", name)
		}
		seen[name] = true
	}
	if len(seen) != len(c.hashes) {
		return fmt.Errorf("physical connection schema is incomplete")
	}
	return nil
}

func MinuteSchemaTables() []string {
	return []string{"instrument", "order_book_minute", "order_book_second_delta", "funding_rate_hourly", "yield_route", "yield_observation", "instrument_canonical_mapping", "perpetual_universe_run", "perpetual_universe_member", "source_publication_intents", "source_batch_status", "public_observation_provenance", "cex_book_minute_commit", "funding_forecast_observation", "cex_trade_bar_1m", "cex_fee_schedule_rule", "cex_collateral_risk_rule", "okx_public_capital_parameters", "derivative_contract_spec", "derivative_trading_rule", "derivative_book_minute", "derivative_book_second_delta", "derivative_book_quality_minute", "derivative_book_foundation_commit", "options_live_run", "options_metadata_observation", "options_index_minute", "options_live_minute_commit", "options_catalog_scope_observation", "options_lifecycle_observation", "options_collection_plan", "options_catalog_quality_evidence_minute", "options_catalog_live_minute_commit"}
}
