package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/storage/mysql"
	"github.com/vphoenix/crypto-market-info/internal/storage/redisbuffer"
	"github.com/vphoenix/crypto-market-info/internal/storage/spool"
	"os"
	"time"
)

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}

type Health struct {
	mysql.Health
	Redis *redisbuffer.Health `json:"redis,omitempty"`
	Spool *spool.Health       `json:"spool,omitempty"`
}

func run() error {
	socket := flag.String("socket", os.Getenv("MYSQL_SOCKET"), "private MySQL socket")
	user := flag.String("user", os.Getenv("MYSQL_USER"), "read-only query account")
	password := flag.String("password-file", os.Getenv("MYSQL_PASSWORD_FILE"), "private password file")
	database := flag.String("database", "crypto_market_info", "explicit allowlisted database")
	all := flag.Bool("all", false, "inspect all 12 allowlisted schemas")
	count := flag.Bool("count-facts", false, "explicitly count every fact table (may be expensive on populated history)")
	capitalInstrument := flag.Uint("capital-instrument", 0, "verify one capital observation through the production reader")
	asOf := flag.String("as-of", "", "capital query UTC/RFC3339 time; defaults to now")
	requireCapitalVersions := flag.Bool("require-capital-versions", false, "also require the optional capital version schema")
	redisSocket := flag.String("redis-socket", "", "optional Redis socket for backlog/preview inspection")
	prefix := flag.String("redis-prefix", "cmi:v1", "explicit cache namespace")
	dir := flag.String("spool", "", "optional durable spool directory")
	preview := flag.Bool("preview", false, "read explicitly provisional sampled minute")
	service := flag.String("service", "", "preview producer service")
	generation := flag.Uint64("generation", 0, "preview producer generation")
	scope := flag.String("scope", "", "preview CEX scope or derivative run UUID")
	minute := flag.String("minute", "", "UTC preview minute RFC3339")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if *capitalInstrument > 0 {
		if *capitalInstrument > 1<<32-1 || *database != "crypto_market_info" || *all || *count || *preview || *redisSocket != "" || *dir != "" {
			return fmt.Errorf("capital check requires one main-database instrument and no health/preview options")
		}
		at := time.Now().UTC()
		if *asOf != "" {
			var err error
			at, err = time.Parse(time.RFC3339Nano, *asOf)
			if err != nil {
				return err
			}
		}
		c, err := mysql.Open(ctx, mysql.Config{Socket: *socket, Username: *user, PasswordFile: *password, Database: *database, ReadOnly: true, MaxConnections: 1, CapitalRuleVersions: *requireCapitalVersions})
		if err != nil {
			return err
		}
		defer c.Close()
		if err := c.InitDiscoverySchema(ctx); err != nil {
			return err
		}
		started := time.Now()
		parameters, proof, err := c.LoadDiscoveryCapital(ctx, uint32(*capitalInstrument), at)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]any{
			"instrument_id": *capitalInstrument, "as_of_utc": at.UTC(), "proof": proof,
			"logical_hash": model.DiscoveryHash(parameters.Body()), "query_microseconds": time.Since(started).Microseconds(),
			"quote_tiers": len(parameters.QuoteDiscount), "base_tiers": len(parameters.BaseDiscount),
			"perp_tiers": len(parameters.PerpTiers), "borrow_tiers": len(parameters.BorrowTiers),
		})
	}
	if *asOf != "" || *requireCapitalVersions {
		return fmt.Errorf("--as-of and --require-capital-versions require --capital-instrument")
	}
	var buffer *redisbuffer.Buffer
	if *redisSocket != "" {
		var e error
		buffer, e = redisbuffer.Open(ctx, redisbuffer.Config{Socket: *redisSocket, Prefix: *prefix, WaitTimeout: 2 * time.Second})
		if e != nil {
			return e
		}
		defer buffer.Close()
	}
	if *preview {
		if buffer == nil {
			return fmt.Errorf("preview requires --redis-socket")
		}
		at, e := time.Parse(time.RFC3339, *minute)
		if e != nil {
			return e
		}
		p, e := buffer.PreviewMinute(ctx, *database, *service, *generation, *scope, at)
		if e != nil {
			return e
		}
		return json.NewEncoder(os.Stdout).Encode(p)
	}
	var disk *spool.Queue
	if *dir != "" {
		var e error
		disk, e = spool.Open(*dir, 20<<30)
		if e != nil {
			return e
		}
	}
	catalog, e := mysql.Mappings()
	if e != nil {
		return e
	}
	databases := []string{}
	seen := map[string]bool{}
	for _, t := range catalog.Tables {
		if !seen[t.Database] && (*all || t.Database == *database) {
			seen[t.Database] = true
			databases = append(databases, t.Database)
		}
	}
	if len(databases) == 0 {
		return fmt.Errorf("database outside allowlist")
	}
	out := []Health{}
	for _, name := range databases {
		c, e := mysql.Open(ctx, mysql.Config{Socket: *socket, Username: *user, PasswordFile: *password, Database: name, ReadOnly: true, MaxConnections: 1})
		if e != nil {
			return e
		}
		h, e := c.Inspect(ctx, *count)
		c.Close()
		if e != nil {
			return e
		}
		item := Health{Health: h}
		if buffer != nil {
			h, e := buffer.Inspect(ctx, name)
			if e != nil {
				return e
			}
			item.Redis = &h
		}
		if disk != nil {
			h, e := disk.Inspect(ctx)
			if e != nil {
				return e
			}
			item.Spool = &h
		}
		out = append(out, item)
	}
	if !*all {
		return json.NewEncoder(os.Stdout).Encode(out[0])
	}
	return json.NewEncoder(os.Stdout).Encode(out)
}
