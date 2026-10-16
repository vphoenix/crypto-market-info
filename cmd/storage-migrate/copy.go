package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/google/uuid"
	migration "github.com/vphoenix/crypto-market-info/internal/storage/migrate"
	"github.com/vphoenix/crypto-market-info/internal/storage/mysql"
	"os"
	"time"
)

func migrationCommand(ctx context.Context, args []string) error {
	f := flag.NewFlagSet(args[0], flag.ContinueOnError)
	source := f.String("source", "127.0.0.1:9000", "read-only native ClickHouse address")
	sourceUser := f.String("source-user", "default", "read-only source user")
	sourcePassword := f.String("source-password-file", "", "private source password file")
	snapshot := f.String("source-snapshot-manifest", "", "explicit pinned bootstrap snapshot manifest")
	socket := f.String("socket", "", "private MySQL socket")
	user := f.String("user", "", "migration account")
	password := f.String("password-file", "", "private target password file")
	readTimeout := f.Duration("target-read-timeout", 30*time.Second, "bounded target read timeout, at most 2m")
	family := f.String("family", "", "explicit allowlisted source database")
	target := f.String("target", "", "explicit destination")
	table := f.String("table", "", "one explicitly mapped table")
	run := f.String("run", "", "stable run UUID; plan is required before copy")
	stage := f.String("staging-dir", "", "absolute bounded private staging directory")
	rows := f.Int("rows", 500, "rows per ordinary chunk, complete key groups retained")
	redisSocket := f.String("redis-socket", "", "check realtime backlog before every history chunk")
	delay := f.Duration("delay", 250*time.Millisecond, "yield between history transactions")
	startupRows := f.Bool("startup-rows-only", false, "verify a pinned startup subset; leaves history incomplete")
	if e := f.Parse(args[1:]); e != nil {
		return e
	}
	id, e := uuid.Parse(*run)
	if e != nil || id == uuid.Nil {
		return fmt.Errorf("explicit stable --run UUID required")
	}
	if *target == "" && args[0] != "report" && args[0] != "finalize" {
		return fmt.Errorf("explicit target required")
	}
	cfg := migration.Config{Source: migration.SourceConfig{Address: *source, User: *sourceUser, PasswordFile: *sourcePassword, SnapshotFile: *snapshot}, Target: mysql.Config{Socket: *socket, Username: *user, PasswordFile: *password, MaxConnections: 1, ReadTimeout: *readTimeout}, Run: id, Family: *family, Database: *target, Table: *table, StagingDir: *stage, Rows: *rows, MinFreeBytes: 50 << 30, RedisSocket: *redisSocket, Delay: *delay}
	switch args[0] {
	case "plan":
		if e = migration.Plan(ctx, cfg); e != nil {
			return e
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"run": id.String(), "family": *family, "target": *target, "table": *table, "state": "planned"})
	case "bootstrap":
		return migration.Bootstrap(ctx, cfg, func(r mysql.MigrationResult) { json.NewEncoder(os.Stdout).Encode(r) })
	case "report":
		report, e := mysql.MigrationReport(ctx, cfg.Target, cfg.Run)
		if e != nil {
			return e
		}
		return json.NewEncoder(os.Stdout).Encode(report)
	case "finalize":
		db, e := mysql.Connect(ctx, cfg.Target)
		if e != nil {
			return e
		}
		defer db.Close()
		return mysql.FinalizeMigration(ctx, db, cfg.Run)
	case "copy", "resume":
		return migration.Copy(ctx, cfg, func(r mysql.MigrationResult) { json.NewEncoder(os.Stdout).Encode(r) })
	case "verify":
		if *startupRows {
			return migration.VerifyBootstrapRows(ctx, cfg)
		}
		return migration.Verify(ctx, cfg)
	default:
		return fmt.Errorf("unsupported migration command")
	}
}
