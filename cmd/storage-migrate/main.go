// storage-migrate performs explicit administrative operations. It never stops
// collectors or expands its project database allowlist by prefix matching.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/google/uuid"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	store "github.com/vphoenix/crypto-market-info/internal/storage/mysql"
	"github.com/vphoenix/crypto-market-info/internal/storage/redisbuffer"
	"github.com/vphoenix/crypto-market-info/internal/storage/spool"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if e := run(ctx, os.Args[1:]); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: storage-migrate check-mappings|inventory|init-schema")
	}
	catalog, e := store.Mappings()
	if e != nil {
		return e
	}
	switch args[0] {
	case "plan", "copy", "resume", "bootstrap", "verify", "report", "finalize":
		return migrationCommand(ctx, args)
	case "check-mappings":
		fmt.Printf("schema_version=%d mapped_tables=%d\n", catalog.Version, len(catalog.Tables))
		return nil
	case "inventory":
		f := flag.NewFlagSet(args[0], flag.ContinueOnError)
		addr := f.String("source", "http://127.0.0.1:8123", "read-only ClickHouse endpoint")
		out := f.String("out", "", "snapshot output file")
		if e = f.Parse(args[1:]); e != nil {
			return e
		}
		if *out == "" {
			return fmt.Errorf("inventory requires --out")
		}
		allowed := map[string]bool{}
		names := []string{}
		for _, t := range catalog.Tables {
			if !allowed[t.Database] {
				allowed[t.Database] = true
				names = append(names, "'"+t.Database+"'")
			}
		}
		u, e := url.Parse(*addr)
		if e != nil {
			return e
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			return fmt.Errorf("invalid source endpoint")
		}
		q := u.Query()
		q.Set("readonly", "1")
		q.Set("max_execution_time", "30")
		q.Set("max_threads", "2")
		q.Set("max_memory_usage", "268435456")
		u.RawQuery = q.Encode()
		sql := "SELECT database,name,engine,engine_full,partition_key,sorting_key,primary_key,create_table_query FROM system.tables WHERE database IN (" + strings.Join(names, ",") + ") ORDER BY database,name FORMAT JSONEachRow"
		req, e := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), strings.NewReader(sql))
		if e != nil {
			return e
		}
		resp, e := (&http.Client{Timeout: 40 * time.Second}).Do(req)
		if e != nil {
			return e
		}
		defer resp.Body.Close()
		raw, e := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
		if e != nil {
			return e
		}
		if resp.StatusCode != 200 {
			return fmt.Errorf("source inventory HTTP %d: %.1024s", resp.StatusCode, raw)
		}
		tables := []json.RawMessage{}
		decoder := json.NewDecoder(strings.NewReader(string(raw)))
		for decoder.More() {
			var row json.RawMessage
			if e = decoder.Decode(&row); e != nil {
				return e
			}
			tables = append(tables, row)
		}
		result := struct {
			Captured time.Time         `json:"captured_at_utc"`
			Tables   []json.RawMessage `json:"tables"`
		}{time.Now().UTC(), tables}
		data, e := json.MarshalIndent(result, "", "  ")
		if e != nil {
			return e
		}
		if e = os.WriteFile(*out, append(data, '\n'), 0600); e != nil {
			return e
		}
		fmt.Printf("read-only snapshot: %d tables\n", len(tables))
		return nil
	case "init-schema", "init-all-schemas", "activate-producer", "provision-account", "extend-partitions", "install-receipt-index", "install-dex-indexes", "install-capital-versions", "install-quality-storage", "install-source-storage", "install-keeper-rental-state", "install-keeper-dynamic":
		f := flag.NewFlagSet(args[0], flag.ContinueOnError)
		socket := f.String("socket", "", "private MySQL Unix socket")
		user := f.String("user", "", "DDL account")
		password := f.String("password-file", "", "private password file")
		readTimeout := f.Duration("target-read-timeout", 30*time.Second, "bounded administrative MySQL read timeout, maximum 2m")
		redisPrefix := f.String("redis-prefix", "cmi:v1", "queue namespace for producer drain verification")
		redisSocket := f.String("redis-socket", "", "required drain check when advancing an existing producer")
		spoolDir := f.String("spool", "", "required durable spool drain check")
		through := f.String("through", "", "last included partition month YYYY-MM")
		service := f.String("service", "", "producer service identity")
		expected := f.Uint64("expected-generation", 0, "compare-and-swap generation")
		holder := f.String("holder", "", "new producer UUID")
		account := f.String("account", "", "local account name")
		role := f.String("role", "", "query, live, migration or ddl")
		accountPassword := f.String("account-password-file", "", "private generated password path outside repository")
		family := f.String("family", "", "allowlisted source database")
		target := f.String("target", "", "explicit destination database (may be isolated validation)")
		if e = f.Parse(args[1:]); e != nil {
			return e
		}
		if args[0] == "init-schema" && (*family == "" || *target == "") {
			return fmt.Errorf("init-schema requires explicit --family and --target")
		}
		db, e := store.Connect(ctx, store.Config{Socket: *socket, Username: *user, PasswordFile: *password, MaxConnections: 1, ReadTimeout: *readTimeout})
		if e != nil {
			return e
		}
		defer db.Close()
		switch args[0] {
		case "install-source-storage":
			if *family != "crypto_market_info" || *target == "" {
				return fmt.Errorf("source storage requires explicit main family and target")
			}
			return store.InstallSourceStorageSchema(ctx, db, *target)
		case "install-quality-storage":
			if *family != "crypto_market_info" || *target == "" {
				return fmt.Errorf("quality storage requires explicit main family and target")
			}
			return store.InstallQualityStorageSchema(ctx, db, *target)
		case "install-keeper-dynamic":
			if *family != "crypto_market_info_justlend_keeper" || *target == "" {
				return fmt.Errorf("Keeper dynamic requires explicit family and target")
			}
			return store.InstallKeeperDynamicSchema(ctx, db, *target)
		case "install-keeper-rental-state":
			if *family != "crypto_market_info_justlend_keeper" || *target == "" {
				return fmt.Errorf("keeper rental-state requires explicit Keeper family and target")
			}
			return store.InstallKeeperRentalStateSchema(ctx, db, *target)
		case "install-capital-versions":
			if *family != "crypto_market_info" || *target == "" {
				return fmt.Errorf("capital versions require explicit main family and target")
			}
			return store.InstallCapitalVersionSchema(ctx, db, *target)
		case "install-dex-indexes":
			if *family != "crypto_market_info" || *target == "" {
				return fmt.Errorf("DEX indexes require explicit main family and target")
			}
			return store.InstallDEXQueryIndexes(ctx, db, *target)
		case "install-receipt-index":
			if (*family != "crypto_market_info" && *family != "crypto_market_info_reserve") || *target == "" {
				return fmt.Errorf("receipt index requires an explicit main or Reserve family and target")
			}
			return store.InstallReceiptHashIndex(ctx, db, *target)
		case "extend-partitions":
			month, e := time.Parse("2006-01", *through)
			if e != nil {
				return e
			}
			return store.ExtendPartitions(ctx, db, *family, *target, month)
		case "init-all-schemas":
			if e = store.InitAllSchemas(ctx, db); e != nil {
				return e
			}
			fmt.Println("schema ready: 12 allowlisted databases, 125 typed fact tables")
		case "provision-account":
			if e = store.ProvisionAccount(ctx, db, *account, *role, *accountPassword); e != nil {
				return e
			}
			fmt.Printf("local account ready: %s role=%s password_file=%s\n", *account, *role, *accountPassword)
		case "activate-producer":
			id, e := uuid.Parse(*holder)
			if e != nil {
				return fmt.Errorf("activation requires explicit holder UUID")
			}

			if *expected > 0 {
				if *redisSocket == "" || *spoolDir == "" {
					return fmt.Errorf("generation advancement requires --redis-socket and --spool drain checks")
				}
				buffer, e := redisbuffer.Open(ctx, redisbuffer.Config{Socket: *redisSocket, Prefix: *redisPrefix, WaitTimeout: 2 * time.Second})
				if e != nil {
					return e
				}
				defer buffer.Close()
				checked := map[string]bool{}
				for _, t := range catalog.Tables {
					if checked[t.Database] {
						continue
					}
					checked[t.Database] = true
					pending, e := buffer.ServicePending(ctx, t.Database, *service, *expected)
					if e != nil {
						return e
					}
					if pending {
						return fmt.Errorf("sealed producer deliveries must drain before activation")
					}
				}
				disk, e := spool.Open(*spoolDir, 20<<30)
				if e != nil {
					return e
				}
				pending, e := disk.ServicePending(ctx, *service, *expected)
				if e != nil {
					return e
				}
				if pending {
					return fmt.Errorf("spooled producer deliveries must drain before activation")
				}
			}
			generation, e := store.ActivateProducer(ctx, db, *service, *expected, id)
			if e != nil {
				return e
			}
			return json.NewEncoder(os.Stdout).Encode(store.Producer{Service: *service, Generation: generation, Holder: id})
		default:
			if e = store.InitSchema(ctx, db, *family, *target); e != nil {
				return e
			}
			fmt.Printf("schema ready: %s (family %s)\n", *target, *family)
		}
		return nil

	default:
		return fmt.Errorf("unsupported migration operation %q", args[0])
	}
}
