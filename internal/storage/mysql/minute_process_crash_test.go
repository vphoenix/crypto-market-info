package mysql

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	driver "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/replay"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
	"github.com/vphoenix/crypto-market-info/internal/storage/redisbuffer"
	"github.com/vphoenix/crypto-market-info/internal/storage/spool"
)

type minuteCrashFixture struct {
	Database          string
	Producer          Producer
	Redis             redisbuffer.Config
	Dir, Frame, Stage string
}

// The child stops at a real transaction/ACK boundary. The parent uses SIGKILL,
// so no deferred rollback, lease release or graceful spool cleanup can run.
func TestMinuteProcessCrashChild(t *testing.T) {
	path := os.Getenv("CMI_MINUTE_CRASH_FIXTURE")
	if path == "" {
		t.Skip("subprocess only")
	}
	var f minuteCrashFixture
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	if !strings.HasPrefix(f.Database, "cmi_test_") {
		t.Fatal("isolated test database required")
	}
	c, err := Open(ctx, Config{Socket: os.Getenv("CMI_MYSQL_TEST_SOCKET"), Username: os.Getenv("CMI_MYSQL_TEST_USER"), PasswordFile: os.Getenv("CMI_MYSQL_TEST_PASSWORD_FILE"), Database: f.Database, Family: "crypto_market_info", Producer: f.Producer, MaxConnections: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	b, err := redisbuffer.Open(ctx, f.Redis)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	q, err := spool.Open(f.Dir, 20<<30)
	if err != nil {
		t.Fatal(err)
	}
	raw, err = os.ReadFile(f.Frame)
	if err != nil {
		t.Fatal(err)
	}
	d, err := spool.Unmarshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	cs, err := b.Candidates(ctx, d.Database, 0, 256)
	if err != nil || len(cs) != 1 {
		t.Fatal("candidate", len(cs), err)
	}
	const owner = "crashing-worker"
	if ok, e := b.Claim(ctx, cs[0], owner, time.Second); e != nil || !ok {
		t.Fatal("claim", ok, e)
	}
	var trace CommitTrace
	stop := func(stage string) {
		if stage != f.Stage {
			return
		}
		fmt.Printf("CRASH_READY %s %d\n", stage, trace.ConnectionID)
		<-ctx.Done()
		t.Fatal("parent did not kill child")
	}
	ctx = WithCommitTrace(ctx, &trace)
	ctx = context.WithValue(ctx, minuteFaultKey{}, func(stage string, _ *sql.Tx) error { stop(stage); return nil })
	p, err := c.PrepareDelivery(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.CommitPrepared(ctx, p); err != nil {
		t.Fatal(err)
	}
	stop("before_ack")
	if err = q.Ack(ctx, d, c.Committed); err != nil {
		t.Fatal(err)
	}
	stop("after_spool_ack")
	// The short lease may have expired while fsync was pending. Reclaim under
	// the same owner rather than treating lease expiry as successful ACK.
	if ok, e := b.Renew(ctx, d, owner, time.Minute); e != nil {
		t.Fatal(e)
	} else if !ok {
		if ok, e = b.Claim(ctx, cs[0], owner, time.Minute); e != nil || !ok {
			t.Fatal("reclaim", ok, e)
		}
	}
	if err = b.AckOwned(ctx, d, owner, c.Committed); err != nil {
		t.Fatal(err)
	}
	stop("after_ack")
	t.Fatal("fault boundary not reached")
}

func TestMinuteProcessKillPreservesAtomicFactsAndBothCopies(t *testing.T) {
	if os.Getenv("REDIS_INTEGRATION") != "1" {
		t.Skip("isolated Redis required")
	}
	c := integrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	if err := c.InitDiscoverySchema(ctx); err != nil {
		t.Fatal(err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	stages := []string{"claimed", "partial_facts", "facts", "before_commit", "after_commit", "before_ack", "after_spool_ack", "after_ack"}
	for n, stage := range stages {
		t.Run(stage, func(t *testing.T) {
			minute := time.Now().UTC().Truncate(time.Minute).Add(-time.Duration(n+120) * time.Minute)
			batch := testBatch(t, uint32(n+1), minute, 5)
			ds, err := c.PrepareBookDeliveries(ctx, model.CompletedMinute{MinuteTime: minute, Batches: []model.MinuteBatch{batch}})
			if err != nil {
				t.Fatal(err)
			}
			d := ds[0]
			dir := t.TempDir()
			q, err := spool.Open(dir, 20<<30)
			if err != nil {
				t.Fatal(err)
			}
			if err = q.Accept(ctx, d); err != nil {
				t.Fatal(err)
			}
			rcfg := redisbuffer.Config{Socket: os.Getenv("CMI_REDIS_TEST_SOCKET"), Prefix: "cmi:test:process-crash:" + uuid.NewString(), WaitTimeout: 5 * time.Second}
			b, err := redisbuffer.Open(ctx, rcfg)
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()
			if err = b.Accept(ctx, d); err != nil {
				t.Fatal(err)
			}
			// Keep an independent original for verification after ACK deletes the
			// queue copy. It is never used as a replacement for a missing copy.
			original, err := spool.Marshal(d)
			if err != nil {
				t.Fatal(err)
			}
			frame := filepath.Join(t.TempDir(), "original.batch")
			if err = os.WriteFile(frame, original, 0600); err != nil {
				t.Fatal(err)
			}
			f := minuteCrashFixture{Database: c.database, Producer: c.producer, Redis: rcfg, Dir: dir, Frame: frame, Stage: stage}
			raw, err := json.Marshal(f)
			if err != nil {
				t.Fatal(err)
			}
			fixture := filepath.Join(t.TempDir(), "fixture.json")
			if err = os.WriteFile(fixture, raw, 0600); err != nil {
				t.Fatal(err)
			}
			childCtx, stopChild := context.WithTimeout(ctx, 40*time.Second)
			defer stopChild()
			cmd := exec.CommandContext(childCtx, exe, "-test.run=^TestMinuteProcessCrashChild$")
			cmd.Env = append(os.Environ(), "CMI_MINUTE_CRASH_FIXTURE="+fixture)
			pipe, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			var stderr strings.Builder
			cmd.Stderr = &stderr
			if err = cmd.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = cmd.Process.Kill() }()
			var connection uint64
			ready := false
			var childOutput strings.Builder
			scan := bufio.NewScanner(pipe)
			for scan.Scan() {
				if childOutput.Len() < 4096 {
					childOutput.WriteString(scan.Text() + "\n")
				}
				var at string
				if _, e := fmt.Sscanf(scan.Text(), "CRASH_READY %s %d", &at, &connection); e == nil && at == stage {
					ready = true
					break
				}
			}
			if !ready {
				_ = cmd.Wait()
				t.Fatal("child failed before boundary", childOutput.String(), stderr.String(), scan.Err())
			}
			if err = cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			if err = cmd.Wait(); err == nil {
				t.Fatal("child was not killed")
			}
			if stage == "after_commit" && os.Getenv("CMI_REDIS_TEST_RESTART_CONTAINER") != "" {
				restartMinuteTestRedis(t, ctx, rcfg)
				recovered, e := b.Load(ctx, d.Database, d.ID)
				if e != nil || !recovered.SameHeader(d) || string(recovered.Payload) != string(d.Payload) {
					t.Fatal("Redis restart lost sealed bytes", e)
				}
			}
			deadline := time.Now().Add(10 * time.Second)
			for {
				gone, e := c.AttemptGone(ctx, connection)
				if e != nil {
					t.Fatal(e)
				}
				if gone {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("killed worker connection survived", connection)
				}
				time.Sleep(20 * time.Millisecond)
			}
			committed, err := c.Committed(ctx, d)
			if err != nil || committed != (n >= 4) {
				t.Fatal("commit boundary", committed, err)
			}
			for table, key := range map[string]string{"order_book_minute": "id", "order_book_second_delta": "minute_id"} {
				var count int
				if err = c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+c.table(table)+" WHERE "+key+"=?", batch.Minute.ID).Scan(&count); err != nil {
					t.Fatal(err)
				}
				if !committed && count != 0 {
					t.Fatal("visible partial facts", table, count)
				}
			}
			_, statErr := os.Stat(filepath.Join(dir, spool.Name(d)))
			if (statErr == nil) != (n < 6) {
				t.Fatal("spool removed at wrong boundary", statErr)
			}
			if n < 6 {
				got, e := os.ReadFile(filepath.Join(dir, spool.Name(d)))
				if e != nil || string(got) != string(original) {
					t.Fatal("retained original changed", e)
				}
			}
			if !committed {
				if err = q.Ack(ctx, d, c.Committed); err == nil {
					t.Fatal("uncommitted file acknowledged")
				}
			}
			if err = c.Commit(ctx, d); err != nil {
				t.Fatal("recover", err)
			}
			if err = q.Ack(ctx, d, c.Committed); err != nil {
				t.Fatal(err)
			}
			deadline = time.Now().Add(3 * time.Second)
			for n < 7 {
				cs, e := b.Candidates(ctx, d.Database, 0, 256)
				if e != nil || len(cs) != 1 {
					t.Fatal("retained Redis copy", len(cs), e)
				}
				ok, e := b.Claim(ctx, cs[0], "recovered-worker", time.Minute)
				if e != nil {
					t.Fatal(e)
				}
				if ok {
					if e = b.AckOwned(ctx, d, "recovered-worker", c.Committed); e != nil {
						t.Fatal(e)
					}
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("crashed worker lease did not expire")
				}
				time.Sleep(20 * time.Millisecond)
			}
			cs, err := b.Candidates(ctx, d.Database, 0, 256)
			if err != nil || len(cs) != 0 {
				t.Fatal("Redis copy not acknowledged", len(cs), err)
			}
			stored, err := c.loadCommittedBook(ctx, uint32(n+1), minute)
			if err != nil {
				t.Fatal(err)
			}
			for sec := range 60 {
				want, valid, err := replay.AtSecond(batch.Minute, batch.Deltas, uint8(sec))
				if err != nil {
					t.Fatal(err)
				}
				got, gotValid, err := replay.AtSecond(stored.Minute, stored.Deltas, uint8(sec))
				if err != nil || gotValid != valid || fmt.Sprint(got.Bids, got.Asks) != fmt.Sprint(want.Bids, want.Asks) {
					t.Fatal("recovery replay mismatch", sec, err)
				}
			}
		})
	}
	t.Run("publication_invariants", func(t *testing.T) { checkCEXPublicationLookup(t, c) })
}

// This destructive fault is explicitly opted into and restricted to the
// already provisioned, network-isolated local validation Redis container.
func restartMinuteTestRedis(t *testing.T, ctx context.Context, cfg redisbuffer.Config) {
	t.Helper()
	const base = "/home/ubuntu/crypto-market-info/var/storage/write-backlog-improvement/bench-redis"
	name := os.Getenv("CMI_REDIS_TEST_RESTART_CONTAINER")
	if name != "cmi-backlog-validation-redis" || cfg.Socket != base+"/run/redis.sock" {
		t.Fatal("refusing restart of unreviewed Redis target")
	}
	commandCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	raw, err := exec.CommandContext(commandCtx, "docker", "inspect", "--format", `{"Network":{{json .HostConfig.NetworkMode}},"Mounts":{{json .Mounts}}}`, name).Output()
	if err != nil {
		t.Fatal(err)
	}
	var container struct {
		Network string
		Mounts  []struct {
			Source, Destination string
			RW                  bool
		}
	}
	if err = json.Unmarshal(raw, &container); err != nil {
		t.Fatal(err)
	}
	data, config := false, false
	for _, m := range container.Mounts {
		data = data || (m.Source == base+"/data" && m.Destination == "/data")
		config = config || (m.Source == "/home/ubuntu/crypto-market-info/deploy/storage/redis.conf" && !m.RW)
	}
	if container.Network != "none" || !data || !config {
		t.Fatal("Redis isolation or persistence configuration not pinned")
	}
	probe := redis.NewClient(&redis.Options{Network: "unix", Addr: cfg.Socket, MaxRetries: 0})
	defer probe.Close()
	for key, want := range map[string]string{"appendonly": "yes", "appendfsync": "everysec", "maxmemory-policy": "noeviction"} {
		got, e := probe.ConfigGet(ctx, key).Result()
		if e != nil || got[key] != want {
			t.Fatal("Redis durability differs", key, got[key], e)
		}
	}
	if out, e := exec.CommandContext(commandCtx, "docker", "restart", "--signal", "SIGKILL", name).CombinedOutput(); e != nil {
		t.Fatal("isolated Redis restart", e, string(out))
	}
	deadline := time.Now().Add(15 * time.Second)
	for probe.Ping(ctx).Err() != nil {
		if time.Now().After(deadline) {
			t.Fatal("isolated Redis did not recover")
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Log("SIGKILL restarted the pinned isolated Redis; durable batch and MySQL proof checked before ACK")
}

func checkCEXPublicationLookup(t *testing.T, c *Client) {
	t.Helper()
	ctx := context.Background()
	minute := time.Now().UTC().Truncate(time.Minute).Add(-24 * time.Hour)
	batch := testBatch(t, 100, minute, 5)
	all := model.CompletedMinute{MinuteTime: minute, KnownFromMS: minute.Add(2 * time.Minute).UnixMilli(), Batches: []model.MinuteBatch{batch}}
	ds, err := c.PrepareBookDeliveries(ctx, all)
	if err != nil {
		t.Fatal(err)
	}
	p, err := c.PrepareDelivery(ctx, ds[0])
	if err != nil {
		t.Fatal(err)
	}
	publish := func() error {
		return c.transaction(ctx, func(tx *sql.Tx) error { return c.publishCEXMinutes(ctx, tx, *p.cex, p.cexPublications) })
	}
	if err = publish(); err != nil {
		t.Fatal(err)
	}
	want := p.cexPublications[0]
	p.cexPublications[0].KnownMS++
	if err = publish(); err != nil {
		t.Fatal("retry first knowledge", err)
	}
	var known int64
	if err = c.db.QueryRowContext(ctx, "SELECT known_from_ms FROM "+c.table("source_batch_status")+" WHERE source_kind=? AND source_id=?", want.Kind, want.ID).Scan(&known); err != nil || known != want.KnownMS {
		t.Fatal("first knowledge changed", known, err)
	}
	p.cexPublications[0].Hash = strings.Repeat("f", 64)
	if err = publish(); !errors.Is(err, contracts.ErrConflict) {
		t.Fatal("changed content accepted", err)
	}
	p.cexPublications[0] = want
	status, _ := c.mapping("source_batch_status")
	hash, err := keyHash(status, record{"source_kind": want.Kind, "source_id": want.ID})
	if err != nil {
		t.Fatal(err)
	}
	cols := columns(status)
	if _, err = c.db.ExecContext(ctx, "INSERT INTO "+c.table(status.Name)+" ("+cols+",_key_hash,_occurrence) SELECT "+cols+",_key_hash,1 FROM "+c.table(status.Name)+" WHERE _key_hash=? AND _occurrence=0", hash[:]); err != nil {
		t.Fatal(err)
	}
	if err = publish(); !errors.Is(err, contracts.ErrConflict) {
		t.Fatal("historical duplicate accepted", err)
	}
	// Force a physical hashed-key collision with a different business identity.
	// The absent-business fast path must propagate this fact-table duplicate,
	// never interpret it as the ingest_batch winner's durable proof.
	batch = testBatch(t, 101, minute, 5)
	ds, err = c.PrepareBookDeliveries(ctx, model.CompletedMinute{MinuteTime: minute, Batches: []model.MinuteBatch{batch}})
	if err != nil {
		t.Fatal(err)
	}
	p, err = c.PrepareDelivery(ctx, ds[0])
	if err != nil {
		t.Fatal(err)
	}
	intent, _ := c.mapping("source_publication_intents")
	collision, err := keyHash(intent, record{"source_kind": p.cexPublications[0].Kind, "source_id": p.cexPublications[0].ID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.db.ExecContext(ctx, "UPDATE "+c.table(intent.Name)+" SET _key_hash=? WHERE source_kind=? AND source_id=?", collision[:], want.Kind, want.ID); err != nil {
		t.Fatal(err)
	}
	err = c.CommitPrepared(ctx, p)
	var duplicate *driver.MySQLError
	if !errors.As(err, &duplicate) || duplicate.Number != 1062 {
		t.Fatal("fact duplicate not propagated", err)
	}
	if ok, e := c.Committed(ctx, ds[0]); e != nil || ok {
		t.Fatal("fact duplicate acknowledged batch", ok, e)
	}
	var count int
	if err = c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+c.table("order_book_minute")+" WHERE id=?", batch.Minute.ID).Scan(&count); err != nil || count != 0 {
		t.Fatal("conflict leaked partial minute", count, err)
	}
}
