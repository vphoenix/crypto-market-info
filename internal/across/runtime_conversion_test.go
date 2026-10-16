package across

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	runtimeinputtest "github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint/testutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
)

type runtimeConversionDB struct{ *runtimeTestDB }

func (d *runtimeConversionDB) AcrossCaptures(ctx context.Context, manifest string) ([]Capture, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.historyReads++
	var out []Capture
	for _, b := range d.rows {
		if b.Capture.ManifestHash == manifest {
			out = append(out, b.Capture)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].StartedAt.Equal(out[j].StartedAt) {
			return out[i].CaptureId < out[j].CaptureId
		}
		return out[i].StartedAt.Before(out[j].StartedAt)
	})
	return out, ctx.Err()
}
func (d *runtimeConversionDB) AcrossCapturePage(ctx context.Context, manifest string, after []byte, limit int) ([]Capture, []byte, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var out []Capture
	for _, b := range d.rows {
		if b.Capture.ManifestHash == manifest && (len(after) == 0 || bytes.Compare([]byte(b.Capture.CaptureId), after) > 0) {
			out = append(out, b.Capture)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CaptureId < out[j].CaptureId })
	if len(out) > limit {
		out = out[:limit]
	}
	if len(out) == 0 {
		return out, nil, ctx.Err()
	}
	return out, []byte(out[len(out)-1].CaptureId), ctx.Err()
}
func TestRuntimeExplicitConversionMatchesLegacyAndRestartReadsNoHistory(t *testing.T) {
	o, db, _, binding, b, m := runtimeOwnerFixture(t)
	if err := o.close(); err != nil {
		t.Fatal(err)
	}
	if err := db.WriteAcrossBatch(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	source := &runtimeConversionDB{db}
	c := Collector{Manifest: m, Store: source}
	dir := filepath.Join(t.TempDir(), "runtime")
	check := func(context.Context) (runtimecheckpoint.Binding, error) { return binding, nil }
	report, err := c.ConvertRuntime(context.Background(), dir, binding, runtimeinputtest.Inventory(t, binding, "across"), check, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Verified || report.Headers != 1 || report.Active != 1 || report.RawCursors[b.Capture.ChainId] != 10 {
		t.Fatal(report)
	}
	reads := db.historyReads
	if err = c.OpenRuntime(context.Background(), dir, binding, nil); err != nil {
		t.Fatal(err)
	}
	if db.historyReads != reads {
		t.Fatal("restart reran conversion")
	}
	if err = c.CloseRuntime(); err != nil {
		t.Fatal(err)
	}
	if _, err = c.ConvertRuntime(context.Background(), dir, binding, runtimeinputtest.Inventory(t, binding, "across"), check, nil); err == nil {
		t.Fatal("overwrote installed state")
	}
}
func TestRuntimeConversionResumesAllPrimaryKeyPages(t *testing.T) {
	o, db, _, binding, _, m := runtimeOwnerFixture(t)
	if err := o.close(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 501; i++ {
		b := batchLoadFixture(fmt.Sprint(i))
		b.Capture.ManifestHash = m.Hash
		db.rows[b.Capture.CaptureId] = b
	}
	source := &runtimeConversionDB{db}
	c := Collector{Manifest: m, Store: source}
	dir := filepath.Join(t.TempDir(), "runtime")
	check := func(context.Context) (runtimecheckpoint.Binding, error) { return binding, nil }
	ctx, cancel := context.WithCancel(context.Background())
	_, err := c.ConvertRuntime(ctx, dir, binding, runtimeinputtest.Inventory(t, binding, "across"), check, func(line string) {
		if strings.HasPrefix(line, "conversion_headers") {
			cancel()
		}
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatal("conversion interruption not preserved", err)
	}
	report, err := c.ConvertRuntime(context.Background(), dir, binding, runtimeinputtest.Inventory(t, binding, "across"), check, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Verified || report.Headers != 501 || report.FactBatches != 501 || report.FactPages != 2 {
		t.Fatal("only first page converted", report)
	}
}
func TestRuntimeConversionRejectsChangedDatabaseBindingBeforePublish(t *testing.T) {
	o, db, _, binding, b, m := runtimeOwnerFixture(t)
	if err := o.close(); err != nil {
		t.Fatal(err)
	}
	db.rows[b.Capture.CaptureId] = b
	c := Collector{Manifest: m, Store: &runtimeConversionDB{db}}
	calls := 0
	_, err := c.ConvertRuntime(context.Background(), filepath.Join(t.TempDir(), "runtime"), binding, runtimeinputtest.Inventory(t, binding, "across"), func(context.Context) (runtimecheckpoint.Binding, error) {
		calls++
		v := binding
		if calls > 1 {
			v.RecoveryEpoch = "restored"
		}
		return v, nil
	}, nil)
	if !errors.Is(err, runtimecheckpoint.ErrConflict) {
		t.Fatal("changed database restoration accepted", err)
	}
}

func TestRuntimeConversionResumesAfterReadyCheckpointBeforePublication(t *testing.T) {
	o, db, _, binding, b, m := runtimeOwnerFixture(t)
	if err := o.close(); err != nil {
		t.Fatal(err)
	}
	db.rows[b.Capture.CaptureId] = b
	c := Collector{Manifest: m, Store: &runtimeConversionDB{db}}
	dir := filepath.Join(t.TempDir(), "runtime")
	calls := 0
	_, err := c.ConvertRuntime(context.Background(), dir, binding, runtimeinputtest.Inventory(t, binding, "across"), func(context.Context) (runtimecheckpoint.Binding, error) {
		calls++
		if calls == 3 {
			return binding, context.Canceled
		}
		return binding, nil
	}, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err = os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("published before final fence", err)
	}
	if _, err = os.Stat(dir + ".ready/checkpoint.current"); err != nil {
		t.Fatal(err)
	}
	report, err := c.ConvertRuntime(context.Background(), dir, binding, runtimeinputtest.Inventory(t, binding, "across"), func(context.Context) (runtimecheckpoint.Binding, error) { return binding, nil }, nil)
	if err != nil || !report.Verified {
		t.Fatal(report, err)
	}
	if err = c.OpenRuntime(context.Background(), dir, binding, nil); err != nil {
		t.Fatal(err)
	}
	defer c.CloseRuntime()
	// Conversion provenance must not share the subsequently mutable cursor map.
	if err = c.runtime.change(context.Background(), func(s *runtimeState) (any, error) { s.Watch.Cursors[b.Capture.ChainId] = 123; return true, nil }, false); err != nil {
		t.Fatal(err)
	}
	var original map[uint64]uint64
	if err = c.runtime.view(context.Background(), func(s *runtimeState) (any, error) { return s.Conversion.RawCursors, nil }, &original); err != nil || original[b.Capture.ChainId] != 10 {
		t.Fatal(original, err)
	}
}

func TestRuntimeNormalStartRejectsUnverifiedOrSemanticallyInvalidCheckpoint(t *testing.T) {
	for _, kind := range []string{"unverified", "bad_cursor"} {
		t.Run(kind, func(t *testing.T) {
			o, db, dir, binding, b, m := runtimeOwnerFixture(t)
			if err := o.close(); err != nil {
				t.Fatal(err)
			}
			files, err := runtimecheckpoint.Open(dir, binding, 0)
			if err != nil {
				t.Fatal(err)
			}
			var state runtimeState
			if err = runtimecheckpoint.UnmarshalTyped(files.State(), &state); err != nil {
				t.Fatal(err)
			}
			if kind == "bad_cursor" {
				state.Conversion = RuntimeConversionReport{Version: 1, Verified: true, Binding: binding, InputDigest: strings.Repeat("a", 64)}
				if err = state.capture(b.Capture); err != nil {
					t.Fatal(err)
				}
				state.Watch.Cursors[b.Capture.ChainId]++
			}
			raw, err := runtimecheckpoint.MarshalTyped(state)
			if err != nil {
				t.Fatal(err)
			}
			if err = files.Checkpoint(raw, nil); err != nil {
				t.Fatal(err)
			}
			files.Close()
			c := Collector{Manifest: m, Store: db}
			if err = c.OpenRuntime(context.Background(), dir, binding, nil); !errors.Is(err, runtimecheckpoint.ErrRecovery) {
				t.Fatal("invalid state accepted", err)
			}
			if db.historyReads != 0 || db.writes != 0 {
				t.Fatal("invalid state reached database")
			}
		})
	}
}

func TestRuntimeRollbackResolvesCurrentPendingBeforeControlledLegacyRebuild(t *testing.T) {
	o, db, _, binding, b, m := runtimeOwnerFixture(t)
	if err := o.close(); err != nil {
		t.Fatal(err)
	}
	c := Collector{Manifest: m, Store: &runtimeConversionDB{db}}
	dir := filepath.Join(t.TempDir(), "runtime")
	if _, err := c.ConvertRuntime(context.Background(), dir, binding, runtimeinputtest.Inventory(t, binding, "across"), func(context.Context) (runtimecheckpoint.Binding, error) { return binding, nil }, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.OpenRuntime(context.Background(), dir, binding, nil); err != nil {
		t.Fatal(err)
	}
	db.loseReturn = true
	if err := c.runtime.writeBatch(context.Background(), db, b); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if err := c.CloseRuntime(); err != nil {
		t.Fatal(err)
	}
	if err := c.OpenRuntime(context.Background(), dir, binding, nil); err != nil {
		t.Fatal(err)
	}
	defer c.CloseRuntime()
	backup := filepath.Join(t.TempDir(), "rollback-current")
	report, err := c.RollbackRuntime(context.Background(), backup)
	if err != nil {
		t.Fatal(err)
	}
	if report.Current.Files.Pending != 0 || report.RebuiltRawCursors[b.Capture.ChainId] != 10 || db.writes != 2 {
		t.Fatal(report, db.writes)
	}
	files, err := runtimecheckpoint.Open(backup, binding, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	if files.Metrics().Pending != 0 {
		t.Fatal("export lost pending commit proof")
	}
}

type firstPageFails struct{ *runtimeConversionDB }

func (d firstPageFails) AcrossCapturePage(context.Context, string, []byte, int) ([]Capture, []byte, error) {
	return nil, nil, errors.New("source temporarily unavailable")
}
func TestRuntimeConversionPinsInventoryBeforeFirstSourcePage(t *testing.T) {
	o, db, _, binding, _, m := runtimeOwnerFixture(t)
	if err := o.close(); err != nil {
		t.Fatal(err)
	}
	c := Collector{Manifest: m, Store: firstPageFails{&runtimeConversionDB{db}}}
	dir := filepath.Join(t.TempDir(), "runtime")
	path := runtimeinputtest.Inventory(t, binding, "across")
	check := func(context.Context) (runtimecheckpoint.Binding, error) { return binding, nil }
	if _, err := c.ConvertRuntime(context.Background(), dir, binding, path, check, nil); err == nil {
		t.Fatal("source failure accepted")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw = bytes.Replace(raw, []byte("2026-01-01"), []byte("2026-01-02"), 1)
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	c.Store = &runtimeConversionDB{db}
	if _, err = c.ConvertRuntime(context.Background(), dir, binding, path, check, nil); err == nil || !strings.Contains(err.Error(), "inventory changed") {
		t.Fatal("changed resume inventory", err)
	}
}
