package optionslive

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/exchange/deribit"
	"github.com/vphoenix/crypto-market-info/internal/options"
)

type payloadCaptureSink struct {
	catalogMemorySink
	writeError error
	catalogs   []options.CatalogObservation
	lifecycles []options.LifecycleObservation
}

func (s *payloadCaptureSink) WriteOptionsCatalog(_ context.Context, o options.CatalogObservation) error {
	if err := o.Validate(); err != nil {
		return err
	}
	s.catalogs = append(s.catalogs, o)
	return s.writeError
}

func (s *payloadCaptureSink) WriteOptionsLifecycle(_ context.Context, o options.LifecycleObservation) error {
	if err := o.Validate(); err != nil {
		return err
	}
	s.lifecycles = append(s.lifecycles, o)
	return s.writeError
}

func (s *payloadCaptureSink) WriteOptionsLifecycles(_ context.Context, oo []options.LifecycleObservation) error {
	for _, o := range oo {
		if err := o.Validate(); err != nil {
			return err
		}
	}
	s.lifecycles = append(s.lifecycles, oo...)
	return s.writeError
}

func catalogPayloadJob(t *testing.T, kind string) *catalogJob {
	t.Helper()
	at := time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)
	if kind != "catalog" {
		raw := []byte(`{"jsonrpc":"2.0","id":3,"result":{"locked":"partial","locked_indices":["btc_usd"]}}`)
		o, err := deribit.DecodeLifecycle(deribit.StreamEvent{Kind: "status", Channel: "public/status", Epoch: uuid.New(), Sequence: 7, Raw: raw}, at)
		if err != nil {
			t.Fatal(err)
		}
		return &catalogJob{life: &o, raw: raw}
	}
	raw, err := os.ReadFile("../exchange/deribit/testdata/metadata-BTC-option.json")
	if err != nil {
		t.Fatal(err)
	}
	items, _, err := deribit.DecodeInstruments(raw, "BTC", "option", at)
	if err != nil {
		t.Fatal(err)
	}
	url := "https://www.deribit.com/api/v2/public/get_instruments"
	for n := range items {
		items[n].Rule.SourceURL = url
	}
	return &catalogJob{kind: "catalog", result: &deribit.ScopeResult{
		RequestID: uuid.New(), Scope: deribit.Scope{Currency: "BTC", Kind: "option"}, URL: url,
		RequestedAt: at, ObservedAt: at, PayloadHash: options.PayloadHash(raw), Raw: raw,
		Status: "complete", RawCount: uint32(len(items)), Instruments: items,
	}}
}

func persistPayloadJob(s *catalogSupervisor, kind string, j *catalogJob) error {
	if kind == "lifecycle_batch" {
		return s.persistLifecycles(context.Background(), []*catalogJob{j})
	}
	return s.persist(context.Background(), j)
}

func TestCatalogPersistsTypedObservationsWithoutArchive(t *testing.T) {
	for _, kind := range []string{"catalog", "lifecycle", "lifecycle_batch"} {
		for _, archiveState := range []string{"missing", "old_corrupt_file"} {
			t.Run(kind+"/"+archiveState, func(t *testing.T) {
				s, _, _, _ := supervisorFixture(t)
				sink := &payloadCaptureSink{}
				s.sink = sink
				j := catalogPayloadJob(t, kind)
				hash := ""
				if j.life != nil {
					hash = j.life.PayloadHash
				} else {
					hash = j.result.PayloadHash
				}
				dir := t.TempDir()
				s.cfg.EvidenceDir = filepath.Join(dir, "removed-archive")
				oldPath := filepath.Join(dir, hash+".json.gz")
				if archiveState == "old_corrupt_file" {
					s.cfg.EvidenceDir = dir
					if err := os.WriteFile(oldPath, []byte("obsolete invalid gzip"), 0600); err != nil {
						t.Fatal(err)
					}
				}
				// A failed DB write is retried using the same typed observation,
				// regardless of whether old response files exist or are readable.
				sink.writeError = errors.New("temporary database failure")
				if err := persistPayloadJob(s, kind, j); !errors.Is(err, sink.writeError) {
					t.Fatalf("did not reach database: %v", err)
				}
				sink.writeError = nil
				if err := persistPayloadJob(s, kind, j); err != nil {
					t.Fatal(err)
				}
				if kind == "catalog" {
					if len(sink.catalogs) != 2 || sink.catalogs[0].Hash() != sink.catalogs[1].Hash() || sink.catalogs[1].PayloadHash != hash || len(sink.catalogs[1].Symbols) == 0 || sink.catalogs[1].URL != j.result.URL {
						t.Fatal("catalog facts or retry identity changed")
					}
				} else if len(sink.lifecycles) != 2 || sink.lifecycles[0].Hash() != sink.lifecycles[1].Hash() || sink.lifecycles[1].PayloadHash != hash || sink.lifecycles[1].LockMode != "partial" || sink.lifecycles[1].LockedIndexes[0] != "btc_usd" {
					t.Fatal("lifecycle facts or retry identity changed")
				}
				files, err := os.ReadDir(dir)
				if err != nil {
					t.Fatal(err)
				}
				if archiveState == "missing" && len(files) != 0 {
					t.Fatal("persistence recreated response archive")
				}
				if archiveState == "old_corrupt_file" {
					b, err := os.ReadFile(oldPath)
					if err != nil || string(b) != "obsolete invalid gzip" || len(files) != 1 {
						t.Fatal("persistence modified legacy response files")
					}
				}
			})
		}
	}
}

func TestCatalogRejectsPayloadMismatchBeforeDatabaseWrite(t *testing.T) {
	for _, kind := range []string{"catalog", "lifecycle", "lifecycle_batch"} {
		for _, raw := range [][]byte{nil, []byte("different response")} {
			t.Run(kind+"/"+string(raw), func(t *testing.T) {
				s, _, _, _ := supervisorFixture(t)
				sink := &payloadCaptureSink{}
				s.sink = sink
				j := catalogPayloadJob(t, kind)
				if j.life != nil {
					j.raw = raw
				} else {
					j.result.Raw = raw
				}
				if err := persistPayloadJob(s, kind, j); err == nil {
					t.Fatal("accepted response not matching its source hash")
				}
				if len(sink.registry) != 0 || len(sink.catalogs) != 0 || len(sink.lifecycles) != 0 {
					t.Fatal("mismatched response reached database")
				}
			})
		}
	}
}
