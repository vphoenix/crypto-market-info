package mysql

import (
	"context"
	"errors"
	"testing"

	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

func TestMetadataCacheCopiesEvictsAndNeverCachesAbsence(t *testing.T) {
	c := &Client{database: "test", metadata: NewMetadataCache(512)}
	calls := 0
	load := func(context.Context) ([]string, error) { calls++; return []string{"immutable"}, nil }
	a, e := cachedMetadata(context.Background(), c, "key", load)
	if e != nil {
		t.Fatal(e)
	}
	a[0] = "modified"
	b, e := cachedMetadata(context.Background(), c, "key", load)
	if e != nil || b[0] != "immutable" || calls != 1 {
		t.Fatal("cached object aliased", b, calls, e)
	}
	for n := 0; n < 10; n++ {
		c.metadata.put(string(rune('a'+n)), make([]byte, 200))
	}
	_, e = cachedMetadata(context.Background(), c, "key", load)
	if e != nil || calls != 2 {
		t.Fatal("cache did not evict", calls, e)
	}
	missingCalls := 0
	missing := func(context.Context) (string, error) { missingCalls++; return "", contracts.ErrNotFound }
	for range 2 {
		if _, e := cachedMetadata(context.Background(), c, "absent", missing); !errors.Is(e, contracts.ErrNotFound) {
			t.Fatal(e)
		}
	}
	if missingCalls != 2 {
		t.Fatal("absence cached")
	}
	c.producer.Generation++
	_, e = cachedMetadata(context.Background(), c, "key", load)
	if e != nil || calls != 3 {
		t.Fatal("generation reused old cache", calls, e)
	}
}

func TestOptionsCachedPreparationMatchesColdValidation(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	r, e := catalogDBFixture(t, c)
	raw, err := codec.EncodeOptionsCatalog(e)
	if err != nil {
		t.Fatal(err)
	}
	d := c.Delivery("options_catalog", r.ID.String(), e.Live.MinuteTime, uint32(len(e.Live.Books)), raw)
	c.SetMetadataCache(NewMetadataCache(32 << 20))
	for range 2 {
		p, err := c.PrepareDelivery(ctx, d)
		if err != nil {
			t.Fatal(err)
		}
		if p.MemoryBytes() <= int64(len(raw)) {
			t.Fatal("decoded memory estimate omitted objects")
		}
	}
	c.metadata.Clear()
	if _, err = c.PrepareDelivery(ctx, d); err != nil {
		t.Fatal(err)
	}
	if err = c.Commit(ctx, d); err != nil {
		t.Fatal(err)
	}
}
