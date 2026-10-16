package spool

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

func TestHeaderIndexNotificationRebuildAndConsumerLock(t *testing.T) {
	ctx := context.Background()
	q, e := Open(t.TempDir(), 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	lock, e := q.ConsumerLock()
	if e != nil {
		t.Fatal(e)
	}
	defer lock.Close()
	if second, e := q.ConsumerLock(); e == nil {
		second.Close()
		t.Fatal("second consumer accepted")
	}
	i, e := q.NewIndex(ctx, 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	d := delivery()
	if e = q.Accept(ctx, d); e != nil {
		t.Fatal(e)
	}
	if e = i.Refresh(ctx); e != nil {
		t.Fatal(e)
	}
	cs := i.Snapshot()
	if len(cs) != 1 || len(cs[0].Header.Payload) != 0 || cs[0].Bytes != int64(len(d.Payload)) {
		t.Fatal(cs)
	}
	got, e := q.Load(ctx, cs[0])
	if e != nil || !got.SameHeader(d) {
		t.Fatal(e)
	}
	i.Close()
	i, e = q.NewIndex(ctx, 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	defer i.Close()
	if len(i.Snapshot()) != 1 {
		t.Fatal("index could not rebuild")
	}
	if e = q.Ack(ctx, d, func(context.Context, contracts.Delivery) (bool, error) { return false, nil }); e == nil {
		t.Fatal("uncommitted file removed")
	}
	if _, e = os.Stat(filepath.Join(q.Directory, Name(d))); e != nil {
		t.Fatal(e)
	}
	if e = q.Ack(ctx, d, func(context.Context, contracts.Delivery) (bool, error) { return true, nil }); e != nil {
		t.Fatal(e)
	}
	if e = i.Refresh(ctx); e != nil || len(i.Snapshot()) != 0 {
		t.Fatal("index missed deletion", e)
	}
}

func TestAckDoesNotHoldDirectoryLockDuringCommitProof(t *testing.T) {
	ctx := context.Background()
	q, _ := Open(t.TempDir(), 1<<20)
	d := delivery()
	if e := q.Accept(ctx, d); e != nil {
		t.Fatal(e)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- q.Ack(ctx, d, func(context.Context, contracts.Delivery) (bool, error) { close(entered); <-release; return true, nil })
	}()
	<-entered
	deadline, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if e := q.locked(deadline, func() error { return nil }); e != nil {
		close(release)
		t.Fatal("DB proof held directory lock", e)
	}
	close(release)
	if e := <-done; e != nil {
		t.Fatal(e)
	}
}
