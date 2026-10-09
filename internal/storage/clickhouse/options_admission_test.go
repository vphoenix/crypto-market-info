package clickhouse

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCatalogExecutionBudgetStartsAfterAdmission(t *testing.T) {
	slots := make(chan struct{}, 1)
	slots <- struct{}{}
	done := make(chan error, 1)
	go func() {
		ctx, release, err := acquireCatalogWriteSlot(context.Background(), slots, 50*time.Millisecond)
		if err != nil {
			done <- err
			return
		}
		defer release()
		deadline, _ := ctx.Deadline()
		if time.Until(deadline) < 35*time.Millisecond {
			done <- errors.New("queue wait consumed execution budget")
			return
		}
		done <- ctx.Err()
	}()
	// Queue time deliberately exceeds the execution budget.
	time.Sleep(100 * time.Millisecond)
	<-slots
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if len(slots) != 0 {
		t.Fatal("admission slot leaked")
	}
}

func TestCatalogAdmissionCancellationNeverTakesSlot(t *testing.T) {
	slots := make(chan struct{}, 1)
	slots <- struct{}{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, _, err := acquireCatalogWriteSlot(ctx, slots, time.Minute)
	if !errors.Is(err, context.DeadlineExceeded) || len(slots) != 1 {
		t.Fatal("canceled waiter changed admission", err)
	}
}
