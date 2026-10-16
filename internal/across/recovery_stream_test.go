package across

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

type recoveryStreamStore struct {
	historyBulkStore
	chunks []int
	badID  string
}

func (s *recoveryStreamStore) AcrossBatches(ctx context.Context, caps []Capture) (BatchLoad, error) {
	s.chunks = append(s.chunks, len(caps))
	if len(caps) > recoveryReadCaptureLimit {
		return BatchLoad{}, errors.New("unbounded_recovery_read")
	}
	loaded, err := s.historyBulkStore.AcrossBatches(ctx, caps)
	if err == nil {
		if _, ok := loaded.Batches[s.badID]; ok {
			loaded.Errors[s.badID] = errors.New("fixture_member_digest_mismatch")
		}
	}
	return loaded, err
}

func recoveryStreamFixture(t *testing.T) (*Collector, *recoveryStreamStore) {
	t.Helper()
	m := runnerManifest(t)
	store := &recoveryStreamStore{}
	for n := 0; n < recoveryReadCaptureLimit+1; n++ {
		b := historyReceiptFixture(t, m, fmt.Sprintf("stream-fill-%d", n), "logs", strings.Repeat("a", 32), false)
		if err := store.WriteAcrossBatch(context.Background(), b); err != nil {
			t.Fatal(err)
		}
	}
	return &Collector{Manifest: m, Store: store}, store
}

func TestRecoveryStreamsAllFactsAndReceiptsWinAcrossChunks(t *testing.T) {
	for _, startup := range []bool{true, false} {
		t.Run(fmt.Sprintf("restore=%t", startup), func(t *testing.T) {
			c, store := recoveryStreamFixture(t)
			// Choose a receipt identity ordered before every fill. The final
			// chunk still contains a duplicate fill for this completed receipt.
			label := ""
			for n := 0; ; n++ {
				candidate := fmt.Sprintf("stream-complete-%d", n)
				before := true
				for _, cap := range store.caps {
					before = before && ID(candidate) < cap.CaptureId
				}
				if before {
					label = candidate
					break
				}
			}
			b := historyReceiptFixture(t, c.Manifest, label, "receipts", strings.Repeat("a", 32), true)
			if err := store.WriteAcrossBatch(context.Background(), b); err != nil {
				t.Fatal(err)
			}
			if startup {
				if _, err := c.restore(context.Background()); err != nil {
					t.Fatal(err)
				}
			} else if err := c.PrepareRepair(context.Background()); err != nil {
				t.Fatal(err)
			}
			if len(store.chunks) != 2 || store.chunks[0] != recoveryReadCaptureLimit || store.chunks[1] != 2 {
				t.Fatalf("not all captures read in bounded chunks: %v", store.chunks)
			}
			if len(c.receiptQueue) != 0 {
				t.Fatal("later duplicate fill requeued an already complete receipt")
			}
		})
	}
}

func TestRecoveryRejectsLateChunkCorruptionWithoutPublishingNewReceiptWork(t *testing.T) {
	c, store := recoveryStreamFixture(t)
	latest, err := latestReadCaptures(store.caps)
	if err != nil {
		t.Fatal(err)
	}
	store.badID = latest[len(latest)-1].CaptureId
	old := receiptTask{ChainID: 8453, BlockHash: strings.Repeat("b", 32), TxHash: strings.Repeat("c", 32)}
	c.receiptQueue = map[string]receiptTask{ID(old): old}
	if err := c.RefreshRepairReceipts(context.Background()); err == nil {
		t.Fatal("late member corruption accepted")
	}
	if len(store.chunks) != 2 || len(c.receiptQueue) != 1 || c.receiptQueue[ID(old)] != old {
		t.Fatal("failed streaming validation replaced existing work")
	}
}
