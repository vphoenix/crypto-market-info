package binance

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"
)

func TestSubscriptionBatchesBoundEncodedBytesAndRetainAllTopics(t *testing.T) {
	for _, suffix := range []string{"@depth@100ms", "@markPrice@1s"} {
		streams := make([]string, 477)
		for i := range streams {
			streams[i] = fmt.Sprintf("asset%04dusdt%s", i, suffix)
		}
		streams[200] = "币安人生usdt" + suffix // UTF-8 byte length, not rune count.
		batches, err := subscriptionBatches(streams)
		if err != nil {
			t.Fatal(err)
		}
		var restored []string
		for _, batch := range batches {
			payload, _ := json.Marshal(map[string]any{"method": "SUBSCRIBE", "params": batch, "id": int64(math.MaxInt64)})
			if len(payload) > 4000 || len(batch) > 200 || len(batch) == 0 {
				t.Fatalf("batch exceeds budget topics=%d bytes=%d", len(batch), len(payload))
			}
			restored = append(restored, batch...)
		}
		if len(restored) != len(streams) {
			t.Fatal("topic lost in splitting")
		}
		for i := range streams {
			if restored[i] != streams[i] {
				t.Fatal("topic order changed")
			}
		}
	}
	if _, err := subscriptionBatches([]string{strings.Repeat("x", 4000)}); err == nil {
		t.Fatal("oversized single topic accepted")
	}
}

func TestNestedWebsocketControlErrorIsNotMarketData(t *testing.T) {
	payload := []byte(`{"error":{"code":3,"msg":"Invalid JSON: EOF while parsing a string at line 1 column 4096"}}`)
	ack, err := parseSubscriptionACK(payload, 1)
	if !ack || err == nil || !strings.Contains(err.Error(), "4096") {
		t.Fatalf("nested control error misclassified ack=%v err=%v", ack, err)
	}
}
