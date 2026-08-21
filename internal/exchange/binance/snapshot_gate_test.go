package binance

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"
	"time"
)

func TestDepthSnapshotsShareClientRequestGate(t *testing.T) {
	var mu sync.Mutex
	requestTimes := make([]time.Time, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		requestTimes = append(requestTimes, time.Now())
		mu.Unlock()
		_, _ = w.Write([]byte(`{"lastUpdateId":100,"T":1000,"bids":[["100","0.001"]],"asks":[["101","0.001"]]}`))
	}))
	defer server.Close()

	client := NewClient()
	client.FuturesBaseURL = server.URL
	client.HTTP = server.Client()
	client.SnapshotInterval = 80 * time.Millisecond
	btc := testInstrument()
	eth := btc
	eth.ID, eth.ExchangeSymbol, eth.BaseAsset = 2, "ETHUSDT", "ETH"

	start := make(chan struct{})
	errors := make(chan error, 2)
	for _, instrument := range []struct{ instrumentID int }{{1}, {2}} {
		selected := btc
		if instrument.instrumentID == 2 {
			selected = eth
		}
		go func() {
			<-start
			_, err := client.DepthSnapshot(context.Background(), selected)
			errors <- err
		}()
	}
	close(start)
	for range 2 {
		if err := <-errors; err != nil {
			t.Fatal(err)
		}
	}

	mu.Lock()
	times := append([]time.Time(nil), requestTimes...)
	mu.Unlock()
	if len(times) != 2 {
		t.Fatalf("requests=%d", len(times))
	}
	sort.Slice(times, func(i, j int) bool { return times[i].Before(times[j]) })
	if spacing := times[1].Sub(times[0]); spacing < 70*time.Millisecond {
		t.Fatalf("snapshot requests were only %s apart", spacing)
	}
}
