package marinade

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type identityCheck struct {
	calls int
	err   error
}

func (i *identityCheck) ValidateMSOLIdentity(context.Context) error { i.calls++; return i.err }

func TestMSOLCollectorMapsRollingAPYLabels(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/rolling-apy/liquid-pool-token/MarBmsSgKXdrN1egZf5sqe1TMai9K1rChYNDJgjq7aD" || r.URL.Query().Get("window") != "1209600" {
			t.Fatalf("request=%s", r.URL.String())
		}
		fmt.Fprint(w, `{"times":[1787443200,1787529600],"values":[0.05,0.06],"labels":[{"lowerTime":1786233600,"upperTime":1787443200,"lowerPrice":1.3,"upperPrice":1.31},{"lowerTime":1786320000,"upperTime":1787529600,"lowerPrice":1.31,"upperPrice":1.32}]}`)
	}))
	defer server.Close()
	identity := &identityCheck{}
	collector := NewMSOLCollector(server.URL, identity)
	collector.Now = func() time.Time { return time.Date(2026, 8, 25, 1, 0, 0, 0, time.UTC) }
	batch, err := collector.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if identity.calls != 1 || len(batch.Items) != 2 || batch.Items[1].Observation.Rate.String() != "0.06" || batch.Items[1].Observation.ExposureRatio.String() != "1.32" || batch.Items[1].Observation.BlockHeight != nil {
		t.Fatalf("batch=%+v calls=%d", batch, identity.calls)
	}
	if batch.Items[0].Route.ProductCode != "msol" || batch.Items[0].Route.PositionAssetKey == batch.Items[0].Route.DepositAssetKey {
		t.Fatalf("route=%+v", batch.Items[0].Route)
	}
	if err = batch.NormalizeAndValidateForLiveCollection(); err != nil {
		t.Fatal(err)
	}
}

func TestNativeCollectorIgnoresLabelPrices(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"times":[1787529600],"values":[0.055],"labels":[{"lowerTime":0,"upperTime":0,"lowerPrice":0,"upperPrice":0}]}`)
	}))
	defer server.Close()
	collector := NewNativeCollector(server.URL)
	collector.Now = func() time.Time { return time.Date(2026, 8, 25, 1, 0, 0, 0, time.UTC) }
	batch, err := collector.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	o := batch.Items[0].Observation
	if batch.Items[0].Route.ProductCode != "marinade-native" || o.ExposureRatio.String() != "1" || o.UnbondingSeconds != nil {
		t.Fatalf("item=%+v", batch.Items[0])
	}
}

func TestMSOLCollectorRejectsMisalignedLabel(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"times":[1787529600],"values":[0.05],"labels":[{"lowerTime":1786320000,"upperTime":1787529599,"lowerPrice":1.3,"upperPrice":1.31}]}`)
	}))
	defer server.Close()
	collector := NewMSOLCollector(server.URL, &identityCheck{})
	collector.Now = func() time.Time { return time.Date(2026, 8, 25, 1, 0, 0, 0, time.UTC) }
	if _, err := collector.Collect(context.Background()); err == nil {
		t.Fatal("misaligned mSOL label accepted")
	}
}
