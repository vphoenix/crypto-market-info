package tron

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/exchange"
)

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func TestClientFetchesCompleteSnapshotAndDetectsMaintenanceChange(t *testing.T) {
	tests := []struct {
		name         string
		changed      bool
		badBrokerage int
		trailing     bool
		wantError    bool
	}{
		{name: "complete"}, {name: "maintenance changed", changed: true, wantError: true}, {name: "brokerage parse failure", badBrokerage: 64, wantError: true}, {name: "trailing JSON", trailing: true, wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := NewClient("https://tron.invalid")
			client.Now = func() time.Time { return time.UnixMilli(20_000_000) }
			client.Retry = exchange.HTTPRetryConfig{MaxAttempts: 1, Cooldown: exchange.NewRequestGate(0)}
			maintenanceCalls, blockCalls, brokerageCalls := 0, 0, 0
			client.HTTP = &http.Client{Transport: transportFunc(func(request *http.Request) (*http.Response, error) {
				var body string
				switch request.URL.Path {
				case "/wallet/getnextmaintenancetime":
					maintenanceCalls++
					next := 21_600_000
					if test.changed && maintenanceCalls == 2 {
						next++
					}
					body = `{"num":` + jsonNumber(next) + `}`
				case "/walletsolidity/getblock":
					blockCalls++
					id := strings.Repeat(map[bool]string{false: "a", true: "b"}[blockCalls == 2], 64)
					number := 100 + blockCalls
					body = `{"blockID":"` + id + `","block_header":{"raw_data":{"number":` + jsonNumber(number) + `,"timestamp":20000000}}}`
				case "/walletsolidity/getpaginatednowwitnesslist":
					witnesses := validTRONSnapshot().Witnesses
					encoded, _ := json.Marshal(map[string]any{"witnesses": witnesses})
					body = string(encoded)
				case "/wallet/getchainparameters":
					body = `{"chainParameter":[{"key":"getAllowUpdateAccountName"},{"key":"getMaintenanceTimeInterval","value":21600000},{"key":"getWitnessPayPerBlock","value":8000000},{"key":"getWitness127PayPerBlock","value":128000000},{"key":"getUnfreezeDelayDays","value":14}]}`
					if test.trailing {
						body += ` {}`
					}
				case "/walletsolidity/getBrokerage":
					brokerageCalls++
					payload, _ := io.ReadAll(request.Body)
					var input struct {
						Address string `json:"address"`
						Visible bool   `json:"visible"`
					}
					if json.Unmarshal(payload, &input) != nil || !input.Visible || input.Address == "" {
						t.Fatalf("invalid brokerage request %s", payload)
					}
					if brokerageCalls == test.badBrokerage {
						body = `{}`
					} else {
						body = `{"brokerage":20}`
					}
				default:
					t.Fatalf("unexpected request %s", request.URL.String())
				}
				return &http.Response{StatusCode: 200, Status: "200 OK", Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
			})}
			snapshot, err := client.Fetch(context.Background())
			if test.wantError {
				if err == nil {
					t.Fatal("invalid response accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(snapshot.Witnesses) != 127 || len(snapshot.Brokerage) != 127 || maintenanceCalls != 2 || blockCalls != 2 || brokerageCalls != 127 {
				t.Fatalf("snapshot/counts=%d/%d calls=%d/%d/%d", len(snapshot.Witnesses), len(snapshot.Brokerage), maintenanceCalls, blockCalls, brokerageCalls)
			}
		})
	}
}

func jsonNumber(value int) string { encoded, _ := json.Marshal(value); return string(encoded) }
