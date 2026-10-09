package across

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestReceiptEndpointIsExplicitAndDefaultsToPrimary(t *testing.T) {
	c := ChainConfig{RPCEnv: "ACROSS_TEST_RPC_URL", DefaultRPC: "https://default.example"}
	t.Setenv("ACROSS_TEST_RPC_URL", "")
	t.Setenv("ACROSS_TEST_RECEIPT_RPC_URL", "")
	if c.ReceiptEndpoint() != c.DefaultRPC {
		t.Fatal("missing default")
	}
	t.Setenv("ACROSS_TEST_RPC_URL", "https://logs.example")
	if c.ReceiptEndpoint() != c.Endpoint() {
		t.Fatal("implicit receipt override")
	}
	t.Setenv("ACROSS_TEST_RECEIPT_RPC_URL", "https://receipts.example")
	if c.ReceiptEndpoint() != "https://receipts.example" || c.Endpoint() != "https://logs.example" {
		t.Fatal("receipt setting changed primary")
	}
}

func TestReceiptSourcePreservesAnchorsFeesAndActualSource(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure"}[fail], func(t *testing.T) {
			m := runnerManifest(t)
			chain, _ := m.Chain(8453)
			main := protocolChainReader(t, chain, func(method string, _ json.RawMessage) (any, bool) {
				t.Fatalf("receipt fell back to log source: %s", method)
				return nil, false
			})
			hash, tx, addr := strings.Repeat("h", 32), strings.Repeat("t", 32), Hex(strings.Repeat("a", 20))
			r := mockReader(t, 8453, func(method string, _ json.RawMessage) any {
				switch method {
				case "eth_getTransactionByHash":
					return map[string]any{"hash": Hex(tx), "blockHash": Hex(hash), "blockNumber": "0x7b", "transactionIndex": "0x0", "from": addr, "to": addr, "type": "0x2", "input": "0x12345678", "value": "0x0", "chainId": "0x2105"}
				case "eth_getTransactionReceipt":
					return map[string]any{"transactionHash": Hex(tx), "blockHash": Hex(hash), "blockNumber": "0x7b", "transactionIndex": "0x0", "from": addr, "to": addr, "status": "0x1", "gasUsed": "0xc8", "effectiveGasPrice": "0x3", "l1Fee": "0x32", "logs": []any{}}
				case "eth_getBlockByNumber":
					return protocolHeader(123, hash, time.Unix(1000, 0).UTC())
				case "eth_call":
					return "0x" + strings.Repeat("0", 64)
				}
				t.Fatal(method)
				return nil
			})
			r.Chain = chain
			r.RPC.SourceID = "receipts.example"
			if fail {
				r.RPC.BeforeRequest = func(context.Context, int) error { return errors.New("receipt_source_unavailable") }
			}
			store := &coreStore{}
			task := receiptTask{8453, hash, tx}
			key := ID(task)
			c := Collector{Manifest: m, Store: store, Archive: main.RPC.Archive, Readers: map[uint64]*Reader{8453: main}, ReceiptReaders: map[uint64]*Reader{8453: r}, receiptQueue: map[string]receiptTask{key: task}}
			if err := c.DrainReceipts(context.Background(), 1); err != nil {
				t.Fatal(err)
			}
			if len(store.caps) != 1 || store.caps[0].SourceId != "receipts.example" {
				t.Fatal("wrong committed source")
			}
			b := store.batches[store.caps[0].CaptureId]
			if fail {
				if b.Capture.Status != "error" || b.Capture.CompletedTasks != 0 || len(c.receiptQueue) != 1 {
					t.Fatal("failure erased queue or claimed coverage")
				}
			} else {
				if len(b.Receipts) != 1 || !b.Receipts[0].FeeComplete || b.Receipts[0].TotalFeeWei.String() != "650" || b.Receipts[0].BlockHash != hash || len(b.Transfers) != 1 || len(c.receiptQueue) != 0 {
					t.Fatal("fee, transfer or anchor lost")
				}
				if _, err := ReadCaptureEvidence(c.Archive, b.Capture); err != nil {
					t.Fatal(err)
				}
			}
			worker := c.worker()
			if worker.ReceiptReaders[8453] == r || worker.receiptReader(8453).RPC.SourceID != r.RPC.SourceID || worker.Readers[8453].RPC.SourceID != main.RPC.SourceID {
				t.Fatal("worker lost independent source")
			}
		})
	}
}

func TestReceiptSourceConfirmedFaultIsSharedWithWorkerAndStopsWatch(t *testing.T) {
	c, _ := priorityProbeCollector(t)
	r := protocolChainReader(t, c.Manifest.Chains[0], nil)
	c.ReceiptReaders = map[uint64]*Reader{8453: r}
	clone := c.worker().ReceiptReaders[8453].RPC
	clone.AuditFailures = true
	clone.HTTP.Transport = protocolTransport(func(*http.Request) (*http.Response, error) {
		return nil, &net.DNSError{Err: "no such host", Name: "example.invalid", IsNotFound: true}
	})
	if result := clone.One(context.Background(), "eth_chainId", nil); result.Err == nil {
		t.Fatal("fixture missing fault")
	}
	if err := c.networkFaultError(); err == nil || !strings.Contains(err.Error(), "network_issue_stop") {
		t.Fatal("receipt source fault hidden", err)
	}
}
