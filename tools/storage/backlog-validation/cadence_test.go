package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

func TestCapacityIncludesCommitAndCopyACKCadence(t *testing.T) {
	from := time.Date(2026, 10, 14, 20, 0, 0, 0, time.UTC)
	start := from.Add(24 * time.Hour)
	m := manifest{From: from, To: from.Add(3 * time.Minute)}
	for i := 0; i < 3; i++ {
		m.Entries = append(m.Entries, entry{Header: contracts.Delivery{
			Database: "crypto_market_info", Kind: "cex", ID: fmt.Sprint(i),
			Minute: from.Add(time.Duration(i) * time.Minute), Members: 337,
		}, Bytes: 1000})
	}
	line := func(i int, seconds int, members uint32) string {
		d := m.Entries[i].Header
		b, err := json.Marshal(map[string]any{"time": start.Add(time.Duration(seconds) * time.Second),
			"msg": "batch committed", "database": d.Database, "kind": d.Kind,
			"batch_id": d.ID, "members": members, "bytes": m.Entries[i].Bytes})
		if err != nil {
			t.Fatal(err)
		}
		return string(b) + "\n"
	}
	good := line(0, 20, 337) + line(1, 60, 337) + line(2, 100, 337)
	tests := []struct {
		name, log string
		pass      bool
	}{
		{"sustained", good, true},
		{"drained_after_window", line(0, 20, 337) + line(1, 60, 337) + line(2, 121, 337), false},
		{"stalled_then_caught_up", line(0, 90, 337) + line(1, 100, 337) + line(2, 110, 337), false},
		{"late_second_copy_ack", good + line(2, 130, 337), false},
		{"missing_batch", line(0, 20, 337) + line(2, 100, 337), false},
		{"wrong_member_count", line(0, 20, 336) + line(1, 60, 337) + line(2, 100, 337), false},
		{"old_logs_do_not_fill_missing", line(0, -10, 337) + line(1, 60, 337) + line(2, 100, 337), false},
		{"early_completion", line(0, 20, 337) + line(1, 10, 337) + line(2, 100, 337), false},
		{"truncated_log", good + "{", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := assessCadence(m, start, 1.5, strings.NewReader(tt.log))
			if (err == nil) != tt.pass || r.Passed != tt.pass {
				t.Fatalf("pass=%v report=%+v error=%v", tt.pass, r, err)
			}
		})
	}
	// Rotation must not lose completion records or depend on file order.
	r, err := assessCadence(m, start, 1.5, strings.NewReader(line(2, 100, 337)),
		strings.NewReader(line(1, 60, 337)+line(0, 20, 337)))
	if err != nil || !r.Passed || r.Completed != 3 {
		t.Fatalf("rotated logs: %+v %v", r, err)
	}
}
