package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

type cadenceReport struct {
	StartedAt, Deadline     time.Time
	Expected, Completed     int
	MaxCompletionLagSeconds float64
	MaxAllowedLagSeconds    float64
	Late, Missing           int
	Passed                  bool
}

// A successful batch log is emitted only after durable proof and ACK. The
// latest success for an identity includes a subsequently discovered copy.
// Draining after the measurement window remains useful correctness evidence,
// but cannot count as sustained capacity within that window.
func assessCadence(m manifest, start time.Time, speed float64, logs ...io.Reader) (cadenceReport, error) {
	r := cadenceReport{StartedAt: start, Expected: len(m.Entries)}
	if speed <= 0 || !m.To.After(m.From) || len(m.Entries) == 0 {
		return r, fmt.Errorf("invalid capacity measurement window")
	}
	r.Deadline = start.Add(time.Duration(float64(m.To.Sub(m.From)) / speed))
	maxLag := time.Duration(float64(2*time.Minute) / speed)
	r.MaxAllowedLagSeconds = maxLag.Seconds()
	expected := make(map[string]entry, len(m.Entries))
	for _, v := range m.Entries {
		if v.Header.Minute.Before(m.From) || !v.Header.Minute.Before(m.To) {
			return r, fmt.Errorf("capacity entry outside manifest window")
		}
		key := v.Header.Identity()
		if _, exists := expected[key]; exists {
			return r, fmt.Errorf("duplicate capacity manifest identity")
		}
		expected[key] = v
	}
	completed := make(map[string]time.Time, len(expected))
	for _, log := range logs {
		scanner := bufio.NewScanner(log)
		scanner.Buffer(make([]byte, 4096), 1<<20)
		for scanner.Scan() {
			var row struct {
				Time     time.Time `json:"time"`
				Message  string    `json:"msg"`
				Database string    `json:"database"`
				Kind     string    `json:"kind"`
				ID       string    `json:"batch_id"`
				Members  uint32    `json:"members"`
				Bytes    int64     `json:"bytes"`
			}
			if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
				return r, fmt.Errorf("invalid capacity diagnostic log: %w", err)
			}
			if row.Message != "batch committed" || row.Time.Before(start) {
				continue
			}
			key := (contracts.Delivery{Database: row.Database, Kind: row.Kind, ID: row.ID}).Identity()
			v, exists := expected[key]
			if !exists || row.Members != v.Header.Members || row.Bytes != v.Bytes {
				return r, fmt.Errorf("capacity completion identity or member/byte count mismatch")
			}
			if row.Time.After(completed[key]) {
				completed[key] = row.Time
			}
		}
		if err := scanner.Err(); err != nil {
			return r, err
		}
	}
	for key, v := range expected {
		at, exists := completed[key]
		if !exists {
			r.Missing++
			continue
		}
		r.Completed++
		due := start.Add(time.Duration(float64(v.Header.Minute.Sub(m.From)) / speed))
		lag := at.Sub(due)
		r.MaxCompletionLagSeconds = max(r.MaxCompletionLagSeconds, lag.Seconds())
		if at.Before(due) || at.After(r.Deadline) || lag > maxLag {
			r.Late++
		}
	}
	r.Passed = r.Missing == 0 && r.Late == 0
	if !r.Passed {
		return r, fmt.Errorf("sustained capacity not demonstrated: %d missing, %d outside completion cadence", r.Missing, r.Late)
	}
	return r, nil
}

func checkCapacityCadence(m manifest, start time.Time, speed float64) error {
	var logs []io.Reader
	for i, path := range []string{base + "/bench-writer.log.1", base + "/bench-writer.log"} {
		f, err := os.Open(path)
		if os.IsNotExist(err) && i == 0 {
			continue
		}
		if err != nil {
			return err
		}
		defer f.Close()
		logs = append(logs, f)
	}
	r, err := assessCadence(m, start, speed, logs...)
	if saveErr := save(base+"/bench-capacity-cadence.json", r); saveErr != nil {
		return saveErr
	}
	return err
}
