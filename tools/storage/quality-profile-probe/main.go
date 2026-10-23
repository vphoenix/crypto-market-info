// Read-only prototype for typed per-minute quality profiles. Production facts
// are never changed. Reconstruct and byte-compare every physical column; report
// logical bytes only, not predicted InnoDB/page/doublewrite savings.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/storage/mysql"
)

const root = "/home/ubuntu/crypto-market-info"

type result struct {
	Table                                                                    string
	Minute                                                                   time.Time
	Rows, Profiles, OriginalProfileBytes, SharedProfileBytes, ReferenceBytes int
	ExactReconstruction                                                      bool
	QueryMS                                                                  int64
}

func run() error {
	at := flag.String("minute", "", "UTC minute, RFC3339")
	flag.Parse()
	minute, e := time.Parse(time.RFC3339, *at)
	if e != nil || !minute.Equal(minute.Truncate(time.Minute)) {
		return fmt.Errorf("explicit minute required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, e := mysql.Connect(ctx, mysql.Config{Socket: root + "/var/storage/mysql/run/mysql.sock", Username: "root", PasswordFile: "/home/ubuntu/.config/crypto-market-info-storage/mysql-root-password", MaxConnections: 1, ReadOnly: true, SelectTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second})
	if e != nil {
		return e
	}
	defer db.Close()
	var id string
	if e = db.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&id); e != nil {
		return e
	}
	if id != "b820454a-c654-11f1-80fa-a04da03d4fd6" {
		return fmt.Errorf("wrong server")
	}
	for _, name := range []string{"derivative_book_quality_minute", "options_catalog_quality_evidence_minute"} {
		t, e := mysql.Mapping("crypto_market_info", name)
		if e != nil {
			return e
		}
		fields := []string{}
		for _, c := range t.Columns {
			if strings.HasSuffix(c.Name, "_offsets") || strings.HasSuffix(c.Name, "_values") {
				fields = append(fields, "`"+c.Name+"`")
			}
		}
		if name == "derivative_book_quality_minute" {
			// Only shared connection/capture/market provenance, never per-instrument
			// source time, sequence, price, size, validity or rule identity.
			fields = nil
			for _, n := range []string{"captured_times", "connection_confirmed_times", "connection_epochs", "market_state_times", "rule_published_times"} {
				for _, s := range []string{"_offsets", "_values"} {
					fields = append(fields, "`compact_"+n+s+"`")
				}
			}
		}
		start := time.Now()
		rows, e := db.QueryContext(ctx, "SELECT "+strings.Join(fields, ",")+" FROM crypto_market_info.`"+name+"` FORCE INDEX(batch_lookup) WHERE minute_time=? LIMIT 4001", minute)
		if e != nil {
			return e
		}
		r := result{Table: name, Minute: minute, ExactReconstruction: true}
		profiles := map[[32]byte][]byte{}
		for rows.Next() {
			vals := make([][]byte, len(fields))
			ptr := make([]any, len(fields))
			for i := range ptr {
				ptr[i] = &vals[i]
			}
			if e = rows.Scan(ptr...); e != nil {
				rows.Close()
				return e
			}
			var packed bytes.Buffer
			for _, v := range vals {
				binary.Write(&packed, binary.BigEndian, uint32(len(v)))
				packed.Write(v)
				r.OriginalProfileBytes += len(v)
			}
			raw := packed.Bytes()
			hash := sha256.Sum256(raw)
			if old, ok := profiles[hash]; ok {
				if !bytes.Equal(old, raw) {
					return fmt.Errorf("profile digest collision")
				}
			} else {
				profiles[hash] = bytes.Clone(raw)
				r.SharedProfileBytes += len(raw) + 32
			}
			reader := bytes.NewReader(profiles[hash])
			for _, v := range vals {
				var n uint32
				if e = binary.Read(reader, binary.BigEndian, &n); e != nil {
					return e
				}
				back := make([]byte, n)
				if _, e = reader.Read(back); e != nil {
					return e
				}
				if !bytes.Equal(v, back) {
					return fmt.Errorf("profile field changed")
				}
			}
			if reader.Len() != 0 {
				return fmt.Errorf("profile trailing bytes")
			}
			r.Rows++
			r.ReferenceBytes += 32
		}
		if e = rows.Err(); e != nil {
			return e
		}
		rows.Close()
		r.QueryMS = time.Since(start).Milliseconds()
		r.Profiles = len(profiles)
		if r.Rows > 4000 {
			return fmt.Errorf("sample quota exceeded")
		}
		if e = json.NewEncoder(os.Stdout).Encode(r); e != nil {
			return e
		}
	}
	return nil
}
func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
