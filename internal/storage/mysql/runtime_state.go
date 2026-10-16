package mysql

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

// RuntimeBinding is a bounded startup/maintenance verification. It neither
// activates a producer nor reads domain history, and uses the existing pool.
func (c *Client) RuntimeBinding(ctx context.Context, scope, epochFile string, stateVersion uint32) (runtimecheckpoint.Binding, error) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	var server string
	if err := c.db.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&server); err != nil {
		return runtimecheckpoint.Binding{}, err
	}
	var generation uint64
	var holder []byte
	if err := c.db.QueryRowContext(ctx, "SELECT generation,holder FROM cmi_storage.producer_generation WHERE service=?", c.producer.Service).Scan(&generation, &holder); err != nil {
		return runtimecheckpoint.Binding{}, err
	}
	if generation != c.producer.Generation || !bytes.Equal(holder, c.producer.Holder[:]) {
		return runtimecheckpoint.Binding{}, contracts.ErrFenced
	}
	family := c.family
	if family == "" {
		family = c.database
	}
	catalog, err := Mappings()
	if err != nil {
		return runtimecheckpoint.Binding{}, err
	}
	var tables []string
	digests := map[string][32]byte{}
	for _, t := range catalog.Tables {
		if t.Database == family {
			tables = append(tables, t.Name)
			digests[t.Name] = sha256.Sum256([]byte(t.DDL))
		}
	}
	if len(tables) == 0 {
		return runtimecheckpoint.Binding{}, fmt.Errorf("runtime schema family not found")
	}
	if err = c.checkSchema(ctx, tables); err != nil {
		return runtimecheckpoint.Binding{}, err
	}
	sort.Strings(tables)
	h := sha256.New()
	for _, name := range tables {
		h.Write([]byte(name))
		h.Write([]byte{0})
		v := digests[name]
		h.Write(v[:])
	}
	epoch, err := runtimecheckpoint.ReadEpoch(epochFile, server+"/"+c.database)
	if err != nil {
		return runtimecheckpoint.Binding{}, err
	}
	return runtimecheckpoint.Binding{Service: c.producer.Service, Database: c.database, ServerUUID: server, ScopeHash: scope, SchemaHash: hex.EncodeToString(h.Sum(nil)), Producer: c.producer.Service, Holder: c.producer.Holder.String(), RecoveryEpoch: epoch.ID, ProducerGeneration: generation, StateVersion: stateVersion}, nil
}
