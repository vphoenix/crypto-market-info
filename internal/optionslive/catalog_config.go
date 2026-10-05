package optionslive

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/vphoenix/crypto-market-info/internal/options"
)

func (c Config) catalogDefaults() Config {
	if c.MaxBooks == 0 {
		c.MaxBooks = 4096
	}
	if c.MaxConnections == 0 {
		c.MaxConnections = 20
	}
	if c.ChannelsPerConnection == 0 {
		c.ChannelsPerConnection = 256
	}
	if c.MaxBookLevels == 0 {
		c.MaxBookLevels = 20000
	}
	if c.MaxTotalLevels == 0 {
		c.MaxTotalLevels = 2_000_000
	}
	if c.MaxIngressBytes == 0 {
		c.MaxIngressBytes = 64 << 20
	}
	if c.EvidenceDir == "" {
		c.EvidenceDir = "var/options-evidence"
	}
	return c
}
func (c Config) validateCatalog() error {
	c = c.catalogDefaults()
	if c.MaxBooks < 1 || c.MaxBooks > 16384 || c.MaxConnections < 3 || c.MaxConnections > 28 || c.ChannelsPerConnection < 1 || c.ChannelsPerConnection > 512 || c.MaxBookLevels < 10 || c.MaxBookLevels > 20000 || c.MaxTotalLevels < int64(c.MaxBookLevels) || c.MaxIngressBytes < 1<<20 || c.EvidenceDir == "" {
		return fmt.Errorf("invalid options catalog capacity")
	}
	return nil
}

// The lock is held through writer drain. All R5 instances targeting this store
// on this host must use the same identity, irrespective of collection policy.
func acquireCatalogLock(identity string) (func(), error) {
	path := filepath.Join(os.TempDir(), "crypto-options-"+options.PayloadHash([]byte(identity))+".lock")
	f, e := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	if e = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		f.Close()
		return nil, fmt.Errorf("options catalog writer already running: %w", e)
	}
	return func() { syscall.Flock(int(f.Fd()), syscall.LOCK_UN); f.Close() }, nil
}
