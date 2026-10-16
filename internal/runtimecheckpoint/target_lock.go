package runtimecheckpoint

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// LockTarget supplements legacy path locks. The actual SQL server/database and
// producer, rather than a CLI database alias or evidence directory, own one
// runtime writer. Generation fencing is still verified independently in SQL.
func LockTarget(binding Binding) (*os.File, error) {
	if err := binding.validate(); err != nil {
		return nil, err
	}
	dir := filepath.Join(os.TempDir(), fmt.Sprintf("crypto-market-info-runtime-locks-%d", os.Getuid()))
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return nil, err
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.IsDir() || info.Mode().Perm()&0077 != 0 || int(st.Uid) != os.Getuid() {
		return nil, errors.New("runtime target lock directory ownership")
	}
	digest := sha256.Sum256([]byte(binding.ServerUUID + "\x00" + binding.Database + "\x00" + binding.Producer))
	path := filepath.Join(dir, hex.EncodeToString(digest[:])+".lock")
	fd, err := syscall.Open(path, syscall.O_CREAT|syscall.O_RDWR|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0600)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path)
	if err = syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("runtime target already owned: %w", err)
	}
	return f, nil
}
