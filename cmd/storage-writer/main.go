package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/storage/mysql"
	"github.com/vphoenix/crypto-market-info/internal/storage/redisbuffer"
	"github.com/vphoenix/crypto-market-info/internal/storage/writer"
)

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run() error {
	socket := flag.String("socket", "", "MySQL private Unix socket")
	user := flag.String("user", "", "live writer account")
	password := flag.String("password-file", "", "private password file")
	redisSocket := flag.String("redis-socket", "", "Redis private Unix socket")
	prefix := flag.String("redis-prefix", "cmi:v1", "isolated namespace")
	dir := flag.String("spool", "", "bounded durable spool directory")
	databases := flag.String("databases", "crypto_market_info", "explicit destinations")
	once := flag.Bool("once", false, "consume at most one batch and exit")
	workers := flag.Int("workers", 1, "bounded database commit workers, 1 or 2")
	preparers := flag.Int("preparers", 2, "bounded preparation workers, 1 or 2")
	prepareBytes := flag.Int64("prepare-bytes", 64<<20, "decoded preparation byte budget")
	cacheBytes := flag.Int64("cache-bytes", 32<<20, "immutable metadata byte budget")
	sourceV2 := flag.Bool("source-storage-v2", false, "write administratively installed append-layout source registry tables")
	qualityV2 := flag.Bool("quality-storage-v2", false, "write administratively installed minute quality/profile tables")
	logFile := flag.String("log-file", "", "optional private bounded JSON log (64 MiB plus one rotated file)")
	archiveDir := flag.String("archive-dir", "", "optional bounded .batch fixture capture directory")
	archiveFrom := flag.String("archive-from", "", "inclusive fixture UTC minute RFC3339")
	archiveTo := flag.String("archive-to", "", "exclusive fixture UTC minute RFC3339")
	flag.Parse()
	var from, to time.Time
	if *archiveDir != "" {
		var e error
		from, e = time.Parse(time.RFC3339, *archiveFrom)
		if e != nil {
			return e
		}
		to, e = time.Parse(time.RFC3339, *archiveTo)
		if e != nil {
			return e
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var output io.Writer = os.Stderr
	if *logFile != "" {
		file, e := writer.OpenDiagnosticLog(*logFile, 64<<20)
		if e != nil {
			return e
		}
		defer file.Close()
		output = file
	}
	logger := slog.New(slog.NewJSONHandler(output, nil))
	err := writer.Run(ctx, writer.Config{MySQL: mysql.Config{Socket: *socket, Username: *user, PasswordFile: *password, MaxConnections: 2, QualityStorageV2: *qualityV2, SourceStorageV2: *sourceV2}, Redis: redisbuffer.Config{Socket: *redisSocket, Prefix: *prefix, WaitTimeout: 2 * time.Second}, Spool: *dir, Databases: strings.Split(*databases, ","), Workers: *workers, Preparers: *preparers, PrepareBytes: *prepareBytes, CacheBytes: *cacheBytes, Once: *once, ArchiveDir: *archiveDir, ArchiveFrom: from, ArchiveTo: to}, logger)
	// Only cancellation of the signal context is an intentional shutdown.
	// A cancelled internal request must remain a failure so systemd can retry.
	if ctx.Err() != nil && errors.Is(err, context.Canceled) {
		return nil
	}
	if err != nil {
		logger.Error("writer stopped", "error", err)
	}
	return err
}
