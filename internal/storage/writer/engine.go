package writer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
	"github.com/vphoenix/crypto-market-info/internal/storage/mysql"
	"github.com/vphoenix/crypto-market-info/internal/storage/redisbuffer"
	"github.com/vphoenix/crypto-market-info/internal/storage/spool"
)

type Config struct {
	MySQL                    mysql.Config
	Redis                    redisbuffer.Config
	Spool                    string
	Databases                []string
	Workers, Preparers       int
	PrepareBytes, CacheBytes int64
	ArchiveDir               string
	ArchiveFrom, ArchiveTo   time.Time
	Once                     bool
}
type job struct {
	task         task
	ctx          context.Context
	cancel       context.CancelFunc
	owner        string
	payloadBytes int64
	d            contracts.Delivery
	client       *mysql.Client
	prepared     *mysql.PreparedDelivery
	owned        bool
	reserved     int64
	buffer       *redisbuffer.Buffer
	started      time.Time
	prepare      time.Duration
	streams      []string
}
type result struct {
	job       *job
	err       error
	trace     mysql.CommitTrace
	ack       time.Duration
	elapsed   time.Duration
	committed bool
	work      time.Duration
	queue     time.Duration
}

func Run(ctx context.Context, cfg Config, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.Workers < 1 || cfg.Workers > 2 || cfg.Preparers < 1 || cfg.Preparers > 2 || cfg.PrepareBytes < 8<<20 || cfg.CacheBytes < 1<<20 {
		return fmt.Errorf("invalid bounded writer settings")
	}
	// Only the main family currently has sealed minute deliveries. Other
	// families keep their typed direct writers and one-connection recovery pools.
	if len(cfg.Databases) != 1 || (cfg.Databases[0] != "crypto_market_info" && cfg.MySQL.Family != "crypto_market_info") {
		return fmt.Errorf("minute writer requires the main destination; low frequency remains direct")
	}
	disk, e := spool.Open(cfg.Spool, 20<<30)
	if e != nil {
		return e
	}
	lock, e := disk.ConsumerLock()
	if e != nil {
		return e
	}
	defer lock.Close()
	if e = startupPhase(ctx, logger, "spool_recovery", disk.Recover); e != nil {
		return e
	}
	var index *spool.Index
	if e = startupPhase(ctx, logger, "spool_index", func(c context.Context) error { var err error; index, err = disk.NewIndex(c, 32<<20); return err }); e != nil {
		return e
	}
	defer index.Close()
	cache := mysql.NewMetadataCache(cfg.CacheBytes)
	writeCfg := cfg.MySQL
	writeCfg.Database = cfg.Databases[0]
	writeCfg.MaxConnections = 2
	writeCfg.SchemaTables = mysql.MinuteSchemaTables()
	writeCfg.SchemaInvalidated = cache.Clear
	var write *mysql.Client
	if e = startupPhase(ctx, logger, "mysql_write_pool", func(c context.Context) error { var err error; write, err = mysql.Open(c, writeCfg); return err }); e != nil {
		return e
	}
	defer write.Close()
	readCfg := writeCfg
	// Preparation reads keep their own two-connection pool.
	readCfg.MaxConnections = 2
	readCfg.ReadOnly = true
	readCfg.PrefetchMinuteIndexes = true
	readCfg.SelectTimeout = 15 * time.Second
	var read *mysql.Client
	if e = startupPhase(ctx, logger, "mysql_read_pool", func(c context.Context) error { var err error; read, err = mysql.Open(c, readCfg); return err }); e != nil {
		return e
	}
	defer read.Close()
	read.SetMetadataCache(cache)
	write.SetMetadataCache(cache)
	checkSchema := func(c context.Context) error {
		if err := read.InitMinuteSchema(c); err != nil {
			return err
		}
		return write.InitMinuteSchema(c)
	}
	if e = startupPhase(ctx, logger, "schema_check", checkSchema); e != nil {
		return e
	}
	var buffer *redisbuffer.Buffer
	e = startupPhase(ctx, logger, "redis_open", func(c context.Context) error { var err error; buffer, err = redisbuffer.Open(c, cfg.Redis); return err })
	if e != nil {
		logger.Warn("Redis unavailable; spool remains durable", "error", e)
		buffer = nil
	}
	if buffer != nil {
		defer buffer.Close()
		if e = startupPhase(ctx, logger, "redis_ready_repair", func(c context.Context) error { return buffer.RepairReady(c, cfg.Databases[0]) }); e != nil {
			return e
		}
	}
	// Pools and buffer are immutable for the life of this run. A Redis outage
	// is retried by the driver on subsequent bounded operations; no worker
	// swaps or closes a shared client under another worker.
	prepareJobs := make(chan *job, cfg.Preparers)
	commitJobs := make(chan *job, cfg.Workers)
	results := make(chan result, cfg.Workers+cfg.Preparers+4)
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	budget := newByteBudget(cfg.PrepareBytes)
	var archive *FrameArchive
	if cfg.ArchiveDir != "" {
		archive, e = OpenFrameArchive(cfg.ArchiveDir, cfg.ArchiveFrom, cfg.ArchiveTo)
		if e != nil {
			return e
		}
	}

	var workers sync.WaitGroup
	var blockedMu sync.RWMutex
	blockedStreams := map[string]bool{}
	release := func(j *job) {
		j.cancel()
		if j.prepared != nil {
			_ = j.prepared.Close()
		}
		if j.reserved > 0 {
			budget.Release(j.reserved)
			j.reserved = 0
		}
	}
	startRenewal := func(j *job) {
		if !j.owned {
			return
		}
		header := j.d
		workers.Add(1)
		go func() {
			defer workers.Done()
			tick := time.NewTicker(15 * time.Second)
			defer tick.Stop()
			for {
				select {
				case <-j.ctx.Done():
					return
				case <-tick.C:
					ok, e := j.buffer.Renew(j.ctx, header, j.owner, time.Minute)
					if e != nil || !ok {
						logger.Warn("delivery lease renewal lost", "batch_id", header.ID, "error", e)
						j.cancel()
						return
					}
				}
			}
		}()
	}
	for range cfg.Preparers {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				select {
				case <-runCtx.Done():
					return
				case j := <-prepareJobs:
					func() {
						prepareStarted := time.Now()
						finish := func(e error) { results <- result{job: j, err: e, elapsed: time.Since(j.started)} }
						candidate := j.task.candidate
						if j.buffer != nil {
							var err error
							j.owned, err = j.buffer.Claim(j.ctx, candidate, j.owner, time.Minute)
							if err != nil && (errors.Is(err, contracts.ErrConflict) || !j.task.disk) {
								finish(err)
								return
							}
							if !j.owned && j.task.redis && !j.task.disk {
								state, _ := j.buffer.State(j.ctx, candidate.Header)
								if state != "committed" && state != "" {
									finish(errLeased)
									return
								}
								ok, e := read.Committed(j.ctx, candidate.Header)
								if e != nil || !ok {
									if e == nil {
										e = errLeased
									}
									finish(e)
									return
								}
								j.d = candidate.Header
								select {
								case commitJobs <- j:
								case <-runCtx.Done():
									finish(runCtx.Err())
								}
								return
							}
						}
						j.d = candidate.Header
						startRenewal(j)
						// An earlier attempt may have committed and removed one
						// copy before its other ACK failed. Durable proof must
						// be checked before trying to load a possibly gone file.
						already, proofErr := read.Committed(j.ctx, j.d)
						if proofErr != nil {
							finish(proofErr)
							return
						}
						if already {
							j.payloadBytes = candidate.Bytes
							j.prepare = time.Since(prepareStarted)
							select {
							case commitJobs <- j:
							case <-runCtx.Done():
								finish(runCtx.Err())
							}
							return
						}
						var err error
						if j.task.disk {
							j.d, err = disk.Load(j.ctx, candidate)
						} else if j.buffer != nil {
							j.d, err = j.buffer.Load(j.ctx, candidate.Header.Database, candidate.Header.ID)
						} else {
							err = fmt.Errorf("Redis unavailable")
						}
						if err != nil {
							finish(err)
							return
						}
						if !j.d.SameHeader(candidate.Header) {
							finish(contracts.ErrConflict)
							return
						}
						if archive != nil {
							if captureErr := archive.Capture(j.ctx, j.d); captureErr != nil {
								logger.Error("fixture capture failed; production delivery continues", "batch_id", j.d.ID, "error", captureErr)
							}
						}
						j.payloadBytes = int64(len(j.d.Payload))
						ok, err := read.Committed(j.ctx, j.d)
						if err != nil {
							finish(err)
							return
						}
						if !ok {
							p, err := read.CurrentProducer(j.ctx, j.d.Service)
							if err != nil {
								finish(err)
								return
							}
							if p.Generation != j.d.Generation {
								finish(contracts.ErrFenced)
								return
							}
							j.client = write.WithProducer(p)
							preparer := read.WithProducer(p)
							j.prepared, err = preparer.PrepareDeliveryBounded(j.ctx, j.d, cfg.PrepareBytes)
							if err != nil {
								finish(err)
								return
							}
							j.streams = j.prepared.Streams()
							blockedMu.RLock()
							blocked := false
							for _, stream := range j.streams {
								blocked = blocked || blockedStreams[stream]
							}
							blockedMu.RUnlock()
							if blocked {
								finish(contracts.ErrConflict)
								return
							}
							j.d.Payload = nil
							actual := j.prepared.MemoryBytes()
							if actual > cfg.PrepareBytes {
								finish(fmt.Errorf("decoded delivery exceeds preparation budget: %w", contracts.ErrCapacity))
								return
							}
							if actual > j.reserved {
								if !budget.TryAcquire(actual - j.reserved) {
									finish(errBudget)
									return
								}
								j.reserved = actual
							}
						}
						j.prepare = time.Since(prepareStarted)
						select {
						case commitJobs <- j:
						case <-runCtx.Done():
							finish(runCtx.Err())
						}
					}()
				}
			}
		}()
	}
	for range cfg.Workers {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				select {
				case <-runCtx.Done():
					return
				case j := <-commitJobs:
					r := result{job: j}
					commitStarted := time.Now()
					if j.prepared != nil {
						r.err = j.client.CommitPrepared(mysql.WithCommitTrace(j.ctx, &r.trace), j.prepared)
					}
					if r.err == nil {
						ok, e := read.Committed(j.ctx, j.d)
						r.err = e
						if e == nil && !ok {
							r.err = fmt.Errorf("commit lacks durable proof")
						}
						r.committed = ok && e == nil
					}
					if r.err == nil {
						began := time.Now()
						// Always check the disk copy: a producer may have fallen back to
						// spool after our metadata snapshot or a Redis durability timeout.
						if e := disk.Ack(j.ctx, j.d, read.Committed); e != nil {
							r.err = e
						}
						if j.owned && j.buffer != nil {
							if e := j.buffer.AckOwned(j.ctx, j.d, j.owner, read.Committed); e != nil && r.err == nil {
								r.err = e
							}
						}
						r.ack = time.Since(began)
					}
					r.elapsed = time.Since(j.started)
					r.work = j.prepare + time.Since(commitStarted)
					r.queue = max(time.Duration(0), r.elapsed-r.work)
					results <- r
				}
			}
		}()
	}
	defer func() {
		stop()
		workers.Wait()
		for {
			select {
			case r := <-results:
				release(r.job)
			case j := <-prepareJobs:
				release(j)
			case j := <-commitJobs:
				release(j)
			default:
				return
			}
		}
	}()
	indexErrors := make(chan error, 1)
	workers.Add(1)
	go func() {
		defer workers.Done()
		timer := time.NewTicker(time.Second)
		defer timer.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-timer.C:
				if e := index.Refresh(runCtx); e != nil {
					select {
					case indexErrors <- e:
					case <-runCtx.Done():
					}
					return
				}
			}
		}
	}()
	s := newScheduler()
	var offset int64
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	metrics := time.NewTicker(10 * time.Second)
	defer metrics.Stop()
	var lastRefresh, lastSchema, lastCleanup, lastRedisAttempt, lastRedisComplete time.Time
	lastSchema = time.Now()
	var cleanupRunning bool
	cleanupDone := make(chan error, 1)
	commits, members, bytes := uint64(0), uint64(0), uint64(0)
	var prepareTime, lockTime, sqlTime, commitTime, ackTime time.Duration
	var unknown sync.Map
	inflight := 0
	observed := uint64(0)
	merge := func(c contracts.Candidate, isRedis bool) error {
		if c.Header.Database != cfg.Databases[0] {
			return fmt.Errorf("spool destination outside writer allowlist")
		}
		key := c.Header.Identity()
		t := s.tasks[key]
		if t == nil {
			if len(s.tasks) >= 40000 {
				return fmt.Errorf("writer metadata budget exhausted: %w", contracts.ErrCapacity)
			}
			t = &task{candidate: c, seen: time.Now()}
			s.tasks[key] = t
			observed++
		} else if !t.candidate.Header.SameHeader(c.Header) {
			t.blocked = true
			logger.Error("conflicting queue copies retained", "database", c.Header.Database, "kind", c.Header.Kind, "batch_id", c.Header.ID)
			return nil
		}
		if c.Bytes > t.candidate.Bytes {
			t.candidate.Bytes = c.Bytes
		}
		if isRedis {
			t.redis = true
		} else {
			t.disk = true
		}
		return nil
	}
	for {
		select {
		case e := <-indexErrors:
			return e
		case <-ctx.Done():
			return ctx.Err()
		case r := <-results:
			j := r.job
			t := s.tasks[j.task.candidate.Header.Identity()]
			if t == nil {
				release(j)
				return fmt.Errorf("inflight task disappeared")
			}
			work := r.work
			if work == 0 {
				work = r.elapsed
			}
			s.finish(t, work)
			inflight--
			prepareTime += j.prepare
			lockTime += r.trace.Lock
			sqlTime += r.trace.SQL
			commitTime += r.trace.Commit
			ackTime += r.ack
			if r.err == nil {
				commits++
				members += uint64(j.d.Members)
				bytes += uint64(j.payloadBytes)
				index.Remove(j.d)
				delete(s.tasks, j.d.Identity())
				logger.Info("batch committed", "database", j.d.Database, "kind", j.d.Kind, "batch_id", j.d.ID, "members", j.d.Members, "bytes", j.payloadBytes, "already_committed", j.prepared == nil, "old_backlog", j.task.group == 1, "lag_seconds", time.Since(j.d.Minute.Add(time.Minute)).Seconds(), "queue_ms", r.queue.Milliseconds(), "prepare_ms", j.prepare.Milliseconds(), "lock_ms", r.trace.Lock.Milliseconds(), "sql_ms", r.trace.SQL.Milliseconds(), "commit_ms", r.trace.Commit.Milliseconds(), "ack_ms", r.ack.Milliseconds(), "connection_id", r.trace.ConnectionID, "bulk", r.trace.Bulk)
			} else {
				t.failures++
				delay := min(time.Second<<min(t.failures-1, 6), time.Minute)
				if errors.Is(r.err, errLeased) {
					delay = time.Second
				}
				t.retryAt = time.Now().Add(delay)
				if errors.Is(r.err, contracts.ErrConflict) || errors.Is(r.err, contracts.ErrFenced) {
					t.blocked = true
					blockedMu.Lock()
					for _, stream := range j.streams {
						blockedStreams[stream] = true
					}
					blockedMu.Unlock()
				}
				if j.owned && j.buffer != nil {
					releaseCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
					_, _ = j.buffer.Release(releaseCtx, j.d, j.owner, t.retryAt)
					cancel()
				}
				if r.trace.ConnectionID != 0 && !r.trace.RollbackConfirmed && !r.trace.Committed {
					unknown.Store(t.candidate.Header.Identity(), r.trace.ConnectionID)
				}
				logger.Error("sealed batch retained", "kind", t.candidate.Header.Kind, "batch_id", t.candidate.Header.ID, "error", r.err, "retry_at", t.retryAt, "connection_id", r.trace.ConnectionID, "rollback_confirmed", r.trace.RollbackConfirmed, "durable_commit_verified", r.committed)
			}
			release(j)
			if cfg.Once {
				return r.err
			}
		case e := <-cleanupDone:
			cleanupRunning = false
			if e != nil {
				logger.Warn("retained cache cleanup deferred", "error", e)
			}
		case <-metrics.C:
			var old, recent, blockedCount, redisCount, diskCount int
			var uniqueBytes, diskBytes, redisBytes int64
			var oldest time.Time
			for _, t := range s.tasks {
				if groupOf(t, time.Now()) == 1 {
					old++
				} else {
					recent++
				}
				if t.blocked {
					blockedCount++
				}
				if t.redis {
					redisCount++
					redisBytes += t.candidate.Bytes
				}
				if t.disk {
					diskCount++
					diskBytes += t.candidate.Bytes
				}
				uniqueBytes += t.candidate.Bytes
				if oldest.IsZero() || t.candidate.Header.Minute.Before(oldest) {
					oldest = t.candidate.Header.Minute
				}
			}
			if index.Complete() && diskBytes >= 10<<30 {
				severity := "warning"
				if diskBytes >= 15<<30 {
					severity = "critical"
				}
				logger.Error("spool quota alarm", "severity", severity, "known_payload_bytes", diskBytes, "quota_bytes", int64(20<<30), "history_import_allowed", false)
			}
			logger.Info("writer metrics", "pid", os.Getpid(), "workers", cfg.Workers, "observed_batches", observed, "committed_batches", commits, "committed_members", members, "committed_bytes", bytes, "spool_index_complete", index.Complete(), "redis_index_complete", buffer != nil && !lastRedisComplete.IsZero() && time.Since(lastRedisComplete) <= 45*time.Second, "unique_pending", len(s.tasks), "unique_pending_bytes", uniqueBytes, "recent_pending", recent, "old_pending", old, "redis_pending", redisCount, "spool_pending", diskCount, "spool_payload_bytes", diskBytes, "redis_payload_bytes", redisBytes, "prepare_reserved_bytes", budget.Used(), "metadata_cache", cache.Stats(), "blocked", blockedCount, "oldest_minute", oldest, "inflight", inflight, "recent_work_ms", s.worked[0]/float64(time.Millisecond), "old_work_ms", s.worked[1]/float64(time.Millisecond), "prepare_ms", prepareTime.Milliseconds(), "lock_ms", lockTime.Milliseconds(), "sql_ms", sqlTime.Milliseconds(), "commit_ms", commitTime.Milliseconds(), "ack_ms", ackTime.Milliseconds())
		case <-tick.C:
			if buffer == nil && cfg.Redis.Socket != "" && time.Since(lastRedisAttempt) >= 10*time.Second {
				lastRedisAttempt = time.Now()
				reconnect, cancel := context.WithTimeout(ctx, 5*time.Second)
				opened, err := redisbuffer.Open(reconnect, cfg.Redis)
				cancel()
				if err == nil {
					buffer = opened
					defer opened.Close()
					if err = startupPhase(ctx, logger, "redis_ready_reconnect", func(c context.Context) error { return opened.RepairReady(c, cfg.Databases[0]) }); err != nil {
						return err
					}
				} else {
					logger.Warn("Redis reconnect deferred", "error", err)
				}
			}
			if time.Since(lastRefresh) >= time.Second {
				lastRefresh = time.Now()
				for _, c := range index.Snapshot() {
					if e = merge(c, false); e != nil {
						return e
					}
				}
				if buffer != nil {
					pageCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
					cs, next, err := buffer.CandidatePage(pageCtx, cfg.Databases[0], offset, 256)
					cancel()
					if err != nil {
						logger.Warn("Redis candidate refresh deferred", "error", err)
					} else {
						for _, c := range cs {
							if e = merge(c, true); e != nil {
								return e
							}
						}
						offset = next
						if next == 0 {
							lastRedisComplete = time.Now()
							offset = 0
						}
					}
				}
			}
			if time.Since(lastSchema) >= 5*time.Minute {
				schemaStarted := time.Now()
				schemaCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
				e = checkSchema(schemaCtx)
				cancel()
				if e != nil {
					cache.Clear()
					return e
				}
				lastSchema = time.Now()
				logger.Info("writer schema verified", "elapsed_ms", time.Since(schemaStarted).Milliseconds(), "tables", len(mysql.MinuteSchemaTables()), "pools", 2)
			}
			if buffer != nil && !cleanupRunning && time.Since(lastCleanup) >= 10*time.Second {
				lastCleanup = time.Now()
				cleanupRunning = true
				workers.Add(1)
				go func() {
					defer workers.Done()
					c, cancel := context.WithTimeout(runCtx, 5*time.Second)
					defer cancel()
					e := buffer.Cleanup(c, cfg.Databases[0], time.Now().UTC(), 100)
					if e == nil {
						e = buffer.CleanupPreview(c, cfg.Databases[0], time.Now().UTC(), read.PreviewCommitted)
					}
					if e == nil {
						e = buffer.CleanupAbandonedPreview(c, cfg.Databases[0], time.Now().UTC(), func(c context.Context, s string) (uint64, error) {
							p, e := read.CurrentProducer(c, s)
							return p.Generation, e
						})
					}
					cleanupDone <- e
				}()
			}
			if inflight >= cfg.Preparers+cfg.Workers || (cfg.Once && inflight > 0) {
				continue
			}
			t := s.pick(time.Now())
			if t == nil {
				if cfg.Once && inflight == 0 && index.Complete() {
					return nil
				}
				continue
			}
			if id, ok := unknown.Load(t.candidate.Header.Identity()); ok {
				probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
				gone, err := read.AttemptGone(probeCtx, id.(uint64))
				cancel()
				if err != nil || !gone {
					s.finish(t, 0)
					t.retryAt = time.Now().Add(5 * time.Second)
					logger.Warn("previous server attempt still unconfirmed", "connection_id", id, "batch_id", t.candidate.Header.ID, "error", err)
					continue
				}
				unknown.Delete(t.candidate.Header.Identity())
			}
			reserve := max(int64(2<<20), t.candidate.Bytes*8+int64(t.candidate.Header.Members)*(128<<10))
			reserve = min(reserve, cfg.PrepareBytes)
			if !budget.TryAcquire(reserve) {
				s.finish(t, 0)
				continue
			}
			attempt, cancel := context.WithTimeout(runCtx, 40*time.Second)
			j := &job{task: *t, ctx: attempt, cancel: cancel, owner: uuid.NewString(), reserved: reserve, buffer: buffer, started: time.Now(), d: t.candidate.Header}
			select {
			case prepareJobs <- j:
				inflight++
			default:
				release(j)
				s.finish(t, 0)
			}
		}
	}
}

var errLeased = errors.New("delivery leased by another consumer")
var errBudget = errors.New("preparation byte budget busy")
