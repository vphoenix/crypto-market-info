# Task 1 — typed rental-state storage

Implemented in `codex/keeper-opportunity-probes`, without commits or production changes.

## Delivered

- Added `RentalStateObservation` (38 typed columns) and `RentalStateBatchProof` (capture identity, row count, canonical digest), appended two fields to `Batch`. `Capture` is unchanged.
- `Seal` creates an independent proof for every rental-state batch, including empty batches. `Validate` verifies counts/digests, unique observation indices, kind isolation, UInt256 bounds, source identity, request/response provenance, and before/after heads for successful states. Old member digests remain unchanged.
- An already performed conditional probe survives a failed trailing head only when the state retains a fully parsed true tuple, successful API/TVM result, exact verified implementation, and hashes. Implementation freshness is measured at response time, so a later rate-limit/storage delay does not erase trusted observations. Positive reward still requires the separate per-observation validator.
- MySQL runtime mappings and explicit `install-keeper-rental-state` administration create two separate compressed tables. Frozen 125-table `mapping.json`, generated `schema.sql`, and old seven-table ClickHouse DDL are unchanged. Both backends offer a read-only `CheckKeeperRentalStateSchema`; old seven-table readers remain supported. Partial/unregistered installations fail rather than silently discarding facts.
- MySQL and ClickHouse read/write both member and proof tables, including expected-empty members when installed. Capture is published last. Retrying an existing Capture separately compares its extension proof, detecting new-member mutation despite unchanged historical Capture columns.
- Export v3 adds `rental_states.csv` and `rental_state_batches.csv`, authenticates proofs, preserves `unknown` values and decimal integer strings, and exports the binary proof digest as hexadecimal.
- Updated `docs/market-data-storage.md` and the documented ClickHouse DDL. Root owns the command's `init-rental-state-schema` dispatch and opportunity-mode startup check.

## TDD evidence

Observed failing tests before connecting implementation: missing rental-state proof was accepted; extension mappings were absent. Further targeted regressions caught an unencoded binary digest in CSV, a forged implementation hash passing the conditional-state gate, and a 20-minute trailing-head delay invalidating an otherwise trusted tuple. Each was corrected and rerun successfully.

Final targeted command (2026-10-24, exit 0):

```bash
GOCACHE=/tmp/cmi-keeper-go-cache go test \
  ./internal/justlendkeeper ./internal/storage/mysql ./internal/storage/clickhouse \
  -run '^(TestRentalState.*|TestKeeperRentalState.*|TestMySQLKeeperRentalState.*|TestGenuineOldFrozenGobAndBootstrapBaseline|TestLegacyFrozenAndUnfrozenPrefixMigration|TestFactDigestDifferentProcesses|TestCanonicalUnknownZeroAndDecimalEquality|TestCaptureDigestFrozenRetryAndStateCorruption|TestDataExportExactFieldsNoProfitAndRefusesOverwrite)$' \
  -count=1
```

All three packages passed. Coverage includes empty proof, unknown/null semantics, missing/extra/duplicate/tampered members, frozen retry, max UInt256, NULL/false/true SQL codecs, old genuine frozen gob, old prefix migration, cross-process canonical digest, and old export compatibility. `git diff --check` passed. No diff exists for the frozen mapping, generated MySQL schema, or old seven-table ClickHouse schema.

`TestMySQLKeeperRentalStateLegacySchemaAndFrozenRetry` is implemented but **skipped**, since `MYSQL_INTEGRATION` was not enabled and the documented isolated socket `/home/ubuntu/crypto-market-info/var/storage/write-backlog-improvement/bench-mysql/run/mysql.sock` does not exist. No database was started, no production database was used, and no actual MySQL/ClickHouse DDL or roundtrip success is claimed. The optional MySQL test covers missing-schema rejection, idempotent installation, unknown/UInt256 roundtrip, frozen retry mutation, empty proofs, unexpected members on legacy captures, and missing-proof rejection. Full-suite/race/vet verification is reserved for root's final integrated run to avoid duplicate concurrent work.

## Independent checkpoint review

Reviewed `runtime_owner.go`, `checkpoint.go`, and `scheduler_checkpoint_test.go`. New struct fields are appended, preserving typed-codec field-position compatibility. Scheduler objects are written before the checkpoint and included in its retained references; candidate references are reused when unchanged. Legacy v3 typed encoding preserves known false/zero while old v2 remains readable. Native Version 2 requires a scheduler reference, and unknown scheduler versions fail closed.

Reported the edge case where new rental-state pending work without a scheduler could otherwise be encoded as native Version 1. Root added rejection of that write, Version 1 snapshots containing a scheduler/new pending work, and scheduler-version validation. No additional blocking issue found in this narrow review. Root's fresh full-change reviewer remains responsible for integrated behavior and final validation.
