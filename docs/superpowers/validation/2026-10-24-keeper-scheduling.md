# Task 3 scheduler implementation report

Scope: `internal/justlendkeeper/opportunity.go`, `opportunity_test.go`, and `watch.go`. No production service, database, runtime state or installed binary was changed. No commit was created. Parent owns shared collector/client/checkpoint/CLI integration.

## Implemented behavior

- Retain the fixed research cohort. Invalid members produce no empty probe captures. The research lane follows the pair's latest verified origin, including partial returns, without replacing the audit member.
- Persist a compact generation ledger and stable fair cursor rather than a second active queue. There is one in-flight operation owner. Research and opportunity admissions share a 1:3 phase; seed verification consumes a slot. Empty lanes can borrow slots. Every fourth opportunity admission is fair even while verified liquidatable candidates remain prioritized.
- An opportunity generation needs exact source verification before rental-state inspection. Seed retries are limited to three per parser revision and origin, with a 30-minute delay. Existing same-origin cohort retry exhaustion is inherited. Ledger records survive pool rotation and restart; only generations absent from the current pool/cohort and in-flight operations are pruned.
- Validate current generation immediately before relevant RPCs through the parent hook. Verified events apply monotonically by block/transaction/log position; old rent replay cannot reopen a later closed generation. A new origin permits admission again. Same-block earlier liquidation does not close a later rent. Revert alone never closes a candidate.
- `Liquidate` joins Rent/Return in the opportunity freshness gate. New-mode `AddSeed`, `RetrySeed`, `RefillCohort`, and bootstrap cohort selection cannot bypass shared admission.
- Admission records `InFlightCapture`; only matching completion releases it and starts the 30-second pause. Disabling new opportunity admission still cleans up an existing restored scheduler owner.
- Rental-state requests use `rental_state_before`, `rental_state`, `rental_state_after`; every request is marked `Research`. Parent adds the conditional liquidation request and enforces the persisted actual-request gate.

## Integration hooks

- `scheduleResearch(now)`: new-mode watch admission.
- `observeResearchEvents(events)`: consume committed, receipt-verified events before/with the existing domain event projection. It does not mutate cold candidates.
- `researchGenerationCurrent(operation)`: stale-operation guard.
- `completeResearch(operation)`: call once after successful capture commit and before removing the operation. Includes seed verification and trusted rental-state priority feedback.
- `addRentalState(candidate, lane, scheduled)`: construct the bounded current-state request chain.
- `SchedulerState` is a separately persisted object. `InFlightCapture` was appended to preserve typed codec field indices. Parent handles load-time version rejection and serialization of legacy pending chains.

## Validation

Observed behavioral red failures before fixes:

- Stalled liquidation feed was ignored.
- Replayed old rent reopened a later closed generation.
- A verified partial return excluded an otherwise valid research member.
- Direct legacy seed/refill entry points bypassed shared admission.
- Admission did not persist chain ownership.
- Disabling admission stranded a restored owner.

The initial new-interface test run also failed to compile for the deliberately absent scheduler types/methods. All fourteen `TestOpportunity*` cases now pass, covering the above plus 300-candidate fairness, priority starvation protection, retry persistence, no cold-candidate mutation, new-origin recovery, 1:3 admission accounting, one in-flight chain and the 30-second pause.

Latest command passed:

```sh
GOCACHE=/tmp/cmi-keeper-go-cache go test ./internal/justlendkeeper -run '^Test(Opportunity|RentalStateControlsConditionalProbe|RestoredResearchChainsDrainSerially)' -count=1
```

`git diff --check` passed. An earlier complete keeper-package run took 44 seconds and reported one concurrent Task 2 failure: `TestMalformedRewardEvidencePreservesKnownAmounts`, `reward_validation_test.go:163`, `invalid_unsigned_integer`. It had no scheduler failures. Parent was notified and owns the final full-suite/race/integration run; this report does not claim the earlier full suite passed.

## Pure selector measurement

Same worktree/container, Intel i7-12700, 1,500 synthetic unverified candidates, ten iterations, `BenchmarkOpportunitySelectPool1500`, no RPC or checkpoint I/O:

| Implementation | Time/op | Allocated bytes/op | Allocations/op |
| --- | ---: | ---: | ---: |
| Initial repeated lookup/decode | 41,683,848 ns | 6,355,772 | 112,128 |
| One pair index, decode only eligible work | 4,393,841 ns | 2,704,869 | 25,440 |

This measures CPU/allocation cost only, not production throughput or disk-write reduction. Scheduler selection leaves `State.Candidates` unchanged; actual checkpoint reference reuse and fixed-request I/O measurements are parent integration responsibilities.
