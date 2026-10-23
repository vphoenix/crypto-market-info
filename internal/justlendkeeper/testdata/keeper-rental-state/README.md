# Parser fixture provenance

`rent-info.raw.json` is an unchanged public TronGrid constant-call response captured on 2026-10-02. Its request, UTC capture times and SHA-256 are in `rent-info.meta.json`; the regression test verifies that SHA-256. It decodes to `(0, 1054569691767344913, false)` and is **not** a successful liquidation.

`abi-audit.json` records the current published getter interface and three bounded public read-only queries on 2026-10-24 (UTC+8), with response hashes. Both current node ABIs are empty. The published subset includes `getRentInfo` but omits the `liquidate` function; its signature and reward return are documented separately. The implementation bytecode matches the pinned fingerprint. Limited dispatcher inspection establishes a nonzero native CALLVALUE rejection and one-word normal return encoding; it is not complete TVM replay.

All positive responses built in `reward_validation_test.go` are explicitly synthetic parser tests. They exercise individual observation certification and rejection of contradictory/missing evidence. They are not captured positive constant responses, and successful on-chain transaction receipts have not been relabeled as constant-call fixtures.

The current-state tuple uses integer sun and a uint256 rent index, with an exact ABI bool. Before/after node heads bound an unpinned latest-state observation; they do not turn it into historical replay or a guaranteed executable opportunity.
