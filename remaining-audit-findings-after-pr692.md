# Remaining audit findings after PR #692

This note keeps the bug summaries that are **not closed** by
[PR #692](https://github.com/zakura-core/zakura/pull/692)
(`fix(header-chain): close remaining V12 findings`, head
`56402acd552e2f15fad9a7e5a25b9838db668b2f`).

Severity markers are copied from the source reports. Explanations are condensed
from those reports, then checked against the PR #692 tree.

## Sources

| Source | Artifact | Audited revision | Finding IDs |
| --- | --- | --- | --- |
| Daybreak critical audit of PR #586 and PR #668 | [`daybreak-pr586-pr668-critical-audit.md`](daybreak-pr586-pr668-critical-audit.md) | PR #586 head `139c4d1c3269421f616275986e760f23f4560925` | `DB-CRIT-001`, `DB-CRIT-002` |
| AI risk-prioritized header-chain security audit | [`audit-report.pdf`](audit-report.pdf) | `b47572d8f91e542cd1b4af05199fd7b75f69504d` | Issues A–G |
| Closure filter | [PR #692](https://github.com/zakura-core/zakura/pull/692) | head `56402acd552e2f15fad9a7e5a25b9838db668b2f` | V12 `F-225509`–`F-225522` remainder, plus follow-up recovery/policy repairs |

PR #692 does not change native block-sync, the block-sync driver, header-sync
stream negotiation, or the serialized writer’s deferred-stall loop. Those paths
are where the remaining findings live.

## What PR #692 did close

Omitted from the remaining list because the PR implements them:

- V12 remainder: `F-225509`, `F-225510`, `F-225511`, `F-225512`, `F-225514`,
  `F-225516`, `F-225520`, `F-225521`, `F-225522`.
- Follow-up repairs named in the PR: durable network-policy digest, source-engine
  binding, verified-projection recovery, due-deferral settlement at startup, V1
  Mainnet-only migration, and historical current-frontier / headers-only witness
  authentication.

Issue G below is the residual of the PDF finding after that historical-witness
work. The independent integrated `FullState` receipt gap is still open.

## Remaining findings

### DB-CRIT-001 — Four version races discard deterministic invalid-body evidence and terminate native sync

- **Source:** Daybreak critical audit (`daybreak-pr586-pr668-critical-audit.md`)
- **Severity:** Critical
- **Class:** Critical operational failure
- **Confidence:** High
- **Production reachability:** Testnet
- **Status after PR #692:** Not addressed. `stale_refresh_exhausted` still
  terminates the sole native body-sync driver. `TransitionEvent::body_owner()`
  still returns `None` for `ConsensusBodyInvalid`, so that evidence still takes
  the global compare-and-swap path.

**Explanation.** A Testnet peer can supply a body that matches the selected
header’s commitments and then fails a body/state consensus rule. The sequencer
emits `RecordBodyInvalid` with only the then-current global `state_version`.
`from_full_verifier` seals that evidence with no body-work owner. Header
insertions and invalid-body evidence share the same serialized non-finalized
writer. Authenticated header insertions ignore the global version and each
effective insertion advances it; invalid-body evidence requires exact equality.

If one valid header-target insertion is queued ahead of the initial evidence
request and each of the three retries, the writer returns `Stale` four times.
On the fourth stale receipt, `stale_refreshes >= 3` returns
`FailedClosed("stale_refresh_exhausted")` without committing the tombstone. The
driver is spawned once, is not restarted, and in `network.p2p_stack = "zakura"`
there is no legacy fallback. Restart does not recover the missing evidence.

This finding composes with PDF Issues A and D: a stranded queue claim or a
driver exit that leaves Native apply advertised makes the same stall durable.

**Fix direction from the report.** Make intrinsic `ConsensusBodyInvalid`
persistence immune to unrelated global-version churn. Rebind the exact
hash/evidence against the current durable snapshot and commit the invalidity
idempotently. A bounded retry that exits the sole driver is not a substitute.

---

### DB-CRIT-002 — The v8-only cutover permanently strands v7-only Zakura deployments

- **Source:** Daybreak critical audit (`daybreak-pr586-pr668-critical-audit.md`)
- **Severity:** Critical
- **Class:** Critical operational failure
- **Confidence:** High
- **Production reachability:** both Mainnet and Testnet
- **Status after PR #692:** Not addressed. Production header-sync still declares
  only capability bit 5 / stream version 8. The registry can select a common
  alternative, but the service supplies an array of length one.

**Explanation.** A rolling-upgrade canary with `network.p2p_stack = "zakura"`
advertises only v8. Current `main`/v7 peers advertise bit 4 and stream version
7. Capability intersection opens no kind-5 header-sync stream. Stream-6 block
sync cannot invent the missing historical header path; its needed-body query is
derived from the local selected-header projection. Zakura-only mode has no
legacy TCP peer set, so the stall watchdog stays in `WarnOnly` and never falls
back to ChainSync.

A fresh node, a node returning after downtime, or the first upgraded canary
cannot discover or validate the valid greatest-work suffix until a v8 peer
appears or an operator changes networking mode.

**Fix direction from the report.** Declare both v7 and v8 to the existing
registry for a rolling compatibility window, and select the highest common
version before decoding. Alternatively, keep a proven standard-header fallback
in every supported upgrade configuration until v8 peers are guaranteed. Never
route v7 bytes through the v8 decoder.

---

### Issue A — Same-target scope advance strands stale body failures

- **Source:** AI header-chain security audit (`audit-report.pdf`)
- **Severity:** Medium
- **Primary impact:** Process-lifetime body-sync stall; invalidity evidence loss
- **Status after PR #692:** Not addressed. A completion with no live semantic
  generation still removes the applying entry and finishes the submission
  without rolling the work-queue claim or the download floor. `WorkQueue::extend`
  still skips heights that are already pending or in-flight.

**Explanation.** A verified body commit advances `verified_generation` and
publishes the new snapshot before a later concurrent block-apply future returns.
When the selected header target is unchanged, block sync preserves already
downloaded bodies and their old-scope queue claims. Later non-verified
completions are judged against the new generation and classified stale.

The stale path drops the applying entry but does not rebind or retire the
`WorkQueue` claim, roll back the download floor, trigger scheduling, record
retry/unavailability, persist deterministic invalidity, or score the supplier.
The queue then refuses to reacquire that height.

A remote peer can produce the liveness impact with a payload mismatch. If the
commitment-matching body is consensus-invalid, the permanent tombstone is also
suppressed until the body is downloaded and rejected again. The issue does not
authorize an arbitrary verified frontier.

**Fix direction from the report.** Either atomically rebind preserved bodies and
queue claims to the new authority, or retire the old claim and roll the floor
back on stale non-verified completion. Authenticate deterministic invalidity
from the stored applying identity rather than discarding it solely because
verified progress advanced the generation.

---

### Issue B — Deferred resource stall hot-loops the serialized state writer

- **Source:** AI header-chain security audit (`audit-report.pdf`)
- **Severity:** Medium
- **Primary impact:** CPU, lock, and state-throughput denial of service
- **Status after PR #692:** Not addressed. `apply_deferred_reevaluation` still
  discards the typed `ApplyResult`. When a due deadline is already in the past,
  `receive_until_deferred_deadline` still reevaluates and `continue`s with no
  sleep.

**Explanation.** A due deferred header can become eligible when every retention
victim is protected by an active retained-path lease. The engine correctly
returns `ResourceStalled` and leaves the due row unchanged. Deferred
maintenance ignores that result. The scheduler reloads the same already-due
deadline and retries immediately.

An attacker who can keep many valid forks leased (peers renew 30-second path
leases with low-rate continuation reads) can hold the topology in that stall.
The loop consumes a CPU core and repeatedly does RocksDB reads, graph scans,
retention work, invariant checks, and lock acquisition on the serialized
writer. Queued messages are still polled between iterations, so absolute
starvation was not established, but state throughput and sync can be degraded.

**Fix direction from the report.** Handle every `ApplyResult`. On
`ResourceStalled`, wait for a lease/resource-change notification or use bounded
exponential backoff. Never immediately reread an unchanged due row.

---

### Issue C — Queued header apply outlives timeout and shutdown revocation

- **Source:** AI header-chain security audit (`audit-report.pdf`)
- **Severity:** Medium
- **Primary impact:** Late mutation, false peer penalties, duplicate work
- **Status after PR #692:** Not addressed. State still queues
  `ApplyHeaderChainInsert` on the non-finalized writer and returns the oneshot
  receiver. Dropping that future does not dequeue the message. The writer still
  does not consult reactor revocation.

**Explanation.** After a valid target passes the live registration gate, state
synchronously queues the insert. If the reactor’s generic request deadline
expires during local writer delay, the reactor removes ownership, releases
capacity, and penalizes the peer. Shutdown similarly retires reactor work. The
already queued message remains. If branch coordinates are still current, the
writer can commit and publish after timeout or shutdown.

Consensus and exact-branch checks still constrain the committed headers. The
damage is lifecycle: an honest supplier can be falsely penalized or
disconnected, completed-target coverage is not marked, the target can be
downloaded again, and shutdown/restart loses exactly-once terminal
interpretation.

**Fix direction from the report.** Once state accepts an apply, disarm the
peer/network timeout and retain the registration until state returns. Separate
peer responsiveness deadlines from local-health watchdogs. Drain accepted
applies during shutdown, or check a revocation epoch immediately before commit.

---

### Issue D — Native apply capability remains live after block-driver exit

- **Source:** AI header-chain security audit (`audit-report.pdf`)
- **Severity:** Medium
- **Primary impact:** Persistent verified-sync outage until restart
- **Status after PR #692:** Not addressed. `request_apply_shutdown` still only
  requests operation cancellation. The coordinator can remain in
  `ApplyPhase::Native` after the sole action receiver is dropped.

**Explanation.** The block action driver exits if it cannot durably persist
current deterministic body invalidity. That local fail-close is appropriate.
The shared coordinator, however, keeps advertising `ServingAndApplying` while
the consumer is gone. The driver task is retained but not supervised or
restarted.

The trigger is an adversarial invalid body plus a local persistence failure,
timeout, resource stall, unexpected response, or the repeated stale race in
`DB-CRIT-001`. A Zakura-only node can look reachable while verified body sync
is dead until process restart. Dual-stack fallback can temporarily progress,
but dropping its lease resumes Native without recreating the consumer.

**Fix direction from the report.** Make every driver exit revoke its capability:
move the coordinator to `Failed` and publish serving-only demand, terminate the
process, or install a replacement consumer before Native can be advertised.
Supervise the task and refuse `ResumeNative` without a live driver for the new
epoch.

---

### Issue E — Fallible precommit rollback leaves uncommitted auxiliary state usable

- **Source:** AI header-chain security audit (`audit-report.pdf`)
- **Severity:** Medium
- **Primary impact:** In-memory/durable split authority under storage faults
- **Status after PR #692:** Not addressed.
  `restore_transition_engine_after_staging_error` still logs and returns the
  reload error without replacing or poisoning the staged engine.

**Explanation.** The combined VCT auxiliary-authentication/checkpoint path
installs the first transition into the shared engine before the combined
RocksDB batch commits. If a later operation fails, compensation reloads the
engine from durable rows. That reload can also fail. Locks are then released
normally. Readers and later planners can use auxiliary state that was neither
committed nor published.

A peer alone cannot trigger this. Exploitation needs a local storage-write
fault plus a reconstruction/read fault. The runtime then has split authority:
later plans can derive from an auxiliary fact or version absent from durable
state, causing repeated coherence failures, stuck writers, or fail-closed
restart. No immediate wrong-fork publication was shown.

**Fix direction from the report.** Plan the composite operation on an isolated
engine or overlay and install only after the one durable batch succeeds. If
shared staging remains, keep an infallible before-image. A failed rollback must
poison/retire the runtime and terminate the writer before any reader can
acquire the staged engine.

---

### Issue F — Circular recovery authority accepts coherently forged body invalidity

- **Source:** AI header-chain security audit (`audit-report.pdf`)
- **Severity:** Medium
- **Primary impact:** Persistent valid-branch suppression after corruption
- **Status after PR #692:** Not addressed.
  `FullStateBodyValidationEvidenceAuthorityDisk::attests_to_body_validation_state`
  is still equality against fields copied from the header node that recovery
  later authenticates. The production encoder still mints that row from the
  claimant.

**Explanation.** Recovery treats `HEADER_BODY_EVIDENCE_AUTHORITY` as independent
full-state authority for durable `Verified` and `ConsensusInvalid` states. The
only production encoder constructs that row by copying the public fields from
the header node. Attestation is only equality of hash, evidence, and rule.
Those rows share provenance, so equality does not prove an independent verifier
or full-state conclusion.

A privileged corrupt writer, faulty migration/restore, or correlated durable
corruption can insert a consensus-invalid tombstone and a matching copied
authority row for a valid public hash. Recovery accepts them. A no-node
tombstone can survive pruning and later mark the valid block invalid when it is
inserted. The node can permanently suppress a valid present or future branch
across restart. This is not remotely reachable through normal protocol
requests.

**Fix direction from the report.** Persist verifier-owned body conclusions in an
independent full-state namespace that the header change-set encoder cannot
mint. Bind each receipt to network, hash, rule, evidence, verifier
domain/version, transition identity, and canonical/full-state location.
Recovery should enumerate those receipts and reject claimant-only or
receipt-only conclusions.

---

### Issue G — Startup accepts unauthenticated finality history (residual)

- **Source:** AI header-chain security audit (`audit-report.pdf`)
- **Severity:** Low
- **Primary impact:** Forensic and migration provenance loss
- **Status after PR #692:** Partially addressed. The PR authenticates each
  historical current frontier through predecessor context or the canonical
  index, and authenticates headers-only depth witnesses through the canonical
  index or a retained-row walk. The integrated independent-receipt gap remains.

**Remaining explanation.** Recovery still accepts integrated records from the
`FinalitySource::FullState { .. }` enum variant without consulting an
independent finalization receipt:

```rust
(EngineMode::Integrated, None, FinalitySource::FullState { .. }) => true,
```

A privileged writer or correlated corruption can still change an integrated
evidence ID while preserving record continuity, epochs, and the genuine
terminal frontier. The current finalized tip remains protected by a separate
authenticated canonical-height lookup, so this residual alone does not move
active finality to an arbitrary hash. Operators can still lose trustworthy
forensic and migration provenance for irreversible integrated finality
decisions.

The original headers-only numeric-depth-without-ancestry half of this finding
is the part PR #692 closed.

**Fix direction from the report, still open.** Persist integrated finalization
receipts independently and bind every epoch, previous/current frontier, mode,
network, and evidence identity. Migration must reject history whose original
proof cannot be authenticated.

## Intentionally omitted

- PDF Appendix B, “Unmatched body adoption reuses another request’s owner”:
  validated and dismissed by that audit; not a finding.
- PDF future-work clock/deferral hypotheses: explicitly not validated findings.
- V12 IDs listed under “What PR #692 did close”.

## Related but out of this filter

[PR #707](https://github.com/zakura-core/zakura/pull/707) stacks on #692 for
additional durable-recovery hardening (V1 authority-row migration, headers-only
finalized frontiers, process-local auxiliary outcomes). It is not treated as
closing any remaining finding above unless a later review shows that it does.
