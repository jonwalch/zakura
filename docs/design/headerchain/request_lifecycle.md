# The Request Lifecycle in the Fork-Aware Header Chain

Parent: `docs/specs/fork-aware-header-chain-engine.md` (rules cited as `LC-*`).

## Why the path is split into layers

Four independent sources want the tip to move. Peers deliver headers, full state
verifies bodies, an operator invalidates a branch, and finality advances. If each source
computes the move, the node runs as many fork-choice implementations as it has sources,
and they disagree. Timing breaks it even when they agree: a source that computed its
answer against an old view lands late and overwrites a newer one. That is the production
incident this design replaced.

The answer is one rule. A source states what it observed, and one planner turns that
evidence into every consequence.

The rule holds only if no source can reach past its own layer. A layer that can reach the
database, publish a frontier, or supply the history its own claim is judged against can
restate the rule in its own terms, and then there are two planners again. So a type
carries each boundary rather than a convention: the value that crosses is shaped so the
mistake the boundary exists to prevent cannot be expressed.

This document follows one delivery of headers from the socket to the planner and back.
Each layer below names four things: what crosses, what the sender cannot do, what the
receiver guarantees, and the abstractions that hold the line.

## The round trip

```mermaid
sequenceDiagram
  participant N as peer
  participant P as reactor · zakura-network
  participant W as state writer · zakura-state
  participant R as runtime · writer lock
  participant E as engine · zakura-header-chain
  participant D as disk
  N->>P: Headers · stream kind 5
  P->>P: Gate::check — does this still own its branch?
  P->>W: prepare_header_target — off the writer lock
  W-->>P: PreparedHeaderBatch, sealed under AdapterKey
  P->>W: apply_header_target
  W->>R: HeaderChainRuntime::apply(request, context)
  R->>R: seal the context · leases, finality path, retention
  R->>E: HeaderChainEngine::apply — pure, mutates nothing
  E-->>R: TransitionPlan
  R->>D: one atomic DiskWriteBatch
  R->>E: apply_committed
  R-->>W: ApplyResult
  R->>P: Publisher — the committed snapshot
  P->>N: GetHeaders for the next target
```

The loop does not close where it started. The reactor does not learn what happened from
the value its own call returned. It learns from the publisher, so every scheduling
decision it makes stands on a frontier that is already on disk.

Two methods are spelled `apply` and they promise opposite things.
`HeaderChainEngine::apply` takes `&self`, mutates nothing, and returns a plan the caller
may drop. `HeaderChainRuntime::apply` writes a batch, installs the plan, and publishes.
This document writes both in full wherever the distinction matters.

## Division of responsibility

Each layer decides one thing. The reactor owns peers, sockets, and timers, and decides
nothing about the chain. The state writer decides when a transition runs and what else
lands in the same write. The runtime decides which history the evidence is judged against.
The engine decides which chain is best.

What each layer cannot do defines it just as much. The reactor cannot reach the database,
run a selection, or publish a frontier. The state writer cannot decide what a transition
means. The runtime cannot invent a fact the store does not hold. The engine cannot perform
an effect at all, so it cannot commit the answer it just derived, and something else must
carry that answer to disk.

The build enforces two of those prohibitions.

| Abstraction | Location | Unrepresentable mistake |
| --- | --- | --- |
| `Port` trait | `zakura-node-services/src/header_chain.rs` | a call from `zakura-network` into `zakura-state`. The reactor sits above the state crate, so a direct call would invert the stack; the trait lives below both and exposes seven named operations, so the database is not in the reactor's vocabulary |
| `architecture_dependencies_stay_sync_only_and_layered` | `zakura-header-chain/src/lib.rs` | a manifest naming `tokio`, `tower`, `zakura-state`, `zakura-network`, or `zakura-consensus`. Purity is a property of the dependency graph before it is a property of the code, so the test fails the build rather than a review (LC-SCOPE-06) |
| `UnavailablePort`, `InertHeaderChainPort` | `zakura-node-services/src/header_chain.rs` | a reactor that behaves differently when the port is missing. The absent case is a value, not a branch at every call site |

## Layer 1: peer to reactor

**What crosses.** A `Headers` message on stream kind 5, stream version 8, behind
capability bit five, under a 2 MiB cap. Four discriminants exist and they are closed:
`Status`, `GetHeaders`, `Headers`, and `HeadersOutcome`. New wire data needs an advertised
auxiliary schema bit or a successor stream version (LC-WIRE-15). A peer that cannot speak
version 8 never negotiates the stream rather than half-speaking it.

**What the sender cannot do.** Nothing a peer says becomes a fact here. The framing height
a peer attaches to a header is never evidence, because height comes from a checked parent
increment further down. A request id is nonzero by wire construction (LC-WIRE-01), so zero
decodes as a failure rather than matching every outstanding request. The codec bounds every
collection before allocating.

**What the receiver guarantees.** The reactor gives up authority and keeps its clock. It
still decides when to ask, whom to ask, and how much to ask for, because those are timing
questions and it holds the only timer. Everything else moved below it: whether a header is
valid, which chain is best, whether a late response still counts.

**The abstractions.** The reactor owns no chain state, and the shape of its own bookkeeping
keeps that true. Work that used to sit in its fields now sits in five modules under
`network/src/zakura/header_sync/scheduler/`, each keyed by generation and branch, so one
retirement pass retires exactly the work a reset invalidated and leaves the rest alive.

| Abstraction | Location | Unrepresentable mistake |
| --- | --- | --- |
| `BranchId` | `zakura-header-chain/src/ids.rs` | naming a branch by height. It is `(anchor_hash, target_tip_hash)` and carries no height field, so a reset to a different chain of equal height cannot pass for the same branch |
| `Gate` | `zakura-header-chain/src/ownership.rs` | a response acting before anyone asked whether it still owns its branch. It is the sole decision point over `PendingOwners` |
| `PendingOwners` | `zakura-header-chain/src/ownership.rs` | an owner surviving its peer's reconnect. The key is `(SourceId, request_id)` and the owner carries the session id, so a reply on a new session for an old request is `OwnerMismatch` |
| `SourceId` | `zakura-header-chain/src/ids.rs` | a key that drifts with the reactor's own peer bookkeeping. It is an opaque digest of peer identity and connection domain |
| `scheduler/peer_work.rs` | `network/src/zakura/header_sync/scheduler/peer_work.rs` | one target starving the others. The chunk budget divides the same 4,000 headers across receiving, preparation, and application, so the total in flight cannot exceed one transition's worth |
| `CompletedHeaderTargets` | `network/src/zakura/header_sync/scheduler/completed_targets.rs` | a completed target aliasing across a generation or a branch. The key is `(generation, branch)`, and the file is 147 lines with nothing else in it |

### When a late response is stale

`Gate::check` compares the durable coordinates first (the branch anchor,
`header_generation`, and `verified_generation` for body-authorized work), then consults the
registry, and returns `Current` or one of five typed `StaleReason` values.

It deliberately ignores `state_version`. That counter advances on every committed
transition that can affect a frontier, including transitions on branches a given request
never touches, so binding header work to it would cancel in-flight requests faster than
they complete for no correctness gain.

A stale decision produces no frontier, coverage, retry, repair, scheduling, publication,
body-task, or peer-score effect (LC-GEN-04, LC-ACCEPT-03). Stale is not misbehaviour, and
the reactor does not charge a peer that answered honestly a moment too late. The gate here
only avoids wasted work; the planner repeats the same comparison against the
pre-transition snapshot with `validate_header_sync_owner`, and that check is the authority.

## Layer 2: reactor to state writer

**What crosses.** `prepare_header_target` outbound, a sealed `PreparedHeaderTarget`
inbound, and `apply_header_target` outbound again. Both are port operations, implemented by
the driver in `zakurad`, the only file where header-sync policy and the state service know
each other's names.

Preparation runs off the writer lock, on a blocking thread. It reads a validation lease for
the common ancestor through the read service, derives the rules in force at that height
from that lease, and runs `prepare_context_free_headers`. Those are exactly the rules that
read no ancestor: canonical version and hash, checked height inference, commitment
structure for that height, compact-target domain and network limit, hash at or below
target, and Equihash.

**What the sender cannot do.** The reactor cannot forge or substitute a prepared batch
between the two calls. A header whose time runs more than two hours ahead of the local
clock comes back `DeferredUntil` rather than rejected (LC-VAL-08), so our own clock skew
never becomes a peer fault.

**What the receiver guarantees.** Preparation off the lock is an optimization that cannot
become a loophole. The planner trusts nothing it concluded once the lock is held: it
rechecks the receipt against the live config, and a batch prepared under a config that has
since moved fails with `StalePreparation` (LC-VAL-11).

| Abstraction | Location | Unrepresentable mistake |
| --- | --- | --- |
| `AdapterKey` | `zakura-node-services/src/header_chain.rs` | a prepared batch substituted between the two calls. The adapter seals the target on the way out and is the only holder that can open it on the way back, which is what makes two calls as safe as one |
| `PreparedHeaderBatch` and its receipt | `zakura-header-chain/src/transition/types/preparation.rs` | a result that does not say what it was prepared against. The receipt names the parent frontier, the network, and the trust-anchor digest, all three rechecked in the planner |
| rules derived from a lease | `zakura-header-chain/src/validation/` | preparation guessing which rules are in force. The height that selects them comes from a leased ancestor, not from the peer's framing |
| `DeferredUntil` | `zakura-header-chain/src/validation/` | a local clock skew recorded as a peer fault. It is the only non-passing sealed result |

## Layer 3: state writer to runtime

**What crosses.** `HeaderChainRuntime::apply(request, context)`, or one of two wider
shapes. Every production call is in one file,
`crates/zakura-state/src/service/write.rs`, and nothing outside `zakura-state` holds a
runtime, so serialization holds by construction.

Three kinds of call reach that file. The reactor arrives through `ApplyHeaderChainInsert`
on the write task's channel. The state writer itself submits block commits, body outcomes,
auxiliary results, verified-chain changes, finality, and operator actions. The write loop's
timer submits `ReevaluateDeferred` when the deadline from `earliest_deferred()` elapses,
which is the one transition no component observed.

**What the sender cannot do.** The state writer decides when a transition runs and what
else lands in the same write. It does not decide what the transition means, and it cannot
publish: the reactor holds no runtime, and the runtime is the sole publisher (LC-GEN-05).

**What the receiver guarantees.** One committed transition makes one `db.write`
(LC-TXN-01). The runtime writes the store once per transition, inside the writer lock, and
nowhere else in the steady state, with no accumulation and no background flush. The call
shapes differ only in how much goes into that one write.

| Call shape | Contents of the single write |
| --- | --- |
| `apply` | the header-chain rows alone |
| `apply_combined` | those rows appended to the block batch the state writer already filled |
| `apply_aux_then_checkpoint_combined` | auxiliary authentication and the checkpoint advance that depends on it, planned into the same batch |

Combining exists because a block and the header node it depends on must not disagree across
a crash, and one batch is the only way to guarantee that (LC-INT-03). That range of widths
is also why the engine hands its write set back instead of performing it: it can neither
open the batch, which spans full-state column families in a crate it must not link, nor
close it, since the runtime may still add a second transition.

Two writes sit outside this path, both before the engine exists. Startup commits one repair
batch when the audit is not clean, and first run against a store that predates the DAG
commits the migration batch that builds the initial nodes and the migrated finality pin.

| Abstraction | Location | Unrepresentable mistake |
| --- | --- | --- |
| `DiskWriteBatch` | `zakura-state` | a transition split across two writes. The runtime builds one from the change set and calls `db.write` once |
| `HeaderChainRuntime` | `state/src/service/finalized_state/header_chain.rs` | a second writer or a second publisher. It holds the store, the engine mutex, the publisher, and the lease registry, and nothing outside the crate holds one |
| `ApplyHeaderChainInsert` | `state/src/service/write.rs` | the reactor committing anything. Its whole reach into the write path is one channel message |
| `ApplyResult` | `zakura-header-chain` | an ambiguous outcome. `Committed`, `NoChange`, `Stale`, and `ResourceStalled` are distinct, so `FullStateResourceStalled` is a case the writer must handle rather than a success it can assume |
| `SyncCoordinator`, `ApplyPhase` | `zakurad/src/commands/start/zakura/coordinator.rs`, `node-services/src/sync_lifecycle.rs` | the native and legacy paths applying blocks at once. Phases have declared edges, and an apply permit is held for the operation's life |
| `MigratedPinRefutation` | `zakura-header-chain/src/transition/types/event/finality.rs` | rolling back an imported finality pin. A migrated pin cannot be un-migrated, so refuting one is an event that alarms and fails the node closed |

## Layer 4: runtime to engine

**What crosses.** The typed event, plus the transition context and the durable facts
(`HeaderValidationFacts`, `HeaderInsertionFacts`): what holds still across the run, and the
durable rows this one transition reads. The engine fetches nothing, so a gap in either is a
refusal, never a default the planner invents.

### Why the runtime supplies the context

A header is not valid or invalid by itself. Zcash derives expected difficulty from
`MedianTime(height) - MedianTime(height - PoWAveragingWindow)`, so checking one header
takes the 17 headers of the averaging window plus the 11 of the median span, which is
`POW_ADJUSTMENT_BLOCK_SPAN`, 28 blocks. That window also fixes the median-time-past a
header's own timestamp is bounded against, and the height decides which rules apply at all.
So one header is valid below one branch and invalid below another: validity is a claim
about a header and a stretch of history together.

Something must choose that history, and it must not be whoever supplied the header. A peer
that also chose the 28 predecessor facts would be choosing the difficulty its own header is
compared against, and a fabricated but well-formed window would pass. The context is
exactly what an untrusted source may not supply.

So the runtime chooses it. Under the lock that will commit the transition, it reads the
parent and the `POW_PREDECESSOR_CONTEXT_SPAN` predecessors below it from its own committed
store, and seals them with the network policy and the trust-anchor digest into one
validation lease.

Finality moves that history while requests are in flight, which is the second thing the
runtime supplies. Advancing `finalized` moves the anchor, so a batch authorized below the
old one no longer roots anywhere. `validate_finality_rebase_path` walks the durable
finality history back from the current anchor to the owner's original one, all or nothing,
and a second lease for the pre-transition anchor travels with it, so the planner re-roots
only when the move is proved (LC-FINAL-01). The third is retention: merged serving-lease
references keep a page a peer is reading from being evicted (LC-WIRE-05).

The division is the same for every event: a caller supplies what it observed, and the
runtime supplies what the observation is judged against.

**The abstractions.** This layer carries the whole capability model, and
`transition/authority.rs` is 55 lines of it. One question is required and the other three
default to false, so an implementation that forgets an override refuses evidence instead of
admitting it.

| Abstraction | Location | Unrepresentable mistake |
| --- | --- | --- |
| `ValidationLease` | `zakura-header-chain/src/transition/types/preparation.rs` | predecessor facts arriving loose. Parent, up to 28 facts, network policy, and trust-anchor digest are sealed under one `context_digest` |
| `StateIssuedAuthority` | `state/src/service/finalized_state/header_chain.rs` | a lease from anywhere but this call. It wraps the caller's authority over exactly the leases the runtime just issued |
| `is_coherent` | `zakura-header-chain/src/transition/types/preparation.rs` | a mutated lease that reached the planner passing anyway. It re-derives the digest and re-walks the backward hash links (LC-ANCHOR-03) |
| `FullStateEvidenceAuthority` | `zakura-header-chain/src/transition/authority.rs` | an implementation that admits evidence by omission. Every method except `authorizes_full_state` defaults to `false` |
| the transition context | `zakura-header-chain/src/transition/authority.rs` | a caller outside the state writer vouching for anything. It holds the authority as an `Option`, so absent authority has no representation but `TransitionFailure::Authority` |
| `Clock` | `zakura-header-chain/src/transition/authority.rs` | an event supplying its own time |
| `HeaderValidationFacts`, `HeaderInsertionFacts` | `zakura-header-chain/src/transition/engine.rs` | the engine reading a durable row it was not handed |

### Evidence with no bounded window

Some evidence answers to no fixed number of predecessors. Peer-supplied note-commitment
roots are the example. Nothing below the last checkpoint verifies the header commitment
field they feed, and authenticating one takes a running ZIP-221 history tree over the
committed body tip and every selected header above it. That is history with no bound, which
no lease can seal, so that evidence never travels through one.

The state writer owns a durable ascending root-authentication frontier instead, advances it
against its own history-tree frontier, and reports each verdict back as ordinary auxiliary
evidence. Demotion contains the exception, not a new privilege: the delivery is
unauthenticated until that frontier clears it, and it drives no validity and no fork choice
in the meantime (LC-AUX-02, LC-AUX-04).
`docs/design/header-sync-vct-root-authentication.md` carries that design in full.

### How much history each event needs

What the runtime must prove about the past sorts the twelve events into three groups.

- **Needs a sealed window** — `InsertHeaders` alone, the only event that admits material
  from an untrusted source.
- **Needs one sealed predecessor** — the three verified-chain events, each leased against a
  different frontier, plus a pin refutation, which takes a durable row instead and only when
  the store holds that pin as migrated.
- **Needs nothing** — the other eight, whose evidence is self-contained.

`FullStateFinalized` sits in the third group despite doing the most: it moves the anchor and
prunes every non-descendant while reading nothing, because the caller hands over the
verified ancestry with it.

## Layer 5: inside the engine

**What the engine cannot do.** Reach a disk, a socket, a task, or a clock of its own. Time
arrives through `Clock` and durable rows through the facts types, so the engine can observe
nothing it was not handed.

Purity here has its usual meaning: same inputs, same output, no effect the caller did not
ask for. The engine hides no state, because the caller passes it in. `HeaderChainEngine::apply`
takes `&self`, mutates nothing, and returns the next state as a plan to commit. Planning the
same event against the same state twice returns the same plan, and dropping a plan leaves
nothing behind: no write, no publication, no change in the engine. Fork choice is therefore
reproducible from a graph, a config, a clock, and an ordered event list, and testable
without a node.

**The abstractions.** Each type here exists to make one mistake unrepresentable.

| Abstraction | Location | Unrepresentable mistake |
| --- | --- | --- |
| `GraphOverlay` | `src/graph/overlay.rs` | planning that touches the live graph. Reads see the base plus what the overlay staged; writes land in the overlay's own maps |
| `GraphDelta` | `src/graph/overlay.rs` | a partial or hand-assembled difference. Its fields are crate-private, a node inserted then deleted appears in neither list, and a node written back unchanged appears in neither |
| `HeaderGraphView`, `HeaderGraphEdit` | `src/graph.rs` | selection behaving differently on staged state than on committed state. One implementation runs against both |
| `TransitionPlan` | `src/transition/planner.rs` | a batch reaching disk that the planner did not derive. `change_set()`, `before()`, `cause()`, and `is_no_change()` are the whole caller surface; the delta and invariant inputs stay private |
| `before()` and `StaleSource` | `src/transition/engine.rs` | installing a plan against state it was not derived from. `HeaderChainEngine::apply` takes `&self`, so two planners can race, and only one can install |
| `Frontier` | `src/frontier.rs` | naming a position on a chain by height alone. It is a height and a hash together, and it is the only way this design names a position |
| `WorkCoordinate` | `src/frontier.rs` | comparing accumulated work across origins. It carries the origin hash, so a mismatched pair raises an error instead of yielding a smaller number that decides fork choice with nothing logged |
| generation counters | `src/ids.rs` | a wrapped counter. `checked_next` fails closed at `u64::MAX` |
| `RetentionPlan` | `src/retention.rs` | a resource limit passing for a verdict about a chain. When protected paths alone fill the node bound it sets `admission_refused` and `resource_stalled` rather than evicting protected state or synthesizing finality for room (LC-RETAIN-01) |
| eligibility as durable reasons | `src/header_node.rs` | fork choice depending on arrival order. Ineligibility is a set of reasons rather than a flag, and a flag is last-writer-wins, which is arrival order in disguise |
| `Attribution` on every error | `src/error.rs` | a generic conversion charging a local disk failure to a peer |

### The four validation passes

Validation runs four times before disk, each pass against more context than the last.

1. **Off the writer lock**, `prepare_context_free_headers` runs the rules that read no
   ancestor and seals the result under a receipt.
2. **Under the lock, before planning**, the runtime reads the predecessor facts itself
   rather than accepting any the caller supplies, and seals them into leases.
3. **In the planner, against the graph**, the receipt's parent, network, and trust-anchor
   digest must still match the live config. The planner then re-derives, per header, the
   parent link, the hash, the height increment, and the work from the compact target, and
   runs the contextual difficulty and time check against ancestry it reads from the graph
   and the lease.
4. **After planning and before disk**, `verify_plan` re-derives the projected result:
   hashes and linkage, index round-trips, work coordinates, inherited eligibility, both
   projections' contiguity, that `header_best` really is the maximum eligible score, trust
   pins, protected nodes, frozen limits, generation increments, and auxiliary provenance.

No pass trusts the one before it. The fourth checks the plan against a graph rebuilt from
the delta with `GraphOverlay::from_delta`, not against the overlay the planner mutated, so
the verifier approves the transition the engine will install rather than the one it staged.
A disagreement between the planner and the verifier is `InvariantViolation`, one of thirteen
numbered violations in `transition/invariants.rs`, and that batch never reaches disk. A
fourteenth variant, `SourceSnapshot`, carries no number because it reports that the source
view moved under the verifier rather than that a projected invariant failed.

Startup adds a pass that answers to no caller: `audit_store` re-derives what the DAG
determines before the engine exists.

**What crosses back.** `TransitionPlan`: the change set the store will write, the private
graph delta the engine will install, and `before()`, the snapshot it was derived from.

## Layer 6: engine to disk, memory, and observers

**What crosses, and in what order.** Disk is what the next restart reads, so disk must lead
memory, and memory must lead observers. In that order a crash leaves disk ahead of memory,
which the startup audit repairs by rehydrating from the authoritative node rows. In any
other order it leaves an observer holding a frontier that is not on disk, which nothing
repairs.

The schema, not the code path, is what makes one order repairable. The authoritative rows
are the per-node rows in `header_node_by_hash_v1` and the singleton in
`header_engine_meta_v1`. Children, heights, eligibility roots, deferrals, and both
projections are caches the startup audit rebuilds from those node rows.

`apply_combined_inner` is that path, and every call shape reaches it. It refuses on a
`migrated_pin_refuted` alarm before any effect, and checks a combined caller's staged headers
against the projected DAG before the write. Three outcomes then leave early.

- A no-change plan commits the caller's batch alone and publishes nothing.
- A `ResourceStalled` plan commits its alarm-only change set with a fresh batch, so the
  state writer maps that outcome to `FullStateResourceStalled` and stops rather than treating
  its own rows as written (LC-RETAIN-01).
- A plan carrying `migrated_pin_refuted` commits and installs, then returns without
  publishing, so the node fails closed on the alarm it just made durable (LC-FINAL-04).

A failure between the write and the install fails closed: the runtime returns the store
error, publishes nothing, and the next open rehydrates the engine from disk.

| Abstraction | Location | Unrepresentable mistake |
| --- | --- | --- |
| `FaultPoint` | `state/src/service/finalized_state/header_chain.rs` | an untested crash window. Four ordered points let the recovery tests interrupt each step and check that reopening finds the complete before state or the complete after state (LC-ACCEPT-02, LC-RECOVER-02) |
| `audit_store`, `RecoveryPlan` | `src/transition/recovery/` | startup trusting a stored answer. It re-derives what the DAG determines, recomputes selection, and repairs only reconstructible categories |
| `RecoveryFailure` | `src/transition/recovery/contracts.rs` | opening on a store that is not one coherent chain. Startup fails closed with publication still disabled (LC-RECOVER-01) |
| `apply_committed` | `src/transition/engine.rs` | memory disagreeing with disk. It is the only mutator, and it refuses a plan whose `before()` no longer matches |

## Layer 7: runtime to peer

**What crosses.** A committed snapshot on a latest-value watch channel. `snapshot()` and
`subscribe()` are the entire published surface, and nothing else may publish (LC-GEN-05).

**Why the loop closes here and not on the return value.** The sole-publisher rule is the
direct fix for the incident this design replaced. A driver that published the raw result of
its own range commit could announce a frontier derived from work whose branch had already
been reset, so an obsolete completion undid that reset. Any component publishing a commit
result it did not obtain from the serialized writer reintroduces the defect. The reactor
therefore learns what committed from the publisher, which means every frontier it acts on is
one the writer committed, in the order the writer committed them.

**What the reactor must do before acting.** A committed transition returns `RetiredWork`:
the two generation-changed flags plus the exact owners retired for narrower causes. The
reactor applies retirement before scheduling any forward work for the new branch
(LC-WORK-01, LC-AUX-03). Retiring afterwards would either sweep away a just-scheduled request
or leave a dead one alive.

Then it schedules again from committed state, and only committed state. A projected value
escaping the read surface would put peers to work on a frontier that may never commit.

| Abstraction | Location | Unrepresentable mistake |
| --- | --- | --- |
| `Publisher` | `state/src/service/finalized_state/header_chain.rs` | a published frontier that is not on disk. It is a latest-value channel fed only from inside the writer lock, so a published snapshot is by construction one the writer committed |
| `reader()` | `state/src/service/finalized_state/header_chain.rs` | a compound read straddling a commit. It shares the store and the engine mutex, so its reads serialize against transitions |
| `RetiredWork` | `zakura-header-chain` | a reset that leaves dead work alive, or a retirement that sweeps away a just-scheduled request. It names the generation flags and the exact owners |
| `HeaderLocator` | `src/locator.rs` | a short locator that looks valid. It builds from the committed selected projection at offsets `0, 1, 2, 4, …, 512, 1000` plus `finalized`, capped at 13 hashes, and fails with `StoreError::Incoherent` on a gap |
| `RetainedPathLease` | `state/src/header_chain.rs` | evicting a page a peer is reading. It binds to `(peer, session_id, scope)`, acquisition re-checks the scope under the writer lock, and the reactor releases any lease whose scope stops matching |
| `HeaderSyncWorkOwner::rebase_header` | `src/ids.rs` | rebasing work that must not be rebased. It returns `None` for body-authorized repair, so the narrow finality rebase cannot reach it |
| cancelled-id window | `network/src/zakura/header_sync/pipe.rs` | an honest answer to a cancelled request scored as misbehaviour. Up to 64 ids for 30 seconds are dropped instead |

## Guarantees by layer

| Layer | Owns | May not | Guarantees | Enforced by |
| --- | --- | --- | --- | --- |
| peer | its own claims | — | nothing; every layer below re-derives every claim | the bounded codec |
| reactor · `zakura-network` | peers, sessions, timing, scheduling, serving | reach the database, run a selection, publish a frontier, hold a runtime | bounded decode, gated completions, retirement before new work | `Gate`, `PendingOwners`, `scheduler/peer_work.rs` |
| port · `zakura-node-services` | seven named operations | widen the reactor's reach | a prepared batch cannot be forged or substituted | `Port`, `AdapterKey` |
| state writer · `zakura-state` | when a transition runs, what else lands in the same write | decide what a transition means, publish | one transition makes one `db.write`; a block and its header node agree across a crash | `DiskWriteBatch`, `ApplyHeaderChainInsert`, `SyncCoordinator` |
| runtime · `zakura-state` | the writer lock, the store, the publisher, the leases | invent a fact the store does not hold | the context is state-issued; disk leads memory leads observers | `ValidationLease`, `StateIssuedAuthority`, `Publisher`, `FaultPoint` |
| engine · `zakura-header-chain` | the DAG, projections, selection, the planner | perform any effect, observe anything it was not handed | same inputs, same plan; a dropped plan leaves nothing behind | `GraphOverlay`, `GraphDelta`, `TransitionPlan`, `verify_plan` |

## Conformance rules

The body above explains why these hold; this section states them so a reviewer can check
them.

**The wire.** Request ids are nonzero by construction, so zero is a decode failure rather
than a wildcard (LC-WIRE-01). The four discriminants are closed; new wire data needs an
advertised auxiliary schema bit or a successor stream version (LC-WIRE-15).

**Ownership.** A branch is identified by `(anchor_hash, target_tip_hash)`, never height, and
height monotonicity is not evidence of growth (LC-INT-02). A `Stale` decision produces no
frontier, coverage, retry, repair, scheduling, publication, body-task, or peer-score effect
(LC-GEN-04, LC-ACCEPT-03). The reactor applies retirement before scheduling forward work for
a new branch (LC-WORK-01, LC-AUX-03).

**Preparation.** `prepare_context_free_headers` runs only the rules that read no ancestor.
`PreparedHeaderBatch` seals its results with a receipt naming the parent, network policy,
and trust-anchor digest, which the planner rechecks against the live config (LC-VAL-11).
`DeferredUntil` is the only non-passing sealed result (LC-VAL-08).

**The context.** The runtime supplies every predecessor fact a contextual rule reads; a
caller never supplies its own. A lease is admitted only when the runtime issued it for this
call, and `is_coherent` re-derives the digest and re-walks the backward hash links
(LC-ANCHOR-03). The finality rebase path is all or nothing (LC-FINAL-01). Auxiliary evidence
with no bounded window drives no validity and no fork choice until its own check clears it
(LC-AUX-02, LC-AUX-04).

**Purity.** The engine's manifest may not name `tokio`, `tower`, `zakura-state`,
`zakura-network`, or `zakura-consensus`, and it exposes no wallet, FlyClient, or block-sync
surface (LC-SCOPE-04, LC-SCOPE-05, LC-SCOPE-06). Time arrives only through `Clock`, and
durable rows only through the facts types.

**The durable order.** One transition makes one atomic `db.write` (LC-TXN-01). The runtime
installs the plan before it publishes anything, so a published snapshot is always one the
engine holds (LC-FRONTIER-04). A commit that cannot be installed fails closed rather than
serving a memory view that disagrees with disk. Only the component that committed may
publish (LC-GEN-05). When protected paths alone fill the node bound, retention refuses
admission rather than evicting protected state (LC-RETAIN-01). A refuted migrated pin alarms
and fails the node closed (LC-FINAL-04).

**Attribution.** Any boundary that classifies an outcome uses `ErrorCategory` with an
explicit `Attribution` and names its `ErrorSubject`. A generic conversion never turns a local
or stale category into peer misbehaviour (LC-ERR-01), and `is_automatic_header_peer_fault`
holds only for `MalformedProtocol` and `InvalidHeader` from a `HeaderPeer` (LC-ERR-02).

## Defaults

| Parameter | Value | Meaning |
| --- | --- | --- |
| stream | kind 5, version 8 | capability bit five, 2 MiB message cap |
| headers per page | 1,000 default, 4,000 cap | one response page |
| `MAX_HEADERS_PER_TRANSITION_V1` | 4,000 | headers in one sealed batch |
| `MAX_HEADER_LOCATOR_HASHES` | 13 | entries in one locator |
| `MAX_AUX_DELIVERIES_PER_HEADER_V1` / `_TOTAL_V1` | 16 / 65,536 | per header, then total |
| `MAX_RETENTION_REFERENCES_V1` | 26 | protected references per transition |
| `POW_AVERAGING_WINDOW` | 17 | blocks the difficulty adjustment averages over |
| `POW_MEDIAN_BLOCK_SPAN` | 11 | blocks in one median-time-past |
| `POW_ADJUSTMENT_BLOCK_SPAN` | 28 | facts sealed into one validation lease |
| `POW_PREDECESSOR_CONTEXT_SPAN` | 27 | predecessors below the separately sealed parent |
| `BLOCK_MAX_TIME_SINCE_MEDIAN` | 90 min | a header's time above its median-time-past |
| retained-path lease idle | 30 s | serving-lease expiry without a page read |
| cancelled-response grace | 64 ids, 30 s | responses dropped rather than scored |
| `FaultPoint` | 4, ordered | crash points the recovery tests interrupt |
| `HeaderChainDiskVersion` | 1 | durable schema version the audit accepts |

## Related documents

| Document | Detail it adds |
| --- | --- |
| `docs/specs/fork-aware-header-chain-engine.md` | the `LC-*` rules this document cites |
| `docs/design/headerchain/production_code_header-chain.md` | the files that implement each layer, and what the change deleted |
| `docs/design/header-sync-vct-root-authentication.md` | the root-authentication frontier behind the unbounded-window exception |
| `docs/design/verified-commitment-trees.md` | the commitment-tree state those roots feed |
