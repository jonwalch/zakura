# Testnet transaction spam — Ironwood release derisking

Status: draft plan, not yet executed.
Owner: roman
Created: 2026-07-27

## Why

It has been observed that transactions were not making it into Zakura mempools over
the gossip path. The behaviour has since changed, but nobody has deliberately
driven enough traffic through the fleet to say the class of bug is gone. This
plan drives sustained Ironwood transaction load in both directions between
Zakura and Zebra, ahead of the Ironwood release.

The deliverable is not "a script ran". It is a per-transaction propagation
matrix — for every submitted txid, which nodes saw it and when — plus a written
result for each acceptance criterion in Phase 6.

## Three parallel agent tracks

Topology and tooling are staffed as **three** parallel workstreams so infra and
kresko wallet work do not block the zecd smoke.

| Track | Name | Owns | Goal |
| --- | --- | --- | --- |
| **1** | Raw / quick POC | Fleet + Zebra monitors, zecd smoke (0.5), matrix poller, Phase 1–4 **once** Track 3 is green (or interim zecd-only fan-out if explicitly chosen) | Fast path to a real v6 matrix on the network where the bug was reported |
| **2** | Ephemeral load lab | Extend PR-node bake (`zebrad` + fleet tip volume); **six** droplets (3 Zakura + 3 Zebra); tip-wait/peering; re-run same harness on ephemeral RPCs only | Repeatable isolated spam lab |
| **3** | Kresko existing-Ironwood path | Teach kresko public txblast to **fund from Ironwood notes already in the mnemonic wallet** — no transparent deposit, no Orchard migration | Unblock multi-node `prepare`/`run` for the 2.13 TAZ Ironwood inventory |

**Track 1 agent:** zecd smoke against fleet (+ Zebra), peering/matrix v0, then
POC fan-out + 600-tx spam using **kresko** once Track 3 lands.

**Track 2 agent:** bake + six-droplet bring-up/teardown (independent of Track 3
for infra; needs Track 3 only when running kresko spam on ephemeral nodes).

**Track 3 agent (parallel):** kresko changes / operator flow so the controller
can:

1. Init (or restore) a public-testnet wallet from the mnemonic / recovery
   material without requiring a t-addr deposit.
2. Scan or import the **existing 12 Ironwood notes** (see Balance layout).
3. Fan those notes into ~50k-zat lanes (Phase 1 sizing) and install hot keys.
4. `run` via existing per-node `txblast-local` → localhost on the submit set.

Exit for Track 3: a dry `prepare`+small `run` against one lab or fleet RPC that
spends only pre-existing Ironwood value (balance history reconciles fees only).
No transparent UTXOs created.

Shared artifacts: pinned refs, fund-handling rules, matrix schema. Tracks 2 and
3 must not wait on each other; Track 1’s **kresko** spam waits on Track 3;
Track 1’s **zecd smoke** does not.

```text
Track 1 ──► zecd smoke ──► (wait Track 3) ──► kresko fan-out + fleet/Zebra spam ──► matrix
                │                                      ▲
Track 2 ──► bake + 6 droplets ─────────────────────────┴── ephemeral spam (needs Track 3 for kresko)
Track 3 ──► kresko: spend existing Ironwood notes ─────┘
```

## Peering and dissemination

**Intent:** every Zakura and Zebra in a track’s observation set is **fully
meshed** — Zakura↔Zakura, Zebra↔Zebra, and Zakura↔Zebra on a shared gossip
stack — so a transaction accepted into one mempool **should** reach every other
node via inventory gossip. That is why the pass bar is “every other
Zakura+Zebra saw it,” not a weaker cross-impl minimum.

**Precondition, not assumption.** Before any spam run:

1. Configure explicit peers (or confirm the fleet already forms the mesh).
2. Verify with `getpeerinfo` (and equivalent) that the expected links are up —
   including at least one **cross-impl** path between every Zakura and every
   Zebra in the set.
3. Record the peer graph next to the baseline matrix. A miss caused by a
   missing edge is indistinguishable at the RPC layer from the mempool bug
   under test.

**Shared stack required for Zakura↔Zebra.** Zebra speaks Zcash P2P. Zakura
nodes on `dual` or `legacy` can gossip with Zebra on that path; a Zakura-v2-only
link does not. When pinning `p2p_stack` for path experiments, keep a legacy (or
dual) path in the mesh whenever Zebra is in the observation set.

**Accept ≠ relay.** Full mesh is necessary but not sufficient. A node can
accept a tx over RPC and still fail to announce it, drop it under mempool
pressure, or reject a peer’s offering for policy/verification reasons. The
per-txid matrix exists to prove dissemination actually happened — the same
class of failure Josh reported (peered network, tx somewhere, missing from
Zakura mempools).

**Default pass bar:** given verified full mesh, require observation on every
other Zakura and every Zebra in the set within the deadline (or mined after
appearing there). Soften only if a documented peer-slot limit makes full mesh
impossible; write that exception down before the run.

## Ground truth as of 2026-07-27

Verified against the live fleet and the repository at `1bb0d001c`.

### Ironwood is NU6.3, and it is already active on testnet

Ironwood is NU6.3, consensus branch ID `0x37a5165b`, activated on testnet at
height **4,134,000**. The fleet is at 4,204,430 — roughly 70,000 blocks past
activation — and reports `chaintip: 37a5165b`.

**V6 transactions and the Ironwood shielded pool are exactly what NU6.3 turned
on**, so both are spendable on public testnet today:

- `zakura-consensus/src/transaction.rs:1119` — `verify_v6_transaction_network_upgrade`
  rejects V6 when `network_upgrade < Nu6_3`.
- `zakura-chain/src/transaction/serialize.rs:815,1177` — the Ironwood
  shielded-data slot (`nActionsIronwood`, `flagsIronwood`, `anchorIronwood`, …)
  is serialized and deserialized only at `>= Nu6_3`.
- `zakura-consensus/src/transaction/tests/prop.rs:361` — valid transaction
  versions are `(4, 6)` under NU6.3, against `(4, 5)` for NU6.2 and earlier.
- `zakura-consensus/src/primitives/halo2.rs:300` — NU6.3 onward uses the
  extended `PostNu6_3` Orchard circuit.

Ironwood shares Orchard's proof system and action shape but keeps distinct
note-commitment and nullifier state (`zakura-chain/src/ironwood.rs`).

NU7 is a separate, future upgrade with no testnet activation height and a
placeholder branch ID (`network_upgrade.rs:241`). **It is out of scope for this
plan.** No private fork and no local-genesis chain is required: both tracks run
on **public testnet**. Track 1 submits to the live fleet; Track 2 submits only
to ephemeral nodes that sync that same tip.

One inert detail worth knowing so it is not misread during a run: there is a
40-block grace period after NU6.3 activation during which a NU6.2 branch ID in
the mempool is not scored as peer misbehaviour
(`zakura-consensus/src/transaction.rs:82`,
`WrongConsensusBranchIdNu6_3GracePeriod`). At ~70,000 blocks past activation
this has long expired — a stale-branch-ID transaction now counts as misbehaviour
and will be rejected.

### Fleet

All six testnet nodes answer `getblockchaininfo` on port 18232 and are at the
same height.

| Node | IP | Notes |
| --- | --- | --- |
| zakura-testnet-1 | 167.99.103.111 | also hosts the self-hosted deploy runner |
| zakura-testnet-2 | 167.99.110.145 | |
| zakura-testnet-3 | 138.68.229.254 | |
| zakura-testnet-eu | 164.92.209.78 | |
| zakura-testnet-as | 206.189.148.0 | |
| zakura-compat | 206.189.208.228 | process-managed Zakura, `p2p_stack = "legacy"`; hosts a zcashd sidecar that is **out of scope** for this plan |

Fleet config is generated in `.github/workflows/zakura-testnet-deploy.yml`
(the `nodes.ci.toml` heredoc) and rendered by `deploy/deployer/deploy.py`.
The five service-managed nodes run `p2p_stack = "dual"`, so both the legacy
Zcash gossip path and the Zakura path on port 8234 are live and both need
coverage. `zakura-compat` is legacy-only Zakura, which makes it a natural
control for gossip-path experiments. The co-located **zcashd sidecar is not in
scope** — do not submit to it, do not require it in the observation matrix, and
do not block the run on its mempool behaviour.

Metrics are bound to `127.0.0.1:9999`, i.e. not reachable off-host. RPC on
18232 is public and cookie auth is disabled. Measurement therefore goes through
RPC polling, not the metrics endpoint, unless we tunnel.

Testnet block spacing is the post-Blossom 75 seconds; NU6.3 does not change it.

### Zebra supports Ironwood in released binaries

NU6.3/Ironwood is merged in Zebra: `zebra-chain/src/ironwood.rs`, the
`add_ironwood_tree` state upgrade, Ironwood-pool shielded coinbase (#10880),
and mainnet activation (#10938, merged 2026-07-10). Release **v6.2.2**
(2026-07-24) postdates all of it.

A downloaded release binary is therefore sufficient — no source build and no
`zcash_unstable` cfg required.

### Wallet tooling — phased: zecd first, then kresko

Two tools, two jobs. Do not collapse them into one path.

| Phase | Tool | Job |
| --- | --- | --- |
| **Smoke** | **zecd** (`ghcr.io/zecrocks/zecd:0.5.0-rc1`) | Cheap proof that a real Ironwood (v6) tx is accepted and gossips into Zakura **and** Zebra mempools |
| **Sustained spam** | **kresko** public txblast | Lane inventory, multi-round fan-out, resumable prepare/run, round-robin submit, recovery/withdraw — the full propagation-matrix run |

**Why zecd first.** It is a prebuilt Ironwood-capable wallet image: import the
mnemonic, sync against an Ironwood-ready lightwalletd (e.g.
`https://testnet.zec.rocks:443`), spend an **existing Ironwood note** (no
Orchard→Ironwood migration in initial scope), and observe `getrawmempool` on
both implementations. That answers “does Ironwood land in mempools at all?”
without waiting on blast automation or the 699-lane fan-out. If it fails, stop
and debug before investing in lanes.

**Why kresko for the real run.** Kresko is the better long-term driver once the
smoke is green — not a fallback because zecd failed. It already has the
public-network blast lifecycle (distinct from the local-genesis CI path):

- `src/commands/txblast_public.rs` (~129 KB), documented in
  `docs/public-txblast.md`, with
  `wallet init` → `deposit` → `plan` → `prepare` → `run` → `withdraw` /
  `recover`.
- `src/txblast/orchard/builder.rs` has a `ShieldedPool::{Orchard, Ironwood}`
  split that **selects the pool by height against NU6.3 activation** — at or
  above the activation height it builds Ironwood, using `add_ironwood_spend`,
  `add_ironwood_output` and `BundleVersion::ironwood_v3()`. On testnet today
  that means the default path already produces Ironwood transactions.
- `prepare`, `run`, `withdraw` and `recover` appear implemented. `stop` and
  `status` are still stubbed through `guarded_lifecycle()`
  (`txblast_public.rs:1527`, `:1538`) — drive stop/status externally for the
  first runs, or finish the stubs if painful.
- `scripts/txblast_public_runbook.sh` documents the lifecycle but its status
  header predates the current implementation; trust the code, not the header.
- Kresko also provides the Python `Fleet` API for provisioning and peering
  zebrad nodes.

The existing regtest CI workflow (`.github/workflows/zakura-mempool-load.yml`
plus `scripts/mempool-load-*.py`) is local-genesis only and premine-funded, so
it cannot be pointed at testnet. Two pieces of it are worth lifting: the
propagation/backpressure grading in `mempool-load-monitor.py`, and the
key-material guard on collected artifacts.

**Gate (Track 1).** Fan-out and kresko spam assume (a) zecd smoke 0.5 green and
(b) Track 3 can spend existing Ironwood notes. Track 2 infra bake does **not**
wait on Track 1 or 3.

### Snapshots

`https://ironwood.zakura.valargroup.dev/` (published from
`deploy/deployer/testnet/snapshots-site/`) carries pre-activation testnet
snapshots at heights around 4.13M. Use them as **fast catch-up** for newly
provisioned nodes (restore → sync ~70k blocks to tip). Not a fork point.

For **Track 2**, prefer baking the **latest usable testnet tip/pruned state**
into the load-test volume (same idea as `zakura-pr-node-bake.yml` testnet
`tip/`), so ephemeral nodes spend minutes not hours reaching tip. The public
pre-activation archives remain the fallback if a fresher tip snapshot is not
published yet.

## Coverage matrix

NU6.3 still accepts versions 4–6 and both Orchard and Ironwood pools, so a
full derisk would cover three cases. **This plan starts with Ironwood only.**

| Case | Status |
| --- | --- |
| V6 with Ironwood actions | **In scope** — primary target for both tracks |
| V6 with Orchard actions | Deferred — follow-up after Ironwood spam is green |
| V5 (Orchard) | Deferred — same |

Kresko’s builder already selects Ironwood by height above NU6.3 activation, so
the default public-testnet path matches this scope. Forcing Orchard (former
Phase 0.3) is not required until the deferred rows are picked up. Do not split
Orchard lanes in Phase 1.

## Fund handling

- The mnemonic is a live testnet key. Treat `testnet_wallet` in the repo root
  as secret material: add it to `.gitignore` before any commit from this tree.
  It is currently untracked and unignored.
- **zecd smoke:** keep wallet data and any seed env under a root-owned `0600`
  path (e.g. `/root/ironwood-spam/`); never collect that directory into
  artifacts.
- **kresko spam:** `recovery.json` from `kresko txblast wallet init` is what
  recovers blast funds. Keep it off CI artifacts. The guard in
  `zakura-mempool-load.yml` ("Guard against collected key material") is the
  pattern to copy — it checks both filenames and file contents, because the
  leak that motivated it was `secret_key_hex` inside a `config.json`.
- Do not paste seed words, `secret_key_hex`, or `recovery.json` contents into
  logs, chat, PRs, or agent transcripts.

### Wallet inventory — how 0.1 was measured (reproducible)

The mnemonic in `testnet_wallet` was validated and inventoried on 2026-07-27
without the seed ever leaving the local machine.

Shape check: 24 words, all lowercase alpha, all distinct, all present in the
BIP39 English wordlist, **checksum valid**, 256-bit entropy. The file holds the
bare phrase only — no birthday and no derivation path, which is why the
birthday had to be supplied out of band (1 July 2026 ≈ height 4,125,800;
4,120,000 was used as the scan floor for margin).

Procedure, and the reason for it:

1. Locally, `zecd init --restore --mnemonic-file` (mnemonic mounted read-only),
   then `zecd export-ufvk` to obtain the `uviewtest1…` unified **viewing** key.
   The local datadir was wiped immediately afterwards — it holds an
   age-encrypted seed next to the identity file that decrypts it, so it is
   effectively plaintext at rest.
2. On `zakura-testnet-1`, the static `x86_64-unknown-linux-musl` zecd 0.5.0-rc3
   release binary (sha256 verified) under `/mnt/data/zecd-check/`, initialized
   **watch-only** from that UFVK — balances and history, no spending material
   on the node.
3. Scan against **loopback** RPC (`zebra://127.0.0.1:18232`), then
   `listunspent`, which carries a per-note `pool` field, aggregated by pool.

**Run this on a node, not a laptop.** The same scan pointed at the fleet's
public RPC from a workstation scanned *zero* blocks in 12 minutes: zecd fetches
in 10,000-block chunks at one WAN round-trip per block and the wallet actor
blocks while it does, so even `getnewaddress` stops answering. On loopback the
same scan ran at ~10,000 blocks per 2 minutes and covered 84k blocks in about
15 minutes. This is worth knowing before Phase 0.5's smoke, which has the same
shape.

State left behind: `/mnt/data/zecd-check/` on `zakura-testnet-1` (~139 MB,
daemon stopped, nothing listening). Restarting it resumes without a rescan.
It also contains `ufvk.txt` (mode 0600) — view-only, but still a privacy
capability; delete the directory when the plan no longer needs it.

### Balance layout — tracking baseline

**Last verified: 2026-07-27, chain tip 4,204,479.** This is the pre-spend
baseline; re-run the inventory (restart the watch-only zecd, `listunspent`)
after each phase and append a dated row to "Balance history" so drift is
visible.

Totals: **1028.11499809 TAZ, 33 notes, 25 transactions, 0 unspendable, no
transparent balance.**

**Ironwood — 12 notes, 2.13047609 TAZ.** This is the POC's entire funding
source. The "lanes" column is how many 50,000-zat lanes each note yields in one
64-wide fan-out round, after its ZIP-317 fee.

| # | TAZ | zats | confs | lanes @50k |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 0.67986401 | 67,986,401 | 59,279 | 64 |
| 2 | 0.37362940 | 37,362,940 | 56,787 | 64 |
| 3 | 0.31889433 | 31,889,433 | 56,537 | 64 |
| 4 | 0.18117142 | 18,117,142 | 56,523 | 64 |
| 5 | 0.12372221 | 12,372,221 | 56,548 | 64 |
| 6 | 0.11608032 | 11,608,032 | 56,511 | 64 |
| 7 | 0.10183715 | 10,183,715 | 56,691 | 64 |
| 8 | 0.08536747 | 8,536,747 | 56,548 | 64 |
| 9 | 0.06790063 | 6,790,063 | 59,266 | 64 |
| 10 | 0.04878932 | 4,878,932 | 56,899 | 64 |
| 11 | 0.02247509 | 2,247,509 | 56,531 | 40 |
| 12 | 0.01074474 | 1,074,474 | 56,511 | 19 |
| **tot** | **2.13047609** | **213,047,609** | | **699** |

All twelve are old (56.5k–59.3k confirmations, i.e. heights ~4,145,000–4,148,000
— comfortably post-activation), so none is at reorg risk. Every note clears the
fan-out floor; notes 11 and 12 simply take a narrower fan-out than 64.

**Orchard — 21 notes, 1025.98452200 TAZ.** Not used by the POC; recorded so the
deferred decision has a starting point.

| Bucket | Notes | TAZ | Note |
| --- | ---: | ---: | --- |
| 974.01562800 | 1 | 974.01563 | 95% of everything, confs 110 |
| 20.00015000 | 1 | 20.00015 | confs 110 |
| 9.90000000 | 1 | 9.90000 | confs 74,607 |
| 5.00015000 | 1 | 5.00015 | confs 110 |
| 2.474–2.475 | 3 | 7.42371 | confs ~74,510–74,570 |
| 1.237–1.238 | 7 | 8.66161 | confs ~74,486–74,564 |
| ≤ 0.50015 | 7 | 0.98327 | confs 110, incl. 0.00237200 tail |

The confirmation split is informative: the ~74.5k-confirmation notes are the
original funding, while the eleven notes at confs 110 (~2.3 hours before the
snapshot) are recent activity — someone was already exercising this wallet.
Confirm nobody else is spending from it before a run, or the propagation matrix
will contain transactions this plan did not send.

### Balance history

| Date | Tip | Total TAZ | Orchard | Ironwood | Notes | Event |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| 2026-07-27 | 4,204,479 | 1028.11499809 | 1025.98452200 | 2.13047609 | 33 | baseline inventory |

---

## Phase 0 — Prerequisites

Zebra v6.2.2 ships Ironwood, and kresko already builds Ironwood bundles. What
remains is inventory, pins, kresko submit/pool knobs, and the **zecd smoke
gate**.

0.1 **Inventory the funds — DONE, 2026-07-27.** Measured with a watch-only
zecd 0.5.0-rc3 wallet (see "Wallet inventory" below). Result: **1028.115 TAZ,
33 spendable notes, 25 transactions**, and the funds are **almost entirely
Orchard, not Ironwood**:

| Pool | Notes | Total TAZ | Smallest | Largest |
| --- | ---: | ---: | ---: | ---: |
| orchard | 21 | 1025.98452200 | 0.00237200 | 974.01562800 |
| ironwood | 12 | 2.13047609 | 0.01074474 | 0.67986401 |

Consequences:

- **The 2.13 TAZ of Ironwood is enough for the POC on its own.** See
  "POC sizing" / Phase 1 — 600 transactions from existing Ironwood notes after
  one fan-out round. **Orchard→Ironwood migration is out of initial scope**
  (see [Deferred follow-ups](#deferred-follow-ups)).
- **The Orchard side is extremely concentrated** (one note ≈ 974 TAZ). Recorded
  for later; unused by the POC.
- **No transparent balance.** Irrelevant to the Ironwood-only POC path. The
  kresko transparent-deposit entry point is only a problem if/when a later
  phase needs kresko’s deposit lifecycle against this wallet — deferred with
  Orchard (see follow-ups).

POC Phase 1 spends **existing shielded Ironwood notes** (fan-out + spam). It
does **not** require a transparent hop or migration.

0.2 **Pin refs.** Record the Zakura ref under test, the kresko ref,
`zebrad v6.2.2` (or later), and the zecd image digest/tag in this directory.
Confirm the downloaded zebrad reports NU6.3 in `getblockchaininfo` before
trusting it.

0.3 **Coverage scope (decided):** Ironwood-only for this run. Defer forcing
the Orchard pool / V5 rows until a follow-up. No builder flag work required in
Phase 0.

0.4 **Kresko multi-node submit (resolved — see below).** Public `run` does
**not** need a central round-robin patch. It SSHs to each planned node and
starts `txblast-local` against `http://localhost:{rpc_port}`, so every selected
node submits to itself. Confirm the plan’s `--nodes` list is the intended
submit set, remotes have `kresko` + hot keys, and observation still polls every
Zakura+Zebra RPC for the matrix.

### Kresko submit model (code review, `valargroup/kresko` `origin/main`)

Reviewed `run_public` in `src/commands/txblast_public.rs` (post
`feat/ironwood-txblast`).

**How `run` works:**

1. Load plan → `active_instances_for_plan` (kresko `config.json` miners with
   real IPs).
2. Require a prepared hot key per selected node.
3. Build a remote script that runs:
   `kresko txblast-local --rpc-endpoint http://localhost:{rpc_port} ...`
4. `tmux::run_script_in_tmux` SSHs that script onto **every** selected instance
   in parallel.

So multi-node submission is **per-node agents → localhost RPC**, not one
controller round-robining `sendrawtransaction`. That is **better** for Josh’s
bug class than a single driver: each Zakura under test is the submitter.

**Ironwood:** `ShieldedPool::at_height` in `src/txblast/orchard/builder.rs`
selects Ironwood at/after NU6.3 — default public-testnet path is V6-Ironwood.
Pin a kresko ref that includes `feat/ironwood-txblast` (on `origin/main` as of
this review); older local checkouts without that merge will still build Orchard
only.

**`stop` / `status`:** still stubbed; message points at
`kresko kill-session --session txblast` for teardown. Drive stop that way for
v1.

**Recommendation:** use public txblast as designed for Track 1 and Track 2.
Do **not** build an external round-robin wrapper unless you deliberately want a
central submitter (e.g. zecd smoke). Requirements: SSH to every submit host,
`kresko` on those hosts, fleet `config.json` listing Zakura (Phase 3) then
Zebra (Phase 4) instances, `plan --nodes` covering that set, and a central
matrix poller over all observation RPCs. `prepare` may use a single operator
RPC for shield/fanout; that does not affect multi-node `run`.

0.5 **zecd v6 smoke (gate for Phases 1–4).**

1. Use zecd **0.5.0-rc3** (or later; matches the inventory measurement), not an
   older rc1 pin.
2. Import the mnemonic; prefer **loopback** or on-node RPC for sync (WAN public
   RPC is too slow — see Wallet inventory).
3. Sync; spend from **existing Ironwood notes only** — do **not** migrate
   Orchard in this phase.
4. Submit a small number of V6 Ironwood self-sends (order of 1–10) to **fleet
   RPC** and, once available, to at least one **Zebra** RPC from Track 1.
5. Record txids and a mini observation matrix: accepted where, first seen on
   which Zakura / Zebra, mined height if any.

If this smoke fails, **do not start the Phase 1 fan-out**. Debug acceptance /
propagation first. The smoke costs 10,000 zats per transaction and spends an
existing Ironwood note, so it is recoverable and cheap to repeat.

**Exit criteria:** refs pinned (including kresko at Ironwood-capable `main` if
used later), and at least one V6 Ironwood txid observed in both a Zakura and a
Zebra mempool (or mined after appearing in both).

---

## Phase 1 — Ironwood-only POC fan-out

**POC target: 10 tx/s for 60 seconds = 600 transactions.** Funded entirely from
the existing 2.13 TAZ of Ironwood. No Orchard migration, no transparent hop.

**Driver:** **kresko**, via Track 3’s existing-Ironwood path (not the stock
transparent-deposit entry). Phase 1 steps below assume Track 3 can scan/import
the 12 notes and fan them out; do not start the 699-lane fan-out until that
exit criterion is met. zecd remains the smoke tool (0.5) only.

### There is no minimum note value — the floor is the fee

Verified in the tree:

- **No dust rule applies to shielded outputs.** `is_dust()` exists only on
  transparent outputs (`zakura-chain/src/transparent.rs:349`). Arbitrarily small
  Ironwood notes are valid.
- **The binding floor is ZIP-317:** `fee = 5000 × max(logical_actions, 2)`
  (`zip317.rs:23,26`). A lane self-send is 2 actions, so it pays the 2-action
  grace minimum: **10,000 zats = 0.0001 TAZ per transaction.**
- **Ironwood actions are counted** in that fee alongside Orchard
  (`zip317.rs:157,166`) — no accidental discount for the new pool.
- **There is no cheaper option.** `BLOCK_UNPAID_ACTION_LIMIT = 0`
  (`zip317.rs:50`) and `mempool_checks` rejects any unpaid action
  (`zip317.rs:187`), so the full conventional fee is mandatory for relay.

So a lane must hold **more than 10,000 zats** to fund even one spend; N spends
need N × 10,000 plus residue.

### Splitting smaller does not buy more transactions

Total volume is capped by value ÷ fee, not by note count:

> 213,047,609 zats ÷ 10,000 = **~21,300 transactions**, however it is sliced.

Lane **size** sets how long a lane survives. Lane **count** sets concurrency.
Minimum-size lanes only mean more lanes that die sooner. Size the split for
concurrency, not for volume.

### What actually constrains lane count at 60 seconds

A lane cannot be respent until its replacement note confirms. **At 75-second
block spacing, a 60-second run is shorter than one block — nothing recycles
during the POC.** Therefore:

> **600 transactions require 600 distinct lanes, one spend each.**

This is the single most important number in the phase, and it is easy to get
wrong by reasoning from steady-state lane recycling (which is what a longer run
would need — roughly `rate × confirm_latency`).

### Sizing

600 lanes needed; the inventory yields 699 in one round (see "Balance layout").

| Quantity | Value |
| --- | --- |
| Lane size | 50,000 zats (0.0005 TAZ) |
| Lanes produced | 699, from one 64-wide round over all 12 notes |
| Lanes needed for POC | 600 |
| Fan-out fees | 10 × 320,000 + 200,000 + 95,000 = **3,495,000 zats (0.035 TAZ)** |
| Spam fees for one run | 600 × 10,000 = **6,000,000 zats (0.060 TAZ)** |
| Total consumed per POC cycle | **~0.095 TAZ of 2.13 available** |

Lane size 50,000 rather than the ~11,000 minimum buys **4 spends per lane**
(4 × 10,000 = 40,000, leaving 10,000), so the same 699 lanes support **about
four sequential POC runs** before a re-fan-out is needed. That headroom is
deliberate: the first run will not be the last, and re-fanning out costs a
block-confirmation round trip each time.

### Steps

1.1 Fan out each of the 12 Ironwood notes to 50,000-zat lanes in **one round**,
width per note capped at 64 and floored by value:
`outputs_i = clamp(floor((v_i − 5000 × outputs_i) / 50,000), 1, 64)`.
Notes 1–10 take the full 64; note 11 takes 40; note 12 takes 19. A 64-action
transaction is ~52 KB (an Ironwood action shares Orchard's ~820-byte shape),
comfortably inside the 2 MB block limit.

1.2 The 12 fan-out transactions are **independent** — each spends a different
note — so submit them together rather than serially. This is the one place the
Ironwood side parallelizes, precisely because it is *not* concentrated the way
the Orchard side is.

1.3 Wait for confirmation and scan-back. Verify 699 (or the planned count)
confirmed 50,000-zat Ironwood lane notes before proceeding. Append a row to
"Balance history".

1.4 Fan-out above the activation height produces **Ironwood** notes by default —
that is what we want. Do not split Orchard lanes in this phase.

**Exit criteria:** ≥600 confirmed Ironwood lane notes at 50,000 zats, wallet
state scanned, balance history updated.

### Do not let mempool eviction counterfeit the bug

`tx_cost_limit = 80,000,000` with `eviction_memory_time = 1 hour`
(`zakurad/src/components/mempool/config.rs:70,72`). Evicted transactions go onto
a recently-evicted list and are **rejected if re-offered within that hour**.

At sustained load that produces "transaction accepted somewhere, absent from
mempools" — *indistinguishable from the symptom Josh reported*. Keep the POC
burst below the eviction threshold so a baseline miss means something, and
exercise eviction deliberately in a **follow-up** edge-case suite instead of
tripping over it. 600 transactions is far below the limit, which is another
reason the 60s POC is a clean first signal.

---

## Phase 2 — Stand up monitors (split by track)

### Track 1 — Fleet + Zebra (POC agent)

2.1 Confirm the pinned Zakura ref is what the six fleet nodes run (deploy via
**Deploy Zakura testnet fleet** / `deploy-specific` only if a bump is required).
Do **not** block the POC on an unrelated fleet redeploy.

2.2 Add **1–3 Zebra nodes** running v6.2.2 (or later), peered to the Zakura
fleet. Prefer the kresko Python `Fleet` API (`fleet.add` / `deploy` / `run`,
tagged teardown via `kresko-fleet down <name>`). Bootstrap from the snapshot
site or a tip volume so they reach tip quickly.

2.3 Measurement on Track 1: poll `getrawmempool` / `getblockchaininfo` on every
fleet Zakura RPC and every Zebra RPC; build the per-txid propagation matrix
(submitting node, first-seen per node, mined height). Adapt
`mempool-load-monitor.py` grading; lift the key-material artifact guard.
Observation set = Zakura + Zebra only (no zcashd).

2.4 Gate: common tip, **full mesh peering among the observation set** per
[Peering and dissemination](#peering-and-dissemination) (verify with
`getpeerinfo`, record the peer graph), clean baseline matrix under zero load.

### Track 2 — Ephemeral 3+3 infra (parallel agent)

Work in parallel with Track 1; no dependency on fan-out or spam results.

2.A **Pre-bake image + tip volume (decided):**

- **Extend** [`zakura-pr-node-bake.yml`](../../.github/workflows/zakura-pr-node-bake.yml)
  / [`pr-node-bake.sh`](../../.github/workflows/scripts/pr-node-bake.sh): keep
  existing deps, warm cargo, and prebuilt **kresko**; additionally **ship a
  `zebrad` release binary** (v6.2.2+) on the image so ephemeral Zebra nodes do
  not build from source at bring-up.
- **Tip volume:** pre-bake a DigitalOcean volume from a **fresh recent testnet
  snapshot taken on one of the live fleet nodes** (stop or `backup`-style
  copy of `/mnt/data/zakura-cache` at tip — not the pre-activation public
  archive). Clone/attach that volume (or copies) into Track 2 runs so 3+3
  catch up in minutes. Re-bake when tip drifts enough that sync time hurts.
- Tag spam droplets distinctly (e.g. `zakura-testnet-spam`) so the reaper can
  TTL them without touching the persistent fleet.

2.B **Host layout (decided): six droplets** — one process per host (3×
`zakurad` + 3× `zebrad`). True multi-host gossip; stock kresko `run_public`
(SSH per instance → `txblast-local` → localhost RPC) with no loopback bind
tricks. Tip volume is cloned once per droplet (or attached from a snapshot
clone). Provision scripts under `scripts/ironwood-spam/` (or
`plans/testnet-spam/scripts/`): create six droplets from the baked image,
hydrate each from the fleet-derived tip volume, start the node binary, **peer
all six to each other** (full mesh per
[Peering and dissemination](#peering-and-dissemination)) plus public testnet /
fleet bootstrap peers, wait for tip and verify `getpeerinfo`. Record the peer
graph before spam. Tag all six `zakura-testnet-spam` for reaper TTL.

One-box and hybrid layouts are out of scope for v1 (revisit only if six-host
ops cost becomes the bottleneck).

2.C **Submit targets are only the six ephemeral RPCs** — never the live fleet
RPCs for Track 2 spam. Fleet may appear only as P2P peers for sync (and as the
**source** of the tip snapshot used in the bake).

2.D Reuse Track 1’s matrix schema and (when ready) the same kresko/zecd driver
pointed at ephemeral endpoints. Teardown: destroy all six droplets/volumes;
leave the persistent Zakura fleet untouched.

**Exit criteria (Track 1):** fleet + Zebra at common tip, baseline matrix OK.  
**Exit criteria (Track 2):** baked image (with zebrad) + fleet-derived tip
volume exist; six droplets can be brought to tip, meshed, and torn down in one
operator runbook without manual DO console work.

---

## Phase 3 — Spam over Zakura submit targets (kresko)

Sustained load via kresko public txblast. The zecd smoke already proved one v6
path works; this phase produces the full propagation matrix.

**Track 1:** submit round-robin across the **six fleet Zakura** nodes; observe
fleet + Zebra.  
**Track 2:** same plan/prepare/run, but round-robin across the **three ephemeral
Zakura** RPCs only; observe all six ephemeral nodes.

3.1 `kresko txblast plan --nodes ... --target-block-bytes <N>`, then
`prepare --plan <id>` (resumable), then `run --plan <id>`.

3.2 **Round-robin across that track’s Zakura submit set** (see Phase 0.4). A bug
where node 0 works and others do not is exactly the reported symptom.

3.3 Assert, per transaction:
- accepted by the submitting node,
- observed in `getrawmempool` on every other Zakura in the observation set
  within a deadline,
- observed on every Zebra in the observation set within a deadline,
- eventually mined.

Any transaction that is accepted but never propagates, or that reaches Zebra but
not Zakura or the reverse, is a finding — record it with its txid, submitting
node, and the observation matrix.

3.4 **Gossip-path experiments (dual / legacy / Zakura-only) are out of initial
scope** — see [Deferred follow-ups](#deferred-follow-ups). For the POC, run
against the fleet’s existing `p2p_stack` as deployed; document which configs
were observed, do not reconfigure production for path isolation.

3.5 Coverage for this run is **V6-Ironwood only** (see Coverage matrix).

3.6 **POC run parameters: 10 tx/s for 60 seconds = 600 transactions**, one spend
per lane from the Phase 1 inventory.

Read the 60-second window for what it is. It is **shorter than one 75-second
block**, so:

- Nothing recycles — hence 600 lanes for 600 transactions (Phase 1).
- Most transactions will still be *unmined* when the window closes. The mined
  assertion in 3.3 must therefore be evaluated on a **drain window after the
  blast**, not at t=60s, or every transaction will look like a failure.
- It exercises mempool acceptance and gossip, **not** eviction, re-announcement,
  or block-boundary churn.

That last point is the deliberate trade: the 60s POC is a clean, cheap first
signal on the reported symptom. It is not evidence about steady-state mempool
behaviour. Once it is green, extend to a multi-block run (several minutes,
crossing block boundaries) before drawing any conclusion about sustained load —
that longer run needs lane recycling and therefore roughly `rate ×
confirm_latency` lanes, not one per transaction.

**Exit criteria:** Ironwood propagation matrix per track over the 600
transactions, evaluated after a post-blast drain window, anomalies explained or
filed.

---

## Phase 4 — Spam over Zebra submit targets

4.1 Repeat Phase 3 **submitting to that track’s Zebra nodes** and asserting
propagation into Zakura. Separate phase so it is not skipped under time
pressure.

4.2 Rotate across all Zebra nodes in the track, not just one.

---

## Phase 5 — Adversarial and edge cases

**Out of initial scope.** Checklist for a later interactive planning pass after
the POC matrix is green. Do not block Phase 6 acceptance on these.

5.1 Transactions with a stale NU6.2 branch ID — must be rejected and scored as
misbehaviour, since the 40-block grace period expired long ago.

5.2 Expired-by-`expiry_height` transactions, and transactions expiring while
in the mempool.

5.3 Double-spends of the same lane note submitted to two different nodes
simultaneously — exercises conflict handling across the gossip boundary.

5.4 A burst well above sustained rate, to exercise mempool size limits,
eviction, and backpressure rather than the steady state.

5.5 Restart a node mid-run and confirm it re-acquires mempool contents and
rejoins the tip.

---

## Phase 6 — Acceptance criteria and teardown (initial POC)

Per track, the **initial** run is a pass when all of the following have a
written result:

1. Every transaction accepted by any node propagated to every other node in that
   track’s Zakura+Zebra observation set within the deadline, in both
   Zakura→Zebra and Zebra→Zakura directions.
2. The above holds for each submitting node individually, not just in aggregate.
3. The above holds for **V6-Ironwood** over the 600-tx POC.
4. No node crashed, stalled, or fell off the tip during the run.
5. **POC-specific:** all 600 transactions accounted for, with the mined
   assertion evaluated over a post-blast drain window rather than at t=60s.
   State plainly that a 60-second run says nothing about eviction,
   re-announcement, or block-boundary behaviour — those need a follow-up
   multi-block run.

**Not required for initial pass:** Phase 5 edge cases; dual/legacy/Zakura-only
gossip isolation; Orchard migration; transparent hop; V6-Orchard/V5.

Teardown:

6.1 **POC:** nothing to withdraw. The spam is Ironwood self-sends, so value
stays in the wallet minus fees (~0.095 TAZ per cycle). Re-run the inventory and
append to "Balance history" — that reconciliation *is* the teardown, and a
balance that does not match fees-spent is itself a finding.

6.2 **Kresko-driven phases only:** `kresko txblast withdraw --to <t-addr>
--amount all`. Keep `recovery.json` until the returned balance is confirmed; use
`kresko txblast recover inventory` / `recover sweep --from-height <H>` if the
withdrawal comes up short.

6.3 Track 1: `kresko-fleet down <fleet>` for the POC Zebra nodes. The Zakura
testnet fleet is persistent and stays up.

6.4 Track 2: destroy ephemeral spam droplets/volumes (reaper tag
`zakura-testnet-spam` or equivalent); never tear down the persistent fleet as
part of Track 2 cleanup.

6.5 Remove `/mnt/data/zecd-check/` from `zakura-testnet-1` once the plan no
longer needs the watch-only wallet (it holds `ufvk.txt`, a view-only privacy
capability).

6.6 Write findings into this directory and open issues for anything found.

---

## Deferred follow-ups

Explicitly **out of initial scope.** Revisit only after the POC has a clean
propagation matrix, in a **separate interactive planning** pass.

### Orchard migration and large Ironwood top-up

The 1025.98 TAZ of Orchard is unused by the POC. When revisited, choose among
stay Ironwood-only / migrate a bounded slice / migrate the bulk; then decide
transparent hop vs kresko shielded `deposit import`, and which tool owns
migration.

### Gossip-path isolation

Vary dual vs legacy vs Zakura-only (`zakura-compat` as control; ephemeral
`p2p_stack` pins on Track 2). Do not reconfigure the live fleet for the POC.

### Phase 5 edge cases

Stale branch ID, expiry, double-spend, eviction burst, mid-run restart — plus
multi-block sustained runs (lane recycling, block-boundary churn).

### Coverage rows

V6-Orchard and V5 once Ironwood spam is green.

---

## Open questions

- **Track 3 design detail (kresko):** exact API shape for existing Ironwood
  notes — e.g. mnemonic restore + birthday scan, UFVK+spend key import, or
  note/witness import into `state.json` / recovery. Pick the smallest change
  that feeds `prepare`/`run` without a t-addr deposit. Implementation lives in
  `valargroup/kresko` (Track 3 agent).
- **`stop` / `status` stubs** — use `kresko kill-session --session txblast`
  for v1; finish stubs only if painful.
- **zecd headless automation** — script vs interactive for smoke (0.5) only;
  do not block Tracks 2–3 on UX polish.
- **Resolved (POC driver):** **kresko** for fan-out + 600-tx spam once Track 3
  lands; **zecd** for smoke only. No zecd-owned Phase 1 unless Track 3 slips
  and an interim is explicitly approved.
- **Resolved (POC funding):** existing **2.13 TAZ Ironwood** only; no migration
  and no transparent hop in initial scope.
- **Resolved:** kresko public `run` is multi-node via remote `txblast-local` →
  localhost; no central round-robin patch required.
- **Resolved (0.1 / sizing / Track 2):** see Balance layout, Phase 1, Track 2
  bake+six droplets.
- **Deferred (not open):** Orchard migration, transparent hop, gossip-path
  isolation, Phase 5, V6-Orchard/V5 — see Deferred follow-ups.
