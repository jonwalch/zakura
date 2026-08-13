---
name: core-rust-readability
description: >-
  Keep core Rust code semantically named, discoverable, cohesive, and
  understandable as a black box. Prefer facade modules with private children
  over overloaded files. For key Rust crates (zakura-chain, zakura-consensus,
  zakura-header-chain, zakura-network, zakura-rpc, zakura-script, zakura-state),
  confirm with the user during planning before applying; do not auto-apply
  mid-implementation. Also use when the user asks for readability, naming,
  module decomposition, or facade-package refactors in those crates.
---

# Human-Readable Core Rust

Apply this skill when editing the key Rust crates listed in the description.

Confirm application with user during planning process instead of auto-applying. If already implementing the plan, do not auto-apply and do not interrupt the plan.

Write for a human reviewer who should understand the architecture and each API
without asking an AI to reconstruct intent.

## Semantic Naming

- Name symbols for the complete domain concept they represent, not merely the action or underlying type.
- Avoid generic names such as `body`, `node`, `best`, `finalized`, or `advance_finalized` when more specific names such as `body_validation_state`, `header_node`, `best_header_chain`, `finalized_frontier`, or `advance_finalized_frontier` remove ambiguity.
- Give files and modules discoverable domain names. For example, prefer `header_node.rs` over `node.rs`, and place behavior where readers would naturally search for its domain concept.
- Apply this standard consistently to fields, parameters, local variables, methods, types, files, and modules.

## Black-Box Contracts

Treat documented functions as black boxes: a reader should know when to call
them, what changes, and what must remain true—without reading the body.
Comments explain intent and constraints, not syntax. Prefer key details over
completeness; omit anything inferable from the signature or name. Explain
domain terms and enum variants whose meaning is not self-evident. Organize
long implementations into clearly named logical groups.

### Public APIs

Document crate-public and other caller-facing entry points with a focused
specification:

- purpose and caller ownership before/after the call;
- important effects (success vs failure; what durable or in-memory state
  changes, and what does not);
- invariants, postconditions, and freshness/stale rules the caller must respect.

Use short structured sections such as `# Returns` only when they clarify the
caller path. Avoid narrative walkthroughs of internal phases.

### Internal APIs

Keep `pub(crate)`, `pub(super)`, and private helpers brief: purpose plus the
one or two constraints a future editor must not violate. Do not restate the
public caller lifecycle or install/publish contract on internal helpers.

## Cohesive Abstractions

- Keep each layer's API at its own abstraction level. Low-level stores should expose domain-neutral storage operations rather than encode higher-level actors or workflows.
- Prefer typed domain abstractions or parameter structs when several values form one concept.
- Keep one authoritative representation of each fact. Avoid duplicated state, parallel mutation paths, and collections that can drift out of sync.
- Prefer one cohesive typed transition API over several overlapping special-case methods.
- Use distinct, accurate errors for distinct states so debugging does not require reading the implementation.

## Module Packages

Do not grow overloaded multi-responsibility files. When a module mixes a high-level API with lower-level helpers or phases, keep a thin facade and move cohesive clusters into private children.

- Facade shows entry points and phase order; children hide mechanism (`pub(super)` / package-private).
- Split by responsibility or phase (admit → apply → settle; load → audit → derive → classify), not by arbitrary line count.
- Keep one-way dependencies: leaves and DTOs below planners/recovery/engine; do not park shared durable types under a single consumer.
- Prefer behavior-grouped tests with shared fixtures over source-layout guards that break when files move.
- Pure module moves should preserve the external API; visibility tightening is a separate change.

Reference: `crates/zakura-header-chain/src/transition/{planner,types,recovery}`.

## Production and Test Boundaries

- Keep test-only fields, return values, and `#[cfg(test)]` branches out of core production data paths when black-box tests or test helpers can observe the same behavior.
- Add focused unit tests for non-trivial private helpers and invariant-preserving transitions.
