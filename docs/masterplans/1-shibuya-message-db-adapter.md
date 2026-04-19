# Shibuya Adapter for message-db-hs Subscriptions

Intention: intention_01kpgme50se0ranxp41ghfhajf

This MasterPlan is a living document. The sections Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.

This document is maintained in accordance with `.claude/skills/master-plan/MASTERPLAN.md`.


## Vision & Scope

After this initiative is complete, a Haskell application that uses the Shibuya
queue-processing framework (see `/Users/shinzui/Keikaku/bokuno/shibuya-project/shibuya`)
can consume messages from a message-db (see
`/Users/shinzui/Keikaku/work/libraries/haskell/message-db-hs-master`) category stream by
plugging in a `messageDbAdapter` alongside any other Shibuya adapter. Users write a
`Handler es MessageDb.Message` and wire it into `runApp`; the adapter owns all
message-db-specific concerns — polling the Postgres event store, translating
`MessageDb.Message` to `Shibuya.Envelope`, persisting and resuming from durable
checkpoints, retry and dead-letter semantics, optional consumer-group partitioning, and
graceful shutdown.

"message-db" is the PostgreSQL-backed event store at `message-db/message-db` (referenced
in Haskell via `tan/message-db-hs`'s `message-db-hs`, `message-db-effectful`,
`message-db-subscription`, and `message-db-checkpoint-store` packages). "Shibuya" is a
Broadway-inspired supervised queue-processing framework whose core package,
`shibuya-core`, defines the `Adapter`, `Envelope`, and `AckDecision` contract that every
adapter must satisfy. After this work, that family of adapters includes a message-db
adapter sitting alongside the existing `shibuya-pgmq-adapter` and
`shibuya-kafka-adapter`.

What is in scope:

- A new Cabal package `shibuya-message-db-adapter` at the repository root, exposing
  `Shibuya.Adapter.MessageDb`, `Shibuya.Adapter.MessageDb.Config`,
  `Shibuya.Adapter.MessageDb.Convert`, and `Shibuya.Adapter.MessageDb.Internal`.
- A polling stream that reads messages from a given `CategoryStream` in message-db and
  emits `Ingested es MessageDb.Message` values on a Streamly stream.
- Durable checkpointing via the existing `message-db-checkpoint-store` package, with
  contiguous-prefix advancement so that a retried or in-flight message never causes a
  later message's checkpoint to be persisted prematurely.
- Ack semantics mapped onto message-db reality: `AckOk` and `AckDeadLetter` advance the
  checkpoint; `AckRetry delay` re-emits the same message after the delay without
  advancing past it; `AckHalt` stops polling and leaves the checkpoint at the last
  successfully-advanced position so restart resumes cleanly.
- Dead-letter strategies: skip-and-log (the default) and write-to-stream (produce a
  `$dead-letter` message back into message-db).
- Optional consumer-group partitioning consistent with
  `message-db-subscription`'s Murmur3-64 hash of the category name.
- First-class per-stream ordered dispatch: when configured, messages for the same
  `MessageDb.Stream` (e.g., `orders-42`) are delivered strictly in write order while
  messages for different streams within the same category (e.g., `orders-42` and
  `orders-43`) are dispatched concurrently. This fulfils the `PartitionedInOrder`
  ordering contract that `shibuya-core` declares but delegates to the adapter
  (see `shibuya-core/src/Shibuya/Policy.hs:20` and
  `shibuya-project/shibuya/docs/BROADWAY_COMPARISON.md:92`).
- A companion `shibuya-message-db-adapter-jitsurei` package with runnable examples and an
  integration test suite running against an ephemeral Postgres (via `shinzui/ephemeral-pg`).
- A benchmarks package `shibuya-message-db-adapter-bench` for the conversion hot path
  (delivered by EP-6).
- Hackage-ready release metadata — README, CHANGELOG, LICENSE, cabal descriptions,
  and a clean Haddock sweep (delivered by EP-8 as the closing act of the initiative).

What is explicitly excluded:

- Writing messages back to message-db for purposes other than the DLQ
  (`shibuya-message-db-adapter` is a read-side adapter; writing events is the
  application's concern).
- Replacing or duplicating `message-db-subscription` — this adapter wraps and re-uses its
  primitives (checkpoint store, category polling, partition helpers).
- Implementing an HTTP management server. Shibuya already exposes introspection through
  `shibuya-metrics`; we do not need the `MessageDb.Worker` HTTP endpoints.
- Supporting non-Postgres message-db backends, or anything beyond `ghc912` on the Nix
  flake-provided toolchain.


## Decomposition Strategy

The work divides naturally along **what the adapter promises to the framework** versus
**which message-db primitives back that promise**. Each work stream below produces an
independently verifiable behavior and an incremental build-up of the adapter's contract.
The decomposition is driven by three principles from
`.claude/skills/master-plan/MASTERPLAN.md`:

1. *Group by functional concern, not by file*: conversion, checkpointing, retry/DLQ,
   partitioning, examples, and release are distinct concerns even though several of them
   touch `Shibuya.Adapter.MessageDb.Config` and the cabal file.
2. *Maximize independent verifiability*: every plan ends at a point where `cabal test`
   (and, where relevant, `cabal run <jitsurei-example>`) passes and demonstrates a new
   user-observable capability.
3. *Respect natural ordering*: the adapter cannot do anything without a conversion and a
   polling stream, so that is EP-1. Durable checkpointing is meaningless without
   something to poll, so it is EP-2. Retry and DLQ build on checkpoint accounting, so
   they are EP-3. Partitioning interacts with checkpoint scope, so it is EP-4. Examples,
   per-stream dispatch, and benchmarks come after the adapter is feature-complete
   (EP-5, EP-7, EP-6); release metadata closes the initiative as EP-8.

Alternatives considered and rejected:

- *One big plan.* The initial `shibuya-kafka-adapter` was delivered as a single plan, but
  Kafka's offset model is simpler than the inflight/contiguous-prefix bookkeeping this
  adapter needs, and message-db-subscription has more surface area (checkpoint store,
  consumer groups, producer/consumer/worker split) to absorb. Splitting into eight plans
  keeps each one reviewable and lets EP-4 (partitioning) run partly in parallel with
  EP-3 (retry/DLQ).
- *Splitting conversion from polling in EP-1.* The conversion function is small and only
  meaningful when exercised by the polling stream; merging them keeps EP-1 a single
  demonstrable milestone.
- *Merging EP-5, EP-6, and EP-8 (examples, tests, benchmarks, release).* These are three
  distinct audiences — integration tests and examples serve users learning the adapter,
  benchmarks serve regression tracking, release metadata serves Hackage readers — and
  can be picked up by different contributors. Keeping them separate preserves review
  focus.
- *Bundling benchmarks with release metadata in a single EP-6.* The original plan
  combined them, but benchmarks land as soon as the main library exists (EP-1) while
  release metadata must describe every shipped feature including the per-stream
  dispatcher (EP-7). Bundling forced one of two bad choices: either run benchmarks
  late so the release notes could mention them, or ship release metadata that ignored
  later-landing features. Splitting into EP-6 (benchmarks-only) and EP-8
  (release-only) lets benchmarks land early and lets the release describe the full
  feature set.


## Exec-Plan Registry

| # | Title | Path | Hard Deps | Soft Deps | Status |
|---|-------|------|-----------|-----------|--------|
| 1 | Scaffold project and implement minimal message-db adapter | docs/plans/1-scaffold-and-minimal-adapter.md | None | None | Not Started |
| 2 | Durable checkpoints and contiguous-prefix ack accounting | docs/plans/2-checkpoint-and-ack-accounting.md | EP-1 | None | Not Started |
| 3 | Retry, dead-letter, and halt handling | docs/plans/3-retry-dlq-halt.md | EP-2 | None | Not Started |
| 4 | Consumer-group partitioning | docs/plans/4-consumer-group-partitioning.md | EP-2 | EP-3 | Not Started |
| 5 | Jitsurei examples and integration test suite | docs/plans/5-jitsurei-and-integration-tests.md | EP-3 | EP-4 | Complete |
| 6 | Benchmarks | docs/plans/6-benchmarks.md | EP-1 | EP-2, EP-4 | Not Started |
| 7 | Per-stream ordered dispatch (PartitionedInOrder) | docs/plans/7-per-stream-ordered-dispatch.md | EP-3 | EP-5, EP-8 | Not Started |
| 8 | Release metadata and Hackage prep | docs/plans/8-release-metadata.md | EP-7 | EP-2, EP-3, EP-4, EP-5, EP-6 | Not Started |

Status values: Not Started, In Progress, Complete, Cancelled.
Hard Deps and Soft Deps reference other rows by their # prefix.


## Dependency Graph

EP-1 has no dependencies; it scaffolds the package, writes the conversion module, and
produces a minimal polling adapter with stubbed ack handling (every ack simply logs and
advances a process-local position). Until EP-1 exists, there is no `Shibuya.Adapter.MessageDb`
module for later plans to extend.

EP-2 hard-depends on EP-1 because it replaces the stubbed ack handling with durable
checkpoint bookkeeping and resume-on-startup logic. It imports `Shibuya.Adapter.MessageDb.Internal`
and extends `Shibuya.Adapter.MessageDb.Config` with the checkpoint subscription name and
persistence cadence.

EP-3 hard-depends on EP-2 because `AckRetry` requires the checkpoint to **not** advance
past the retried message. The inflight-set data structure introduced by EP-2 is the
basis for EP-3's retry-buffer interleave. `AckDeadLetter` must advance the checkpoint
past the message while optionally producing a DLQ message; without EP-2 there is no
checkpoint to advance. `AckHalt` must leave the checkpoint at the last
successfully-advanced position.

EP-4 hard-depends on EP-2 and soft-depends on EP-3. Partitioning changes the **scope**
of what a single subscription consumes (only the subset of category messages whose hash
falls in the adapter's partition); the checkpoint name now includes the partition
number. EP-4 can technically be implemented before EP-3 — an `AckRetry` on a partitioned
subscription behaves the same way as on a single-partition one — but conceptually the
adapter's ack semantics should be feature-complete before layering partitioning on top.
If a contributor wants to parallelize, EP-4 may start once EP-2 is complete.

EP-5 hard-depends on EP-3 and soft-depends on EP-4. The jitsurei examples must
demonstrate the full ack story (AckOk, AckRetry, AckDeadLetter, AckHalt). A
partitioning example is desirable; if EP-4 is not complete when EP-5 begins, the
partitioning example can be stubbed or omitted and added in a follow-up.

EP-6 hard-depends on EP-1 (the `shibuya-message-db-adapter-bench` package must live
alongside the main package). It soft-depends on EP-2 (the InflightState benchmark
milestone is gated on EP-2's module existing) and EP-4 (the categoryPartition
benchmark milestone is gated on EP-4's helper existing). When EP-6 is implemented
before EP-2 or EP-4 lands, the conditional milestones are skipped with an entry in
the Decision Log; they can be revisited as a follow-up once the gating plan lands.
EP-6 covers benchmarks **only**; release metadata is EP-8.

EP-7 hard-depends on EP-3 — the per-stream dispatcher interacts with every terminal
ack decision (`AckOk`, `AckDeadLetter`, `AckHalt`) and with `AckRetry`'s
delayed-re-emission path, so it cannot be built without the full ack surface. EP-7
soft-depends on EP-5 (to add a `PerStreamOrderingDemo` example and per-stream
integration tests into the jitsurei package if it exists) and EP-8 (to add a README
section for the feature). If EP-5 or EP-8 have not landed when EP-7 begins, EP-7
includes a standalone harness for its integration tests and writes its README
section as an in-plan fragment that EP-8 later incorporates.

EP-8 hard-depends on EP-7 — the last feature plan. The release CHANGELOG, README,
and Haddock must describe per-stream dispatch alongside everything else, so EP-7
must land before EP-8 can produce the canonical `0.1.0.0` release notes. EP-8
soft-depends on EP-2 through EP-6 because each adds modules, fields, examples, or a
separate package that the release describes. Running EP-8 before any soft-dep plan
lands is permitted but yields an incomplete release; the soft-dep label communicates
"wait for these in practice." EP-8 is intended as the **last act before tagging a
release**.

**Parallelism opportunities.** Once EP-1 is complete, EP-6 (benchmarks) can run in
parallel with the entire feature track (EP-2 through EP-5, EP-7) because it depends
only on the main library being importable. Once EP-2 is complete, EP-3 and EP-4 can
run in parallel on separate branches. They touch `Shibuya.Adapter.MessageDb.Config`
and `Shibuya.Adapter.MessageDb.Internal` but at different call sites (retry buffer
vs. partition filter). Merging order should be EP-3 first, then EP-4, because EP-4's
integration tests exercise retry behavior. Once EP-3 is complete, EP-7 (per-stream
dispatch) can run in parallel with EP-4 and EP-5 — they touch distinct subsystems
(per-stream FIFO + ack-wrapping vs. category-level hash filter vs. test harness).
EP-8 (release metadata) is the natural sequencing tail and runs last.


## Integration Points

1. **`Shibuya.Adapter.MessageDb.Config.MessageDbAdapterConfig`** — record type defined
   by EP-1, extended by EP-2, EP-3, EP-4, and EP-7. Each extending plan **adds new
   fields** (never renames or removes existing fields) and provides sensible defaults
   via a `defaultConfig` smart constructor. EP-1 owns the initial definition:
   `category`, `batchSize`, `pollInterval`, `drainTimeout`. EP-2 adds
   `subscriptionName :: SubscriptionName`, `checkpointInterval :: NominalDiffTime`.
   EP-3 adds `dlqStrategy :: DlqStrategy`. EP-4 adds
   `consumerGroup :: Maybe ConsumerGroupConfig`. EP-7 adds
   `streamOrdering :: StreamOrderingMode` (default `CategoryUnordered`).

2. **`Shibuya.Adapter.MessageDb.Convert.messageToEnvelope`** — pure conversion function
   defined by EP-1 and consumed unchanged by every later plan. Signature:
   `messageToEnvelope :: MessageDb.Message -> Envelope MessageDb.Message`. Later plans
   must not modify the conversion; they layer partitioning on top by passing only the
   messages that belong to the adapter's partition into the conversion.

3. **Inflight-set STM state** — defined in
   `Shibuya.Adapter.MessageDb.Internal.InflightState` by EP-2 and extended by EP-3 to
   also hold the retry buffer. The two plans must agree that the `InflightState` is an
   opaque type exposed only through `newInflightState`, `recordIngested`,
   `recordAckResult`, and `advanceCheckpointTo` — internal fields are not stable.

4. **`shibuya-message-db-adapter.cabal`** — touched by every plan. Each plan adds its
   new modules to the `exposed-modules` list, adds its dependencies (e.g., EP-2 adds
   `message-db-checkpoint-store`; EP-4 adds `message-db-subscription`), and extends the
   test-suite's `other-modules` as needed. Plans must **not** reorder existing entries;
   additions go at the bottom of their respective lists to minimise diff noise.

5. **`mori.dhall`** — created by EP-1 with the `shibuya-message-db-adapter` package
   entry and baseline dependencies. Extended by EP-5 (adds `shibuya-message-db-adapter-jitsurei`)
   and EP-6 (adds `shibuya-message-db-adapter-bench`). Cross-plan dependency additions
   go into the top-level `dependencies` list in the plan that first introduces them.

6. **Checkpoint table** — the `message-db-checkpoint-store` package owns its own
   Postgres schema and migrations. EP-2 is responsible for invoking the migration as
   part of the adapter's startup when integration tests bootstrap a fresh database;
   EP-5 re-uses the same bootstrap logic in its integration harness.

7. **Per-stream dispatcher state** — introduced in EP-7 as an opaque type in
   `Shibuya.Adapter.MessageDb.Internal.PerStreamDispatch` with operations
   `newPerStreamDispatch`, `enqueueForDispatch`, `yieldReady`, and `releaseStream`.
   EP-7 owns this module; later plans must not touch it. The dispatcher composes
   with EP-2's `InflightState` (the two track different axes — globalPosition for
   checkpointing, stream for ordering) and with EP-3's retry buffer (retries keep
   the per-stream inflight flag set so newer messages for the same stream do not
   jump ahead). EP-7 documents these interactions in its *Context and Orientation*
   section.

8. **Ordering semantics contract** — Shibuya's `QueueProcessor.ordering` field
   (`Shibuya.Policy.Ordering = StrictInOrder | PartitionedInOrder | Unordered`) is
   validated at startup but not enforced by the runner. EP-7 documents that
   `PartitionedInOrder + Async N` is the valid way to run the adapter's per-stream
   ordered dispatch, and that the adapter — not Shibuya — guarantees per-stream
   order by limiting in-flight messages to one per stream. EP-8's README writes
   this contract into the user-facing documentation by inserting EP-7's
   *Documentation Fragment* verbatim.

9. **Release metadata fields** — EP-8 owns the `synopsis`, `description`,
   `homepage`, `bug-reports`, `maintainer`, `copyright`, `license`, `license-file`,
   `extra-doc-files`, `category`, and `source-repository head` fields on every
   `.cabal` file in the repository. Earlier plans add their packages to
   `cabal.project` and `mori.dhall` and may set placeholder `synopsis` values, but
   the canonical metadata is filled in by EP-8 in a single uniform pass. EP-8 also
   owns the top-level `LICENSE`, `README.md`, and `CHANGELOG.md` files.


## Progress

Use this aggregate checklist to mirror milestone-level state across all child plans.
Each entry is added or checked off when the corresponding milestone closes in its child
plan.

- [ ] EP-1: Project scaffolding (cabal.project, .cabal, flake, Justfile, mori.dhall) compiles.
- [ ] EP-1: `Shibuya.Adapter.MessageDb.Config` and `.Convert` implemented with unit tests passing.
- [ ] EP-1: `messageDbAdapter` produces a polling stream with stubbed ack handling; demo executable prints messages from a local Postgres.
- [ ] EP-2: `InflightState` and contiguous-prefix checkpoint advancement implemented.
- [ ] EP-2: `message-db-checkpoint-store` integrated; checkpoints persisted periodically and on shutdown.
- [ ] EP-2: Resume-from-checkpoint verified with an integration test.
- [ ] EP-3: `AckRetry delay` re-emits messages without advancing the checkpoint.
- [ ] EP-3: `AckDeadLetter` with `DlqSkipAndLog` and `DlqWriteToStream` strategies implemented.
- [ ] EP-3: `AckHalt` invokes adapter shutdown and preserves checkpoint.
- [ ] EP-4: `ConsumerGroupConfig` filters incoming messages by Murmur3-64 of category.
- [ ] EP-4: Partition field populated on the envelope; partition-scoped checkpoint name.
- [x] EP-5: Jitsurei package scaffolded with BasicConsumer, RetryDemo, DeadLetterDemo, CheckpointRestart, MultiPartition examples.
- [x] EP-5: Integration test suite running against `ephemeral-pg` covers full lifecycle (basicProduceConsume, checkpointResume, retryReDelivery, deadLetterSkipAndLog, deadLetterWriteToStream, haltPreservesCheckpoint, consumerGroupExactlyOnce).
- [ ] EP-6: `shibuya-message-db-adapter-bench` package scaffolded; tasty-bench wired in.
- [ ] EP-6: Conversion benchmarks (`messageToEnvelope`, `extractTraceContext`) reporting timings.
- [ ] EP-6: InflightState benchmarks added (conditional on EP-2) or skipped with Decision Log entry.
- [ ] EP-6: categoryPartition benchmark added (conditional on EP-4) or skipped with Decision Log entry.
- [ ] EP-7: `StreamOrderingMode` config and per-stream dispatcher implemented.
- [ ] EP-7: Per-stream ordering preserved under concurrent dispatch (unit tests).
- [ ] EP-7: Integration test with interleaved writes across streams proves per-stream order + cross-stream parallelism.
- [ ] EP-7: `PerStreamOrderingDemo` jitsurei example shipped.
- [ ] EP-7: README section and CHANGELOG entry documenting `PartitionedInOrder` semantics (drafted as fragment for EP-8).
- [ ] EP-8: LICENSE, README, and CHANGELOG written at repo root.
- [ ] EP-8: `.cabal` metadata (synopsis, description, homepage, source-repository) filled on every package; `cabal check` clean.
- [ ] EP-8: Haddock sweep — module and function docs on the public surface; `-Wmissing-docs` clean on the main library.
- [ ] EP-8: `cabal sdist all` dry-run produces tarballs for every package.


## Surprises & Discoveries

(None yet.)


## Decision Log

- Decision: The adapter's payload type is the `MessageDb.Message` record, not a decoded
  domain type or `ByteString`.
  Rationale: message-db's messages are already structured JSON with rich metadata
  (stream name, position, global position, messageType, correlation, causation). The
  user's handler needs this context to route by `messageType`. Forcing a JSON decode at
  the adapter boundary would strip information and tie the adapter to a single decoder
  strategy. Users decode inside their handler, by pattern-matching on `messageType` and
  running `Aeson.fromJSON` on `messageData`.
  Date: 2026-04-18

- Decision: Re-use `message-db-checkpoint-store` for checkpoint persistence rather than
  building a new checkpoint scheme in the adapter.
  Rationale: The checkpoint-store package already handles the schema, migrations,
  `SubscriptionName`-keyed lookup, and an in-memory backend for tests. Duplicating it in
  the adapter would create a second, incompatible source of truth.
  Date: 2026-04-18

- Decision: Contiguous-prefix checkpoint advancement via in-process STM, not a
  per-message ack table.
  Rationale: message-db has no per-message ack mechanism (unlike PGMQ's visibility
  timeout or SQS). The adapter must translate Shibuya's per-message AckDecision into a
  position-advancement decision. Storing an ack table per message would reimplement half
  of PGMQ; using STM to track the inflight set and advance only through contiguous
  `AckOk|AckDeadLetter` results is correct, cheap, and survives restarts because any
  un-acked inflight messages are simply re-polled from the last persisted checkpoint.
  Date: 2026-04-18

- Decision: `AckRetry` is implemented as an in-process delayed re-emission into the
  adapter's stream, not by rewinding the checkpoint or sleeping the entire subscription.
  Rationale: Rewinding the checkpoint would force replay of every message between the
  failure and now, which breaks independent retry. Sleeping the subscription would stall
  other messages. An in-process retry buffer keyed by (globalPosition, notBefore) lets
  the handler re-try the one message at its requested delay without affecting siblings.
  Date: 2026-04-18

- Decision: Dead-letter strategies are pluggable with two built-ins: `DlqSkipAndLog` and
  `DlqWriteToStream Stream`. The adapter does not define a mandatory DLQ stream schema.
  Rationale: Applications differ. Some want failed messages dropped-with-logging for
  analytics; others want a durable DLQ for ops tooling. Making this configurable with
  two sensible defaults (plus an escape hatch `DlqCustom (Message -> DeadLetterReason -> Eff es ())`
  considered for EP-3) matches Shibuya's "adapter expresses mechanics" posture.
  Date: 2026-04-18

- Decision: The adapter's integration tests run against `shinzui/ephemeral-pg`, and the
  process-compose.yaml is retained for dev-loop productivity only.
  Rationale: `ephemeral-pg` gives hermetic tests that don't require the developer to
  have a running process-compose. The Justfile's `process-up` remains available for
  interactive experimentation via `cabal run` on the jitsurei examples.
  Date: 2026-04-18

- Decision: Consumer-group partitioning uses the same Murmur3-64 hash of the **category
  name** as `message-db-subscription`'s Consumer partitioning. The adapter exposes the
  hash function through a re-export so examples can compute the expected partition
  deterministically.
  Rationale: Using a different hash would partition the same category differently across
  adapters in the same consumer group, which defeats the purpose of partitioning.
  Date: 2026-04-18

- Decision: Per-stream ordered dispatch (EP-7) is an in-adapter responsibility, not a
  `shibuya-core` change. The adapter's source stream yields at most one in-flight
  message per `MessageDb.Stream` at any time; Shibuya's `Async N` concurrency naturally
  fans the source across up to N streams concurrently.
  Rationale: `shibuya-core/src/Shibuya/Policy.hs:20` declares
  `PartitionedInOrder` as a valid ordering policy and
  `shibuya-project/shibuya/docs/BROADWAY_COMPARISON.md:92` explicitly states that
  "the framework doesn't route messages by partition key—this is left to the adapter."
  Implementing this inside the adapter is the intended path.
  Date: 2026-04-18

- Decision: Split the original EP-6 (Benchmarks and release metadata) into EP-6
  (Benchmarks only) and a new EP-8 (Release metadata and Hackage prep). The EP-6
  file was renamed `docs/plans/6-benchmarks.md`; EP-8 lives at
  `docs/plans/8-release-metadata.md`. EP-7's references to EP-6's README/CHANGELOG
  fragment slots were redirected to EP-8.
  Rationale: Benchmarks and release metadata serve different timing constraints.
  Benchmarks land as soon as the main library exists (EP-1) — they're a
  regression-tracking concern that benefits from being available early. Release
  metadata is the closing act that must describe every shipped feature including
  per-stream dispatch (EP-7). Bundling them forced one of two bad choices: either
  delay benchmarks until every feature plan landed, or ship release notes that
  ignored later-landing features. Splitting decouples the two timelines and lets
  EP-8 own the canonical `0.1.0.0` release as the natural sequencing tail.
  Date: 2026-04-18

- Decision: Per-stream dispatch was made its own plan (EP-7) rather than being folded
  into EP-4 (consumer-group partitioning).
  Rationale: The two features address different scaling axes — EP-4 distributes a
  category across process-level consumer-group members; EP-7 distributes a category's
  streams across fibers within a single process. They compose (an EP-4 member can also
  use EP-7 dispatch internally) but the implementations are independent. Merging them
  would have made EP-4 too large and conflated two distinct abstractions.
  Date: 2026-04-18

- Decision: On `AckRetry`, the per-stream inflight flag stays set (the stream remains
  "in flight" until a terminal disposition).
  Rationale: If the flag cleared on `AckRetry`, newer messages for the same stream would
  be yielded before the retry landed, violating per-stream ordering. Keeping the flag
  set until the retry reaches AckOk or AckDeadLetter preserves ordering; the cost is
  that a stream with a permanently-failing handler stalls indefinitely. EP-3's
  max-retry-buffer-overflow-to-DLQ rule bounds this.
  Date: 2026-04-18


## Outcomes & Retrospective

(To be filled during and after implementation.)


## Revisions

- 2026-04-18: Split EP-6 (Benchmarks and release metadata) into EP-6 (Benchmarks
  only) and a new EP-8 (Release metadata and Hackage prep). Renamed
  `docs/plans/6-benchmarks-and-release.md` to `docs/plans/6-benchmarks.md` and
  trimmed its content to Milestones 1-4 (scaffold, conversion benchmarks,
  conditional InflightState benchmarks, conditional categoryPartition benchmark).
  Created `docs/plans/8-release-metadata.md` with the release content (LICENSE /
  README / CHANGELOG, .cabal polish, Haddock sweep, sdist dry-run). EP-6's deps
  changed from `Hard EP-1, Soft EP-5+EP-7` to `Hard EP-1, Soft EP-2+EP-4`. EP-8
  added with `Hard EP-7, Soft EP-2..EP-6`. EP-7's soft-deps updated from
  `EP-5, EP-6` to `EP-5, EP-8` (it now hands its README fragment to EP-8 instead
  of EP-6). Progress checklist regrouped accordingly. Decision Log entry added.
  Integration Points #8 redirected from EP-6 to EP-8, and a new #9 documents
  EP-8's ownership of release-metadata fields.
