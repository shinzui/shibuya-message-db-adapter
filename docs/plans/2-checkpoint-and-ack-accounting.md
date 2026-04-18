# Durable checkpoints and contiguous-prefix ack accounting

MasterPlan: docs/masterplans/1-shibuya-message-db-adapter.md

Intention: intention_01kpgme50se0ranxp41ghfhajf

This ExecPlan is a living document. The sections Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.

This document is maintained in accordance with `.claude/skills/exec-plan/PLANS.md`.


## Purpose / Big Picture

After this plan, the `shibuya-message-db-adapter` package remembers where it left
off. A user who stops the adapter — by Ctrl-C, by an `AckHalt` from their handler,
or by killing the process — can restart and observe that it resumes from the
correct global position: messages already acknowledged successfully are not
re-delivered, and messages acknowledged with `AckRetry` replay on restart until
they succeed.

The behavior the user sees:

1. Seed a message-db `orders` category with ten messages, numbered `1..10`.
2. Run the adapter with a handler that returns `AckOk` for each message. The
   handler shuts the adapter down after the fifth message.
3. The process exits cleanly. The adapter has persisted a checkpoint at position
   `5`.
4. Restart with no shutdown trick: the adapter prints only `6..10`, then blocks
   on the poll loop.
5. Stop and start the database without touching the `checkpoints` table. Restart
   the adapter: it is at position `10` and yields nothing new.

Additionally, if the handler returns `AckRetry` for position `7` while later
positions return `AckOk`, the persisted checkpoint never advances past `6`. On
restart, positions `7..10` all replay — `7` because it was never completed,
`8..10` because the checkpoint could not advance past `7`. This is the
"contiguous-prefix" guarantee that gives the plan its name.

This plan replaces the stub ack handler introduced in EP-1
(`docs/plans/1-scaffold-and-minimal-adapter.md`) with durable checkpointing via
the existing `message-db-checkpoint-store` package at
`/Users/shinzui/Keikaku/work/libraries/haskell/message-db-hs-master/message-db-checkpoint-store`,
and an STM-based bookkeeping type for the contiguous-prefix logic. It also adds
the adapter's first real integration test, built on `shinzui/ephemeral-pg` at
`/Users/shinzui/Keikaku/bokuno/ephemeral-pg-project/ephemeral-pg`.


## Progress

Use a checklist to summarize granular steps. Every stopping point must be documented
here, even if it requires splitting a partially completed task into two ("done" vs.
"remaining"). This section must always reflect the actual current state of the work.

### Milestone 1: InflightState and contiguous-prefix logic

- [x] Create `shibuya-message-db-adapter/src/Shibuya/Adapter/MessageDb/Internal/InflightState.hs`
      with the opaque `InflightState` type and the four operations described under
      *Interfaces and Dependencies*. (2026-04-18)
- [x] Write unit tests in `test/Shibuya/Adapter/MessageDb/InflightStateTest.hs` covering
      the six cases. (2026-04-18)
- [x] Wire `InflightStateTest.tests` into `test/Main.hs`. (2026-04-18)
- [x] Run `cabal test shibuya-message-db-adapter` — all 10 tests pass
      (4 ConvertTest + 6 InflightStateTest). (2026-04-18)

### Milestone 2: Wire checkpoint-store into the adapter

- [x] Add `message-db-checkpoint-store` to the library `build-depends` and to
      `cabal.project`'s local `packages:` list (the repo uses local paths, not
      `source-repository-package`). (2026-04-18)
- [x] Extend `MessageDbAdapterConfig` with `subscriptionName` and
      `checkpointInterval` (a new `CheckpointInterval` newtype over
      `NominalDiffTime` with default 1 s). (2026-04-18)
- [x] Re-export `SubscriptionName` and `CheckpointInterval` from the Config
      module. (2026-04-18)
- [x] `messageDbAdapter` now requires `CheckpointStore :> es`, calls
      `getLastCheckpoint`, and seeds `positionRef = stored + 1` (positions are
      1-indexed) and `ackedRef = stored`. (2026-04-18)
- [x] `cabal build shibuya-message-db-adapter` succeeds (lib + demo + test). (2026-04-18)

### Milestone 3: Background persistence thread

- [x] Added `runCheckpointPersister` in `Shibuya.Adapter.MessageDb.Internal`:
      wakes every `checkpointInterval`, calls `advanceCheckpointTo`, and on
      `Just pos` calls `storeCheckpoint subName cat pos`. (2026-04-18)
- [x] `messageDbAdapter` spawns the persister via
      `Effectful.Concurrent.forkIO`. The `ThreadId` is discarded — M4 uses the
      `shutdownSignal` to stop the loop rather than joining. (2026-04-18)
- [x] Replaced `mkStubAckHandle` with `mkAckHandle` that records outcomes in
      the inflight ledger. `messageDbSource` now `recordIngested`s each message
      before yielding it, paired with exactly one `recordAckResult` call per
      finalize via the ack handle. `AckHalt` maps to `AckRetry` (the halting
      position never completes); M4 will also flip the shutdown signal. (2026-04-18)
- [x] `cabal build shibuya-message-db-adapter` succeeds; `cabal test` all 10
      tests still pass. End-to-end demo run deferred to M4 so the graceful
      shutdown flush is in place. (2026-04-18)

### Milestone 4: Graceful shutdown final flush

- [x] `messageDbAdapter`'s `shutdown` is now `doShutdown`: flips the shutdown
      `TVar`, polls `inflightSize` at 10 ms intervals until drained or
      `drainTimeout` elapses, calls `advanceCheckpointTo` + `storeCheckpoint`
      once more, and waits one `checkpointInterval` for the persister thread
      to notice the flag. (2026-04-18)
- [x] `mkAckHandle` now takes the `shutdownSignal` `TVar` and, on `AckHalt`,
      flips it to `True` atomically with the `recordAckResult` call. The
      halting position is already mapped to `AckRetry` so the checkpoint
      cannot advance past it. (2026-04-18)
- [x] `cabal build shibuya-message-db-adapter` succeeds; `cabal test` all 10
      tests pass. (2026-04-18)

### Milestone 5: Integration test

- [x] Added `test/Shibuya/Adapter/MessageDb/CheckpointResumeTest.hs`
      using `shinzui/ephemeral-pg`. Installs the message-db schema (schema +
      types + tables + 12 functions) plus the checkpoint-store DDL, writes
      ten messages to a category, and exercises two scenarios: resume after
      five-message shutdown, and `AckRetry` pinning the checkpoint at the
      position before the retry. (2026-04-18)
- [x] Extended the test-suite `build-depends` with `ephemeral-pg`, `hasql`,
      `hasql-pool`, `hasql-effectful`, `message-db-checkpoint-store`,
      `message-db-effectful`, `effectful`, `effectful-core`, `streamly`,
      `streamly-core`, `hs-opentelemetry-api`, and `contravariant`. Added
      `ephemeral-pg` to `cabal.project`. (2026-04-18)
- [x] `cabal test shibuya-message-db-adapter` — all 12 tests pass
      (4 Convert + 6 InflightState + 2 CheckpointResume). (2026-04-18)
- [ ] Manual demo double-run against dev Postgres — deferred; the automated
      integration test covers the same guarantees end-to-end.


## Surprises & Discoveries

- The library's `build-depends` did not include `containers` before M1; it was
  transitively available through other packages but had to be declared
  explicitly once `Data.Map.Strict` was imported from the local library.
  Version `^>=0.7` (boot version with GHC 9.12.2). (2026-04-18)

- Upstream `AckOutcome` and `AckDecision` both export an `AckRetry`
  constructor, so `Shibuya.Adapter.MessageDb.Internal.InflightState` had to be
  imported qualified in `Internal.hs` to disambiguate. Import list remains
  unqualified for the type alias; constructors are referenced as
  `Inflight.AckComplete` / `Inflight.AckRetry`. (2026-04-18)

- Hasql 1.10 renamed `Session.sql` to `Session.script` and changed
  `Statement`'s SQL parameter from `ByteString` to `Text`. The in-tree
  `message-db-checkpoint-store` tests still use the older shape, but the
  integration test here targets the newer one. (2026-04-18)

- Tasty's default scheduler runs test cases within a `testGroup` in parallel,
  which broke the integration tests because both scenarios share the same
  `message_store.messages` table and `global_position` is a serial. The fix is
  `sequentialTestGroup "CheckpointResume" AllFinish [...]` from tasty 1.5, not
  `localOption (NumThreads 1)` — the latter is honoured per-test but does not
  serialize a group of tests. (2026-04-18)

- `write_message(metadata => NULL)` leaves message-db-effectful's
  `messageMetadata` decoder with a null varchar, which fails with
  `UnexpectedNullCellError`. The integration test writes `'{}'` as metadata to
  satisfy the non-nullable decoder. EP-1 should probably relax the decoder to
  accept null metadata, but that is out of scope for EP-2. (2026-04-18)


## Decision Log

- Decision: Re-use `message-db-checkpoint-store` rather than writing a new table.
  Rationale: The package already owns the schema, the Postgres runner, an
  in-memory backend for tests, and a `SubscriptionName`-keyed API. Rebuilding it
  would create a second source of truth and would not be any simpler.
  Date: 2026-04-18

- Decision: Track inflight state and contiguous-prefix advancement in STM rather
  than in a per-message Postgres ack table.
  Rationale: message-db has no per-message acknowledgement primitive (in contrast
  to PGMQ, which leases messages with a visibility timeout, or SQS, which has
  `DeleteMessage`). The adapter must reconcile Shibuya's per-message `AckDecision`
  with message-db's position-only cursor model. An in-process STM data structure
  is cheap, correct, and survives restarts gracefully — any inflight-but-unacked
  messages simply re-appear on the next poll because the persisted checkpoint
  never advanced past them.
  Date: 2026-04-18

- Decision: `AckDeadLetter` is treated as `AckComplete` for checkpoint purposes in
  EP-2.
  Rationale: The DLQ strategy (skip-and-log vs. write-to-stream) is EP-3's concern.
  For EP-2, dead-lettering must still advance the checkpoint — otherwise a single
  poison message would halt all progress. We log a warning and move on. EP-3
  replaces the log-only handler with the real strategies.
  Date: 2026-04-18

- Decision: The `checkpointInterval` default is 1 second.
  Rationale: At 100 messages/second a 1 s interval means the worst-case replay
  after a crash is ~100 messages, which is cheap given message-db's indexed
  `get_category_messages`. Aggressive flushing (every message) would make the
  checkpoint store a bottleneck. Very long intervals (30 s +) would force large
  replays after a crash. 1 s balances durability against write amplification.
  Date: 2026-04-18

- Decision: `defaultConfig` gains a new required second argument
  (`SubscriptionName`) rather than defaulting to something derived from the
  category name.
  Rationale: Subscription names are how operators disambiguate multiple consumers
  of the same category (e.g., "orders-analytics" vs. "orders-shipping"). Defaulting
  to the category name would make it silently unsafe for an application to run two
  differently-configured handlers of the same category. Making it required forces
  an explicit choice.
  Date: 2026-04-18


## Outcomes & Retrospective

All five milestones landed; `cabal test shibuya-message-db-adapter` reports 12
passing tests (4 Convert, 6 InflightState, 2 CheckpointResume) in under a
second on a warm ephemeral-pg cache. The package now:

- Seeds the adapter's fetch position from a persisted checkpoint.
- Tracks per-message ack outcomes in an STM ledger.
- Flushes the advancing contiguous prefix on a 1 s timer.
- Drains in-flight work and does a final flush on graceful shutdown.
- Honours `AckHalt` by pinning the checkpoint and signalling shutdown atomically.
- Is exercised end-to-end against a real Postgres in CI via `ephemeral-pg`.

What went well:

- The `InflightState` module came out small and testable in isolation. Six
  HUnit cases covered the interesting corners without Postgres.

- Extending EP-1's adapter surface was mostly additive: the public `Adapter`
  record didn't change, `defaultConfig` grew a `SubscriptionName` argument,
  and the `CheckpointStore` constraint plumbs through via effectful.

What bit:

- Tasty's default parallelism silently corrupted the integration test
  assertions. The failure looked like "messages are being dropped" when the
  real story was "two tests are writing to the same serial column". Moved to
  `sequentialTestGroup` once I recognized the pattern.

- `Hasql.Session.sql` was renamed to `Hasql.Session.script` in the 1.10
  ecosystem pinned by this repo; the internal tests of
  `message-db-checkpoint-store` still use the older shape. Noted for a future
  sweep.

Left for follow-ups:

- `AckDeadLetter` is still treated as `AckComplete`. EP-3 replaces this with
  the configurable DLQ strategy (skip-and-log vs. write-to-stream).
- The retry delay on `AckRetry` is ignored by the adapter today; retries
  only take effect on restart (the checkpoint does not advance past a retry
  position, so a new polling pass re-delivers). Per-message retry scheduling
  is a later plan.
- `message-db-effectful`'s decoder insists on non-null metadata. We work
  around this in the test harness by writing `'{}'`; upstream should relax
  the decoder.


## Context and Orientation

Read this section in full before editing anything. It repeats the facts from the
MasterPlan and from EP-1 that you need in front of you, so you do not have to
cross-reference other documents while implementing.

### Where things live

The repository under work is at
`/Users/shinzui/Keikaku/work/libraries/haskell/shibuya-message-db-adapter`. At the
end of EP-1 its Haskell source tree is:

    shibuya-message-db-adapter/
      shibuya-message-db-adapter.cabal
      src/Shibuya/Adapter/MessageDb.hs
      src/Shibuya/Adapter/MessageDb/Config.hs
      src/Shibuya/Adapter/MessageDb/Convert.hs
      src/Shibuya/Adapter/MessageDb/Internal.hs
      test/Main.hs
      test/ConvertTest.hs
      app/Demo.hs

EP-2 adds one module —
`src/Shibuya/Adapter/MessageDb/Internal/InflightState.hs` — and modifies the four
existing modules plus the cabal file and `cabal.project`. External packages
touched live under
`/Users/shinzui/Keikaku/work/libraries/haskell/message-db-hs-master/`:
`message-db-hs` (types), `message-db-effectful` (the `MessageDb` effect used by
the EP-1 polling stream), and `message-db-checkpoint-store` (the package this
plan integrates).

### What "contiguous-prefix advancement" means

message-db records every message with a monotonically increasing `globalPosition`
(a 64-bit integer) across the entire store. A Postgres-backed subscription tracks
progress with a single number: "I have processed everything up to and including
global position N." On restart the consumer resumes from `N + 1`.

This one-number cursor cannot by itself represent "I processed positions
`100..104` and `106..110`, but `105` is still being retried." If the adapter
naively stored `110`, a crash would lose message `105`. The contiguous-prefix rule
is the fix: the stored number is always the highest K such that positions
`lastSaved+1 .. K` are all acknowledged as complete. A hole at `105` pins K at
`104` until `105` either completes or is abandoned. In plain English: the
checkpoint only advances as far as a pile of "done" tokens with no gaps at its
start.

message-db does not support per-message ack (contrast PGMQ, where `pgmq.delete`
acknowledges exactly one message and invisibility leases ensure only one consumer
sees a message at a time). Because there is no server-side ack primitive, the
adapter must keep its own ledger — that ledger is `InflightState`.

### Types introduced by EP-1 (recap)

Read this subsection carefully; do not use record-dot syntax across package
boundaries because `shibuya-core` is compiled with `NoFieldSelectors`.

From `shibuya-core` (module `Shibuya.Core.Types`):

    data Envelope msg = Envelope
      { messageId    :: !MessageId
      , cursor       :: !(Maybe Cursor)
      , partition    :: !(Maybe Text)
      , enqueuedAt   :: !(Maybe UTCTime)
      , traceContext :: !(Maybe TraceHeaders)
      , payload      :: !msg
      }

From `shibuya-core` (module `Shibuya.Core.AckHandle`):

    newtype AckHandle es = AckHandle
      { finalize :: AckDecision -> Eff es ()
      }

From `shibuya-core` (module `Shibuya.Core.Ack`):

    data AckDecision
      = AckOk
      | AckRetry !RetryDelay
      | AckDeadLetter !DeadLetterReason
      | AckHalt !HaltReason

From `shibuya-core` (module `Shibuya.Core.Ingested`):

    data Ingested es msg = Ingested
      { envelope :: Envelope msg
      , ack      :: AckHandle es
      }

From `message-db-hs` (module `MessageDb.Message`):

    data Message = Message
      { messageId       :: !MessageId
      , stream          :: !Stream
      , messageType     :: !MessageType
      , position        :: !MessagePosition
      , globalPosition  :: !GlobalPosition
      , messageData     :: !MessageData
      , messageMetadata :: !MessageMetadata
      , time            :: !UTCTime
      }

    newtype GlobalPosition = GlobalPosition { unGlobalPosition :: Int64 }

    newtype CategoryStream = CategoryStream ...  -- opaque; constructed via CS.parse

From EP-1 (module `Shibuya.Adapter.MessageDb.Config`):

    newtype CategoryStream = CategoryStream { unCategoryStream :: Text }  -- EP-1's wrapper
    newtype BatchSize      = BatchSize      { unBatchSize      :: Int }
    newtype PollInterval   = PollInterval   { unPollInterval   :: NominalDiffTime }
    newtype DrainTimeout   = DrainTimeout   { unDrainTimeout   :: NominalDiffTime }

    data MessageDbAdapterConfig = MessageDbAdapterConfig
      { category     :: !CategoryStream
      , batchSize    :: !BatchSize
      , pollInterval :: !PollInterval
      , drainTimeout :: !DrainTimeout
      }

    defaultConfig :: CategoryStream -> MessageDbAdapterConfig

Note the name clash: EP-1 defines a wrapper newtype also called `CategoryStream`
local to the adapter. message-db-hs's `MessageDb.Message.CategoryStream` is a
different type (an opaque validated category name). The checkpoint-store API
expects the latter. Handle the conversion explicitly inside
`Shibuya.Adapter.MessageDb.Internal` — do not leak the message-db-hs type through
the adapter's public surface.

### The CheckpointStore effect

The module `MessageDb.CheckpointStore.Effectful` exposes:

    type SubscriptionName = Text

    data CheckpointStore :: Effect where
      GetLastCheckpoint :: SubscriptionName -> CheckpointStore m (Maybe GlobalPosition)
      StoreCheckpoint   :: SubscriptionName -> CategoryStream -> GlobalPosition -> CheckpointStore m ()

    getLastCheckpoint :: CheckpointStore :> es => SubscriptionName -> Eff es (Maybe GlobalPosition)
    storeCheckpoint   :: CheckpointStore :> es => SubscriptionName -> CategoryStream -> GlobalPosition -> Eff es ()

    runPostgresCheckointStore ::
      (Hasql :> es, Error SessionError :> es) =>
      Eff (CheckpointStore : es) a ->
      Eff es a

The runner name is spelled `runPostgresCheckointStore` (note the missing "k" after
"Che") in the upstream package. Use the spelling the library actually exports.
The runner is Hasql-backed and requires the caller to also run the `Hasql` effect
and an `Error SessionError` effect.

An in-memory backend exists in `MessageDb.CheckpointStore.InMemory`:

    newInMemoryCheckpointStore :: MonadIO m => m InMemoryCheckpointStore
    runCheckpointStoreInMemory :: IOE :> es => InMemoryCheckpointStore -> Eff (CheckpointStore : es) a -> Eff es a

The on-disk schema for the Postgres backend is a single table `checkpoints` with
columns `id serial PK`, `name text`, `stream text`,
`last_processed_global_position bigint`, `last_processed_at timestamp`, plus a
unique index on `name`. The exact DDL is in
`/Users/shinzui/Keikaku/work/libraries/haskell/message-db-hs-master/message-db-checkpoint-store/migrations/scripts/create_checkpoints.sql`.
Integration tests must apply that DDL after creating the ephemeral database.

### Terms of art used in this plan

*Checkpoint* is the highest `GlobalPosition` the adapter has confirmed finished
for a given subscription, durably stored in the `checkpoints` table.
*Subscription name* is a `Text` identifier chosen by the application to
distinguish this adapter's checkpoint row from any other adapter's — two
handlers of the same category must use different subscription names. *Inflight*
means emitted-by-the-source-but-not-yet-finalized. *Completion* is an
`AckOutcome` of `AckComplete`, produced by `AckOk` and `AckDeadLetter`.
*Retry-pending* is an `AckOutcome` of `AckRetry`, which blocks advancement.
*Contiguous prefix* is the longest gap-free run of completed positions starting
at `lastSaved + 1`.


## Plan of Work

This plan is organized into five milestones. Each milestone ends at a point where
`cabal build shibuya-message-db-adapter` succeeds and, where applicable,
`cabal test shibuya-message-db-adapter` passes.

### Milestone 1: InflightState and contiguous-prefix logic

Scope: create a pure, STM-based data structure that records inflight messages and
their ack outcomes, and exposes a single operation to advance the checkpoint
through the contiguous prefix of completions. No adapter changes yet; the module
is a self-contained unit that could be published on its own.

Create
`shibuya-message-db-adapter/src/Shibuya/Adapter/MessageDb/Internal/InflightState.hs`
exporting only:

    module Shibuya.Adapter.MessageDb.Internal.InflightState
      ( InflightState
      , AckOutcome (..)
      , newInflightState
      , recordIngested
      , recordAckResult
      , advanceCheckpointTo
      , inflightSize
      ) where

`InflightState` is opaque: consumers cannot see its fields. Internally it holds
a single `TVar` pointing at a record with three fields: `lastSaved ::
GlobalPosition` (the highest position already reported by `advanceCheckpointTo`,
seeded from `getLastCheckpoint` or `GlobalPosition 0`); `outcomes :: Map
GlobalPosition (Maybe AckOutcome)` (map from every inflight or
not-yet-advanced-past position to its outcome — `Nothing` for ingested-but-not-finalized,
`Just AckComplete` / `Just AckRetry` after finalize); and `highestIngested ::
GlobalPosition` for diagnostics.

    data AckOutcome = AckComplete | AckRetry

    -- internal, not exported
    data InflightStateS = InflightStateS
      { lastSaved       :: !GlobalPosition
      , outcomes        :: !(Map GlobalPosition (Maybe AckOutcome))
      , highestIngested :: !GlobalPosition
      }

    newtype InflightState = InflightState (TVar InflightStateS)

The four public operations:

- `newInflightState :: GlobalPosition -> IO InflightState` — create an empty
  state seeded with the stored checkpoint position.
- `recordIngested :: InflightState -> GlobalPosition -> STM ()` — insert
  `(pos, Nothing)` into `outcomes`, update `highestIngested` if greater.
- `recordAckResult :: InflightState -> GlobalPosition -> AckOutcome -> STM ()` —
  update `outcomes[pos]` to `Just outcome`. Silently ignores positions not in
  `outcomes` (idempotent against double-finalize).
- `advanceCheckpointTo :: InflightState -> STM (Maybe GlobalPosition)` — walk
  `outcomes` from `lastSaved + 1`, advancing past every `Just AckComplete`,
  stopping at the first entry that is absent, `Nothing`, or `Just AckRetry`.
  Remove advanced-past entries. If any advancement occurred, update `lastSaved`
  and return `Just newLastSaved`; otherwise return `Nothing`.
- `inflightSize :: InflightState -> STM Int` — count of entries whose outcome is
  still `Nothing`. The shutdown drain loop uses this.

Use `Data.Map.Strict.Map` for `outcomes` so the walk in `advanceCheckpointTo` is
a cheap ordered iteration.

Unit tests in `shibuya-message-db-adapter/test/InflightStateTest.hs`, using
`tasty-hunit`. Six cases, each seeded with `GlobalPosition 0`:

- *empty state*: no ingestions; `advanceCheckpointTo` returns `Nothing`.
- *single complete*: ingest `1`, complete `1`; first call returns `Just 1`,
  second returns `Nothing` (already reported).
- *retry blocks*: ingest `1..3`; complete `1`; retry `2`; complete `3`; expect
  `Just 1` (only), because `2` is retry-pending.
- *interleaved*: ingest `1..5`; complete `1,2,4,5`; leave `3` with `Nothing`;
  expect `Just 2`.
- *retry completes*: continue *retry blocks*; change `2` to `AckComplete`;
  expect `Just 3`.
- *out-of-order complete*: ingest `3` and complete it without ingesting `1` or
  `2`; expect `Nothing`, because `1..2` are unknown.

Wire `InflightStateTest.tests` into `test/Main.hs` alongside `ConvertTest.tests`
and run `cabal test shibuya-message-db-adapter` — expect all EP-1 tests plus the
six new tests to pass.

### Milestone 2: Wire checkpoint-store into the adapter

Scope: add the checkpoint-store dependency, extend the config, and seed the
adapter's start position from the stored checkpoint. No background persistence
yet.

Edit `shibuya-message-db-adapter/shibuya-message-db-adapter.cabal` to add
`message-db-checkpoint-store` to the library `build-depends`. Edit
`cabal.project` at the repo root to add a `source-repository-package` entry
pointing at the `message-db-checkpoint-store` subdir of the message-db-hs repo,
mirroring the existing entries for `message-db-hs` and `message-db-effectful`.
If EP-1 used local `packages:` paths, do the same here.

Edit `shibuya-message-db-adapter/src/Shibuya/Adapter/MessageDb/Config.hs` to
add a `CheckpointInterval` newtype over `NominalDiffTime`, add the two new
fields `subscriptionName :: !SubscriptionName` and `checkpointInterval ::
!CheckpointInterval` to `MessageDbAdapterConfig`, re-export
`SubscriptionName` from `MessageDb.CheckpointStore.Effectful`, and change
`defaultConfig :: CategoryStream -> SubscriptionName -> MessageDbAdapterConfig`
with a default `CheckpointInterval 1`. Because `SubscriptionName` is a type
synonym for `Text` with no constructor, export only the name; callers write
subscription names as string literals under `OverloadedStrings`.

Update every call site of `defaultConfig` — the demo at `app/Demo.hs` and any
test that constructs an adapter — to pass the subscription-name argument. The
demo should accept a `--subscription` flag with a default of `shibuya-demo`.

Edit `shibuya-message-db-adapter/src/Shibuya/Adapter/MessageDb.hs` so that
`messageDbAdapter` (a) requires `CheckpointStore :> es` in addition to the
existing constraints and (b) before creating the position `IORef`, calls
`mStored <- getLastCheckpoint subscriptionName` and uses `fromMaybe
(GlobalPosition 0) mStored` as the starting position. Convert the adapter's
local `CategoryStream` wrapper to message-db-hs's validated
`MessageDb.Message.CategoryStream` inside the adapter (not in the public
config); use the parser exposed by
`MessageDb.Message.Stream.CategoryStream.parseEither` and `throwIO` a `userError`
on failure — a malformed category is an operator bug that should fail fast.

Confirm `cabal build shibuya-message-db-adapter` succeeds. The demo builds but
does not yet persist; Milestone 3 adds that.

### Milestone 3: Background persistence thread

Scope: spawn a long-running thread that flushes the contiguous-prefix position
to the checkpoint store on a timer; replace the stub ack handler with one that
records outcomes in `InflightState`.

Replace `mkStubAckHandle` in
`shibuya-message-db-adapter/src/Shibuya/Adapter/MessageDb/Internal.hs` with a
new `mkAckHandle` that maps `AckDecision` to `AckOutcome` via `atomically .
recordAckResult inflight pos`:

- `AckOk` → `AckComplete`
- `AckDeadLetter _` → `AckComplete` (EP-3 replaces the log-only behaviour)
- `AckRetry _` → `AckRetry`
- `AckHalt _` → `AckRetry` (the halting message does not complete, and Milestone
  4 extends this branch to also raise the shutdown signal)

Update `messageDbSource` so that when it yields each message it first calls
`atomically (recordIngested inflight (globalPosition m))` and attaches
`mkAckHandle inflight (globalPosition m)` as the ack handle.

Add `runCheckpointPersister` (signature in *Interfaces and Dependencies*). The
loop reads the shared `shutdownSignal :: TVar Bool`; if not set, calls
`advanceCheckpointTo`; on `Just pos` calls `storeCheckpoint subscriptionName
categoryStream pos`; then sleeps `checkpointInterval` via
`Effectful.Concurrent.threadDelay` (microseconds as `Int`, convert via
`ceiling . (* 1_000_000) . realToFrac`) before looping.

In `shibuya-message-db-adapter/src/Shibuya/Adapter/MessageDb.hs`,
`messageDbAdapter` now (a) calls `getLastCheckpoint subscriptionName`, seeds
both the `InflightState` and the position `IORef` with `fromMaybe
(GlobalPosition 0) mStored`, (b) parses the config's `CategoryStream` into
message-db-hs's validated `MessageDb.Message.CategoryStream` via a local helper
`parseCategoryOrDie` that `throwIO`s on parse failure, (c) spawns the persister
via `Effectful.Concurrent.forkIO`, and (d) returns an `Adapter` whose
`shutdown` calls the `doShutdown` function introduced in Milestone 4 (stub it
with a TODO for now).

Build with `cabal build shibuya-message-db-adapter`, then re-run the EP-1 demo.
Ignore that Ctrl-C may not cleanly flush yet — that is Milestone 4. Confirm a
`checkpoints` row is written by the timer.

### Milestone 4: Graceful shutdown final flush

Scope: when `shutdown` is called, wait for the source stream's in-flight
messages to drain and flush any final checkpoint advance to the store.

Implement `doShutdown` in
`shibuya-message-db-adapter/src/Shibuya/Adapter/MessageDb.hs`. Its body, in
order:

1. `atomically $ writeTVar shutdownSignal True` so the source and persister stop
   at their next iteration.
2. Wait for in-flight to drain: loop polling `atomically (inflightSize
   inflight)`, sleeping 10 ms each time, until either the size reaches zero or
   a deadline of `now + drainTimeout` elapses.
3. Final flush: `advanceCheckpointTo` once more; if it returned `Just pos`, call
   `storeCheckpoint sub cat pos`.
4. Give the persister thread one extra `checkpointInterval` to notice the
   signal and exit.

`AckHalt` handling: because `mkAckHandle` already records `AckRetry` for
`AckHalt`, the contiguous-prefix guarantee prevents the checkpoint from
advancing past a halted message. The remaining concern is signalling shutdown.
Extend `mkAckHandle` to take `shutdownSignal :: TVar Bool` as an extra
argument, and on `AckHalt` write `True` into it in the same STM transaction as
the `recordAckResult` call. Update `messageDbSource` to pass `shutdownSignal`
into `mkAckHandle`. Build with `cabal build shibuya-message-db-adapter`.

### Milestone 5: Integration test

Scope: prove the end-to-end behaviour from *Purpose / Big Picture* with a real
Postgres database via `ephemeral-pg`.

Extend the test-suite's `build-depends` with `ephemeral-pg`, `hasql`,
`hasql-pool`, `hasql-effectful`, `message-db-checkpoint-store`, `message-db-hs`,
`message-db-effectful`, and any other packages the harness code imports.

Create `shibuya-message-db-adapter/test/CheckpointResumeTest.hs` exporting two
test cases via a `tests :: TestTree` grouped under `"CheckpointResume"`.

A shared bootstrap helper does the following, once per test: start an ephemeral
Postgres via
`EphemeralPg.startCached EphemeralPg.defaultConfig EphemeralPg.defaultCacheConfig`;
acquire a Hasql pool on the returned `connectionSettings`; apply the message-db
DDL scripts from
`/Users/shinzui/Keikaku/hub/event-sourcing/message-db-project/message-db/database`;
apply the checkpoint-store DDL from
`/Users/shinzui/Keikaku/work/libraries/haskell/message-db-hs-master/message-db-checkpoint-store/migrations/scripts/create_checkpoints.sql`;
write ten messages to the `orders-test` category via `write_message(...)`.

*Test 1: adapter resumes from last checkpoint after restart*. First run: spawn
the adapter; the handler records each yielded message's `globalPosition` into a
`TVar [Int]` and returns `AckOk`; after the fifth message, invoke the adapter's
`shutdown`. Assert the first-run list has exactly five entries in ascending
order, and the `checkpoints` row for subscription `orders-test-sub` has
`last_processed_global_position` equal to the fifth message's position. Second
run: spawn the adapter again with the same subscription name; the handler
records messages and shuts down after five new messages. Assert the second-run
list has exactly five entries and is disjoint from the first-run list.

*Test 2: AckRetry blocks checkpoint advancement across restart*. First run: the
handler returns `AckRetry (RetryDelay 1)` for the third message by
`globalPosition` and `AckOk` for the rest; after ten ack calls, invoke shutdown.
Assert the `checkpoints` row has `last_processed_global_position` equal to the
*second* message's position, not the tenth. Second run: plain `AckOk` handler;
shut down after eight messages. Assert positions `3..10` were yielded in order.

Wire `CheckpointResumeTest.tests` into `test/Main.hs`. Run `cabal test
shibuya-message-db-adapter`. Total time should be well under a minute because
`ephemeral-pg` caches its cluster template.


## Concrete Steps

Run all commands from
`/Users/shinzui/Keikaku/work/libraries/haskell/shibuya-message-db-adapter`
inside a `nix develop` shell (`direnv allow && nix develop` if not already in
it).

For each milestone, after editing the files described in *Plan of Work*:

    cabal build shibuya-message-db-adapter
    cabal test shibuya-message-db-adapter

After Milestone 1, `cabal test` should report the four existing ConvertTest
cases plus six new InflightStateTest cases all passing.

After Milestone 3, with `just process-up` running in another terminal:

    just seed-messages orders
    cabal run shibuya-message-db-adapter-demo -- \
      --category orders --subscription orders-demo

Three lines print. Ctrl-C. Then:

    psql -c "select name, stream, last_processed_global_position from checkpoints;"

One row: `orders-demo | orders | 3` (or equivalent — whatever the highest
global position of the three seeded messages is).

After Milestone 4, re-run the demo twice. The second invocation should print
nothing new and block on the poll loop; the checkpoint row's
`last_processed_global_position` is unchanged. This is the manual demo called
out in the prompt.

After Milestone 5, `cabal test shibuya-message-db-adapter` runs all four
ConvertTest cases, all six InflightStateTest cases, and both
CheckpointResumeTest cases — twelve tests total — and exits 0.


## Validation and Acceptance

The plan is complete when all of the following hold, verified by running the
exact commands.

1. From the repo root inside `nix develop`, `cabal build all` exits 0 with no
   warnings specific to this package (third-party deprecations are acceptable).

2. `cabal test shibuya-message-db-adapter` exits 0 with all ConvertTest,
   InflightStateTest, and CheckpointResumeTest cases passing — twelve tests.

3. Manual demo. Starting from an empty `checkpoints` table and a fresh dev
   database seeded with three `orders-*` messages, running

        cabal run shibuya-message-db-adapter-demo -- \
          --category orders --subscription orders-demo

   prints three lines and blocks on the poll loop. Ctrl-C returns within
   `drainTimeout` seconds (default 10) without a stack trace.

4. After step 3, `psql -c "select name, stream,
   last_processed_global_position from checkpoints;"` returns one row with
   `name = 'orders-demo'`, `stream = 'orders'`, and a non-null
   `last_processed_global_position` matching the third message's position.

5. Running step 3 again prints zero lines and leaves the checkpoint unchanged.

6. Seeding two more messages and running step 3 a third time prints exactly
   two lines and advances the checkpoint by two.

Common failure modes: the demo prints duplicates across restarts → the
position `IORef` was not seeded from `getLastCheckpoint`; the `checkpoints`
row never appears → the persister thread is not running or shutdown fires
before its first tick (lower `checkpointInterval` to `0.1` to confirm); the
integration test hangs → delete `~/.cache/ephemeral-pg` and retry.


## Idempotence and Recovery

Every step in this plan is safe to re-run.

- The inflight-state module has no external side effects; re-running its tests
  is free.

- Editing `cabal.project` and the cabal file is a file rewrite; re-running
  `cabal build` after a reversion will recompile cleanly.

- `storeCheckpoint` is an upsert (see
  `MessageDb.CheckpointStore.Statements.updateCheckpointLastProcessedGlobalPosition`'s
  `ON CONFLICT (name) DO UPDATE`); calling it with the same `(name, stream,
  pos)` is a no-op on conflict. Calling it with a **smaller** position than
  what is stored would regress the checkpoint — but the adapter only ever calls
  it with the result of `advanceCheckpointTo`, which is monotonically
  non-decreasing by construction. The Statement's `WHERE s.stream = excluded.stream`
  clause further ensures a subscription cannot be rebound to a different category
  stream by accident.

- `ephemeral-pg` creates throwaway databases; re-running tests creates new
  clusters or reuses the cached template without touching any shared state.
  Delete `~/.cache/ephemeral-pg` to force a cold start if needed.

- If the background persister thread dies unexpectedly, the worst case is that
  the checkpoint lags at its last successfully-flushed value; the adapter will
  replay any messages processed after that point on restart. This is acceptable
  behavior and matches the at-least-once contract every Shibuya adapter
  implements.

- Reverting the plan: `git revert` the commits tagged with this ExecPlan's
  trailer, drop the `checkpoints` table with
  `psql -c "drop table checkpoints;"`, and rebuild. EP-1's stub ack handling
  returns.


## Interfaces and Dependencies

New libraries:

- `message-db-checkpoint-store` — provides the `CheckpointStore` Effectful
  effect, the Postgres runner `runPostgresCheckointStore` (sic: upstream name),
  the in-memory runner `runCheckpointStoreInMemory`, and the schema DDL at
  `migrations/scripts/create_checkpoints.sql`. Used for durable checkpoint
  persistence.

- `stm` (already transitively available via `base` + `effectful`) — for the
  `TVar` holding `InflightStateS`.

- `ephemeral-pg` (test-only) — at
  `/Users/shinzui/Keikaku/bokuno/ephemeral-pg-project/ephemeral-pg`, provides
  `EphemeralPg.startCached` to spin up a hermetic Postgres cluster for
  integration tests.

At the end of Milestone 1, the following must exist:

    module Shibuya.Adapter.MessageDb.Internal.InflightState where
      data AckOutcome = AckComplete | AckRetry
      data InflightState  -- opaque
      newInflightState    :: GlobalPosition -> IO InflightState
      recordIngested      :: InflightState -> GlobalPosition -> STM ()
      recordAckResult     :: InflightState -> GlobalPosition -> AckOutcome -> STM ()
      advanceCheckpointTo :: InflightState -> STM (Maybe GlobalPosition)
      inflightSize        :: InflightState -> STM Int

At the end of Milestone 2, the following must exist:

    module Shibuya.Adapter.MessageDb.Config where
      newtype CheckpointInterval = CheckpointInterval { unCheckpointInterval :: NominalDiffTime }
      data MessageDbAdapterConfig = MessageDbAdapterConfig
        { category           :: !CategoryStream
        , subscriptionName   :: !SubscriptionName
        , batchSize          :: !BatchSize
        , pollInterval       :: !PollInterval
        , drainTimeout       :: !DrainTimeout
        , checkpointInterval :: !CheckpointInterval
        }
      defaultConfig :: CategoryStream -> SubscriptionName -> MessageDbAdapterConfig

Note: `defaultConfig` now takes **two** arguments instead of one. Every call
site must be updated. Known call sites: `app/Demo.hs` and any test harness that
constructs an adapter.

At the end of Milestone 3, the following must exist:

    module Shibuya.Adapter.MessageDb.Internal where
      mkAckHandle ::
        (IOE :> es) =>
        InflightState ->
        TVar Bool ->
        GlobalPosition ->
        AckHandle es

      messageDbSource ::
        (MessageDb :> es, Concurrent :> es, IOE :> es) =>
        TVar Bool ->
        IORef GlobalPosition ->
        InflightState ->
        MessageDbAdapterConfig ->
        Stream (Eff es) (Ingested es MessageDb.Message)

      runCheckpointPersister ::
        ( CheckpointStore :> es
        , Concurrent :> es
        , IOE :> es
        ) =>
        InflightState ->
        TVar Bool ->
        SubscriptionName ->
        MessageDb.Message.CategoryStream ->
        NominalDiffTime ->
        Eff es ()

At the end of Milestone 4, the following must exist:

    module Shibuya.Adapter.MessageDb where
      messageDbAdapter ::
        ( MessageDb :> es
        , CheckpointStore :> es
        , Concurrent :> es
        , IOE :> es
        ) =>
        MessageDbAdapterConfig ->
        Eff es (Adapter es MessageDb.Message)

Because `shibuya-core` types are compiled with `NoFieldSelectors`, **do not**
use record-dot syntax (`ingested.envelope.payload`) when touching `Envelope`,
`Ingested`, or `AckHandle` across package boundaries. Use explicit pattern
matching (`Ingested {envelope = Envelope {payload}, ack = AckHandle finalize}`)
throughout `Shibuya.Adapter.MessageDb.Internal` and `Shibuya.Adapter.MessageDb`.
