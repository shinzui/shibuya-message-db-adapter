# Per-stream ordered dispatch (PartitionedInOrder)

MasterPlan: docs/masterplans/1-shibuya-message-db-adapter.md

Intention: intention_01kpgme50se0ranxp41ghfhajf

This ExecPlan is a living document. The sections Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.

This document is maintained in accordance with `.claude/skills/exec-plan/PLANS.md`.


## Purpose / Big Picture

After this plan is complete, a user can configure `shibuya-message-db-adapter` so that
messages belonging to the same `MessageDb.Stream` (for example `orders-42`) are handled
strictly in their message-db write order, while messages on different streams within
the same category (for example `orders-42` and `orders-43`) are handled concurrently.
This is the Kafka-style "ordered by partition, parallel across partitions" semantic —
in message-db terms, ordered by stream, parallel across streams.

The user configures this by setting
`streamOrdering = PerStreamInOrder { maxConcurrentStreams = 16 }` on the adapter and
pairing the processor with `QueueProcessor { ordering = PartitionedInOrder,
concurrency = Async 16 }` in Shibuya. The framework will then dispatch up to 16
concurrent handlers, but the adapter guarantees each concurrent handler is on a
distinct stream. Per-stream order is preserved because the adapter's source never
yields a second message for a stream until the first message's ack has been finalized.

The user-observable outcome: given three streams in one category, writes interleaved
(`orders-42` position 1, `orders-43` position 1, `orders-42` position 2, `orders-44`
position 1, `orders-43` position 2, `orders-42` position 3), the handler receives
each stream's messages in strict ascending position order regardless of the order
messages were written across streams, and the wall-clock latency for processing all
of them is bounded by the longest-single-stream latency rather than the total sum.

This plan delivers the full feature: the dispatcher implementation, unit tests,
integration tests against an ephemeral Postgres, a runnable jitsurei example, and a
README section. Per-stream ordering is a crucial correctness requirement for
event-sourced consumers; its tests are exhaustive by design.


## Progress

Use a checklist to summarize granular steps. Every stopping point must be documented
here, even if it requires splitting a partially completed task into two ("done" vs.
"remaining"). This section must always reflect the actual current state of the work.

### Milestone 1: Config surface and data structures

- [ ] Define `StreamOrderingMode` sum type in `Shibuya.Adapter.MessageDb.Config`.
- [ ] Extend `MessageDbAdapterConfig` with `streamOrdering :: StreamOrderingMode`
      (additive; default `CategoryUnordered`).
- [ ] Update `defaultConfig` so existing call sites keep the old semantics.
- [ ] Define `PerStreamDispatchState` opaque type in
      `Shibuya.Adapter.MessageDb.Internal.PerStreamDispatch` (new module).
- [ ] Export `newPerStreamDispatch`, `enqueueForDispatch`, `yieldReady`,
      `releaseStream`, `activeStreamCount` from that module.
- [ ] Compile with `cabal build shibuya-message-db-adapter`.

### Milestone 2: Ingest and dispatcher wiring

- [ ] Rework `messageDbSource` in `Shibuya.Adapter.MessageDb.Internal` so that when
      `streamOrdering = PerStreamInOrder{}` it funnels every polled message through
      the dispatcher; otherwise it uses the EP-4 category-filter path unchanged.
- [ ] At pull time, call `recordIngested inflight (globalPosition m)` for every
      message (EP-2 invariant) **before** enqueueing in the per-stream dispatch.
- [ ] Call `enqueueForDispatch dispatch (stream m) (globalPosition m, m)` for each
      message, in the order returned by `getCategoryMessages`.
- [ ] The dispatcher fiber pulls from its internal `TChan` of ready messages and
      yields `Ingested{envelope, ack}` on the source stream.
- [ ] Compile and verify the EP-3 ack path still works unmodified for
      `CategoryUnordered`.

### Milestone 3: Ack-wrapping and retry composition

- [ ] Wrap the `AckHandle` produced for each ingested message so that on any
      **terminal** ack (`AckOk`, `AckDeadLetter`, `AckHalt`) the handler first invokes
      the EP-3 ack logic, then calls `releaseStream dispatch streamName` to clear the
      per-stream inflight flag and release the next queued message.
- [ ] On `AckRetry`, the EP-3 retry buffer is used as before, **but the per-stream
      inflight flag is NOT cleared** — the stream stays marked in-flight until the
      retry reaches a terminal state. Document this invariant.
- [ ] When the retry fiber re-emits a retried message, it bypasses the per-stream
      FIFO and delivers directly to the handler (the stream is still considered in
      flight, and the re-emitted message is the one keeping it so).
- [ ] Compile and run the existing EP-2/EP-3 test suites to confirm no regressions.

### Milestone 4: Backpressure and concurrency bounds

- [ ] Enforce `maxConcurrentStreams` — the dispatcher holds at most this many
      streams as "active" (one in-flight message each); additional ready streams
      wait in a `TQueue StreamName` of pending releases.
- [ ] Enforce `maxQueuedTotal :: Int` (default 10000) — a cap on the total number
      of buffered messages across all per-stream FIFOs. When reached, the adapter
      stops polling message-db until the total drops below the threshold.
- [ ] Add `streamOrderingStats :: PerStreamDispatchState -> STM DispatchStats` for
      introspection by `shibuya-metrics`.

### Milestone 5: Unit tests

- [ ] `test/PerStreamDispatchTest.hs`: pure STM tests for the dispatcher with the
      eight scenarios listed under *Validation and Acceptance*.
- [ ] Tests pass: `cabal test shibuya-message-db-adapter --test-options="-p PerStream"`.

### Milestone 6: Integration tests

- [ ] Extend the integration harness (EP-5's `TestEnv`, or a local harness if EP-5
      is incomplete) to expose a `writeInterleaved :: TestEnv -> [(Stream, Int)] -> IO ()`
      helper that writes messages across multiple streams in a specified interleaving.
- [ ] `integrationTestPerStreamOrder`: the eleven scenarios listed under
      *Validation and Acceptance* under "Integration tests".
- [ ] Tests pass: `cabal test shibuya-message-db-adapter --test-options="-p PerStreamIT"`.

### Milestone 7: `PerStreamOrderingDemo` jitsurei example

- [ ] Add executable `PerStreamOrderingDemo` to `shibuya-message-db-adapter-jitsurei`
      (if the package exists; otherwise create it as a standalone executable in a
      temporary subdirectory and fold it in when EP-5 lands).
- [ ] The demo writes messages across three streams, runs the adapter with
      `PerStreamInOrder { maxConcurrentStreams = 3 }` and `Async 3`, and prints
      `[stream=X pos=Y thread=Z] handled` lines to stdout so the user can visually
      confirm per-stream order and cross-stream concurrency.
- [ ] Document how to run the demo in the plan's *Concrete Steps* section.

### Milestone 8: Documentation

- [ ] Add a dedicated README section "Per-stream ordering" to
      `/README.md` (if EP-6 has landed) OR to this plan's *Documentation Fragment*
      section as a verbatim block that EP-6 copies into the README.
- [ ] Add a CHANGELOG entry under "Unreleased" describing the feature, the new
      config field, and a short note about the PartitionedInOrder contract.
- [ ] Add a module-level Haddock comment on
      `Shibuya.Adapter.MessageDb.Internal.PerStreamDispatch` explaining the
      invariants, the retry interaction, and the memory-bound semantics.


## Surprises & Discoveries

(None yet.)


## Decision Log

- Decision: `PerStreamInOrder` is the only per-stream ordering mode, parameterised
  only by `maxConcurrentStreams`.
  Rationale: "Per-stream order with bounded concurrency" covers the full
  `PartitionedInOrder` contract. A finer-grained mode (e.g., hash-bucket dispatch
  that serializes hash-equal streams rather than all streams) is imaginable but
  unnecessary — the framework can achieve the same by hashing to a smaller stream
  namespace.
  Date: 2026-04-18

- Decision: On `AckRetry`, the per-stream inflight flag stays set.
  Rationale: If the flag cleared on retry, a newer message for the same stream
  could be yielded before the retry completed, violating per-stream order. The cost
  is that a permanently-failing handler stalls its stream indefinitely, which is
  bounded by EP-3's `maxRetryBufferSize` downgrade-to-DLQ rule.
  Date: 2026-04-18

- Decision: Retries bypass the per-stream FIFO and re-deliver directly.
  Rationale: The retried message is the one keeping the stream in flight; queueing
  it behind itself would deadlock. EP-3's retry fiber already has the message and
  its stream identity, so it can call an internal `deliverRetry` path that emits
  to the source stream without going through `enqueueForDispatch`.
  Date: 2026-04-18

- Decision: Messages are `recordIngested` (EP-2 checkpoint bookkeeping) at **pull
  time**, not at **yield time**.
  Rationale: Pull-time registration ensures `advanceCheckpointTo` never returns a
  position past an unhandled message. Yield-time registration would risk advancing
  the checkpoint past a message that is still queued in a per-stream FIFO, causing
  data loss on restart. The tests in Milestone 5 assert this explicitly.
  Date: 2026-04-18

- Decision: `PartitionedInOrder` policy enforcement is the adapter's responsibility,
  not `shibuya-core`'s, consistent with Shibuya's documented design.
  Rationale: `shibuya-core/src/Shibuya/Policy.hs:20` declares the policy;
  `shibuya-project/shibuya/docs/BROADWAY_COMPARISON.md:92` states explicitly that
  the framework does not route by partition key. Our adapter fulfils the contract.
  Date: 2026-04-18


## Outcomes & Retrospective

(To be filled during and after implementation.)


## Context and Orientation

The reader is assumed to know Haskell and to have read or worked through EP-1 through
EP-3 of this MasterPlan. If you come to EP-7 cold, read `docs/plans/1-scaffold-and-minimal-adapter.md`,
`docs/plans/2-checkpoint-and-ack-accounting.md`, and `docs/plans/3-retry-dlq-halt.md`
first — they establish the types, effects, and bookkeeping that this plan composes
with. Even without doing so, the next paragraphs give you enough to succeed.

**Terms of art used in this plan.** A "stream" in message-db is the unit of strict
write ordering — `Stream.stream` in the `MessageDb.Message` record. Stream names
take the form `category-entityId` (e.g., `orders-42`) or `category` (e.g.,
`$all`-style projections). A "category" is the prefix of the stream name before the
first hyphen (here, `orders`). A subscription consumes a category. Within a
category, individual streams (entities) can be written to independently; message-db
preserves per-stream ordering via optimistic concurrency (`expectedPosition`), but
across streams within a category only a monotonic **global position** is
guaranteed — there is no inter-stream ordering guarantee. A "partition" in Shibuya
is whatever the adapter maps `Envelope.partition :: Maybe Text` to; in this plan, a
partition is a stream name.

**The Shibuya adapter contract (recap from EP-1).** The adapter returns an
`Adapter es msg`:

    data Adapter es msg = Adapter
      { adapterName :: !Text
      , source      :: Stream (Eff es) (Ingested es msg)
      , shutdown    :: Eff es ()
      }

`Ingested` pairs an `Envelope` with an `AckHandle`:

    data Ingested es msg = Ingested
      { envelope :: Envelope msg
      , ack      :: AckHandle es
      }

    newtype AckHandle es = AckHandle
      { finalize :: AckDecision -> Eff es ()
      }

The handler's return value — one of `AckOk`, `AckRetry RetryDelay`,
`AckDeadLetter DeadLetterReason`, `AckHalt HaltReason` — is passed to `finalize`
after the handler completes. `shibuya-core` uses `NoFieldSelectors`, so across
package boundaries always pattern-match explicitly:

    handle (Ingested{envelope, ack = AckHandle finalize}) = ...

**Shibuya's Ordering policy (crucial for this plan).** At
`/Users/shinzui/Keikaku/bokuno/shibuya-project/shibuya/shibuya-core/src/Shibuya/Policy.hs:20`:

    data Ordering
      = StrictInOrder       -- Event-sourced subscriptions - must be Serial
      | PartitionedInOrder  -- Kafka-style - parallel across partitions
      | Unordered           -- No ordering guarantees

    validatePolicy :: Ordering -> Concurrency -> Either PolicyError ()
    validatePolicy StrictInOrder (Ahead _) = Left $ InvalidPolicyCombo "..."
    validatePolicy StrictInOrder (Async _) = Left $ InvalidPolicyCombo "..."
    validatePolicy _             _         = Right ()

`PartitionedInOrder + Async N` is a valid combination. However,
`Shibuya.App.spawnOne` in
`/Users/shinzui/Keikaku/bokuno/shibuya-project/shibuya/shibuya-core/src/Shibuya/App.hs:208`
pattern-matches `QueueProcessor adapter handler _ordering concurrency` and passes
only `concurrency` to `runSupervised` — the `ordering` field is **not** enforced at
runtime. Shibuya's own docs at
`/Users/shinzui/Keikaku/bokuno/shibuya-project/shibuya/docs/BROADWAY_COMPARISON.md:92`
state this explicitly:

    `PartitionedInOrder` policy documents the contract but doesn't enforce it. The
    framework doesn't route messages by partition key—this is left to the adapter.

So the adapter is responsible for guaranteeing per-partition (here, per-stream)
order. This plan's dispatcher is how we satisfy that contract.

**The EP-2 `InflightState` contract (recap).** `InflightState` is an opaque STM
structure that tracks in-flight global positions and computes the highest
contiguous-prefix position that has been acknowledged. Its operations:

    newInflightState :: GlobalPosition -> STM InflightState
    recordIngested :: InflightState -> GlobalPosition -> STM ()
    recordAckResult :: InflightState -> GlobalPosition -> AckOutcome -> STM ()
        where AckOutcome = AckComplete | AckRetry
    advanceCheckpointTo :: InflightState -> STM (Maybe GlobalPosition)

A checkpoint-persistence fiber periodically calls `advanceCheckpointTo` and
persists the returned position via `message-db-checkpoint-store`.

**The EP-3 retry buffer (recap).** When a handler returns `AckRetry delay`, EP-3
pushes `(globalPosition, notBefore, message)` to a `TQueue` and marks the position
as `AckRetry` in `InflightState`. A fiber polls the queue and re-emits entries
whose `notBefore` has elapsed. Until the retried message reaches a terminal
decision, its position cannot advance the checkpoint. EP-3 also has an overflow
rule: if the retry buffer reaches `maxRetryBufferSize`, further `AckRetry`
decisions are downgraded to `AckDeadLetter (MaxRetriesExceeded)`.

**The message-db `Message` type and stream name extraction.** From
`/Users/shinzui/Keikaku/work/libraries/haskell/message-db-hs-master/message-db-hs/src/MessageDb/Message.hs`:

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

The `Stream` type has `Stream.stream` (the full stream name) and
`Stream.category` (just the category prefix). In this plan we key the dispatcher
by the full `Stream` value (or its Text representation), not by category — two
different streams within the same category are processed concurrently.

**Memory model.** The per-stream dispatcher holds three in-memory maps keyed by
stream name: a FIFO of pending messages per stream, an in-flight flag per stream,
and a count of active streams. These are STM-managed. The memory cost is
proportional to the number of distinct active streams times the average pending
queue depth; `maxQueuedTotal` bounds this. On a typical workload (1000 streams,
occasional retries, 10 msgs/sec per stream), steady-state memory is negligible.


## Plan of Work

### Milestone 1: Config surface and data structures

At the end of this milestone, the adapter's config exposes `streamOrdering` and the
dispatcher module compiles with unimplemented stubs.

In `shibuya-message-db-adapter/src/Shibuya/Adapter/MessageDb/Config.hs`, add:

    data StreamOrderingMode
      = CategoryUnordered
      -- ^ All messages in the category are eligible for concurrent dispatch with
      -- no per-stream ordering (the default; matches EP-1 through EP-6 behavior).
      | PerStreamInOrder PerStreamConfig
      -- ^ Per-stream ordered dispatch: at most one in-flight message per
      -- 'MessageDb.Stream' at a time; messages for different streams dispatch
      -- concurrently.
      deriving (Eq, Show)

    data PerStreamConfig = PerStreamConfig
      { maxConcurrentStreams :: !Int
      -- ^ The dispatcher will keep at most this many streams in "active" state
      -- (one in-flight message each). Additional streams with queued messages
      -- wait for a slot. Typically set to match the processor's 'Async' N.
      , maxQueuedTotal       :: !Int
      -- ^ Upper bound on total queued messages across all streams. When reached,
      -- the adapter stops polling message-db until the total drops below the
      -- threshold. Default: 10000.
      }
      deriving (Eq, Show)

Extend `MessageDbAdapterConfig`:

    data MessageDbAdapterConfig = MessageDbAdapterConfig
      { category             :: !CategoryStream
      , batchSize            :: !BatchSize
      , pollInterval         :: !PollInterval
      , drainTimeout         :: !DrainTimeout
      , subscriptionName     :: !SubscriptionName      -- EP-2
      , checkpointInterval   :: !NominalDiffTime       -- EP-2
      , dlqStrategy          :: !DlqStrategy           -- EP-3
      , maxRetryBufferSize   :: !Int                   -- EP-3
      , consumerGroup        :: !(Maybe ConsumerGroupConfig)  -- EP-4
      , streamOrdering       :: !StreamOrderingMode    -- EP-7 (this plan)
      }

Update `defaultConfig` to set `streamOrdering = CategoryUnordered`. The default
preserves pre-EP-7 behavior for all existing users.

Add a helper:

    defaultPerStreamConfig :: PerStreamConfig
    defaultPerStreamConfig = PerStreamConfig
      { maxConcurrentStreams = 16
      , maxQueuedTotal       = 10000
      }

Create `shibuya-message-db-adapter/src/Shibuya/Adapter/MessageDb/Internal/PerStreamDispatch.hs`:

    module Shibuya.Adapter.MessageDb.Internal.PerStreamDispatch
      ( PerStreamDispatch  -- opaque
      , DispatchStats (..)
      , newPerStreamDispatch
      , enqueueForDispatch
      , yieldReady
      , releaseStream
      , deliverRetry
      , activeStreamCount
      , totalQueuedCount
      , dispatchStats
      ) where

The module owns the full per-stream state. The opaque type is a record of STM
variables:

    data PerStreamDispatch = PerStreamDispatch
      { psdConfig      :: !PerStreamConfig
      , psdQueues      :: !(TVar (Map Text (Seq QueuedMessage)))
      , psdInflight    :: !(TVar (Set Text))
      , psdReady       :: !(TQueue Text)  -- streams with a queued message and no inflight
      , psdTotalQueued :: !(TVar Int)
      }

(Where `Text` keys are the full stream name via `Stream.toText`.)

Operations — semantics described in the docstrings; implementation details in
Milestones 2-4:

    newPerStreamDispatch     :: PerStreamConfig -> STM PerStreamDispatch
    enqueueForDispatch       :: PerStreamDispatch -> Text -> QueuedMessage -> STM ()
    yieldReady               :: PerStreamDispatch -> STM (Maybe (Text, QueuedMessage))
    releaseStream            :: PerStreamDispatch -> Text -> STM ()
    deliverRetry             :: PerStreamDispatch -> Text -> STM ()
    activeStreamCount        :: PerStreamDispatch -> STM Int
    totalQueuedCount         :: PerStreamDispatch -> STM Int
    dispatchStats            :: PerStreamDispatch -> STM DispatchStats

At the end of Milestone 1, these type signatures exist and the module compiles
with `undefined` or `stm retry` bodies. The adapter's behavior is unchanged.

### Milestone 2: Ingest and dispatcher wiring

At the end of this milestone, messages pulled from message-db are enqueued into
the per-stream dispatcher (when `PerStreamInOrder` is configured) and yielded
to the source one at a time per active stream.

The source stream's inner loop in `Shibuya.Adapter.MessageDb.Internal` branches on
`streamOrdering`:

    case streamOrdering cfg of
      CategoryUnordered -> existingCategoryUnorderedLoop cfg ...
      PerStreamInOrder psCfg -> perStreamLoop cfg psCfg ...

The `perStreamLoop`:

1. Pulls a batch via `MessageDb.getCategoryMessages` as before.
2. For each message `m`, atomically:
    - `recordIngested inflight (globalPosition m)` (EP-2 invariant — pull-time
      registration).
    - `enqueueForDispatch dispatch (streamKey m) (globalPosition m, m)` where
      `streamKey = Stream.toText . MessageDb.stream`.
3. A separate dispatcher fiber, spawned at adapter startup, loops:
    - `atomically (yieldReady dispatch)` blocks until a stream has a queued
      message and no inflight message.
    - Wraps the resulting `QueuedMessage` into `Ingested{envelope, ack}` where
      `envelope = messageToEnvelope m` with `partition = Just (streamKey m)`.
    - Emits onto the Streamly stream that is the adapter's `source`.

`yieldReady` internally: pop the next stream name from `psdReady`, drop the head
of that stream's queue, insert the stream name into `psdInflight`, and return the
message. If `psdReady` is empty, it retries (STM blocks).

`enqueueForDispatch` internally: append the message to `psdQueues[stream]`,
increment `psdTotalQueued`, and — if the stream is not already in
`psdInflight` **and** not already in `psdReady` (i.e., it just became eligible) —
push the stream name onto `psdReady`. Cap on `psdTotalQueued`: if it would exceed
`maxQueuedTotal`, retry (which propagates backpressure to the poll loop, pausing
it until `releaseStream` drains enough).

### Milestone 3: Ack-wrapping and retry composition

At the end of this milestone, every `AckHandle` produced by the dispatcher path is
wrapped so that terminal ack decisions release the stream's inflight slot, and
retries keep the slot held.

The handle construction, conceptually:

    mkPerStreamAckHandle :: ... -> Text -> GlobalPosition -> Message -> AckHandle es
    mkPerStreamAckHandle ctx streamKey pos msg = AckHandle $ \decision -> do
      existingEp3AckLogic ctx pos msg decision   -- EP-3's retry/DLQ/halt handling
      case decision of
        AckOk            -> liftIO . atomically $ releaseStream (psdOf ctx) streamKey
        AckDeadLetter _  -> liftIO . atomically $ releaseStream (psdOf ctx) streamKey
        AckHalt _        -> pure ()   -- adapter is shutting down; no release needed
        AckRetry _       -> pure ()   -- stream stays inflight; EP-3 handles re-delivery

`releaseStream`: remove `streamKey` from `psdInflight`. If `psdQueues[streamKey]`
still has queued messages, push `streamKey` onto `psdReady`. This is the
"unblock the next message for this stream" step.

**Retry composition.** EP-3's retry fiber, when it re-emits a message, must call
`deliverRetry dispatch streamKey` instead of `enqueueForDispatch`. `deliverRetry`:
inserts the retried message at the **head** of `psdQueues[streamKey]` (not the
tail), and — because the stream is still in `psdInflight` from the first delivery
— does NOT push onto `psdReady`. The retry fiber then needs to yield the message
directly to the source stream via the same output channel the dispatcher uses.

Concretely, EP-7 exposes:

    sourceOutput :: PerStreamDispatch -> TChan (Ingested es Message)

Both the dispatcher fiber and the retry fiber write to `sourceOutput`. The
`source` stream reads from it:

    source = Stream.unfoldrM readNext (sourceOutput dispatch)
      where
        readNext chan = do
          i <- atomically (readTChan chan)
          pure (Just (i, chan))

Because the retry fiber writes directly to `sourceOutput` (bypassing
`yieldReady`), the stream's inflight slot remains held through retries, preserving
per-stream order.

### Milestone 4: Backpressure and concurrency bounds

At the end of this milestone, the dispatcher respects `maxConcurrentStreams` and
`maxQueuedTotal`.

`maxConcurrentStreams` cap: `yieldReady` checks `Set.size (psdInflight) <
maxConcurrentStreams` before yielding. If the cap is reached, it retries (STM
blocks) until a `releaseStream` decrements the count. Note this is a correctness
concern only when `maxConcurrentStreams < Async N` — otherwise Shibuya's
`Async N` naturally caps concurrency before the dispatcher does. Setting them
equal is the recommended configuration.

`maxQueuedTotal` cap: `enqueueForDispatch` retries if `psdTotalQueued + 1 >
maxQueuedTotal`. Because `enqueueForDispatch` is called from the poll loop, this
pauses polling, applying backpressure upstream to message-db. `releaseStream`
decrements `psdTotalQueued` when a message is actually dequeued (in `yieldReady`)
OR when a retry completes terminally.

Add `DispatchStats`:

    data DispatchStats = DispatchStats
      { dsActiveStreams   :: !Int
      , dsQueuedStreams   :: !Int
      , dsTotalQueued     :: !Int
      , dsReadyStreamsLen :: !Int
      }
      deriving (Eq, Show)

Exposed for `shibuya-metrics` integration; users can also pull it via a demo
introspection hook.

### Milestone 5: Unit tests

See *Validation and Acceptance* for the exhaustive list. All tests are pure STM
tests — no I/O, no Postgres, no Shibuya runtime. They exercise the dispatcher in
isolation using the `PerStreamDispatch` API.

### Milestone 6: Integration tests

See *Validation and Acceptance* for the full scenario list. Tests use the
ephemeral-pg harness (from EP-5 if available; otherwise a local harness in
`test/PerStreamIT.hs` that inlines enough of the bootstrap to be self-contained).

### Milestone 7: `PerStreamOrderingDemo` jitsurei example

Add executable to `shibuya-message-db-adapter-jitsurei/shibuya-message-db-adapter-jitsurei.cabal`
(or a standalone cabal stanza if the jitsurei package does not yet exist):

    executable PerStreamOrderingDemo
      hs-source-dirs:   src
      main-is:          PerStreamOrderingDemo.hs
      build-depends:    base, shibuya-message-db-adapter, shibuya-core, shibuya-app,
                        message-db-hs, message-db-effectful, effectful,
                        optparse-applicative, text, async, stm
      default-language: GHC2021
      ghc-options:      -Wall -threaded

The demo in `PerStreamOrderingDemo.hs`:

1. Writes 30 messages: 10 to `orders-42`, 10 to `orders-43`, 10 to `orders-44`.
   The writes are interleaved deterministically so global-position ordering does
   not coincide with per-stream ordering.
2. Runs the adapter with `streamOrdering = PerStreamInOrder (PerStreamConfig 3
   10000)` and a processor configured `ordering = PartitionedInOrder,
   concurrency = Async 3`.
3. The handler sleeps for a random 100-500ms (simulating variable work), then
   prints `[stream=%s pos=%d thread=%d t=%s] handled` with the current
   `ThreadId`, `getCurrentTime`, and the envelope's partition and cursor.
4. After 30 messages are processed, prints a per-stream summary: each stream's
   `[pos]` sequence must be `[1, 2, 3, ..., 10]` in exact order.
5. Asserts (via a simple `error` on mismatch) that per-stream order is preserved.

### Milestone 8: Documentation

Write the README section described in *Documentation Fragment* below. If
`/README.md` exists (EP-6 done), insert it. Otherwise, the fragment lives in this
plan's *Documentation Fragment* section for EP-6 to copy verbatim.

Add to `/CHANGELOG.md` (or EP-6's CHANGELOG fragment):

    ## [Unreleased]

    ### Added
    - `StreamOrderingMode` config with `PerStreamInOrder` variant: enforces
      per-stream order and delivers cross-stream parallelism. Fulfils
      `shibuya-core`'s `PartitionedInOrder` ordering contract.
    - `Shibuya.Adapter.MessageDb.Internal.PerStreamDispatch` module exposing
      dispatcher state for advanced users and metrics integration.

Add a comprehensive module-level Haddock to
`Shibuya.Adapter.MessageDb.Internal.PerStreamDispatch` covering:

    * The invariant "at most one in-flight message per stream"
    * How retries preserve the invariant (deliverRetry, no slot release on AckRetry)
    * The memory bound (maxQueuedTotal) and its backpressure semantics
    * The interaction with EP-2 InflightState (orthogonal axis)
    * The Shibuya PartitionedInOrder contract and Policy.hs:20 reference


## Concrete Steps

Run from `/Users/shinzui/Keikaku/work/libraries/haskell/shibuya-message-db-adapter`.

Scaffolding and compile check:

    mkdir -p shibuya-message-db-adapter/src/Shibuya/Adapter/MessageDb/Internal
    # Create and edit the files enumerated in Milestone 1.
    cabal build shibuya-message-db-adapter

Expected: compile succeeds with only "defined but not used" warnings for the
stubbed dispatcher operations.

Run the unit test suite:

    cabal test shibuya-message-db-adapter --test-options="-p PerStream"

Expected output (partial):

    shibuya-message-db-adapter-tests
      PerStreamDispatch
        single stream yields in write order:                      OK
        two streams interleaved preserve per-stream order:        OK
        maxConcurrentStreams caps inflight count:                 OK
        AckRetry holds the stream's inflight slot:                OK
        terminal ack releases the next queued message:            OK
        maxQueuedTotal blocks enqueue on overflow:                OK
        deliverRetry inserts at head, bypasses psdReady:          OK
        empty state blocks yieldReady until enqueue:              OK

    All 8 tests passed

Run the integration test suite (requires `nix develop`):

    cabal test shibuya-message-db-adapter --test-options="-p PerStreamIT"

Expected output (partial):

    PerStreamIT
      basic three-stream ordered handoff:                         OK
      interleaved global-position writes preserve stream order:   OK
      slow stream does not block fast streams:                    OK
      handler latency variance preserves per-stream order:        OK
      cross-stream parallelism > 1 under Async 4:                 OK
      AckRetry on middle message preserves stream order:          OK
      permanent handler failure overflows to DLQ, unblocks:       OK
      checkpoint advances contiguously despite reordered acks:    OK
      restart resumes per-stream state correctly:                 OK
      consumer-group + per-stream compose correctly:              OK
      maxQueuedTotal throttling backpressure to poll loop:        OK

    All 11 tests passed

Run the demo (from a `nix develop` shell with `just process-up` and
`just bootstrap-message-db`):

    cabal run PerStreamOrderingDemo

Expected stdout (excerpted — actual interleaving varies but the per-stream
sequences must be monotonic):

    [stream=orders-42 pos=1 thread=ThreadId 14 t=...] handled
    [stream=orders-43 pos=1 thread=ThreadId 15 t=...] handled
    [stream=orders-44 pos=1 thread=ThreadId 16 t=...] handled
    [stream=orders-42 pos=2 thread=ThreadId 14 t=...] handled
    [stream=orders-43 pos=2 thread=ThreadId 15 t=...] handled
    ...
    Per-stream summary:
      orders-42: positions seen in order = [1,2,3,4,5,6,7,8,9,10]
      orders-43: positions seen in order = [1,2,3,4,5,6,7,8,9,10]
      orders-44: positions seen in order = [1,2,3,4,5,6,7,8,9,10]
    All three streams observed strict per-stream order.

Verify Haddock:

    cabal haddock shibuya-message-db-adapter

Expected: no `-Wmissing-docs` warnings on the
`Shibuya.Adapter.MessageDb.Internal.PerStreamDispatch` module.


## Validation and Acceptance

### Unit tests (Milestone 5)

All tests live in `test/PerStreamDispatchTest.hs` and use Tasty with STM
assertions. Each test constructs a fresh `PerStreamDispatch` via
`atomically (newPerStreamDispatch defaultPerStreamConfig)`, exercises it, and
asserts observable state.

1. **"single stream yields in write order"** — enqueue three messages for stream
   A at positions 1, 2, 3. Call `yieldReady` once, assert it returns
   `Just ("orders-42", msg_pos1)`. Call `releaseStream "orders-42"`. Call
   `yieldReady`, assert `msg_pos2`. Repeat, assert `msg_pos3`.
2. **"two streams interleaved preserve per-stream order"** — enqueue A1, B1, A2,
   B2, A3 in that order. Yield five times with `releaseStream` after each.
   Assert the yields are `A1, B1, A2, B2, A3` in some order such that within
   stream A the positions are `[1,2,3]` and within stream B the positions are
   `[1,2]`.
3. **"maxConcurrentStreams caps inflight count"** — set `maxConcurrentStreams =
   2`. Enqueue one message each for streams A, B, C, D. Yield three times
   without releasing. Assert the third yield retries (STM blocks) — use
   `orElse retry (return Nothing)` trick to detect the block within a bounded
   timeout.
4. **"AckRetry holds the stream's inflight slot"** — enqueue A1, A2. Yield once,
   get A1. Do NOT call `releaseStream`. Enqueue A3. Assert `yieldReady`
   retries (stream A is still in-flight; no yield possible for A2 or A3).
5. **"terminal ack releases the next queued message"** — enqueue A1, A2. Yield
   A1. Call `releaseStream "A"`. Yield, assert A2.
6. **"maxQueuedTotal blocks enqueue on overflow"** — set `maxQueuedTotal = 3`.
   Enqueue three messages. Try to enqueue a fourth — assert `enqueueForDispatch`
   retries (STM blocks). Yield one and `releaseStream`, assert enqueue unblocks.
7. **"deliverRetry inserts at head, bypasses psdReady"** — enqueue A1, A2, A3.
   Yield A1 (stream A now inflight). Simulate retry: `deliverRetry "A"` with
   position 1. Release stream A. Yield — assert A1 comes out first (the retry),
   not A2.
8. **"empty state blocks yieldReady until enqueue"** — `yieldReady` on an empty
   dispatcher retries. Enqueue a message from a concurrent STM transaction;
   assert the yield now returns the message.

### Integration tests (Milestone 6)

All tests live in `test/PerStreamIT.hs` (or a moved location once EP-5 lands).
Each test uses `withEphemeralPg` to start a fresh Postgres, applies the
message-db and checkpoint-store schemas, writes messages, runs the adapter in a
bounded time, and asserts on observed deliveries.

1. **"basic three-stream ordered handoff"** — write 3 streams, 5 messages each,
   in round-robin global order. Run adapter with `PerStreamInOrder{
   maxConcurrentStreams = 3}` and `Async 3`. Collect `(stream, position)` pairs
   as the handler sees them. Assert each stream's positions are `[1..5]`.
2. **"interleaved global-position writes preserve stream order"** — write a
   deliberately pathological interleaving (all A's first, then all B's, then
   half A's again via a second batch) and assert per-stream order is still
   `[1..10]` per stream.
3. **"slow stream does not block fast streams"** — two streams. Stream A has a
   handler that sleeps 500ms per message. Stream B's handler returns in 10ms.
   Write 5 messages to each, interleaved. Measure total wall-clock. Assert
   wall-clock < (5 * 500ms) + reasonable overhead — i.e., stream B's messages
   complete while stream A is still working.
4. **"handler latency variance preserves per-stream order"** — random
   100-500ms per message. Run 20 messages per stream across 4 streams. Assert
   per-stream ordering despite the randomness.
5. **"cross-stream parallelism > 1 under Async 4"** — instrument the handler to
   record the maximum number of concurrent `ThreadId`s observed. Assert the
   max is at least 2 (and in practice 3-4) with 4 streams.
6. **"AckRetry on middle message preserves stream order"** — stream A has 5
   messages. Handler returns `AckRetry (RetryDelay 0.1s)` on message 3's first
   delivery, then `AckOk`. Assert the handler sees messages in strict order
   `[1, 2, 3, 3, 4, 5]` (message 3 twice, no message 4 or 5 before the second
   attempt on 3).
7. **"permanent handler failure overflows to DLQ, unblocks"** — stream A with
   `maxRetryBufferSize = 2`. Handler returns `AckRetry` on message 2 always.
   After 2 retries, EP-3's overflow rule downgrades to `AckDeadLetter`. Assert:
   (a) message 2 landed in DLQ; (b) stream A unblocked and message 3 was
   delivered; (c) checkpoint advanced past message 2.
8. **"checkpoint advances contiguously despite reordered acks"** — write 10
   messages across 3 streams. Acks complete in wildly out-of-order fashion
   (fast handler returns immediately; slow handler sleeps 300ms). Periodically
   read the persisted checkpoint. Assert the checkpoint never advances past an
   un-acked position (i.e., contiguous-prefix invariant holds under per-stream
   dispatch).
9. **"restart resumes per-stream state correctly"** — write 10 messages. Run
   the adapter, handler AckOks 7 messages then exits. Verify persisted
   checkpoint equals the 7th message's global position. Restart; assert the
   adapter delivers exactly messages 8-10, in per-stream order.
10. **"consumer-group + per-stream compose correctly"** — configure two adapter
    instances in one process, each with `consumerGroup = ConsumerGroupConfig
    { groupSize = 2, member = 0|1 }` AND `streamOrdering = PerStreamInOrder`.
    Write messages across 4 categories with 3 streams each. Assert each
    message is handled exactly once AND per-stream ordering holds within each
    adapter instance.
11. **"maxQueuedTotal throttling backpressure to poll loop"** — set
    `maxQueuedTotal = 10`, write 100 messages rapidly to one slow stream.
    Instrument `MessageDb.getCategoryMessages` calls to record call frequency.
    Assert poll frequency drops sharply once the queue fills, and resumes when
    the handler drains it.

### Acceptance criteria (plan-level)

The plan is complete when:

- All 8 unit tests pass with `cabal test shibuya-message-db-adapter --test-options="-p PerStream"`.
- All 11 integration tests pass with `cabal test shibuya-message-db-adapter --test-options="-p PerStreamIT"`.
- `cabal run PerStreamOrderingDemo` prints per-stream summaries showing each
  stream's positions in strict `[1..N]` order.
- `cabal haddock shibuya-message-db-adapter` succeeds with no
  `-Wmissing-docs` warnings on the public dispatch module.
- The MasterPlan's Progress checklist items for EP-7 are all checked.
- A reviewer familiar with message-db and Shibuya can read the README section
  (or the *Documentation Fragment* in this plan) and explain, without looking
  at the code, how `PartitionedInOrder + PerStreamInOrder` is enforced.


## Idempotence and Recovery

All dispatcher state is in-memory STM; it is reconstructed on every adapter
restart from the persisted checkpoint (EP-2). The dispatcher itself has no
persisted state.

If a dispatch-time crash occurs between `recordIngested` and the handler
invoking `finalize`, the message's global position remains in `InflightState`
and is never `recordAckResult`-ed. On restart, `advanceCheckpointTo` correctly
refuses to cross it. The adapter restarts from the checkpoint (strictly before
the crashed message's position) and re-polls, re-delivering the lost message.

The per-stream inflight set is purely in-memory — on restart it is empty, which
is the correct post-restart state. The same message will be re-delivered (from
the checkpoint) and re-registered with the dispatcher.

If `PerStreamInOrder` is configured but `deliverRetry` is not wired correctly —
i.e., retries go through `enqueueForDispatch` instead — retries can end up
behind newer messages and per-stream order can be violated for the retried
message only. The unit test "deliverRetry inserts at head, bypasses psdReady"
and the integration test "AckRetry on middle message preserves stream order"
catch this. If both tests pass, the path is correct.

If `maxConcurrentStreams > Async N`, the dispatcher's cap is slack and Shibuya
caps concurrency first. If `maxConcurrentStreams < Async N`, the dispatcher
caps first; this is suboptimal but not incorrect. Setting them equal is
recommended — the demo and integration tests use equal values.

Dropping the dispatcher state (via a full process restart) is safe. Never
modify the `PerStreamDispatch` fields outside of the module's exported
operations.


## Interfaces and Dependencies

No new library dependencies — this plan builds on `stm`, `containers`,
`streamly`, and the modules introduced in EP-1 through EP-4.

At the end of Milestone 1:

    module Shibuya.Adapter.MessageDb.Config where
      data StreamOrderingMode
        = CategoryUnordered
        | PerStreamInOrder PerStreamConfig
        deriving (Eq, Show)

      data PerStreamConfig = PerStreamConfig
        { maxConcurrentStreams :: !Int
        , maxQueuedTotal       :: !Int
        }
        deriving (Eq, Show)

      defaultPerStreamConfig :: PerStreamConfig

      data MessageDbAdapterConfig = MessageDbAdapterConfig
        { ... -- EP-1 through EP-4 fields ...
        , streamOrdering :: !StreamOrderingMode
        }

At the end of Milestone 4:

    module Shibuya.Adapter.MessageDb.Internal.PerStreamDispatch where
      data PerStreamDispatch  -- opaque
      data DispatchStats = DispatchStats
        { dsActiveStreams   :: !Int
        , dsQueuedStreams   :: !Int
        , dsTotalQueued     :: !Int
        , dsReadyStreamsLen :: !Int
        }

      newPerStreamDispatch :: PerStreamConfig -> STM PerStreamDispatch
      enqueueForDispatch   :: PerStreamDispatch -> Text -> (GlobalPosition, Message) -> STM ()
      yieldReady           :: PerStreamDispatch -> STM (Maybe (Text, (GlobalPosition, Message)))
      releaseStream        :: PerStreamDispatch -> Text -> STM ()
      deliverRetry         :: PerStreamDispatch -> Text -> (GlobalPosition, Message) -> STM ()
      activeStreamCount    :: PerStreamDispatch -> STM Int
      totalQueuedCount     :: PerStreamDispatch -> STM Int
      dispatchStats        :: PerStreamDispatch -> STM DispatchStats

The `Shibuya.Adapter.MessageDb` public module re-exports `StreamOrderingMode`,
`PerStreamConfig`, and `defaultPerStreamConfig`. It does **not** re-export
`PerStreamDispatch` — that is internal.


## Documentation Fragment

This fragment belongs in the repository-level README under a heading
"Per-stream ordering (`PartitionedInOrder`)". If EP-6 has landed by the time
this plan is implemented, insert this into `/README.md` directly. Otherwise,
leave it here; EP-6 will copy it verbatim.

    ## Per-stream ordering (PartitionedInOrder)

    message-db preserves strict ordering **within a stream** (e.g., `orders-42`)
    via optimistic concurrency. It does not preserve ordering **across streams**
    in a category — messages for `orders-42` and `orders-43` are only ordered by
    a monotonic global position.

    For event-sourced applications, per-stream ordering is a correctness
    requirement: the state of entity `orders-42` must be reconstructed by
    applying its events in write order. Cross-stream ordering is usually
    irrelevant — `orders-42` and `orders-43` are independent entities.

    This adapter provides **per-stream ordered dispatch**: when configured,
    messages for the same stream are delivered strictly in message-db order
    (one at a time, blocking until the previous message is acknowledged),
    while messages for different streams are dispatched concurrently, bounded
    by `Async N`.

    Configure it two ways:

    1. On the adapter:

           cfg = defaultConfig categoryName subscriptionName
             { streamOrdering = PerStreamInOrder defaultPerStreamConfig
             }

       `defaultPerStreamConfig` is `PerStreamConfig 16 10000` — at most 16
       streams in flight at once, at most 10000 messages buffered across all
       streams.

    2. On the processor:

           QueueProcessor
             { adapter     = messageDbAdapter cfg
             , handler     = myHandler
             , ordering    = PartitionedInOrder
             , concurrency = Async 16
             }

    Set `maxConcurrentStreams` equal to `Async N`. The adapter guarantees per-
    stream order; Shibuya fans out up to N concurrent handlers. Because each
    concurrent handler is on a distinct stream, the guarantee composes.

    **Why this is an adapter responsibility, not framework-level.**
    `shibuya-core` declares `PartitionedInOrder` as a valid ordering contract
    but delegates enforcement to the adapter (see
    `shibuya-core/src/Shibuya/Policy.hs` and the framework's Broadway
    comparison doc). Different queues partition differently — Kafka by
    partition id, SQS FIFO by MessageGroupId, message-db by stream. The
    adapter is the right place to interpret the partition key.

    **How retries interact with per-stream order.** If your handler returns
    `AckRetry delay`, the stream's in-flight slot is held until the retry
    reaches a terminal state (`AckOk` or `AckDeadLetter`). Newer messages for
    the same stream wait. This preserves per-stream order at the cost of
    stalling the stream on a permanently failing handler. The
    `maxRetryBufferSize` config bounds this — after that many retries the
    adapter downgrades to `AckDeadLetter (MaxRetriesExceeded)` and unblocks
    the stream.

    **Memory.** Pending messages queue in memory per-stream.
    `maxQueuedTotal` caps the total across all streams; once reached, the
    adapter stops polling message-db until the handler drains enough. A
    `dispatchStats` hook exposes active/queued counts for metrics.

    **Observability.** The envelope's `partition` field is populated with
    the stream name, which propagates into Shibuya's tracing
    (`messaging.destination.partition.id`) and into the `dispatchStats`
    record for Prometheus.


## Risks and Open Questions

- **Slow streams stalling.** A handler that takes minutes per message on
  stream A will leave A unable to accept new messages, while A's pending
  messages eat into `maxQueuedTotal`. If many streams behave this way, the
  adapter stops polling. Mitigation: surface `dispatchStats` as a metric,
  document the interaction, recommend per-handler timeouts.

- **Retry storm + per-stream lock.** A handler that always retries quickly
  combined with a low `maxRetryBufferSize` will rapidly downgrade to DLQ.
  Mitigation: `maxRetryBufferSize` default (EP-3) is 1000, which allows
  ~a few minutes of repeated retries before DLQ. Document the interaction
  in the README.

- **Consumer-group composition.** When EP-4's consumer-group partitioning is
  enabled alongside `PerStreamInOrder`, the consumer-group filter runs
  first (category-level hash), then the per-stream dispatcher runs within
  each member. Integration test 10 asserts this composition. If this test
  fails, the open question is whether EP-4's filter needs to call
  `recordIngested + recordAckResult AckComplete` **before** per-stream
  enqueue happens (EP-4's current design) or after — the plan assumes
  before, based on EP-4's specification.
