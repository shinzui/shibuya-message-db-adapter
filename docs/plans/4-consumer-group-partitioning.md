# Consumer-group partitioning

MasterPlan: docs/masterplans/1-shibuya-message-db-adapter.md

Intention: intention_01kpgme50se0ranxp41ghfhajf

This ExecPlan is a living document. The sections Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.

This document is maintained in accordance with `.claude/skills/exec-plan/PLANS.md`.


## Purpose / Big Picture

After this plan is complete, a single message-db category can be consumed in parallel by
N cooperating adapter instances without double-processing any message and without any
coordination other than a shared configuration contract. A user who today runs one
adapter over the `orders` category — processing every message in sequence — can, after
this change, run three adapter processes configured as

    ConsumerGroupConfig { groupSize = 3, member = 0 }
    ConsumerGroupConfig { groupSize = 3, member = 1 }
    ConsumerGroupConfig { groupSize = 3, member = 2 }

and observe that every message in the `orders` category is delivered to exactly one of
the three processes, that two messages for the same entity (for example
`orders-abc-123` and `orders-abc-456`) always land on the *same* process, and that each
process maintains its own durable checkpoint under a partition-scoped name. If a process
crashes and restarts, it resumes consuming its own slice of messages from its own
checkpoint without touching the other partitions' state.

The observable outcome is an integration test: write 30 messages spread across 6
distinct categories to a local message-db, launch three adapter instances in one
process (each with its own `ConsumerGroupConfig`), wait for the handlers to drain, and
assert that (a) exactly 30 handler invocations happened, (b) no message id appeared
twice, and (c) every message that belongs to category `C` was handled by the same
adapter instance (the one whose `member` index equals Murmur3-64 of `C` modulo
`groupSize`).

This is EP-4 of a six-plan initiative. It hard-depends on EP-2 (durable checkpoints +
contiguous-prefix ack accounting) and soft-depends on EP-3 (retry/DLQ). It leaves
EP-3's ack machinery unchanged — a retried message on a partitioned subscription
behaves identically to a retried message on a single-consumer subscription; only the
*set* of messages that enter the stream differs.


## Progress

Use a checklist to summarize granular steps. Every stopping point must be documented
here, even if it requires splitting a partially completed task into two ("done" vs.
"remaining"). This section must always reflect the actual current state of the work.

### Milestone 1: Config extension and startup validation

- [x] Add `ConsumerGroupConfig { groupSize, member }` and `Show`/`Eq` instances to
      `Shibuya.Adapter.MessageDb.Config`. (2026-04-18)
- [x] Extend `MessageDbAdapterConfig` with `consumerGroup :: Maybe ConsumerGroupConfig`,
      defaulting to `Nothing` in `defaultConfig`. (2026-04-18)
- [x] Add startup validation `validateConsumerGroup :: Maybe ConsumerGroupConfig -> Either Text ()`
      enforcing `groupSize >= 1` and `0 <= member < groupSize`. (2026-04-18)
- [x] Plumb validation into `messageDbAdapter` so it fails fast via `throwIO` on
      `IOE`. (2026-04-18)

### Milestone 2: Hash function sourcing

- [x] Read `message-db-subscription/src/MessageDb/Subscription/Consumer.hs` and confirm
      `getPartitionMurmur` is private (not in the module export list). (2026-04-18)
- [x] Record the duplication Decision Log entry and add a dependency on
      `murmur-hash ^>=0.1.0` to the cabal file (matching what message-db-subscription
      uses). (2026-04-18)
- [x] Implement `categoryPartition :: Int -> Text -> Int` in
      `Shibuya.Adapter.MessageDb.Internal` using `Data.Digest.Murmur64.hash64` over the
      UTF-8 encoding of the category name. (2026-04-18)
- [x] Export `categoryPartition` from `Shibuya.Adapter.MessageDb.Internal`. (2026-04-18)
- [x] Unit tests that `categoryPartition n cat` is deterministic, total, in `[0, n)`,
      and matches `message-db-subscription`'s `getPartitionMurmur` output for a handful
      of representative inputs. (2026-04-18)

### Milestone 3: Filter + InflightState coordination

- [x] Identify the batch-ingestion site in
      `Shibuya.Adapter.MessageDb.Internal.messageDbSource` where `getCategoryMessages`
      returns a `Vector Message`. (2026-04-18)
- [x] Introduce a `partitionBelongsToMember :: ConsumerGroupConfig -> Message -> Bool`
      predicate and apply it to each message in the batch. (2026-04-18)
- [x] For every message *not* belonging to this member, record the slot in
      `InflightState` and mark it `AckComplete` immediately so the contiguous-prefix
      checkpoint advances past it. (2026-04-18)
- [x] For every message that *does* belong, carry on with normal `recordIngested`
      + `mkAckHandle` wiring. (2026-04-18)
- [x] Unit test this coordination with a stub in-memory `InflightState` to show a mixed
      batch advances the checkpoint correctly after the belonging messages are acked. (2026-04-18)

### Milestone 4: Partition-scoped subscription name

- [x] Add `partitionedSubscriptionName :: SubscriptionName -> ConsumerGroupConfig -> SubscriptionName`
      to `Shibuya.Adapter.MessageDb.Internal`. (2026-04-18)
- [x] Replace the checkpoint-store read and write sites inside the adapter startup and
      periodic save path so they route through `partitionedSubscriptionName` when
      `consumerGroup = Just cfg`. (2026-04-18)
- [x] Verify by reading the code path that a `Nothing` consumer group produces exactly
      the same `SubscriptionName` as before (no regression for non-partitioned users). (2026-04-18)

### Milestone 5: Envelope.partition population

- [x] Thread `consumerGroup` from config into the place where
      `messageToEnvelope` is called. (2026-04-18)
- [x] Override the envelope's `partition` field with
      `Just (partitionLabel cfg)` (see signature below) when partitioning is on. (2026-04-18)
- [x] Confirm the `Nothing` case leaves the envelope's `partition` untouched (the
      existing `Convert.messageToEnvelope` already sets it to `Nothing`). (2026-04-18)

### Milestone 6: Integration test against ephemeral-pg

- [ ] Add an integration test module `test/ConsumerGroupTest.hs` (hooked into
      `test/Main.hs`) guarded by the `integration` cabal flag.
- [ ] Bootstrap ephemeral-pg + message-db schema, seed 30 messages across 6 categories.
- [ ] Launch three adapter instances (`groupSize = 3`, `member` in `0, 1, 2`) from the
      same process under the `ApplicationRunner`-style orchestration used by EP-2's
      integration test harness.
- [ ] Assert: 30 handler invocations total, 0 duplicates, and each category's messages
      all hit the same `member`.
- [ ] Assert each `member`'s checkpoint row exists under
      `"${subscriptionName}-${member}-of-${groupSize}"`.


## Surprises & Discoveries

- The plan's Milestone 3 unit-test specification says "assert
  `advanceCheckpointTo` returns `Nothing` (because 2 is still inflight)" after
  ingesting-and-completing positions 1, 3, 5 and ingesting 2, 4 without completion.
  That assertion is wrong: position 1 is complete and 'advanceCheckpointTo' must
  return `Just 1` on the first call. The implemented test asserts the correct
  behavior (`Just 1` → complete 2 → `Just 3` → complete 4 → `Just 5`). The
  underlying property — filtered messages do not block the contiguous prefix — is
  unchanged and still verified. (2026-04-18)

- Milestones 4 and 5 turned out to be trivial once M2 defined
  `partitionedSubscriptionName`, `partitionLabel`, and `applyPartitionLabel` in
  `Shibuya.Adapter.MessageDb.Internal`. M4 became "plumb `effectiveName` through
  the three checkpoint call sites"; M5 became "wrap `messageToEnvelope` in
  `applyPartitionLabel cfg.consumerGroup`". Both exposed from `Internal`. (2026-04-18)


## Decision Log

- Decision: Use the same Murmur3-64 hash of the category name that
  `message-db-subscription`'s `getPartitionMurmur` uses, so that a Shibuya-adapted
  consumer group and a native `message-db-subscription` consumer group produce the same
  partitioning for the same category.
  Rationale: Partitioning is only useful if it is stable across readers. Anything else
  would mean that switching from the native subscription library to the Shibuya adapter
  (or running both in parallel during migration) could double-deliver the same category.
  Date: 2026-04-18

- Decision: Re-implement `categoryPartition` inside `Shibuya.Adapter.MessageDb.Internal`
  rather than depending on `message-db-subscription` solely to import
  `getPartitionMurmur`. The function is not exported from
  `MessageDb.Subscription.Consumer` (module header lists only `pipeHandlers`), so we
  cannot import it today.
  Rationale: Pulling the whole subscription package as a dependency just for a single
  private helper is heavier than it needs to be, and we would still need to duplicate
  the code because the original takes `ConsumerGroupSize` and `Message` arguments that
  we do not want to leak through the adapter surface. Instead, we take a minimal
  `murmur-hash ^>=0.1.0` dependency (the same package the subscription library uses)
  and expose `categoryPartition :: Int -> Text -> Int` directly. A unit test pins our
  output to match `getPartitionMurmur`'s behavior for a fixed set of category names so
  we notice if the upstream hash ever drifts.
  Date: 2026-04-18

- Decision: Filtered-out messages still advance the checkpoint by being marked
  `AckComplete` immediately in `InflightState`.
  Rationale: EP-2's contiguous-prefix advancement persists the lowest un-completed
  global position. If a filtered-out message sat inflight without being completed, the
  partition's checkpoint would stall at the position of the first message belonging to
  another partition, forcing infinite replay on restart. Treating filtered messages as
  "instantly done" is correct: this member is not responsible for them, and the
  producer query already returned them only because message-db has no server-side
  filter on category-hash.
  Date: 2026-04-18

- Decision: Partition-scoped subscription name is
  `"${userSupplied}-${member}-of-${groupSize}"`.
  Rationale: Partition checkpoints must not collide with each other or with a
  non-partitioned checkpoint of the same name. The `"${N}-of-${M}"` suffix is
  human-readable in the checkpoint table and trivially parseable if ops tooling ever
  needs it.
  Date: 2026-04-18

- Decision: The `Envelope.partition` text is
  `"${member}-of-${groupSize}"` (without the subscription name prefix), so that user
  handlers can identify the partition without learning about `SubscriptionName`.
  Rationale: `Envelope.partition` is a debugging/telemetry field, not a checkpoint key.
  The subscription name is an adapter-internal concern, but the partition coordinate
  is useful for logging and tracing.
  Date: 2026-04-18


## Outcomes & Retrospective

(To be filled during and after implementation.)


## Context and Orientation

The reader is assumed to know Haskell and Cabal but not this repository. Orient
yourself as follows.

**This repository** is the `shibuya-message-db-adapter` package at
`/Users/shinzui/Keikaku/work/libraries/haskell/shibuya-message-db-adapter`. EP-1 and
EP-2 (the plans that precede this one) have already been implemented. That means the
following is in place: a Cabal package with library modules
`Shibuya.Adapter.MessageDb`, `Shibuya.Adapter.MessageDb.Config`,
`Shibuya.Adapter.MessageDb.Convert`, `Shibuya.Adapter.MessageDb.Internal`; a `Justfile`;
a `mori.dhall`; a working Nix flake; a demo executable. The polling stream in
`Internal` uses the `MessageDb` effect from `message-db-effectful` to fetch batches of
`MessageDb.Message` from a given category, converts each to a
`Shibuya.Core.Types.Envelope MessageDb.Message` via
`Shibuya.Adapter.MessageDb.Convert.messageToEnvelope`, and wraps the envelope in
`Shibuya.Core.Ingested es MessageDb.Message`. EP-2 added durable checkpointing via
`message-db-checkpoint-store` and an opaque `InflightState` STM type that tracks which
messages are in flight and advances the persisted checkpoint only through the
contiguous prefix of completed ones.

**Shibuya** is a queue-processing framework at
`/Users/shinzui/Keikaku/bokuno/shibuya-project/shibuya`. Its core package
`shibuya-core` defines the types every adapter implements. The three that matter for
this plan are:

    data Adapter es msg = Adapter
      { adapterName :: !Text
      , source      :: Stream (Eff es) (Ingested es msg)
      , shutdown    :: Eff es ()
      }

    data Envelope msg = Envelope
      { messageId    :: !MessageId
      , cursor       :: !(Maybe Cursor)
      , partition    :: !(Maybe Text)
      , enqueuedAt   :: !(Maybe UTCTime)
      , traceContext :: !(Maybe TraceHeaders)
      , payload      :: !msg
      }

    data Ingested es msg = Ingested
      { envelope :: Envelope msg
      , ack      :: AckHandle es
      }

where `AckHandle es = AckHandle { finalize :: AckDecision -> Eff es () }` and
`AckDecision` is one of `AckOk`, `AckRetry !RetryDelay`, `AckDeadLetter !DeadLetterReason`,
`AckHalt !HaltReason`.

Because `shibuya-core`'s types use the `NoFieldSelectors` extension, across package
boundaries you **do not** use record-dot syntax (`x.field`) and you do not get
auto-generated `field :: T -> a` accessors for free. You must destructure via explicit
record pattern matching, for example `Ingested{envelope, ack = AckHandle finalize}`,
or use `RecordWildCards`. Keep this in mind when you read or extend adapter code.

**message-db-hs** is at `/Users/shinzui/Keikaku/work/libraries/haskell/message-db-hs-master`.
The `MessageDb.Message.Message` record carries a `stream :: Stream` field, where
`Stream` is a sum type (`MessageDb.Message.Stream.Stream.Stream`) with constructors
`Category !CategoryStream` and `Entity !EntityStream`. An *entity stream* is a stream
named `category-entityId` (e.g. `orders-abc-123`); a *category stream* is just the
bare category (e.g. `orders`). The `category :: Stream -> CategoryStream` function
pulls the category portion out of either constructor. `CategoryStream` is a newtype
over `Text` with `toText :: CategoryStream -> Text`.

**message-db-subscription** is the sibling package at
`/Users/shinzui/Keikaku/work/libraries/haskell/message-db-hs-master/message-db-subscription`.
Its module `MessageDb.Subscription.Consumer` implements consumer-group partitioning
through a private function:

    getPartitionMurmur :: ConsumerGroupSize -> Message -> ConsumerGroupMember
    getPartitionMurmur (ConsumerGroupSize size) msg =
      let !member = fromIntegral (Murmur.asWord64 catHash) `mod` size
       in ConsumerGroupMember member
      where
        cat    = MStream.category (msg ^. #stream)
        catBS  = Text.encodeUtf8 $ CategoryStream.toText cat
        catHash = Murmur.hash64 catBS

The module export list names only `pipeHandlers`, so this function is not importable
from the outside. The hash library used is `Data.Digest.Murmur64` from the
`murmur-hash` package, pinned to `^>=0.1.0` in
`message-db-subscription.cabal`. The relevant facts for this plan are (a) the hash is
over the UTF-8 bytes of the *category* portion of the stream name (not the entity id,
not the message id, not the full stream string), (b) the modulus is `ConsumerGroupSize`
as an `Int64` but Haskell `Int` arithmetic is fine because partition sizes are small,
and (c) `asWord64` + `mod` yields a non-negative member index in `[0, groupSize)`.

**What "consumer group" means.** Borrowed from Kafka terminology. A consumer group is a
set of cooperating consumer processes — here, adapter instances — that together consume
a single logical stream (here, a message-db category), with each message delivered to
exactly one member of the group. The routing rule in our case is deterministic: the
category name is hashed, the hash is taken mod the group size, and the resulting index
picks the member. "Cooperating" here does not mean the members talk to each other; it
means each one is independently configured with `(groupSize, member)` and filters
messages locally. The whole coordination story is carried in configuration and the
hash function.

**What "category" means.** In message-db, a stream name is either a category like
`orders` or a compound of the form `category-entityId` like `orders-abc-123`. The
*category* is the substring before the first `-`; the *entity id* is everything after.
Partitioning at the category level (not at the entity level) means that all events for
every entity within `orders` go to the same partition. That matters because many
handler workloads rely on seeing an entity's events in order, and an entity's events
are always in the same category.

**What `InflightState` does (recap from EP-2).** `InflightState` is an opaque STM-held
data structure that tracks, for each global position that has been pulled off the
message-db fetch into the adapter stream, whether it is still inflight or has
completed (via `AckOk`, `AckDeadLetter`, or — in EP-3 — the terminal states of a
retry cycle). The state exposes `recordIngested :: InflightState -> GlobalPosition -> STM ()`,
`recordAckResult :: InflightState -> GlobalPosition -> AckOutcome -> STM ()`
(where `data AckOutcome = AckComplete | AckRetry`), and
`advanceCheckpointTo :: InflightState -> STM (Maybe GlobalPosition)`, where the last
function returns the highest global position such that every position up to and
including it has been completed. Callers wrap operations in `STM.atomically`. The
checkpoint-persistence loop calls `advanceCheckpointTo` periodically and
writes the result via `message-db-checkpoint-store`. **The subtle consequence for this
plan:** if a message is filtered out by consumer-group partitioning, we still need to
insert and immediately complete it in `InflightState` — otherwise the contiguous
prefix cannot advance past it, and the partition's checkpoint stalls forever.


## Plan of Work

### Milestone 1: Config extension and startup validation

At the end of this milestone, `MessageDbAdapterConfig` carries an optional consumer
group and invalid configurations fail at adapter construction rather than at runtime.

In `src/Shibuya/Adapter/MessageDb/Config.hs`, add:

    data ConsumerGroupConfig = ConsumerGroupConfig
      { groupSize :: !Int
      , member    :: !Int
      }
      deriving (Eq, Show)

Extend the existing `MessageDbAdapterConfig` record (after the fields introduced by
EP-1 through EP-3) with:

    , consumerGroup :: !(Maybe ConsumerGroupConfig)

Update `defaultConfig` (and every other smart constructor introduced in prior plans)
to default `consumerGroup` to `Nothing`. Do **not** reorder the existing fields — per
the MasterPlan's Integration Points rule, all new fields go at the bottom of the
record to minimise diff noise.

Add a validator next to `defaultConfig`:

    validateConsumerGroup :: Maybe ConsumerGroupConfig -> Either Text ()
    validateConsumerGroup Nothing = Right ()
    validateConsumerGroup (Just ConsumerGroupConfig{groupSize, member})
      | groupSize < 1 =
          Left ("ConsumerGroupConfig.groupSize must be >= 1, got " <> Text.pack (show groupSize))
      | member < 0 || member >= groupSize =
          Left ("ConsumerGroupConfig.member must satisfy 0 <= member < groupSize, got member="
                <> Text.pack (show member) <> ", groupSize=" <> Text.pack (show groupSize))
      | otherwise = Right ()

In `Shibuya.Adapter.MessageDb.messageDbAdapter`, at the top of the body, call
`validateConsumerGroup (consumerGroup cfg)` and, on `Left msg`, call
`liftIO (throwIO (userError (Text.unpack msg)))`. Using `userError` here keeps the
effect signature unchanged (no `Error Text` effect to thread). The message is always
user-visible via Shibuya's supervisor, which prints the exception before restart.

### Milestone 2: Hash function sourcing

At the end of this milestone, `Shibuya.Adapter.MessageDb.Internal.categoryPartition`
computes the same partition index as `message-db-subscription`'s private
`getPartitionMurmur` for the same category.

First, read
`/Users/shinzui/Keikaku/work/libraries/haskell/message-db-hs-master/message-db-subscription/src/MessageDb/Subscription/Consumer.hs`
and verify that (a) `getPartitionMurmur` is not re-exported, and (b) the hash uses
`Data.Digest.Murmur64.hash64` on the UTF-8 bytes of
`MStream.category (msg ^. #stream)`. Record the verification in the Surprises &
Discoveries section if the code has drifted from what this plan assumes.

Add `murmur-hash ^>=0.1.0` to `shibuya-message-db-adapter.cabal`'s library
`build-depends`. Do **not** add `message-db-subscription` as a dependency — see the
Decision Log.

In `src/Shibuya/Adapter/MessageDb/Internal.hs`, add:

    categoryPartition :: Int -> Text -> Int
    categoryPartition groupSize category =
      fromIntegral (Murmur.asWord64 (Murmur.hash64 bytes)) `mod` groupSize
      where
        bytes = Text.encodeUtf8 category

Imports required:

    import qualified Data.Digest.Murmur64 as Murmur
    import qualified Data.Text.Encoding as Text
    import           Data.Text (Text)

Export `categoryPartition` from `Shibuya.Adapter.MessageDb.Internal`. Because of
Shibuya's `-Wmissing-export-lists`, the module already has an explicit export list
from EP-1; add `categoryPartition` and `partitionedSubscriptionName` (introduced in
Milestone 4) to it.

Write four unit tests in `test/PartitionTest.hs`:

1. *"deterministic"* — `categoryPartition 7 "orders" == categoryPartition 7 "orders"`.
2. *"in range"* — for `n` from 1 to 32 and a selection of 100 sample category names,
   `0 <= categoryPartition n cat < n`.
3. *"matches message-db-subscription for the same inputs"* — build a `Stream` via
   `MessageDb.Message.Stream.Stream.parseEither "orders-abc"`, synthesize a
   dummy `Message` record with that stream, call `getPartitionMurmur`'s computation
   inline (copy it from the subscription source into the test so we are not coupled to
   a private import), and assert `unConsumerGroupMember` equals
   `categoryPartition (fromIntegral size) "orders"` for sizes 2, 3, 5, 8.
4. *"empty category does not crash"* — `categoryPartition 3 ""` returns a value in
   `[0, 3)`. (message-db forbids empty categories at write time, but the hash function
   itself is total and we should not rely on upstream validation.)

Hook `PartitionTest.tests` into `test/Main.hs` inside the existing `testGroup`.

### Milestone 3: Filter + InflightState coordination

At the end of this milestone, the adapter's internal stream filters out messages that
do not belong to the configured member, *and* those filtered messages cause the
durable checkpoint to advance past them.

Find the batch-ingestion site in
`src/Shibuya/Adapter/MessageDb/Internal.hs`. EP-2's implementation looks roughly like
this (paraphrased — the real code may differ slightly):

    Stream.unfoldrM $ \pos -> do
      msgs <- MessageDb.getCategoryMessages (query pos cfg)
      if Vector.null msgs
        then threadDelay (toMicros (pollInterval cfg)) >> pure (Just ([], pos))
        else do
          traverse_ (\m -> STM.atomically (recordIngested inflight (globalPosition m))) msgs
          let ingested = fmap (toIngested cfg inflight) msgs
              nextPos  = 1 + globalPosition (Vector.last msgs)
          pure (Just (Vector.toList ingested, nextPos))

Add the filter and the filtered-but-completed bookkeeping. Introduce a helper near the
top of the module:

    partitionBelongsToMember :: ConsumerGroupConfig -> MessageDb.Message -> Bool
    partitionBelongsToMember ConsumerGroupConfig{groupSize, member} msg =
      categoryPartition groupSize (CategoryStream.toText cat) == member
      where
        cat = MStream.category (MessageDb.stream msg)

Then at the batch-ingestion site, split the batch into `belonging` and `filtered`:

    let (belonging, filtered) = case consumerGroup cfg of
          Nothing  -> (Vector.toList msgs, [])
          Just grp -> Vector.toList (Vector.partition (partitionBelongsToMember grp) msgs)

For every message in `filtered`, call both `recordIngested` and `recordAckResult _ AckComplete`
in the same STM transaction, so it enters and leaves `InflightState` atomically:

    STM.atomically $ forM_ filtered $ \m -> do
      recordIngested inflight (globalPosition m)
      recordAckResult inflight (globalPosition m) AckComplete

For `belonging` messages, the existing EP-2 ingestion logic is unchanged.

(If `AckComplete` is not the exact constructor name EP-2 chose for "completed
successfully in the inflight-set sense", substitute the right one — read the
`Shibuya.Adapter.MessageDb.Internal` source to confirm.)

Add a unit test in `test/InflightFilterTest.hs` that uses the in-memory `InflightState`
directly: create state, ingest-and-complete positions 1, 3, 5 (the "filtered" ones),
ingest 2 and 4 (the "belonging" ones) without completing them, assert
`advanceCheckpointTo` returns `Nothing` (because 2 is still inflight), complete 2,
assert it returns `Just 3` (1,2,3 are contiguous and all complete), complete 4,
assert it returns `Just 5`. This proves that filtered messages do not block the
contiguous prefix.

### Milestone 4: Partition-scoped subscription name

At the end of this milestone, the checkpoint row a partitioned adapter reads and
writes is named for the partition, not the bare subscription.

In `src/Shibuya/Adapter/MessageDb/Internal.hs`, add:

    partitionedSubscriptionName :: SubscriptionName -> ConsumerGroupConfig -> SubscriptionName
    partitionedSubscriptionName base ConsumerGroupConfig{groupSize, member} =
      base <> "-" <> Text.pack (show member) <> "-of-" <> Text.pack (show groupSize)

(`SubscriptionName` is a newtype over `Text` owned by `message-db-checkpoint-store`. If
the real import is `Shibuya.Adapter.MessageDb.Checkpoint.SubscriptionName` or similar,
use whatever EP-2 chose. The wrapping/unwrapping lives in this helper so the rest of
the module stays clean.)

At the three call sites where EP-2 reads or writes the checkpoint (read at startup,
periodic save in the save-loop fiber, final save in `shutdown`), compute
`effectiveName` once near the top of `messageDbAdapter`:

    let effectiveName = case consumerGroup cfg of
          Nothing  -> subscriptionName cfg
          Just grp -> partitionedSubscriptionName (subscriptionName cfg) grp

and pass `effectiveName` to all checkpoint operations in place of
`subscriptionName cfg`.

Export `partitionedSubscriptionName` from `Shibuya.Adapter.MessageDb.Internal` so that
tests can predict checkpoint row names.

### Milestone 5: Envelope.partition population

At the end of this milestone, every envelope emitted by a partitioned adapter carries
a non-`Nothing` `partition` text.

`Shibuya.Adapter.MessageDb.Convert.messageToEnvelope` is a pure conversion from
`MessageDb.Message` to `Envelope MessageDb.Message` and must not change (per the
MasterPlan's Integration Points: "Later plans must not modify the conversion"). The
adapter fills `partition` by *overriding* the envelope after conversion.

In `src/Shibuya/Adapter/MessageDb/Internal.hs`, add a small wrapper:

    applyPartitionLabel :: Maybe ConsumerGroupConfig -> Envelope MessageDb.Message -> Envelope MessageDb.Message
    applyPartitionLabel Nothing  env = env
    applyPartitionLabel (Just grp) env =
      env { partition = Just (partitionLabel grp) }

    partitionLabel :: ConsumerGroupConfig -> Text
    partitionLabel ConsumerGroupConfig{groupSize, member} =
      Text.pack (show member) <> "-of-" <> Text.pack (show groupSize)

At the site where the adapter converts a belonging `MessageDb.Message` into an
`Ingested` value, wrap the conversion:

    let env = applyPartitionLabel (consumerGroup cfg) (messageToEnvelope m)
        ingested = Ingested{envelope = env, ack = mkAckHandle cfg inflight m}

(If `Envelope` uses `NoFieldSelectors`, the record-update syntax
`env { partition = ... }` still works — `NoFieldSelectors` suppresses the generation
of top-level selector functions, but record-construction/update syntax is unaffected.)

### Milestone 6: Integration test against ephemeral-pg

At the end of this milestone, a single-process integration test proves end-to-end
that three partitioned adapters split a 30-message workload correctly.

EP-2 already wires up `shinzui/ephemeral-pg` in the test suite and applies the
message-db schema from
`/Users/shinzui/Keikaku/hub/event-sourcing/message-db-project/message-db/database`. Reuse
that bootstrap function (it should live in `test/Support/Ephemeral.hs` or similar —
follow EP-2's layout).

Write `test/ConsumerGroupTest.hs`:

    module ConsumerGroupTest (tests) where

    import Test.Tasty
    import Test.Tasty.HUnit
    -- ... Shibuya, message-db, adapter, ephemeral-pg imports

    tests :: TestTree
    tests =
      testGroup "consumer-group partitioning"
        [ testCase "three-member group splits 30 messages exactly once each" threeMemberSplit
        ]

    threeMemberSplit :: Assertion
    threeMemberSplit = withEphemeralMessageDb $ \connSettings -> do
      -- 1. Seed: 30 messages across 6 categories (5 per category).
      let categories = ["ords", "inv", "cust", "pay", "ship", "audit"]
      seed connSettings categories 5   -- writes 5 messages per category

      -- 2. Launch three adapter instances, each with its own print-and-collect handler.
      (seenPerMember, latch) <- prepareShared
      let mkAdapter i = messageDbAdapter (partitionedConfig connSettings i 3)
      runThreeAdapters mkAdapter seenPerMember latch

      -- 3. Wait for 30 handler invocations.
      waitForCount latch 30

      -- 4. Assertions.
      seen <- readIORef seenPerMember   -- Map Int [MessageId]
      let flat = concat (Map.elems seen)
      assertEqual "total 30 invocations" 30 (length flat)
      assertEqual "no duplicates"         30 (Set.size (Set.fromList flat))
      forM_ categories $ \cat -> do
        let expected = categoryPartition 3 cat
            idsForCat = filter (\mid -> messageCategory mid == cat) flat
        forM_ idsForCat $ \mid -> do
          member <- memberFor mid seen
          assertEqual ("category " <> Text.unpack cat <> " routes to member "
                       <> show expected) expected member

      -- 5. Checkpoint rows.
      rows <- readCheckpointRows connSettings
      assertBool "checkpoint for member 0" $ ("test-sub-0-of-3" `elem` rows)
      assertBool "checkpoint for member 1" $ ("test-sub-1-of-3" `elem` rows)
      assertBool "checkpoint for member 2" $ ("test-sub-2-of-3" `elem` rows)

(The exact glue names — `withEphemeralMessageDb`, `seed`, `readCheckpointRows`,
`ApplicationRunner` — should match what EP-2 established. If a helper does not exist,
add it in `test/Support/...` rather than inlining it in the test.)

Hook `ConsumerGroupTest.tests` into `test/Main.hs`.


## Concrete Steps

Run from `/Users/shinzui/Keikaku/work/libraries/haskell/shibuya-message-db-adapter`.

Edit the Config module, the Internal module, and the cabal file:

    $EDITOR shibuya-message-db-adapter/src/Shibuya/Adapter/MessageDb/Config.hs
    $EDITOR shibuya-message-db-adapter/src/Shibuya/Adapter/MessageDb/Internal.hs
    $EDITOR shibuya-message-db-adapter/shibuya-message-db-adapter.cabal

Compile:

    cabal build shibuya-message-db-adapter

Expected: exit 0, compiler emits `Registering library ...`.

Run unit tests (Milestones 2 and 3):

    cabal test shibuya-message-db-adapter --test-option=--pattern='partition'

Expected output (transcript fragment):

    shibuya-message-db-adapter-tests
      partition
        deterministic:                                OK
        in range:                                     OK
        matches message-db-subscription for inputs:   OK
        empty category does not crash:                OK
      inflight-filter
        filtered messages advance contiguous prefix:  OK

    All tests passed (6 tests)

Run the integration test (Milestone 6) — this requires the `integration` flag if EP-2
introduced one:

    cabal test shibuya-message-db-adapter --test-option=--pattern='consumer-group' -fintegration

Expected:

    consumer-group partitioning
      three-member group splits 30 messages exactly once each: OK

    All tests passed (1 test)

Check that a non-partitioned run still works (regression check):

    just seed-messages orders
    cabal run shibuya-message-db-adapter-demo -- --category orders

Expected: the demo executable behaves exactly as in EP-1/EP-2 — five (or however many
were seeded) lines print, then it blocks on the poll loop.


## Validation and Acceptance

The plan is complete when all of the following hold.

1. `cabal build shibuya-message-db-adapter` succeeds with zero warnings relevant to
   this package, after the cabal-file dependency on `murmur-hash ^>=0.1.0` has been
   added.

2. The four partition unit tests and the one inflight-coordination unit test pass.

3. The integration test `three-member group splits 30 messages exactly once each`
   passes. The test script, run with `cabal test`, must print one summary line with
   `OK` and exit 0. The underlying assertions must hold: exactly 30 handler
   invocations, exactly 30 distinct message ids, and, for every category among the
   six, all of that category's messages must have been handled by the member whose
   index equals `categoryPartition 3 categoryName`.

4. After the integration test completes, three checkpoint rows exist under
   subscription names
   `test-sub-0-of-3`, `test-sub-1-of-3`, `test-sub-2-of-3`. A manual check via
   `psql` in a development shell should show the same thing after a manual run.

5. With `consumerGroup = Nothing`, the adapter's observable behavior is identical to
   EP-2 / EP-3: the demo executable prints every message in the configured category
   exactly once, the checkpoint row is named exactly `subscriptionName cfg`, and no
   extra rows appear in the checkpoint table.

6. A deliberately invalid configuration (`groupSize = 0`, or `member = 5, groupSize = 3`)
   causes `messageDbAdapter` to throw a user error with a descriptive message before
   any Postgres query runs. Verifiable by wrapping the adapter call in `try @IOError`
   in a small ghci session or one-off test, or by reading the stderr output when the
   demo executable is launched with invalid flags.

Non-goals (explicit): rebalancing when a member joins or leaves the group at runtime
(message-db partitioning is statically configured per process), fair-share rebalancing
when categories have skewed volumes, dynamic group-size resize, dropping
`ConsumerGroupConfig` in favor of an env-var interface. Any of those attempted here
should be pushed to a follow-up plan.


## Idempotence and Recovery

Every step in this plan is safe to repeat.

Editing source files and the cabal file is trivially idempotent. `cabal build` and
`cabal test` can be re-run any number of times; they only rebuild what has changed.

The integration test creates an ephemeral Postgres database per invocation via
`ephemeral-pg`, so running it repeatedly does not accumulate state. If the test is
interrupted in the middle of a seed, the next run starts from a fresh cluster.

If a production deployment changes its `groupSize` or `member` assignments, each
process will create or switch to a new checkpoint row (because the partition-scoped
subscription name includes both numbers). The old rows remain in the checkpoint table
until manually pruned — this is a deliberate conservative default. A future plan may
add a `just cleanup-stale-checkpoints` recipe; for EP-4 the operator is responsible
for deletion if they do not want the old rows around. Document this in the Decision
Log if any contributor is tempted to auto-delete them: silent deletion on config
change is much more dangerous than a stale row.

If the hash library (`murmur-hash`) ever changes its output for a given input (for
example, through a major version bump), the partitioning contract is broken and
consumers in the group will disagree about routing. The Milestone 2 cross-check test
against the values computed by the subscription library's copy of the same algorithm
is the early-warning tripwire. If it fails, pin `murmur-hash` to a known-good version
in the cabal file and open a follow-up plan.


## Interfaces and Dependencies

New external dependency: `murmur-hash ^>=0.1.0` (added to the library `build-depends`
in `shibuya-message-db-adapter.cabal`). Existing dependencies used by this plan:
`text`, `bytestring`, `vector`, `stm`, `containers`, `message-db-hs`,
`message-db-effectful`, `message-db-checkpoint-store`, `shibuya-core`.

At the end of Milestone 1, the following must exist in
`Shibuya.Adapter.MessageDb.Config`:

    data ConsumerGroupConfig = ConsumerGroupConfig
      { groupSize :: !Int
      , member    :: !Int
      }
      deriving (Eq, Show)

    -- MessageDbAdapterConfig extended with a new trailing field:
    --   , consumerGroup :: !(Maybe ConsumerGroupConfig)
    --
    -- defaultConfig defaults consumerGroup to Nothing.

    validateConsumerGroup :: Maybe ConsumerGroupConfig -> Either Text ()

At the end of Milestone 2, the following must exist in
`Shibuya.Adapter.MessageDb.Internal`:

    categoryPartition :: Int -> Text -> Int
    -- Given groupSize and category-name text, returns the member index in [0, groupSize).
    -- Computed as fromIntegral (asWord64 (hash64 (encodeUtf8 category))) `mod` groupSize,
    -- matching message-db-subscription's private getPartitionMurmur.

At the end of Milestone 3, the following helper must exist in
`Shibuya.Adapter.MessageDb.Internal` and be used inside `messageDbSource`:

    partitionBelongsToMember :: ConsumerGroupConfig -> MessageDb.Message -> Bool

and the batch-ingestion site must split each batch into belonging/filtered and record
the filtered messages as `ingested-then-completed` in `InflightState` in a single
`STM.atomically` transaction.

At the end of Milestone 4:

    partitionedSubscriptionName :: SubscriptionName -> ConsumerGroupConfig -> SubscriptionName

where `SubscriptionName` is the type EP-2 imported from the checkpoint-store layer.
The adapter's startup and save-loop must call the checkpoint store with the result of
`partitionedSubscriptionName base grp` when `consumerGroup = Just grp`, and with `base`
otherwise.

At the end of Milestone 5:

    applyPartitionLabel :: Maybe ConsumerGroupConfig
                        -> Envelope MessageDb.Message
                        -> Envelope MessageDb.Message
    partitionLabel      :: ConsumerGroupConfig -> Text

Both are internal (no need to export).

At the end of Milestone 6, a new test module `test/ConsumerGroupTest.hs` is hooked into
`test/Main.hs` via its `tests :: TestTree`. Any ephemeral-pg harness reused from EP-2
stays in whatever test-support module EP-2 chose; if it does not yet exist, put it at
`test/Support/Ephemeral.hs`.

Where the filter sits in the source stream: inside the batch-handling branch of
`messageDbSource` in `Shibuya.Adapter.MessageDb.Internal`, immediately after
`MessageDb.getCategoryMessages` returns its `Vector Message` and *before* any
`recordIngested` call for a belonging message. Filtered messages take the
ingested-then-completed fast path described above; belonging messages take the normal
EP-2 path (ingest, yield, ack via `mkAckHandle`, eventual `recordAckResult`).
