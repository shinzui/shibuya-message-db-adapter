# Retry, dead-letter, and halt handling

MasterPlan: docs/masterplans/1-shibuya-message-db-adapter.md

Intention: intention_01kpgme50se0ranxp41ghfhajf

This ExecPlan is a living document. The sections Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.

This document is maintained in accordance with `.claude/skills/exec-plan/PLANS.md`.


## Purpose / Big Picture

After this plan is complete, the `shibuya-message-db-adapter` Haskell package at
`/Users/shinzui/Keikaku/work/libraries/haskell/shibuya-message-db-adapter` implements
the *complete* `AckDecision` surface that Shibuya's core contract requires. A user
writing a `Handler es MessageDb.Message` can return any of the four decisions —
`AckOk`, `AckRetry !RetryDelay`, `AckDeadLetter !DeadLetterReason`, or
`AckHalt !HaltReason` — and the adapter will do the right thing: advance the checkpoint
for acknowledged messages, re-deliver retried messages after their delay has elapsed,
route dead-lettered messages to a configurable dead-letter destination (either logged
and skipped, or written to a named message-db stream), and tear the adapter down
cleanly when a handler asks it to halt.

"Shibuya" is the Broadway-inspired queue-processing framework at
`/Users/shinzui/Keikaku/bokuno/shibuya-project/shibuya`. Its core package
`shibuya-core` defines the `Adapter`, `Envelope`, `Ingested`, and `AckDecision` types
that every adapter implements. "message-db" is the PostgreSQL-backed event store at
`message-db/message-db`, wrapped for Haskell by `message-db-hs` and
`message-db-effectful` at
`/Users/shinzui/Keikaku/work/libraries/haskell/message-db-hs-master`. "DLQ" means
*dead-letter queue*, the standard name for a side-channel destination that stores
messages a consumer gave up on; in message-db terms a DLQ is simply another stream that
the adapter writes to via `writeStreamMessage`.

This plan is EP-3 of a 6-plan initiative. EP-1 (at
`docs/plans/1-scaffold-and-minimal-adapter.md`) scaffolded the package and shipped a
minimal polling adapter with stubbed ack. EP-2 (at
`docs/plans/2-checkpoint-and-ack-accounting.md`) introduced `InflightState` — the STM
structure that tracks in-flight messages and advances the checkpoint only through the
contiguous prefix of positions that have been either `AckOk`'d or dead-lettered — and
wired that into durable checkpoint persistence via `message-db-checkpoint-store`. EP-3
builds directly on top of EP-2 to extend `InflightState` with a retry buffer and
implement the remaining three decisions.

Observable outcome after this plan: from a `nix develop` shell with
`just process-up` running, a user can run an extended demo executable whose handler
returns `AckRetry (RetryDelay 2)` once for each message on its first delivery and
`AckOk` on the second delivery. The user observes each message printed twice, roughly
two seconds apart, with the checkpoint advancing only after the second delivery. A
second demo variant configured with `DlqWriteToStream (Stream "orders.dlq")` routes
failed messages to that stream and the user can query it with
`psql -c "SELECT * FROM message_store.messages WHERE stream_name = 'orders.dlq';"` to
see the dead-letter entries. A third variant with an `AckHalt`-returning handler shuts
the adapter down after the first failure and leaves the checkpoint pinned strictly
before the halted message's position.


## Progress

Use a checklist to summarize granular steps. Every stopping point must be documented
here, even if it requires splitting a partially completed task into two ("done" vs.
"remaining"). This section must always reflect the actual current state of the work.

### Milestone 1: Retry buffer and re-emission

- [ ] Extend `Shibuya.Adapter.MessageDb.Internal.InflightState` (from EP-2) to carry a
      `TQueue RetryEntry` where `RetryEntry` records `(GlobalPosition, UTCTime, MessageDb.Message)`.
- [ ] Add a new `AckOutcome` constructor `AckRetrying` (or extend the existing one —
      EP-2's `AckOutcome` was `AckComplete | AckRetry`; keep `AckRetry` and clarify
      that it means "retry scheduled, do not advance past this position").
- [ ] Implement `scheduleRetry :: InflightState -> GlobalPosition -> RetryDelay -> MessageDb.Message -> STM ()`
      that enqueues to the retry buffer and flips the in-flight flag to `AckRetry`.
- [ ] Implement the retry fiber: a background action, launched alongside the poll
      fiber, that atomically peeks at the head of the retry buffer, waits until the
      head's `notBefore` time elapses, then publishes the message to a `TChan MessageDb.Message`
      the source stream consumes.
- [ ] Merge retry publications with polled batches in `messageDbSource` so the user
      handler sees retries interleaved with fresh messages (both wrapped in `Ingested`).
- [ ] Verify by running the demo with a handler that retries once per message and
      observe correct re-emission.

### Milestone 2: AckDeadLetter with DlqSkipAndLog

- [ ] Introduce `data DlqStrategy` with constructors `DlqSkipAndLog` and
      `DlqWriteToStream !MessageDb.Message.Stream` in
      `Shibuya.Adapter.MessageDb.Config`.
- [ ] In `Shibuya.Adapter.MessageDb.Internal.mkAckHandle` (the non-stub ack handle from
      EP-2), add a case for `AckDeadLetter reason`: when `dlqStrategy` is
      `DlqSkipAndLog`, log the reason at warning level and call
      `recordAckResult inflight pos AckComplete`.
- [ ] Confirm compilation and that the existing checkpoint advancement tests from EP-2
      still pass.

### Milestone 3: AckDeadLetter with DlqWriteToStream

- [ ] When `dlqStrategy` is `DlqWriteToStream targetStream`, call
      `MessageDb.Effectful.writeStreamMessage` with a `NewMessage` whose fields are:
      `messageId` derived from the original message's id via UUIDv5 with the namespace
      UUID of `"shibuya-message-db-adapter/dlq"` and the name being the original UUID's
      bytes concatenated with `"-dlq"`; `stream = targetStream`; `messageType = "$DeadLetter"`;
      `messageData = Aeson.toJSON (original messageData)`; `messageMetadata` is a new
      object with keys `correlation`, `causation`, `deadLetterReason`,
      `deadLetteredAt`, and `originalStream`; `expectedPosition = Nothing` (we do not
      enforce ordering on the DLQ stream).
- [ ] The idempotent messageId means re-writing the same DLQ entry returns a
      `WrongExpectedVersion`-free path because message-db's `write_message` is
      idempotent on `(stream_name, message_id)` pairs: the DB will reject the duplicate
      with a unique-violation and we must catch that and treat as success.
- [ ] After the DLQ write (or a recognized idempotent-duplicate failure), call
      `recordAckResult inflight pos AckComplete` so the checkpoint can advance.
- [ ] Add a unit test that constructs two `AckDeadLetter` calls for the same position
      and verifies only one DLQ message ends up in the target stream.

### Milestone 4: AckHalt wiring

- [ ] Reuse the shutdown `TVar Bool` plumbed through EP-1 and EP-2. In the ack handle,
      on `AckHalt reason`: log the `HaltReason` at error level, write `True` to the
      shutdown TVar, and **do not** call `recordAckResult` at all for this position.
- [ ] Because `recordAckResult` is never invoked for the halted position,
      `advanceCheckpointTo` (from EP-2) will not cross it: the contiguous prefix
      terminates at `halted_pos - 1`.
- [ ] Verify by a unit test: given three messages 10, 11, 12 with 10=AckOk and
      11=AckHalt (12 never delivered), `advanceCheckpointTo` returns `Just 10`.
- [ ] Verify by an integration test: after halt, the adapter's stream terminates and a
      fresh `messageDbAdapter` over the same subscription name resumes at position 10,
      re-delivering 11 and onward.

### Milestone 5: Config extensions and defaults

- [ ] Add `dlqStrategy :: DlqStrategy` (default `DlqSkipAndLog`) to
      `MessageDbAdapterConfig`.
- [ ] Add `maxRetryBufferSize :: Int` (default `1000`) to `MessageDbAdapterConfig`.
- [ ] Update `defaultConfig` to set both new fields. Defaults must keep the EP-1 and
      EP-2 call sites compiling (additive change; no field renamed or removed).
- [ ] Verify that when `scheduleRetry` would push onto a full retry buffer, the
      enqueue is dropped and the ack handle instead takes the `AckDeadLetter
      MaxRetriesExceeded` code path (with `MaxRetriesExceeded` being the
      `DeadLetterReason` constructor from `Shibuya.Core.Ack`).

### Milestone 6: Tests

- [ ] Unit test: a newly-created retry buffer honours `notBefore` delays. Enqueue two
      entries with `notBefore` 100ms and 200ms ahead; consume until empty with a small
      sleep loop; assert elapsed time between the two pops is within a sensible
      tolerance of 100ms.
- [ ] Unit test: the retry buffer's full-buffer-downgrade path produces an
      `AckDeadLetter MaxRetriesExceeded` outcome for the scheduler's caller.
- [ ] Unit test: DLQ messageId derivation is deterministic (same input, same output)
      and distinct from the input messageId.
- [ ] Integration test (ephemeral-pg): write 5 messages to category `orders-retrydemo`;
      run a handler that returns `AckRetry (RetryDelay 0.1)` for the 3rd message on
      first delivery and `AckOk` on second delivery (and `AckOk` unconditionally for
      the other four). Assert: all 5 messages eventually see `AckOk`; the final
      persisted checkpoint equals the global position of message 5; the 3rd message's
      handler was invoked exactly twice.
- [ ] Integration test (ephemeral-pg): same 5-message setup but handler returns
      `AckDeadLetter Unprocessable` for message 3 with `DlqWriteToStream (Stream "orders.dlq")`;
      assert the DLQ stream has exactly one message and its `messageMetadata.correlation`
      equals the original message's id.
- [ ] Integration test (ephemeral-pg): handler returns `AckHalt ManualStop` for
      message 3; assert the adapter shuts down, checkpoint persists at message 2's
      position, and a restart re-delivers message 3.


## Surprises & Discoveries

(None yet.)


## Decision Log

- Decision: `AckRetry` is implemented as an in-process re-emission via a `TQueue`
  retry buffer plus a fiber that merges delayed messages into the source stream,
  rather than by rewinding the message-db poll cursor.
  Rationale: Rewinding the cursor would force redelivery of every message between the
  failure and now, which is exactly the behavior users do *not* want when a single
  message transiently fails. An in-process buffer keyed by `(globalPosition,
  notBefore)` retries only the failed message at its requested delay without affecting
  siblings. This also keeps the poll loop lock-free — the retry fiber is independent.
  Date: 2026-04-18

- Decision: The DLQ `messageId` is derived deterministically from the original
  `messageId` via UUIDv5 with a fixed namespace, rather than being generated fresh.
  Rationale: message-db's `write_message` SQL function uses `(stream_name, message_id)`
  as an idempotency key. Deriving the DLQ id from the original id means that if the
  adapter crashes after calling `writeStreamMessage` but before `recordAckResult`, the
  next attempt produces the same `NewMessage`, and message-db rejects it with a
  unique-violation. The adapter catches that specific error and treats it as success,
  preserving at-least-once -> effectively-once DLQ semantics.
  Date: 2026-04-18

- Decision: Retry-buffer overflow downgrades to `AckDeadLetter MaxRetriesExceeded`
  rather than applying back-pressure on the poll loop.
  Rationale: Back-pressure would stall the entire subscription because of one
  misbehaving handler, violating the principle that one bad message should not block
  siblings. Downgrading to DLQ uses Shibuya's existing `DeadLetterReason` taxonomy
  (`MaxRetriesExceeded`), which is semantically the right outcome for "we gave up
  trying to retry this".
  Date: 2026-04-18

- Decision: `DlqStrategy` has exactly two constructors in EP-3 (`DlqSkipAndLog`,
  `DlqWriteToStream !Stream`). An escape hatch
  `DlqCustom (Message -> DeadLetterReason -> Eff es ())` was considered (and mentioned
  in the MasterPlan's original decisions) but deferred to a follow-up.
  Rationale: The two built-ins cover the common cases: analytics-only environments use
  `DlqSkipAndLog`; durable-ops environments use `DlqWriteToStream`. `DlqCustom` adds
  an `Eff es` lambda into a pure-ish config record, which complicates serialization of
  the config for debugging and is a foot-gun if the lambda itself can fail. If a user
  needs truly custom DLQ routing, they can wrap `messageDbAdapter` in their own
  adapter layer.
  Date: 2026-04-18

- Decision: Ordering semantics documentation is part of this plan's deliverable, not a
  later polish step.
  Rationale: Users deploying to production need to know that retries can arrive out
  of order relative to freshly-polled messages because the retry fiber and the poll
  fiber merge asynchronously into the source stream. This is fine for Shibuya's
  `Unordered` processors (the default) but matters for `StrictInOrder` deployments.
  Recording the guidance in the module haddock avoids a future production surprise.
  Date: 2026-04-18


## Outcomes & Retrospective

(To be filled during and after implementation.)


## Context and Orientation

The reader is assumed to know Haskell, Cabal, STM, and basic Postgres but not this
repository or Shibuya's internals. Orient yourself as follows.

**The repository under work** is at
`/Users/shinzui/Keikaku/work/libraries/haskell/shibuya-message-db-adapter`. At the
start of this plan, it contains (produced by EP-1 and EP-2): a Nix `flake.nix`, a
`process-compose.yaml`, a `db/` directory for the direnv-managed Postgres, a `mori.dhall`,
a `Justfile`, a `cabal.project`, and the package `shibuya-message-db-adapter/` with
four library modules (`Shibuya.Adapter.MessageDb`, `.Config`, `.Convert`, `.Internal`),
a test suite, and a demo executable. EP-2 added `Shibuya.Adapter.MessageDb.Checkpoint`
for durable persistence via `message-db-checkpoint-store`.

**Shibuya core contract** (from `/Users/shinzui/Keikaku/bokuno/shibuya-project/shibuya/shibuya-core`):

The adapter's job is to satisfy `Shibuya.Adapter.Adapter`:

    data Adapter es msg = Adapter
      { adapterName :: !Text
      , source      :: Stream (Eff es) (Ingested es msg)
      , shutdown    :: Eff es ()
      }

Each element on the `source` stream is:

    data Ingested es msg = Ingested
      { envelope :: Envelope msg
      , ack      :: AckHandle es
      }

where `Envelope msg` wraps the payload with `messageId`, `cursor`, `partition`,
`enqueuedAt`, `traceContext`, and `payload`. The ack handle is:

    newtype AckHandle es = AckHandle
      { finalize :: AckDecision -> Eff es ()
      }

The framework calls `finalize` exactly once per envelope with the handler's decision:

    data AckDecision
      = AckOk
      | AckRetry       !RetryDelay
      | AckDeadLetter  !DeadLetterReason
      | AckHalt        !HaltReason

`RetryDelay` is a `NominalDiffTime` newtype. `DeadLetterReason` is a sum type with
constructors including `MaxRetriesExceeded`, `Unprocessable`, `ValidationFailed`, and
`CustomReason !Text` (the exact set may vary; refer to
`shibuya-core/src/Shibuya/Core/Ack.hs`). `HaltReason` is similarly an open sum type.

Because `shibuya-core` is compiled with `NoFieldSelectors`, do **not** use record dot
syntax across package boundaries. Destructure with record pattern matching:

    handleAck Ingested{envelope = Envelope{messageId}, ack = AckHandle finalize} = ...

**The InflightState contract from EP-2**, defined in
`shibuya-message-db-adapter/src/Shibuya/Adapter/MessageDb/Internal.hs`, is the key
interface this plan extends. At the start of EP-3 it looks like:

    data InflightState
      -- opaque; internal fields not stable
      --   stores the in-flight set keyed by GlobalPosition
      --   plus the last-contiguous-acked position

    data AckOutcome = AckComplete | AckRetry
      -- AckComplete means "position may advance through me"
      -- AckRetry means "position must not advance past me yet"

    newInflightState :: GlobalPosition -> STM InflightState
      -- initial arg is the position loaded from the checkpoint store on startup

    recordIngested :: InflightState -> GlobalPosition -> STM ()
      -- called when a message is handed to the source stream

    recordAckResult :: InflightState -> GlobalPosition -> AckOutcome -> STM ()
      -- called from the ack handle's finalize

    advanceCheckpointTo :: InflightState -> STM (Maybe GlobalPosition)
      -- returns the highest contiguous-prefix AckComplete position, if it moved

The EP-2 checkpoint fiber calls `advanceCheckpointTo` periodically (on a
`checkpointInterval` timer) and persists the returned position via the
`message-db-checkpoint-store` interpreter. On shutdown it calls `advanceCheckpointTo`
one final time before the adapter exits.

**What EP-3 adds to this contract**: the retry buffer becomes part of `InflightState`
but remains opaque. A new operation `scheduleRetry :: InflightState -> GlobalPosition -> NominalDiffTime -> MessageDb.Message -> STM Bool`
enqueues the message onto the retry buffer and returns `True` on success, `False` if
the buffer is full. The source stream's merge path reads retried messages from a
`TChan MessageDb.Message` that the retry fiber populates.

**message-db has no native support for delayed visibility or dead-letter queues.**
PGMQ has visibility timeouts; SQS has DLQs; message-db has neither. Both features are
therefore implemented in-process by the adapter. For DLQs, the adapter uses
`MessageDb.Effectful.writeStreamMessage` to publish a fresh message to a user-named
"dead-letter stream" — which is simply another message-db stream. The signature, from
`/Users/shinzui/Keikaku/work/libraries/haskell/message-db-hs-master/message-db-effectful/src/MessageDb/Effectful.hs`:

    writeStreamMessage ::
      (HasCallStack, MessageDb :> es, Error WrongExpectedVersion :> es) =>
      MessageDb.Message.NewMessage ->
      Eff es MessageDb.Message.MessagePosition

The `NewMessage` record, from
`/Users/shinzui/Keikaku/work/libraries/haskell/message-db-hs-master/message-db-hs/src/MessageDb/Message.hs`,
is:

    data NewMessage = NewMessage
      { messageId        :: !MessageId
      , stream           :: !Stream
      , messageType      :: !MessageType
      , messageData      :: !MessageData
      , messageMetadata  :: !MessageMetadata
      , expectedPosition :: !(Maybe MessagePosition)
      }

`Stream` is message-db's existing newtype wrapping a `Text` stream name; reuse it
directly rather than introducing a fresh type. Set `expectedPosition = Nothing` for
DLQ writes — we are not enforcing version checks on the DLQ stream.

**Term definitions used in this plan:**

- *Fiber*: a lightweight concurrent Haskell thread, created here via
  `Effectful.Concurrent`'s `forkIO`. The "retry fiber" and "poll fiber" are two such
  threads running inside the adapter.
- *Source stream*: the Streamly `Stream (Eff es) (Ingested es msg)` that the adapter
  exposes. In EP-3 this stream is the merge of polled messages and retried messages.
- *Retry buffer*: the `TQueue RetryEntry` carrying scheduled retries.
- *Notable time (`notBefore`)*: the `UTCTime` at which a buffered retry becomes
  eligible for re-emission. The retry fiber blocks on `registerDelay` or equivalent
  until `getCurrentTime >= notBefore`.
- *DLQ*: dead-letter queue; here, a named message-db stream the adapter writes to via
  `writeStreamMessage`.
- *Idempotent messageId*: a `MessageId` derived deterministically from an input so
  that repeated attempts produce the same identifier, allowing message-db's
  `(stream_name, message_id)` uniqueness to deduplicate.

**The `NoFieldSelectors` caveat** still applies: `Shibuya.Core.Types.Envelope`,
`Shibuya.Core.Ack.AckDecision`, etc., are defined with `NoFieldSelectors` on the
compiler flag, so use pattern matching against their data constructors rather than
record-dot accessors.


## Plan of Work

### Milestone 1: Retry buffer and re-emission

At the end of this milestone, the adapter can re-deliver a retried message after its
delay elapses, without advancing the checkpoint past it.

Starting state: EP-2 has shipped `InflightState` with `AckComplete` / `AckRetry`
outcomes. `advanceCheckpointTo` already refuses to advance past an `AckRetry` position.
What is missing is the *mechanism* to actually redeliver the retried message to the
handler.

Edit `shibuya-message-db-adapter/src/Shibuya/Adapter/MessageDb/Internal.hs` as
follows.

First, add a retry-entry record and extend the InflightState constructor:

    data RetryEntry = RetryEntry
      { retryPosition  :: !MessageDb.GlobalPosition
      , retryNotBefore :: !UTCTime
      , retryMessage   :: !MessageDb.Message
      }

    data InflightState = InflightState
      { inflightSet    :: !(TVar (Map GlobalPosition AckOutcome))
      , contiguousAck  :: !(TVar GlobalPosition)
      , retryBuffer    :: !(TQueue RetryEntry)
      , retryChannel   :: !(TChan MessageDb.Message)
      , retryCapacity  :: !Int
      , retrySize      :: !(TVar Int)
      }

(Exact field names may differ from what EP-2 chose; the shape above is prescriptive
for new fields and illustrative for existing ones. If EP-2 used different names for
`inflightSet` / `contiguousAck`, keep them and simply add the four retry-related
fields.)

Update `newInflightState` to take the `maxRetryBufferSize` from the config, create an
empty `TQueue`, `TChan`, and counter:

    newInflightState ::
      Int ->                       -- ^ maxRetryBufferSize
      MessageDb.GlobalPosition ->  -- ^ initial contiguousAck (from the checkpoint)
      STM InflightState

Implement `scheduleRetry`. It returns `Bool` so callers can detect full-buffer:

    scheduleRetry ::
      InflightState ->
      MessageDb.GlobalPosition ->
      NominalDiffTime ->
      UTCTime ->                   -- ^ current time, passed in for testability
      MessageDb.Message ->
      STM Bool
    scheduleRetry st pos delay now msg = do
      sz <- readTVar (retrySize st)
      if sz >= retryCapacity st
        then pure False
        else do
          writeTQueue (retryBuffer st) RetryEntry
            { retryPosition  = pos
            , retryNotBefore = addUTCTime delay now
            , retryMessage   = msg
            }
          modifyTVar' (retrySize st) (+ 1)
          modifyTVar' (inflightSet st) (Map.insert pos AckRetry)
          pure True

Implement the retry fiber:

    retryFiber ::
      (Concurrent :> es, IOE :> es) =>
      TVar Bool ->                 -- ^ shutdown
      InflightState ->
      Eff es ()
    retryFiber shutdown st = loop
      where
        loop = do
          stop <- liftIO . atomically $ readTVar shutdown
          if stop then pure () else do
            next <- liftIO . atomically $ peekTQueue (retryBuffer st)
            now  <- liftIO getCurrentTime
            let wait = diffUTCTime (retryNotBefore next) now
            when (wait > 0) (liftIO $ threadDelay (ceiling (wait * 1e6)))
            -- re-check shutdown in case it flipped during the sleep
            stop' <- liftIO . atomically $ readTVar shutdown
            unless stop' $ liftIO . atomically $ do
              _ <- readTQueue (retryBuffer st)
              modifyTVar' (retrySize st) (subtract 1)
              writeTChan (retryChannel st) (retryMessage next)
            loop

In `messageDbSource`, merge the retry channel with the polled messages. The polled
batch-producing logic from EP-1/EP-2 stays put; we interleave using Streamly's
`parMergeByM` or a simpler `async`-then-merge pattern. A simple approach: add a second
stream built from the `retryChannel` and merge it with the poll stream.

    messageDbSource cfg shutdown st =
      Stream.parMergeByM' takeEarlier pollStream retryStream
      where
        pollStream  = -- existing EP-1 stream over getCategoryMessages
        retryStream = Stream.repeatM $ liftIO . atomically $ readTChan (retryChannel st)

(If the exact Streamly API differs in the flake-pinned version, a `TMVar` hand-off
from the retry fiber to the source stream is an acceptable equivalent. Keep the merge
local to `Internal.hs` so the rest of the adapter does not need to know about it.)

In the ack handle, `AckRetry delay` becomes:

    AckRetry delay -> do
      now <- liftIO getCurrentTime
      ok  <- liftIO . atomically $
        scheduleRetry st pos (unRetryDelay delay) now msg
      unless ok $ finalize (AckDeadLetter MaxRetriesExceeded)

Where `msg` is captured in the closure when the ack handle is built for a particular
message. The downgrade on overflow is why `finalize` recurses on itself.

### Milestone 2: AckDeadLetter with DlqSkipAndLog

At the end of this milestone, `AckDeadLetter reason` under the default `DlqSkipAndLog`
strategy logs the reason and advances the checkpoint. No new modules are added.

In `shibuya-message-db-adapter/src/Shibuya/Adapter/MessageDb/Config.hs`, add:

    data DlqStrategy
      = DlqSkipAndLog
      | DlqWriteToStream !MessageDb.Message.Stream
      deriving (Eq, Show)

Extend `MessageDbAdapterConfig` with `dlqStrategy :: !DlqStrategy` and update
`defaultConfig` to set it to `DlqSkipAndLog`. Keep the field additive — no renames.

In `Internal.hs`'s ack handle, add the case:

    AckDeadLetter reason -> case dlqStrategy cfg of
      DlqSkipAndLog -> do
        logWarn $ "dead-lettered position "
               <> tshow pos <> ": " <> tshow reason
        liftIO . atomically $ recordAckResult st pos AckComplete
      DlqWriteToStream _ -> -- handled in Milestone 3
        pure ()

Verify `cabal build shibuya-message-db-adapter` still succeeds. EP-2's checkpoint
advancement tests continue to pass because `AckDeadLetter` under `DlqSkipAndLog` flows
through as `AckComplete` at the InflightState level, which is the pre-existing code
path.

### Milestone 3: AckDeadLetter with DlqWriteToStream

At the end of this milestone, dead-lettered messages are written to a named stream in
message-db with deterministic idempotent ids.

In `Internal.hs`, extend the ack handle:

    AckDeadLetter reason -> case dlqStrategy cfg of
      DlqSkipAndLog -> {- as in Milestone 2 -}
      DlqWriteToStream target -> do
        now <- liftIO getCurrentTime
        let newMsg = mkDlqMessage target msg reason now
        result <- tryWriteDlq newMsg
        case result of
          Right _                  -> pure ()
          Left DuplicateDlqEntry   -> pure ()   -- idempotent replay
          Left otherErr            -> logError ("DLQ write failed: " <> tshow otherErr)
                                     >> -- escalate? see note below
                                     pure ()
        liftIO . atomically $ recordAckResult st pos AckComplete

Add a helper:

    mkDlqMessage ::
      MessageDb.Message.Stream ->   -- ^ target DLQ stream
      MessageDb.Message ->          -- ^ original failed message
      DeadLetterReason ->
      UTCTime ->
      MessageDb.Message.NewMessage
    mkDlqMessage target orig reason now = NewMessage
      { messageId        = dlqMessageId (messageId orig)
      , stream           = target
      , messageType      = MessageType "$DeadLetter"
      , messageData      = orig ^. messageData   -- preserve payload
      , messageMetadata  = buildDlqMetadata orig reason now
      , expectedPosition = Nothing
      }

    dlqMessageId :: MessageDb.Message.MessageId -> MessageDb.Message.MessageId
    dlqMessageId original =
      -- UUIDv5 of dlqNamespace + bytes(original) + "-dlq"
      let bytes = UUID.toByteString (unMessageId original) <> "-dlq"
      in MessageId (UUID.generateNamed dlqNamespace (BS.unpack bytes))

    dlqNamespace :: UUID.UUID
    dlqNamespace = UUID.fromWords 0x5486e5b1 0x000a 0x5ec0
                                  0x00 0x00 0x73 0x68 0x62 0x79 0x64 0x6c
      -- "shbydl" in ASCII; any fixed namespace UUID is fine as long as it
      -- is a constant compiled into the adapter.

(The exact UUIDv5 helper name from `uuid` is `Data.UUID.V5.generateNamed`; import
accordingly. If the in-tree `uuid` package lacks v5, substitute any deterministic
256-bit hash of the input truncated to a v4-shaped byte layout; the important
property is determinism, not cryptographic strength.)

The metadata helper:

    buildDlqMetadata ::
      MessageDb.Message ->
      DeadLetterReason ->
      UTCTime ->
      MessageDb.Message.MessageMetadata
    buildDlqMetadata orig reason now = MessageMetadata . Aeson.object $
      [ "correlation"       .= originalCorrelation
      , "causation"         .= UUID.toText (unMessageId (messageId orig))
      , "originalStream"    .= Mdb.toText (stream orig)
      , "deadLetterReason"  .= renderReason reason
      , "deadLetteredAt"    .= now
      ]
      where
        originalCorrelation =
          case Mdb.unMessageMetadata (messageMetadata orig) of
            Aeson.Object obj -> KeyMap.lookup "correlation" obj
            _                -> Nothing

        renderReason = \case
          MaxRetriesExceeded -> "max_retries_exceeded" :: Text
          Unprocessable      -> "unprocessable"
          ValidationFailed   -> "validation_failed"
          CustomReason t     -> t

The idempotent write:

    data DlqWriteError = DuplicateDlqEntry | OtherDlqError Text

    tryWriteDlq ::
      (MessageDb :> es, Error WrongExpectedVersion :> es) =>
      MessageDb.Message.NewMessage ->
      Eff es (Either DlqWriteError MessageDb.Message.MessagePosition)
    tryWriteDlq nm =
      (Right <$> writeStreamMessage nm)
        `catchError` \wev -> pure (Left DuplicateDlqEntry)
        -- message-db surfaces a stream-version mismatch when the (stream, id)
        -- pair already exists. If finer-grained error discrimination is needed,
        -- inspect the WrongExpectedVersion payload and map to DuplicateDlqEntry
        -- only when the SQLSTATE is 23505 (unique_violation).

(The precise mapping from message-db's SQL error to a Haskell error depends on how
`message-db-effectful`'s `runMessageDb` rethrows. Read
`/Users/shinzui/Keikaku/work/libraries/haskell/message-db-hs-master/message-db-hs/src/MessageDb/Db/Errors.hs`
and adapt. If `WrongExpectedVersion` is not the right carrier for "duplicate id",
widen the catch to `Effectful.Error.Static.Error` over the adapter's session error
type, inspect the SQLSTATE, and only swallow unique violations.)

### Milestone 4: AckHalt wiring

At the end of this milestone, a handler returning `AckHalt` tears the adapter down
cleanly and leaves the checkpoint strictly before the halted position.

In `Internal.hs`'s ack handle:

    AckHalt reason -> do
      logError $ "adapter halting at position " <> tshow pos
              <> ": " <> tshow reason
      liftIO . atomically $ writeTVar shutdownTVar True
      -- intentionally no recordAckResult: the in-flight entry for `pos`
      -- stays in the map forever, blocking advanceCheckpointTo from
      -- crossing it.

Make sure the *poll fiber* and *retry fiber* both respect the `shutdownTVar`. The
poll fiber already does (EP-1/EP-2). The retry fiber checks `shutdown` on every loop
iteration, both before and after the `threadDelay`, so a halt mid-sleep still
terminates within one retry cadence.

In `messageDbAdapter`'s top-level action, on shutdown: call `advanceCheckpointTo` one
final time and persist the resulting position via the EP-2 checkpoint interpreter.
Because the halted position was never `recordAckResult`-ed, this final persist will
not include it; the next start will resume from the last-ack'd predecessor and
redeliver the halted position naturally.

### Milestone 5: Config extensions and defaults

At the end of this milestone, `MessageDbAdapterConfig` exposes the new fields and
`defaultConfig` sets sensible defaults.

In `Shibuya.Adapter.MessageDb.Config`:

    data MessageDbAdapterConfig = MessageDbAdapterConfig
      { category             :: !CategoryStream
      , batchSize            :: !BatchSize
      , pollInterval         :: !PollInterval
      , drainTimeout         :: !DrainTimeout
      -- EP-2 additions:
      , subscriptionName     :: !SubscriptionName
      , checkpointInterval   :: !NominalDiffTime
      -- EP-3 additions:
      , dlqStrategy          :: !DlqStrategy
      , maxRetryBufferSize   :: !Int
      }

    defaultConfig :: CategoryStream -> MessageDbAdapterConfig
    defaultConfig cat = MessageDbAdapterConfig
      { category             = cat
      , batchSize            = BatchSize 100
      , pollInterval         = PollInterval 0.5
      , drainTimeout         = DrainTimeout 10
      , subscriptionName     = SubscriptionName "default"  -- from EP-2
      , checkpointInterval   = 1.0                          -- from EP-2
      , dlqStrategy          = DlqSkipAndLog
      , maxRetryBufferSize   = 1000
      }

Verify that EP-1 and EP-2's demo binaries still compile by re-running `cabal build all`.

### Milestone 6: Tests

At the end of this milestone, both the unit test suite and the integration test
harness cover the new behavior.

Unit tests live in `shibuya-message-db-adapter/test/`. Add a new module
`test/RetryBufferTest.hs`:

    module RetryBufferTest (tests) where

    import Test.Tasty
    import Test.Tasty.HUnit
    import Shibuya.Adapter.MessageDb.Internal
            ( newInflightState, scheduleRetry, retryBufferSize )
    import qualified MessageDb.Message as Mdb
    import Data.Time.Clock
    import Control.Concurrent.STM

    tests :: TestTree
    tests = testGroup "retry buffer"
      [ testCase "honours notBefore delay" testHonoursDelay
      , testCase "full buffer returns False" testFullBufferReturnsFalse
      , testCase "scheduled entry eventually surfaces on the channel" testSurfaces
      ]

    -- implementations spelled out for each testCase

Add `test/DlqTest.hs` with:

    - testCase "dlqMessageId is deterministic"
    - testCase "dlqMessageId differs from input id"
    - testCase "buildDlqMetadata preserves correlation/causation"

Register both in `test/Main.hs`:

    main = defaultMain $ testGroup "shibuya-message-db-adapter"
      [ ConvertTest.tests
      , CheckpointTest.tests     -- from EP-2
      , RetryBufferTest.tests
      , DlqTest.tests
      ]

For integration tests, extend `test/IntegrationTest.hs` (introduced by EP-2's
ephemeral-pg harness — if EP-2 deferred this to EP-5, add a temporary test here and
move it into the jitsurei harness in EP-5). The three scenarios are:

1. *retry-then-ok*: 5 messages to `orders-retrydemo`. Handler returns `AckRetry 0.1`
   for the 3rd message on first delivery, else `AckOk`. Wait up to 5 seconds. Assert
   all 5 see `AckOk`, handler invoked 6 times total, final persisted checkpoint
   equals position of message 5.

2. *deadletter-write*: 5 messages. Handler returns `AckDeadLetter Unprocessable` for
   message 3. Config uses `DlqWriteToStream (Stream "orders.dlq")`. After test run,
   query `psql -c "SELECT message_id, stream_name, data, metadata FROM message_store.messages WHERE stream_name = 'orders.dlq';"`
   and assert exactly one row with `metadata->>'correlation'` equal to message 3's id.

3. *halt*: 5 messages. Handler returns `AckHalt ManualStop` for message 3. Assert
   adapter shuts down within `drainTimeout` seconds. Assert persisted checkpoint
   equals position of message 2. Start a fresh adapter with the same subscription
   name; assert the first `Ingested` it emits has `globalPosition` equal to message
   3's position.

Use the ephemeral-pg pattern documented in
`/Users/shinzui/Keikaku/work/libraries/haskell/message-db-hs-master` (if EP-2 has not
yet added this harness, check that project's `package.yaml`/tests for the
`ephemeral-pg` usage pattern — the message-db-hs integration tests are the canonical
example).


## Concrete Steps

Run from `/Users/shinzui/Keikaku/work/libraries/haskell/shibuya-message-db-adapter`,
from inside `nix develop`.

Compile after each milestone:

    cabal build shibuya-message-db-adapter

Expected tail:

    Registering library for shibuya-message-db-adapter-0.1.0.0..

Run the unit tests:

    cabal test shibuya-message-db-adapter

Expected output (approximate — exact counts depend on EP-1 and EP-2's test totals):

    shibuya-message-db-adapter-tests
      convert
        empty metadata yields Nothing traceContext:                  OK
        only traceparent yields one header:                          OK
        traceparent + tracestate yields two headers in order:        OK
        non-object metadata yields Nothing:                          OK
      checkpoint
        ... (EP-2 tests)
      retry buffer
        honours notBefore delay:                                     OK (0.20s)
        full buffer returns False:                                   OK
        scheduled entry eventually surfaces on the channel:          OK (0.11s)
      dlq
        dlqMessageId is deterministic:                               OK
        dlqMessageId differs from input id:                          OK
        buildDlqMetadata preserves correlation/causation:            OK

    All N tests passed

Run the integration suite (requires `just process-up` in another shell, or the
ephemeral-pg harness — see EP-2 for setup):

    cabal test shibuya-message-db-adapter:integration

Expected (approximate):

    integration
      retry-then-ok:     OK (4.2s)
      deadletter-write:  OK (3.1s)
      halt:              OK (3.8s)

Manual demo. Shell 1 (from `nix develop`):

    just process-up

Shell 2 (from `nix develop`):

    just seed-messages orders-retrydemo      # seeds 3 messages
    cabal run shibuya-message-db-adapter-demo -- \
      --category orders-retrydemo \
      --retry-each-once 2

The `--retry-each-once 2` flag (added to `app/Demo.hs` in this plan) configures the
demo handler to return `AckRetry (RetryDelay 2)` on the first delivery of each
message and `AckOk` on the second. Expected stdout:

    message: orders-retrydemo-1 pos 123 (delivery 1) -> retry in 2s
    message: orders-retrydemo-2 pos 124 (delivery 1) -> retry in 2s
    message: orders-retrydemo-3 pos 125 (delivery 1) -> retry in 2s
    message: orders-retrydemo-1 pos 123 (delivery 2) -> ok
    message: orders-retrydemo-2 pos 124 (delivery 2) -> ok
    message: orders-retrydemo-3 pos 125 (delivery 2) -> ok

The three "delivery 2" lines appear approximately 2 seconds after the "delivery 1"
lines. Ctrl-C exits cleanly.

For the DLQ demo variant, add a flag `--dlq-at 3` that configures the handler to
return `AckDeadLetter Unprocessable` for the 3rd message. Run:

    just seed-messages orders-dlqdemo
    cabal run shibuya-message-db-adapter-demo -- \
      --category orders-dlqdemo \
      --dlq-at 3 \
      --dlq-stream orders.dlq

Expected stdout shows four `ok` lines and one `dead-lettered` line. Then:

    psql -c "SELECT message_id, type, metadata FROM message_store.messages WHERE stream_name = 'orders.dlq';"

Shows exactly one row with `type = '$DeadLetter'` and metadata containing the
`correlation` of the original message 3.

For the halt demo variant, add `--halt-at 3`. Run and observe the process exits with
code 0 after printing the first two `ok` lines and the halt log. Re-run **without**
`--halt-at` on the same subscription name and observe delivery resumes from message 3.


## Validation and Acceptance

The plan is complete when:

1. `cabal build all` succeeds with zero new warnings attributable to this plan.
2. `cabal test shibuya-message-db-adapter` reports all unit tests passing, including
   the new retry-buffer and DLQ test groups.
3. `cabal test shibuya-message-db-adapter:integration` reports all three integration
   scenarios (retry-then-ok, deadletter-write, halt) passing.
4. The manual demo with `--retry-each-once 2` prints each of three seeded messages
   twice, two seconds apart, and the final observed checkpoint equals the global
   position of the third message.
5. The manual DLQ demo writes exactly one row to `orders.dlq` whose metadata
   `correlation` matches the original message's id.
6. The manual halt demo exits cleanly and a fresh run resumes at the halted message's
   position.

Non-goals (explicit): custom `DlqCustom` strategy, per-message retry limits (one-retry
vs infinite-retry is the caller's responsibility today; a `maxRetries` config is a
future addition), DLQ-of-DLQ semantics, and replaying from the DLQ back to the
original stream. Any of those attempted here should be pushed to a follow-up plan.


## Idempotence and Recovery

The retry buffer is volatile: entries live only in `TQueue` and do not survive a
process restart. This is intentional. On restart, the adapter's `InflightState` is
reconstructed from the persisted checkpoint (EP-2), and any messages that were in
flight at the time of the crash — including those queued for retry — are simply
re-polled from the last-acked position. The retry delay is effectively reset; a
message that was scheduled for retry in 5 seconds immediately before a crash will be
re-delivered immediately on restart. Users who need persistent retries should build
that on top of the DLQ mechanism (write-to-DLQ, drain-DLQ-separately).

The DLQ write is idempotent by construction: re-writing the same `(target stream,
derived messageId)` fails with a unique-violation, which the adapter catches and
treats as success. This makes the DLQ step safely retryable if the adapter crashes
between `writeStreamMessage` succeeding and `recordAckResult` completing — the next
attempt is a no-op on the DB side and the `recordAckResult` completes on retry.

The halt path is idempotent: setting the shutdown TVar to `True` when it is already
`True` is a no-op. `advanceCheckpointTo` at the final persist is safe to call multiple
times; it returns `Nothing` if nothing has moved since the last call.

Configuration changes are safe to roll forward and back: the new fields
`dlqStrategy` and `maxRetryBufferSize` have defaults that preserve EP-2 behavior. A
user upgrading from an EP-2-only build only needs to recompile; no migration of the
checkpoint store or any DB schema is required by this plan.

Rolling back EP-3 is `git reset --hard` to the EP-2 tip. No schema changes are
introduced by EP-3 (the DLQ target stream is just another message-db stream;
message-db creates streams on first write).


## Interfaces and Dependencies

Libraries used (beyond EP-1 and EP-2):

- `stm` — the retry `TQueue`, `TChan`, and `TVar` counters.
- `uuid`, `uuid-types` — UUIDv5 generation for idempotent DLQ messageIds. If the
  `uuid` package's `generateNamed` is unavailable, fall back to a deterministic
  `Crypto.Hash.SHA256`-based derivation (both packages are already in the
  flake-managed package set).
- `aeson`, `aeson-key-map` — building DLQ metadata JSON.
- `time` — `UTCTime`, `NominalDiffTime`, `addUTCTime`, `diffUTCTime`.
- `effectful-core`, `effectful` — existing effect surface; no new effects.

At the end of Milestone 1, the following must exist:

    module Shibuya.Adapter.MessageDb.Internal where

      data RetryEntry = RetryEntry
        { retryPosition  :: !MessageDb.Message.GlobalPosition
        , retryNotBefore :: !UTCTime
        , retryMessage   :: !MessageDb.Message.Message
        }

      -- InflightState extended with retryBuffer, retryChannel,
      -- retryCapacity, retrySize fields (opaque to callers).

      newInflightState ::
        Int ->                                  -- maxRetryBufferSize
        MessageDb.Message.GlobalPosition ->     -- initial contiguousAck
        STM InflightState

      scheduleRetry ::
        InflightState ->
        MessageDb.Message.GlobalPosition ->
        NominalDiffTime ->
        UTCTime ->
        MessageDb.Message.Message ->
        STM Bool

      retryFiber ::
        (Concurrent :> es, IOE :> es) =>
        TVar Bool ->
        InflightState ->
        Eff es ()

At the end of Milestone 3, the following must exist:

    module Shibuya.Adapter.MessageDb.Config where

      data DlqStrategy
        = DlqSkipAndLog
        | DlqWriteToStream !MessageDb.Message.Stream
        deriving (Eq, Show)

      -- MessageDbAdapterConfig record with the two new fields

    module Shibuya.Adapter.MessageDb.Internal where  -- or a new .Dlq module

      mkDlqMessage ::
        MessageDb.Message.Stream ->
        MessageDb.Message.Message ->
        Shibuya.Core.Ack.DeadLetterReason ->
        UTCTime ->
        MessageDb.Message.NewMessage

      dlqMessageId ::
        MessageDb.Message.MessageId ->
        MessageDb.Message.MessageId

      tryWriteDlq ::
        (MessageDb :> es, Error WrongExpectedVersion :> es) =>
        MessageDb.Message.NewMessage ->
        Eff es (Either DlqWriteError MessageDb.Message.MessagePosition)

At the end of Milestone 4, the ack handle's `finalize` must dispatch on all four
`AckDecision` constructors with no `_` catch-all. A unit test asserting that
dispatch is exhaustive (via `-Wincomplete-patterns` being an error) guards against
future regressions.

At the end of Milestone 5, `defaultConfig` must compile against the existing EP-1 and
EP-2 call sites; the demo executable must build and run without any flag changes.

At the end of Milestone 6, both `cabal test shibuya-message-db-adapter` and
`cabal test shibuya-message-db-adapter:integration` pass; the manual retry demo
exhibits the two-seconds-apart redelivery described under Validation and Acceptance.

**Ordering semantics note** (include as module haddock on
`Shibuya.Adapter.MessageDb`):

Retries scheduled by a handler can arrive out of order relative to newly-polled
messages, because the retry fiber and the poll fiber merge asynchronously into the
source stream. For Shibuya's `Unordered` processors (the default) this is harmless —
each message is handled independently. For `StrictInOrder` processors, users should
set their handler to return `AckRetry` only with `RetryDelay 0` and accept that
retries come back at the head of the inbox with no delay, or configure
`maxRetryBufferSize = 0` to convert all retries into immediate dead-letters. The
adapter does not currently expose a `StrictInOrder`-compatible retry mode; adding one
is a future plan.
