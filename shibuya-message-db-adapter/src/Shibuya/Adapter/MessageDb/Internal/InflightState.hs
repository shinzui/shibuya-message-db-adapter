{- | Contiguous-prefix bookkeeping for inflight messages, plus a bounded
in-process retry buffer.

message-db tracks consumer progress with a single @GlobalPosition@ — the
highest position a subscription has processed. Shibuya handlers, by
contrast, ack each message independently, and may return
@AckRetry@/@AckDeadLetter@/@AckHalt@ on an arbitrary position while later
positions succeed.

This module reconciles the two models. It maintains a per-position
ledger of inflight messages and their ack outcomes, and exposes
'advanceCheckpointTo', which returns the longest gap-free run of
completed positions starting just past the last saved checkpoint. A
pending @AckRetry@ at position N pins the checkpoint at N-1 until N
either completes or is abandoned — the \"contiguous-prefix\" rule.

On top of the ledger sit two retry side-channels: a 'TQueue' of pending
retry entries (time-ordered by their submitting call to 'scheduleRetry')
and a 'TChan' that the retry fiber publishes ready-to-redeliver messages
to. The adapter's poll loop drains the channel each iteration and
interleaves retries with newly-polled messages.

The whole structure is held behind opaque STM-shaped fields; callers use
'STM' to compose @recordIngested@/@recordAckResult@/@scheduleRetry@ with
other transactional bookkeeping (for example, signalling shutdown
atomically with recording a halt).

This module has no external side effects and is fully testable in
isolation.
-}
module Shibuya.Adapter.MessageDb.Internal.InflightState (
    InflightState,
    AckOutcome (..),
    RetryEntry (..),
    newInflightState,
    recordIngested,
    recordAckResult,
    advanceCheckpointTo,
    inflightSize,

    -- * Retry buffer (EP-3)
    scheduleRetry,
    tryPeekRetry,
    awaitRetryHeadOrShutdown,
    popRetryHeadToChannel,
    drainRetryChannel,
    retryBufferSize,
)
where

import Control.Concurrent.STM (
    STM,
    TChan,
    TQueue,
    TVar,
    modifyTVar',
    newTChanIO,
    newTQueueIO,
    newTVarIO,
    orElse,
    peekTQueue,
    readTQueue,
    readTVar,
    retry,
    tryPeekTQueue,
    tryReadTChan,
    writeTChan,
    writeTQueue,
    writeTVar,
 )
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Time.Clock (NominalDiffTime, UTCTime, addUTCTime)
import MessageDb.Message qualified as Mdb

{- | The outcome recorded for a previously-ingested position.

'AckComplete' means the position is safe to advance the checkpoint
past; 'AckRetry' pins the checkpoint until the position is re-run.
-}
data AckOutcome
    = AckComplete
    | AckRetry
    deriving stock (Eq, Show)

{- | A scheduled retry: the failed message plus the earliest time it
may be re-delivered to the handler.

The retry fiber peeks the head of the retry buffer, blocks until
@retryNotBefore@ has passed, then publishes @retryMessage@ on the
retry channel for the poll loop to pick up.
-}
data RetryEntry = RetryEntry
    { retryPosition :: !Mdb.GlobalPosition
    , retryNotBefore :: !UTCTime
    , retryMessage :: !Mdb.Message
    }
    deriving stock (Eq, Show)

-- Internal, not exported.
data InflightStateS = InflightStateS
    { lastSaved :: !Mdb.GlobalPosition
    , outcomes :: !(Map Mdb.GlobalPosition (Maybe AckOutcome))
    , highestIngested :: !Mdb.GlobalPosition
    }

{- | Opaque ledger of inflight messages, ack outcomes, and pending
retries. Construct with 'newInflightState'.
-}
data InflightState = InflightState
    { innerState :: !(TVar InflightStateS)
    , retryBuffer :: !(TQueue RetryEntry)
    , retryChannel :: !(TChan Mdb.Message)
    , retryCapacity :: !Int
    , retrySize :: !(TVar Int)
    }

{- | Create an empty ledger seeded with @stored@ as the initial
@lastSaved@ position and @cap@ as the retry buffer capacity.

Callers pass the value returned by 'MessageDb.CheckpointStore.Effectful.getLastCheckpoint',
defaulting to @GlobalPosition 0@ when the subscription has never run.
@cap@ should come from @MessageDbAdapterConfig.maxRetryBufferSize@.
-}
newInflightState :: Int -> Mdb.GlobalPosition -> IO InflightState
newInflightState cap stored = do
    inner <-
        newTVarIO
            InflightStateS
                { lastSaved = stored
                , outcomes = Map.empty
                , highestIngested = stored
                }
    buf <- newTQueueIO
    chan <- newTChanIO
    sz <- newTVarIO 0
    pure
        InflightState
            { innerState = inner
            , retryBuffer = buf
            , retryChannel = chan
            , retryCapacity = cap
            , retrySize = sz
            }

{- | Record that a message at @pos@ has been emitted by the source
stream but not yet finalized.

Inserts @(pos, Nothing)@ into the ledger and bumps @highestIngested@
if @pos@ exceeds it. No-op if @pos@ is already tracked — importantly,
a retry re-ingesting the same position keeps its existing
@Just AckRetry@ outcome until the handler finalizes again.
-}
recordIngested :: InflightState -> Mdb.GlobalPosition -> STM ()
recordIngested st pos =
    modifyTVar' st.innerState $ \s ->
        s
            { outcomes = Map.insertWith (\_new old -> old) pos Nothing s.outcomes
            , highestIngested = max s.highestIngested pos
            }

{- | Record the ack outcome for a previously-ingested position.

Updates @outcomes[pos]@ to @Just outcome@. Silently ignores positions
not present in the ledger (idempotent against double-finalize, and
against finalize-for-a-position-we-never-ingested).
-}
recordAckResult ::
    InflightState ->
    Mdb.GlobalPosition ->
    AckOutcome ->
    STM ()
recordAckResult st pos outcome =
    modifyTVar' st.innerState $ \s ->
        s
            { outcomes = Map.adjust (const (Just outcome)) pos s.outcomes
            }

{- | Walk the ledger from @lastSaved + 1@ upward, advancing past every
@Just AckComplete@ entry. Stop at the first position that is missing,
still @Nothing@ (ingested-but-not-finalized), or @Just AckRetry@.

Remove advanced-past entries from the ledger so it does not grow
unboundedly. If any advancement occurred, update @lastSaved@ and
return @Just newLastSaved@; otherwise return @Nothing@.
-}
advanceCheckpointTo :: InflightState -> STM (Maybe Mdb.GlobalPosition)
advanceCheckpointTo st = do
    s <- readTVar st.innerState
    let (newLastSaved, remaining) = walk s.lastSaved s.outcomes
    if newLastSaved == s.lastSaved
        then pure Nothing
        else do
            writeTVar st.innerState s{lastSaved = newLastSaved, outcomes = remaining}
            pure (Just newLastSaved)
  where
    walk ::
        Mdb.GlobalPosition ->
        Map Mdb.GlobalPosition (Maybe AckOutcome) ->
        (Mdb.GlobalPosition, Map Mdb.GlobalPosition (Maybe AckOutcome))
    walk current m =
        let Mdb.GlobalPosition n = current
            nextPos = Mdb.GlobalPosition (n + 1)
         in case Map.lookup nextPos m of
                Just (Just AckComplete) -> walk nextPos (Map.delete nextPos m)
                _ -> (current, m)

{- | Number of ledger entries whose outcome is still @Nothing@.

Used by the adapter's shutdown path to wait for in-flight messages to
drain before flushing the final checkpoint.
-}
inflightSize :: InflightState -> STM Int
inflightSize st = do
    s <- readTVar st.innerState
    pure $ Map.size (Map.filter (== Nothing) s.outcomes)

{- | Enqueue @msg@ at @pos@ for re-delivery no earlier than
@now + delay@.

Returns 'True' if the retry was buffered; returns 'False' if the retry
buffer is already at capacity. Callers that receive 'False' should
treat the retry as exhausted — typically by dead-lettering the message
with @MaxRetriesExceeded@.

On successful enqueue, marks the position as 'AckRetry' in the ledger
so 'advanceCheckpointTo' stops at @pos - 1@ until the retry completes.
On failure the ledger is left untouched: the caller's follow-up
@recordAckResult@ from the DLQ path will be the authoritative outcome.
-}
scheduleRetry ::
    InflightState ->
    Mdb.GlobalPosition ->
    NominalDiffTime ->
    UTCTime ->
    Mdb.Message ->
    STM Bool
scheduleRetry st pos delay now msg = do
    sz <- readTVar st.retrySize
    if sz >= st.retryCapacity
        then pure False
        else do
            writeTQueue st.retryBuffer $
                RetryEntry
                    { retryPosition = pos
                    , retryNotBefore = addUTCTime delay now
                    , retryMessage = msg
                    }
            modifyTVar' st.retrySize (+ 1)
            modifyTVar' st.innerState $ \s ->
                s{outcomes = Map.insert pos (Just AckRetry) s.outcomes}
            pure True

{- | Peek at the head of the retry buffer without removing it.

Returns 'Nothing' if the buffer is empty. Used by the retry fiber to
check how long to wait before the head becomes ready.
-}
tryPeekRetry :: InflightState -> STM (Maybe RetryEntry)
tryPeekRetry st = tryPeekTQueue st.retryBuffer

{- | Block in a single STM transaction until either the retry buffer
has a head entry or the shutdown 'TVar' flips to 'True'.

Returns @Just entry@ on the former and @Nothing@ on the latter. This
is the retry fiber's wakeup primitive — it avoids a busy-poll on the
buffer while still responding promptly to shutdown.
-}
awaitRetryHeadOrShutdown :: TVar Bool -> InflightState -> STM (Maybe RetryEntry)
awaitRetryHeadOrShutdown shutdown st =
    (Just <$> peekTQueue st.retryBuffer)
        `orElse` ( do
                    stop <- readTVar shutdown
                    if stop then pure Nothing else retry
                 )

{- | Atomically remove the head of the retry buffer and publish its
message on the retry channel for the poll loop to pick up.

No-op if the buffer is empty. Called by the retry fiber once a head
entry's @retryNotBefore@ has elapsed.
-}
popRetryHeadToChannel :: InflightState -> STM ()
popRetryHeadToChannel st = do
    sz <- readTVar st.retrySize
    if sz <= 0
        then pure ()
        else do
            entry <- readTQueue st.retryBuffer
            modifyTVar' st.retrySize (subtract 1)
            writeTChan st.retryChannel entry.retryMessage

{- | Drain every message currently available on the retry channel in a
single STM transaction.

Returns the drained messages in the order the retry fiber published
them (FIFO). Called by the poll loop at the start of each iteration so
retries interleave with freshly-polled messages.
-}
drainRetryChannel :: InflightState -> STM [Mdb.Message]
drainRetryChannel st = go []
  where
    go acc = do
        m <- tryReadTChan st.retryChannel
        case m of
            Nothing -> pure (reverse acc)
            Just msg -> go (msg : acc)

{- | The number of pending retries currently held in the buffer (not
yet published on the retry channel).

Intended for tests and diagnostics. Does not include retries already
drained to the channel but not yet picked up by the poll loop.
-}
retryBufferSize :: InflightState -> STM Int
retryBufferSize st = readTVar st.retrySize
