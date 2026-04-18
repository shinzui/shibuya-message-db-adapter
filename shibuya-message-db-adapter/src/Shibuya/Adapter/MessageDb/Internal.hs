{- | Internal implementation details for the message-db adapter.

This module is not part of the public API and may change without notice.
The public surface lives in "Shibuya.Adapter.MessageDb".

EP-3 extends the adapter with retry, dead-letter, and halt handling.
The three new pieces are:

* 'retryFiber' — a background thread that consumes
  'Shibuya.Adapter.MessageDb.Internal.InflightState.scheduleRetry'
  submissions, waits out each entry's @notBefore@ window, and publishes
  the message on the retry channel for the poll loop to pick up.

* 'mkAckHandle' — now dispatches on all four 'AckDecision'
  constructors. 'AckOk' and 'AckDeadLetter' advance the ledger;
  'AckRetry' schedules a retry (downgrading to DLQ on buffer overflow);
  'AckHalt' flips the shutdown signal and deliberately does /not/
  record an ack outcome, so the contiguous-prefix checkpoint stops at
  @haltedPos - 1@.

* 'messageDbSource' — the poll loop drains the retry channel each
  iteration and interleaves retried messages with freshly-polled ones.

Retries can arrive out of order relative to newly-polled messages
because the retry fiber and the poll fiber merge asynchronously into
the source stream. For Shibuya's @Unordered@ processors (the default)
this is harmless. For @StrictInOrder@ processors, callers should set
@maxRetryBufferSize = 0@ to convert all retries into immediate
dead-letters, or avoid returning 'Shibuya.Core.Ack.AckRetry' from
their handlers.
-}
module Shibuya.Adapter.MessageDb.Internal (
    messageDbSource,
    mkAckHandle,
    mkIngested,
    mkGetCategoryQuery,
    parseCategoryStream,
    runCheckpointPersister,
    retryFiber,
    nominalToMicros,
)
where

import Control.Concurrent.STM (
    TVar,
    atomically,
    readTVarIO,
    writeTVar,
 )
import Control.Monad (unless, when)
import Control.Monad.IO.Class (liftIO)
import Data.Function ((&))
import Data.IORef (IORef, atomicModifyIORef', readIORef)
import Data.Text qualified as Text
import Data.Text.IO qualified as Text.IO
import Data.Time.Clock (
    NominalDiffTime,
    diffUTCTime,
    getCurrentTime,
    nominalDiffTimeToSeconds,
 )
import Data.Vector (Vector)
import Data.Vector qualified as Vector
import Effectful (Eff, IOE, (:>))
import Effectful.Concurrent (Concurrent, threadDelay)
import Effectful.Error.Static (Error)
import Effectful.Hasql (SessionError)
import MessageDb.CheckpointStore.Effectful (CheckpointStore, SubscriptionName, storeCheckpoint)
import MessageDb.Db.Statements (GetCategoryMessagesQuery (..))
import MessageDb.Db.Statements.BatchSize qualified as MdbBatch
import MessageDb.Effectful (MessageDb, getCategoryMessages)
import MessageDb.Message qualified as Mdb
import MessageDb.Message.Stream.CategoryStream qualified as CategoryStream
import Shibuya.Adapter.MessageDb.Config (
    BatchSize (..),
    CategoryStream (..),
    DlqStrategy (..),
    MessageDbAdapterConfig (..),
    PollInterval (..),
 )
import Shibuya.Adapter.MessageDb.Convert (messageToEnvelope)
import Shibuya.Adapter.MessageDb.Internal.Dlq (
    DlqWriteError (..),
    mkDlqMessage,
    tryWriteDlq,
 )
import Shibuya.Adapter.MessageDb.Internal.InflightState (
    InflightState,
    advanceCheckpointTo,
    awaitRetryHeadOrShutdown,
    drainRetryChannel,
    popRetryHeadToChannel,
    recordAckResult,
    recordIngested,
    scheduleRetry,
 )
import Shibuya.Adapter.MessageDb.Internal.InflightState qualified as Inflight
import Shibuya.Core.Ack (
    AckDecision (..),
    DeadLetterReason (..),
    RetryDelay (..),
 )
import Shibuya.Core.AckHandle (AckHandle (..))
import Shibuya.Core.Ingested (Ingested (..))
import Streamly.Data.Stream (Stream)
import Streamly.Data.Stream qualified as Stream
import Streamly.Data.Unfold qualified as Unfold
import System.IO (stderr)

{- | Parse the adapter's 'CategoryStream' newtype into message-db's
internal @CategoryStream@ type, failing loudly on an invalid name.

Message-db rejects names containing @-@ (reserved for the entity
separator), so an invalid name is a user misconfiguration that should
surface at startup, not silently produce empty polls.
-}
parseCategoryStream :: CategoryStream -> CategoryStream.CategoryStream
parseCategoryStream (CategoryStream txt) =
    case CategoryStream.parseEither txt of
        Right c -> c
        Left err ->
            error $
                "Shibuya.Adapter.MessageDb: invalid category "
                    <> Text.unpack txt
                    <> ": "
                    <> Text.unpack err

{- | Build a @GetCategoryMessagesQuery@ for the current poll cycle.

@startAt@ is the next @GlobalPosition@ the adapter has not yet emitted;
the caller advances it after each batch.
-}
mkGetCategoryQuery ::
    MessageDbAdapterConfig ->
    Mdb.GlobalPosition ->
    GetCategoryMessagesQuery
mkGetCategoryQuery config startAt =
    let BatchSize bsize = config.batchSize
     in GetCategoryMessagesQuery
            { categoryName = parseCategoryStream config.category
            , globalPositionStart = Just startAt
            , batchSize = Just (MdbBatch.Limit (fromIntegral bsize))
            , correlation = Nothing
            , consumerGroupMember = Nothing
            , consumerGroupSize = Nothing
            , condition = Nothing
            }

{- | Create an 'AckHandle' backed by the inflight ledger.

Dispatches exhaustively on every 'AckDecision' constructor. No
catch-all, so missing a new constructor becomes a pattern-match warning
(elevated to an error by the package's @-Wincomplete-record-updates@
companion family).

Mapping:

* 'AckOk' → records 'AckComplete' in the ledger; the contiguous
  checkpoint can advance past this position.

* 'AckDeadLetter reason' → runs the configured 'DlqStrategy'
  (logging-and-skipping or writing to the named stream), then records
  'AckComplete'. A failure during the DLQ write is logged but does
  not block advancement — dropping a DLQ copy under duress is
  preferable to stalling the whole subscription.

* 'AckRetry delay' → calls 'scheduleRetry' with the requested delay.
  On success, the retry fiber picks the entry up once the delay
  elapses and publishes the message on the retry channel. On buffer
  overflow, the decision is downgraded to
  @AckDeadLetter MaxRetriesExceeded@ so the subscription keeps moving.

* 'AckHalt reason' → logs the reason, flips the shutdown @TVar@, and
  /does not/ call 'recordAckResult'. The halted position therefore
  stays as an ingested-but-unfinalized entry in the ledger, and
  'advanceCheckpointTo' stops at @haltedPos - 1@. On restart from the
  persisted checkpoint, the adapter resumes one past the last
  successfully-acked position and redelivers the halted message
  naturally.
-}
mkAckHandle ::
    (MessageDb :> es, Error SessionError :> es, IOE :> es) =>
    MessageDbAdapterConfig ->
    InflightState ->
    TVar Bool ->
    Mdb.Message ->
    AckHandle es
mkAckHandle cfg inflight shutdownSignal msg =
    AckHandle $ \decision -> case decision of
        AckOk ->
            liftIO . atomically $ recordAckResult inflight pos Inflight.AckComplete
        AckDeadLetter reason ->
            finalizeDeadLetter cfg inflight pos msg reason
        AckRetry (RetryDelay d) -> do
            now <- liftIO getCurrentTime
            scheduled <- liftIO . atomically $ scheduleRetry inflight pos d now msg
            unless scheduled $
                finalizeDeadLetter cfg inflight pos msg MaxRetriesExceeded
        AckHalt reason -> do
            liftIO . Text.IO.hPutStrLn stderr $
                "shibuya-message-db-adapter: halting at globalPosition="
                    <> Text.pack (show (Mdb.unGlobalPosition pos))
                    <> " reason="
                    <> Text.pack (show reason)
            liftIO . atomically $ writeTVar shutdownSignal True
  where
    pos = msg.globalPosition

{- | Execute the configured 'DlqStrategy' for a dead-lettered message
and advance the ledger.

'DlqSkipAndLog' writes a warning-level line to stderr and advances.
'DlqWriteToStream' builds the 'Mdb.NewMessage' (with a deterministic
id), writes it via 'tryWriteDlq', logs any non-duplicate failures, and
advances. In either strategy the ledger transition is always to
'AckComplete' — dead-letter semantics mean "we are done with this
position", regardless of whether the DLQ copy made it to durable
storage.
-}
finalizeDeadLetter ::
    (MessageDb :> es, Error SessionError :> es, IOE :> es) =>
    MessageDbAdapterConfig ->
    InflightState ->
    Mdb.GlobalPosition ->
    Mdb.Message ->
    DeadLetterReason ->
    Eff es ()
finalizeDeadLetter cfg inflight pos msg reason = do
    case cfg.dlqStrategy of
        DlqSkipAndLog ->
            liftIO . Text.IO.hPutStrLn stderr $
                "shibuya-message-db-adapter: dead-letter (skip+log) gp="
                    <> Text.pack (show (Mdb.unGlobalPosition pos))
                    <> " reason="
                    <> Text.pack (show reason)
        DlqWriteToStream target -> do
            now <- liftIO getCurrentTime
            let newMsg = mkDlqMessage target msg reason now
            writeResult <- tryWriteDlq newMsg
            case writeResult of
                Right _ -> pure ()
                Left DuplicateDlqEntry -> pure ()
                Left (OtherDlqError errTxt) ->
                    liftIO . Text.IO.hPutStrLn stderr $
                        "shibuya-message-db-adapter: DLQ write failed gp="
                            <> Text.pack (show (Mdb.unGlobalPosition pos))
                            <> " error="
                            <> errTxt
    liftIO . atomically $ recordAckResult inflight pos Inflight.AckComplete

{- | Pair a message with its envelope and an inflight-ledger-backed ack handle.

Records the ingestion in the ledger under an STM transaction so the
persister sees the new entry atomically with subsequent acks.

Lease is @Nothing@: message-db has no visibility-timeout model; the
entire queue is a durable log and checkpoints are the re-read
mechanism.
-}
mkIngested ::
    (MessageDb :> es, Error SessionError :> es, IOE :> es) =>
    MessageDbAdapterConfig ->
    InflightState ->
    TVar Bool ->
    Mdb.Message ->
    Eff es (Ingested es Mdb.Message)
mkIngested cfg inflight shutdownSignal msg = do
    liftIO . atomically $ recordIngested inflight msg.globalPosition
    pure
        Ingested
            { envelope = messageToEnvelope msg
            , ack = mkAckHandle cfg inflight shutdownSignal msg
            , lease = Nothing
            }

{- | The message-db polling source.

Runs in a loop:

1. Drain any retries the retry fiber has published to the retry channel.
2. Read the next-to-fetch 'GlobalPosition' from @positionRef@.
3. Issue @getCategoryMessages@ for the configured category and batch size.
4. If both the retry drain and the poll batch are empty, sleep for
   'pollInterval' and continue.
5. Otherwise, advance @positionRef@ past the last polled message (if
   any) and emit the concatenation of retries followed by fresh
   messages.

Each emitted message is recorded in the inflight ledger via
'mkIngested'; its 'AckHandle' writes the outcome back under STM so the
persister can flush the advancing contiguous prefix. @ackedRef@
remains a process-local high-watermark for diagnostic use only.

The outer stream is gated by @shutdownSignal@: once it flips to
@True@, 'takeWhileM' stops consuming and the stream terminates cleanly.
-}
messageDbSource ::
    forall es.
    (MessageDb :> es, Concurrent :> es, IOE :> es, Error SessionError :> es) =>
    TVar Bool ->
    IORef Mdb.GlobalPosition ->
    IORef Mdb.GlobalPosition ->
    InflightState ->
    MessageDbAdapterConfig ->
    Stream (Eff es) (Ingested es Mdb.Message)
messageDbSource shutdownSignal positionRef ackedRef inflight config =
    Stream.repeatM pollBatch
        & Stream.filter (not . Vector.null)
        & Stream.unfoldEach (Unfold.unfoldr Vector.uncons)
        & Stream.mapM recordAndWrap
        & takeUntilShutdown shutdownSignal
  where
    recordAndWrap :: Mdb.Message -> Eff es (Ingested es Mdb.Message)
    recordAndWrap msg = do
        liftIO $
            atomicModifyIORef' ackedRef $ \current ->
                (max current msg.globalPosition, ())
        mkIngested config inflight shutdownSignal msg

    pollBatch :: Eff es (Vector Mdb.Message)
    pollBatch = do
        retries <- liftIO . atomically $ drainRetryChannel inflight
        let retriesVec = Vector.fromList retries
        startAt <- liftIO (readIORef positionRef)
        batch <- getCategoryMessages (mkGetCategoryQuery config startAt)
        if Vector.null batch
            then
                if Vector.null retriesVec
                    then do
                        let PollInterval p = config.pollInterval
                        threadDelay (nominalToMicros p)
                        pure Vector.empty
                    else pure retriesVec
            else do
                let lastMsg = Vector.last batch
                    -- message-db's get_category_messages filter is
                    -- `global_position >= $2`, so the next request must
                    -- start one past the last emitted position to avoid
                    -- re-reading it. The server skips gaps itself.
                    Mdb.GlobalPosition lastPos = lastMsg.globalPosition
                    next = Mdb.GlobalPosition (lastPos + 1)
                liftIO $ atomicModifyIORef' positionRef $ \_ -> (next, ())
                pure (retriesVec <> batch)

-- | Stop producing once the shutdown 'TVar' flips to 'True'.
takeUntilShutdown ::
    (IOE :> es) =>
    TVar Bool ->
    Stream (Eff es) a ->
    Stream (Eff es) a
takeUntilShutdown signal =
    Stream.takeWhileM $ \_ -> do
        stopped <- liftIO (readTVarIO signal)
        pure (not stopped)

{- | Convert a 'NominalDiffTime' to microseconds as an 'Int' suitable
for 'threadDelay'.
-}
nominalToMicros :: NominalDiffTime -> Int
nominalToMicros t = ceiling (nominalDiffTimeToSeconds t * 1_000_000)

{- | Background thread that flushes the contiguous-prefix checkpoint to
the durable store at a fixed interval.

Wakes every @interval@; if 'advanceCheckpointTo' returned a new
position, calls 'storeCheckpoint' under the current effect stack. The
loop terminates when @shutdownSignal@ flips to @True@; the final flush
is the caller's responsibility (see 'Shibuya.Adapter.MessageDb.messageDbAdapter'
@shutdown@ field).
-}
runCheckpointPersister ::
    ( CheckpointStore :> es
    , Concurrent :> es
    , IOE :> es
    ) =>
    InflightState ->
    TVar Bool ->
    SubscriptionName ->
    CategoryStream.CategoryStream ->
    NominalDiffTime ->
    Eff es ()
runCheckpointPersister inflight shutdownSignal subName cat interval = loop
  where
    loop = do
        stopped <- liftIO (readTVarIO shutdownSignal)
        unless stopped $ do
            mNew <- liftIO (atomically (advanceCheckpointTo inflight))
            case mNew of
                Just pos -> storeCheckpoint subName cat pos
                Nothing -> pure ()
            threadDelay (nominalToMicros interval)
            loop

{- | Background thread that moves retries from the retry buffer to the
retry channel, respecting their @notBefore@ timestamps.

Blocks in STM until either the head of the retry buffer has an entry
or @shutdownSignal@ flips to 'True'. When it wakes on a head entry,
computes @entry.retryNotBefore - now@ and 'threadDelay's for that
interval (no-op if the entry is already ready). After the sleep,
re-checks @shutdownSignal@ and atomically pops the head onto the retry
channel if the adapter is still running.

Because @threadDelay@ is uninterruptible, a shutdown issued mid-sleep
incurs up to one retry-delay worth of lag before the fiber exits.
Production deployments can bound this by keeping @RetryDelay@ values
small; the default EP-3 behavior is measured in seconds rather than
minutes.

Any retries still in the buffer at shutdown are dropped on the floor.
This is intentional: their positions remain in the ledger as
@Just AckRetry@, so the contiguous-prefix checkpoint never crossed
them, and the next start will re-poll those positions from the
persisted checkpoint onward.
-}
retryFiber ::
    (Concurrent :> es, IOE :> es) =>
    TVar Bool ->
    InflightState ->
    Eff es ()
retryFiber shutdownSignal st = loop
  where
    loop = do
        mEntry <-
            liftIO . atomically $ awaitRetryHeadOrShutdown shutdownSignal st
        case mEntry of
            Nothing -> pure ()
            Just entry -> do
                now <- liftIO getCurrentTime
                let waitSecs = realToFrac (diffUTCTime entry.retryNotBefore now) :: Double
                when (waitSecs > 0) $
                    threadDelay (max 1 (ceiling (waitSecs * 1_000_000)))
                stop' <- liftIO (readTVarIO shutdownSignal)
                unless stop' $
                    liftIO . atomically $
                        popRetryHeadToChannel st
                loop
