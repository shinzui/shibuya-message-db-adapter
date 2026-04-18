{- | Internal implementation details for the message-db adapter.

This module is not part of the public API and may change without notice.
The public surface lives in "Shibuya.Adapter.MessageDb".
-}
module Shibuya.Adapter.MessageDb.Internal (
    messageDbSource,
    mkAckHandle,
    mkIngested,
    mkGetCategoryQuery,
    parseCategoryStream,
    runCheckpointPersister,
    nominalToMicros,
)
where

import Control.Concurrent.STM (TVar, atomically, readTVarIO)
import Control.Monad (unless)
import Control.Monad.IO.Class (liftIO)
import Data.Function ((&))
import Data.IORef (IORef, atomicModifyIORef', readIORef)
import Data.Text qualified as Text
import Data.Time.Clock (NominalDiffTime, nominalDiffTimeToSeconds)
import Data.Vector (Vector)
import Data.Vector qualified as Vector
import Effectful (Eff, IOE, (:>))
import Effectful.Concurrent (Concurrent, threadDelay)
import MessageDb.CheckpointStore.Effectful (CheckpointStore, SubscriptionName, storeCheckpoint)
import MessageDb.Db.Statements (GetCategoryMessagesQuery (..))
import MessageDb.Db.Statements.BatchSize qualified as MdbBatch
import MessageDb.Effectful (MessageDb, getCategoryMessages)
import MessageDb.Message qualified as Mdb
import MessageDb.Message.Stream.CategoryStream qualified as CategoryStream
import Shibuya.Adapter.MessageDb.Config (
    BatchSize (..),
    CategoryStream (..),
    MessageDbAdapterConfig (..),
    PollInterval (..),
 )
import Shibuya.Adapter.MessageDb.Convert (messageToEnvelope)
import Shibuya.Adapter.MessageDb.Internal.InflightState (
    AckOutcome,
    InflightState,
    advanceCheckpointTo,
    recordAckResult,
    recordIngested,
 )
import Shibuya.Adapter.MessageDb.Internal.InflightState qualified as Inflight
import Shibuya.Core.Ack (AckDecision (..))
import Shibuya.Core.AckHandle (AckHandle (..))
import Shibuya.Core.Ingested (Ingested (..))
import Streamly.Data.Stream (Stream)
import Streamly.Data.Stream qualified as Stream
import Streamly.Data.Unfold qualified as Unfold

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

The 'AckDecision' is mapped to an 'AckOutcome' and recorded in
@inflight@. The background persister reads the ledger on a timer and
flushes the contiguous prefix of completions to the checkpoint store.

Mapping:

* 'AckOk' → 'AckComplete'
* 'AckDeadLetter' → 'AckComplete' (EP-3 replaces the log-only behavior
  with a configurable DLQ strategy; for now, skip-and-advance so a
  poison message does not stall the whole subscription)
* 'AckRetry' → 'AckRetry' (pins the contiguous prefix)
* 'AckHalt' → 'AckRetry' (the halting message never completes; M4
  extends this branch to also flip the shutdown signal)
-}
mkAckHandle ::
    (IOE :> es) =>
    InflightState ->
    Mdb.GlobalPosition ->
    AckHandle es
mkAckHandle inflight pos =
    AckHandle $ \decision ->
        liftIO $ atomically $ recordAckResult inflight pos (outcomeOf decision)
  where
    outcomeOf :: AckDecision -> AckOutcome
    outcomeOf = \case
        AckOk -> Inflight.AckComplete
        AckDeadLetter _ -> Inflight.AckComplete
        AckRetry _ -> Inflight.AckRetry
        AckHalt _ -> Inflight.AckRetry

{- | Pair a message with its envelope and an inflight-ledger-backed ack handle.

Records the ingestion in the ledger under an STM transaction so the
persister sees the new entry atomically with subsequent acks.

Lease is @Nothing@: message-db has no visibility-timeout model; the
entire queue is a durable log and checkpoints are the re-read
mechanism.
-}
mkIngested ::
    (IOE :> es) =>
    InflightState ->
    Mdb.Message ->
    Eff es (Ingested es Mdb.Message)
mkIngested inflight msg = do
    liftIO $ atomically $ recordIngested inflight msg.globalPosition
    pure
        Ingested
            { envelope = messageToEnvelope msg
            , ack = mkAckHandle inflight msg.globalPosition
            , lease = Nothing
            }

{- | The message-db polling source.

Runs in a loop:

1. Read the next-to-fetch 'GlobalPosition' from @positionRef@.
2. Issue @getCategoryMessages@ for the configured category and batch size.
3. If the batch is empty, sleep for 'pollInterval' and continue.
4. Otherwise, advance @positionRef@ past the last message in the batch
   and flatten the batch into individual 'Ingested' values.

Each emitted message is recorded in the inflight ledger via
'mkIngested'; its 'AckHandle' writes the outcome back under STM so the
persister can flush the advancing contiguous prefix. @ackedRef@
remains a process-local high-watermark for diagnostic use only.

The outer stream is gated by @shutdownSignal@: once it flips to
@True@, 'takeWhileM' stops consuming and the stream terminates cleanly.
-}
messageDbSource ::
    forall es.
    (MessageDb :> es, Concurrent :> es, IOE :> es) =>
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
        mkIngested inflight msg

    pollBatch :: Eff es (Vector Mdb.Message)
    pollBatch = do
        startAt <- liftIO (readIORef positionRef)
        batch <- getCategoryMessages (mkGetCategoryQuery config startAt)
        if Vector.null batch
            then do
                let PollInterval p = config.pollInterval
                threadDelay (nominalToMicros p)
                pure batch
            else do
                let lastMsg = Vector.last batch
                    -- message-db's get_category_messages filter is
                    -- `global_position >= $2`, so the next request must
                    -- start one past the last emitted position to avoid
                    -- re-reading it. The server skips gaps itself.
                    Mdb.GlobalPosition lastPos = lastMsg.globalPosition
                    next = Mdb.GlobalPosition (lastPos + 1)
                liftIO $ atomicModifyIORef' positionRef $ \_ -> (next, ())
                pure batch

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
@shutdown@ field in M4).
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
