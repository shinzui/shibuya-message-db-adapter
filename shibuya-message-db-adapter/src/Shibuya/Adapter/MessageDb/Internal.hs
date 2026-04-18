{- | Internal implementation details for the message-db adapter.

This module is not part of the public API and may change without notice.
The public surface lives in "Shibuya.Adapter.MessageDb".
-}
module Shibuya.Adapter.MessageDb.Internal (
    messageDbSource,
    mkStubAckHandle,
    mkIngested,
    mkGetCategoryQuery,
    parseCategoryStream,
)
where

import Control.Concurrent.STM (TVar, readTVarIO)
import Control.Monad.IO.Class (liftIO)
import Data.Function ((&))
import Data.IORef (IORef, atomicModifyIORef', readIORef)
import Data.Text qualified as Text
import Data.Time.Clock (NominalDiffTime, nominalDiffTimeToSeconds)
import Data.Vector (Vector)
import Data.Vector qualified as Vector
import Effectful (Eff, IOE, (:>))
import Effectful.Concurrent (Concurrent, threadDelay)
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

{- | Create a stub 'AckHandle' for an ingested message.

On 'AckOk', advance the @ackedRef@ high-watermark to this message's
'GlobalPosition' (monotonically — never decrease). All other decisions
are currently no-ops: retry, dead-letter, and halt are EP-3's concerns.
Durable persistence of the high-watermark is EP-2's.
-}
mkStubAckHandle ::
    (IOE :> es) =>
    IORef Mdb.GlobalPosition ->
    Mdb.Message ->
    AckHandle es
mkStubAckHandle ackedRef msg =
    AckHandle $ \case
        AckOk ->
            liftIO $
                atomicModifyIORef' ackedRef $ \current ->
                    (max current msg.globalPosition, ())
        AckRetry _ -> pure ()
        AckDeadLetter _ -> pure ()
        AckHalt _ -> pure ()

{- | Pair a message with its envelope and stub ack handle.

Lease is @Nothing@: message-db has no visibility-timeout model; the
entire queue is a durable log and checkpoints are the re-read
mechanism.
-}
mkIngested ::
    (IOE :> es) =>
    IORef Mdb.GlobalPosition ->
    Mdb.Message ->
    Ingested es Mdb.Message
mkIngested ackedRef msg =
    Ingested
        { envelope = messageToEnvelope msg
        , ack = mkStubAckHandle ackedRef msg
        , lease = Nothing
        }

{- | The message-db polling source.

Runs in a loop:

1. Read the next-to-fetch 'GlobalPosition' from @positionRef@.
2. Issue @getCategoryMessages@ for the configured category and batch size.
3. If the batch is empty, sleep for 'pollInterval' and continue.
4. Otherwise, advance @positionRef@ past the last message in the batch
   and flatten the batch into individual 'Ingested' values.

The outer stream is gated by @shutdownSignal@: once it flips to
@True@, 'takeWhileM' stops consuming and the stream terminates cleanly.

Advancing @positionRef@ in the poll loop — independently of ack
outcomes — is the *stub* EP-1 behavior. EP-2 will make the next fetch
position derive from the persisted high-watermark in @ackedRef@ so
that crashes do not lose messages. EP-1 already updates @ackedRef@ on
@AckOk@ via 'mkStubAckHandle', so the information required for durable
checkpointing is being recorded even though it is not yet consulted on
restart.
-}
messageDbSource ::
    forall es.
    (MessageDb :> es, Concurrent :> es, IOE :> es) =>
    TVar Bool ->
    IORef Mdb.GlobalPosition ->
    IORef Mdb.GlobalPosition ->
    MessageDbAdapterConfig ->
    Stream (Eff es) (Ingested es Mdb.Message)
messageDbSource shutdownSignal positionRef ackedRef config =
    Stream.repeatM pollBatch
        & Stream.filter (not . Vector.null)
        & Stream.unfoldEach (Unfold.unfoldr Vector.uncons)
        & Stream.mapM (pure . mkIngested ackedRef)
        & takeUntilShutdown shutdownSignal
  where
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
                    -- `global_position > $2`, so the next request starts at the
                    -- last emitted position. The server skips gaps itself.
                    next = lastMsg.globalPosition
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

nominalToMicros :: NominalDiffTime -> Int
nominalToMicros t = floor (nominalDiffTimeToSeconds t * 1_000_000)
