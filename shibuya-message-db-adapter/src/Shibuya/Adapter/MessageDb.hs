{- | message-db adapter for the Shibuya queue processing framework.

This adapter polls a category stream in message-db (via
[message-db-effectful](https://github.com/topagentnetwork/message-db-hs)'s
@MessageDb@ effect) and yields a 'Shibuya.Adapter.Adapter' whose payload
is the raw 'MessageDb.Message.Message'. Handlers decode the message's
@data@ payload into a domain type as they see fit.

== EP-2 scope

EP-2 adds durable checkpoints via the @message-db-checkpoint-store@
package and contiguous-prefix ack accounting via
'Shibuya.Adapter.MessageDb.Internal.InflightState'. On start the
adapter seeds its fetch position from the persisted checkpoint; a
background thread flushes the advancing prefix to the store; and
'shutdown' drains inflight work and does one final flush so the
checkpoint reflects all successfully-acked messages.

== Example usage

@
import Shibuya.Adapter.MessageDb (messageDbAdapter, defaultConfig, CategoryStream (..))
import MessageDb.Effectful (runMessageDb)
import MessageDb.CheckpointStore.Effectful (runPostgresCheckointStore)

main :: IO ()
main = runEff . runConcurrent . runMyHasqlStack . runMessageDb . runPostgresCheckointStore $ do
    adapter <- messageDbAdapter (defaultConfig (CategoryStream \"orders\") \"orders-demo\")
    -- hand `adapter` to Shibuya.App.runApp ...
@
-}
module Shibuya.Adapter.MessageDb (
    -- * Adapter
    messageDbAdapter,

    -- * Configuration
    MessageDbAdapterConfig (..),
    CategoryStream (..),
    BatchSize (..),
    PollInterval (..),
    DrainTimeout (..),
    CheckpointInterval (..),
    DlqStrategy (..),
    MaxRetryBufferSize (..),
    ConsumerGroupConfig (..),
    SubscriptionName,
    defaultConfig,
)
where

import Control.Concurrent.STM (TVar, atomically, newTVarIO, writeTVar)
import Control.Exception (throwIO)
import Control.Monad (unless)
import Control.Monad.IO.Class (liftIO)
import Data.IORef (newIORef)
import Data.Maybe (fromMaybe)
import Data.Text qualified as Text
import Data.Time.Clock (
    NominalDiffTime,
    diffUTCTime,
    getCurrentTime,
 )
import Effectful (Eff, IOE, (:>))
import Effectful.Concurrent (Concurrent, forkIO, threadDelay)
import Effectful.Error.Static (Error)
import Effectful.Hasql (SessionError)
import MessageDb.CheckpointStore.Effectful (CheckpointStore, getLastCheckpoint, storeCheckpoint)
import MessageDb.Effectful (MessageDb)
import MessageDb.Message qualified as Mdb
import Shibuya.Adapter (Adapter (..))
import Shibuya.Adapter.MessageDb.Config (
    BatchSize (..),
    CategoryStream (..),
    CheckpointInterval (..),
    ConsumerGroupConfig (..),
    DlqStrategy (..),
    DrainTimeout (..),
    MaxRetryBufferSize (..),
    MessageDbAdapterConfig (..),
    PollInterval (..),
    SubscriptionName,
    defaultConfig,
    validateConsumerGroup,
 )
import Shibuya.Adapter.MessageDb.Internal (
    messageDbSource,
    nominalToMicros,
    parseCategoryStream,
    partitionedSubscriptionName,
    retryFiber,
    runCheckpointPersister,
 )
import Shibuya.Adapter.MessageDb.Internal.InflightState (
    InflightState,
    advanceCheckpointTo,
    inflightSize,
    newInflightState,
 )

{- | Build a message-db adapter for a single category stream.

On start, loads the persisted checkpoint for @subscriptionName@ and
seeds the adapter's fetch position at @checkpoint + 1@ (message-db
positions are 1-indexed and @get_category_messages@ filters
@global_position >= $2@). A background thread flushes the advancing
contiguous prefix of completed acks to the store every
@checkpointInterval@.

On 'shutdown' the adapter flips the shared shutdown 'TVar' (which
stops both the source stream and the persister), waits up to
@drainTimeout@ for any in-flight messages to finalize, does a final
'advanceCheckpointTo' + 'storeCheckpoint', and gives the persister
one extra @checkpointInterval@ to notice the signal before returning.
-}
messageDbAdapter ::
    ( MessageDb :> es
    , CheckpointStore :> es
    , Concurrent :> es
    , IOE :> es
    , Error SessionError :> es
    ) =>
    MessageDbAdapterConfig ->
    Eff es (Adapter es Mdb.Message)
messageDbAdapter config = do
    case validateConsumerGroup config.consumerGroup of
        Right () -> pure ()
        Left msg -> liftIO (throwIO (userError (Text.unpack msg)))
    let effectiveName = case config.consumerGroup of
            Nothing -> config.subscriptionName
            Just grp -> partitionedSubscriptionName config.subscriptionName grp
    mStored <- getLastCheckpoint effectiveName
    let stored = fromMaybe (Mdb.GlobalPosition 0) mStored
        Mdb.GlobalPosition storedN = stored
        startAt = Mdb.GlobalPosition (storedN + 1)
        MaxRetryBufferSize maxRetry = config.maxRetryBufferSize
    shutdownSignal <- liftIO (newTVarIO False)
    positionRef <- liftIO (newIORef startAt)
    ackedRef <- liftIO (newIORef stored)
    inflight <- liftIO (newInflightState maxRetry stored)
    let parsedCat = parseCategoryStream config.category
        CategoryStream catText = config.category
        CheckpointInterval ci = config.checkpointInterval
        DrainTimeout dto = config.drainTimeout
    _ <-
        forkIO $
            runCheckpointPersister
                inflight
                shutdownSignal
                effectiveName
                parsedCat
                ci
    _ <-
        forkIO $
            retryFiber shutdownSignal inflight
    pure
        Adapter
            { adapterName = "message-db:" <> catText
            , source =
                messageDbSource shutdownSignal positionRef ackedRef inflight config
            , shutdown =
                doShutdown
                    shutdownSignal
                    inflight
                    effectiveName
                    parsedCat
                    dto
                    ci
            }

{- | Drain inflight work and flush the final checkpoint.

1. Flip @shutdownSignal@ so the source and persister stop at their
   next iteration.
2. Poll 'inflightSize' every 10 ms until the ledger has no pending
   outcomes, or until @drainTimeout@ elapses.
3. Call 'advanceCheckpointTo' once; if it returned a new position,
   'storeCheckpoint' it.
4. Wait one extra @checkpointInterval@ so the persister has time to
   observe the shutdown flag and exit.
-}
doShutdown ::
    ( CheckpointStore :> es
    , Concurrent :> es
    , IOE :> es
    ) =>
    TVar Bool ->
    InflightState ->
    SubscriptionName ->
    Mdb.CategoryStream ->
    NominalDiffTime ->
    NominalDiffTime ->
    Eff es ()
doShutdown shutdownSignal inflight subName cat drainTimeout interval = do
    liftIO $ atomically $ writeTVar shutdownSignal True
    drainStart <- liftIO getCurrentTime
    drainLoop drainStart
    mFinal <- liftIO $ atomically $ advanceCheckpointTo inflight
    case mFinal of
        Just pos -> storeCheckpoint subName cat pos
        Nothing -> pure ()
    threadDelay (nominalToMicros interval)
  where
    drainLoop start = do
        n <- liftIO $ atomically $ inflightSize inflight
        unless (n == 0) $ do
            now <- liftIO getCurrentTime
            unless (diffUTCTime now start >= drainTimeout) $ do
                threadDelay 10_000
                drainLoop start
