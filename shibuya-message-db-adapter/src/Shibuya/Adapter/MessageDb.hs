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
graceful shutdown waits for in-flight work to drain before one last
flush.

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
    SubscriptionName,
    defaultConfig,
)
where

import Control.Concurrent.STM (atomically, newTVarIO, writeTVar)
import Control.Monad.IO.Class (liftIO)
import Data.IORef (newIORef)
import Data.Maybe (fromMaybe)
import Effectful (Eff, IOE, (:>))
import Effectful.Concurrent (Concurrent)
import MessageDb.CheckpointStore.Effectful (CheckpointStore, getLastCheckpoint)
import MessageDb.Effectful (MessageDb)
import MessageDb.Message qualified as Mdb
import Shibuya.Adapter (Adapter (..))
import Shibuya.Adapter.MessageDb.Config (
    BatchSize (..),
    CategoryStream (..),
    CheckpointInterval (..),
    DrainTimeout (..),
    MessageDbAdapterConfig (..),
    PollInterval (..),
    SubscriptionName,
    defaultConfig,
 )
import Shibuya.Adapter.MessageDb.Internal (messageDbSource)

{- | Build a message-db adapter for a single category stream.

Loads the persisted checkpoint for 'subscriptionName' and seeds the
adapter's fetch position at @checkpoint + 1@ (or @1@ when no checkpoint
exists — message-db positions are 1-indexed). EP-3 will wire the
background persister and graceful-shutdown flush on top of this
scaffold; for now the adapter still behaves like EP-1 between start
and shutdown, except that restart-after-crash resumes from the last
durable point instead of replaying the whole category.
-}
messageDbAdapter ::
    (MessageDb :> es, CheckpointStore :> es, Concurrent :> es, IOE :> es) =>
    MessageDbAdapterConfig ->
    Eff es (Adapter es Mdb.Message)
messageDbAdapter config = do
    mStored <- getLastCheckpoint config.subscriptionName
    let stored = fromMaybe (Mdb.GlobalPosition 0) mStored
        Mdb.GlobalPosition storedN = stored
        startAt = Mdb.GlobalPosition (storedN + 1)
    shutdownSignal <- liftIO (newTVarIO False)
    positionRef <- liftIO (newIORef startAt)
    ackedRef <- liftIO (newIORef stored)
    let CategoryStream catText = config.category
    pure
        Adapter
            { adapterName = "message-db:" <> catText
            , source =
                messageDbSource shutdownSignal positionRef ackedRef config
            , shutdown =
                liftIO (atomically (writeTVar shutdownSignal True))
            }
