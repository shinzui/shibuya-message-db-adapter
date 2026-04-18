{- | message-db adapter for the Shibuya queue processing framework.

This adapter polls a category stream in message-db (via
[message-db-effectful](https://github.com/topagentnetwork/message-db-hs)'s
@MessageDb@ effect) and yields a 'Shibuya.Adapter.Adapter' whose payload
is the raw 'MessageDb.Message.Message'. Handlers decode the message's
@data@ payload into a domain type as they see fit.

== EP-1 scope

EP-1 ships a working polling loop with a stub ack handler: @AckOk@
advances a process-local high-watermark @IORef@; other decisions are
no-ops. Durable checkpointing, retry delays, dead-letter handling,
halt semantics, and consumer-group partitioning are deferred to later
ExecPlans (EP-2 through EP-4).

== Example usage

@
import Shibuya.Adapter.MessageDb (messageDbAdapter, defaultConfig, CategoryStream (..))
import MessageDb.Effectful (runMessageDb)

main :: IO ()
main = runEff . runConcurrent . runMyHasqlStack . runMessageDb $ do
    adapter <- messageDbAdapter (defaultConfig (CategoryStream "orders"))
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
    defaultConfig,
)
where

import Control.Concurrent.STM (atomically, newTVarIO, writeTVar)
import Control.Monad.IO.Class (liftIO)
import Data.IORef (newIORef)
import Effectful (Eff, IOE, (:>))
import Effectful.Concurrent (Concurrent)
import MessageDb.Effectful (MessageDb)
import MessageDb.Message qualified as Mdb
import Shibuya.Adapter (Adapter (..))
import Shibuya.Adapter.MessageDb.Config (
    BatchSize (..),
    CategoryStream (..),
    DrainTimeout (..),
    MessageDbAdapterConfig (..),
    PollInterval (..),
    defaultConfig,
 )
import Shibuya.Adapter.MessageDb.Internal (messageDbSource)

{- | Build a message-db adapter for a single category stream.

Creates a shutdown 'TVar' and two 'IORef's (next-to-fetch position and
highest-acked position), wires them into the polling source, and
returns an 'Adapter' whose 'Shibuya.Adapter.Adapter.shutdown' action
flips the 'TVar'. The source stream terminates on the next iteration.

@positionRef@ starts at @GlobalPosition 1@ (message-db positions are
1-indexed and @get_category_messages@ filters @global_position >= $2@).
@ackedRef@ starts at @GlobalPosition 0@ so the first @AckOk@ strictly
advances the high-watermark. EP-2 will replace both constants with
values loaded from a durable checkpoint store.
-}
messageDbAdapter ::
    (MessageDb :> es, Concurrent :> es, IOE :> es) =>
    MessageDbAdapterConfig ->
    Eff es (Adapter es Mdb.Message)
messageDbAdapter config = do
    shutdownSignal <- liftIO (newTVarIO False)
    -- message-db positions are 1-indexed; the server filter is
    -- `global_position >= $2`, so starting at 1 returns the whole
    -- category (positions 1..). ackedRef stays at 0 so the first
    -- AckOk unambiguously advances the high-watermark.
    positionRef <- liftIO (newIORef (Mdb.GlobalPosition 1))
    ackedRef <- liftIO (newIORef (Mdb.GlobalPosition 0))
    let CategoryStream catText = config.category
    pure
        Adapter
            { adapterName = "message-db:" <> catText
            , source =
                messageDbSource shutdownSignal positionRef ackedRef config
            , shutdown =
                liftIO (atomically (writeTVar shutdownSignal True))
            }
