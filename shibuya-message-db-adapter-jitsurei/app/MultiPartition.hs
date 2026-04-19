{- | Consumer-group partitioning example.

Launches three adapters against the same category
(@jitsurei-partition@) with @ConsumerGroupConfig { groupSize = 3 }@
and member indices 0, 1, 2. Because @categoryPartition@ hashes the
*category name*, all 30 messages land on exactly one member (the
owner); the other two members poll, filter everything, and advance
their partition-scoped checkpoint. The example prints which member
handled which message and, once the owner has drained all 30
messages, prints a summary asserting exactly-once routing.

This mirrors the EP-4 integration test's shape rather than the
plan's original six-category / 18-adapter sketch. @get_category_messages@
in message-db takes a single exact category argument, and EP-4's
partition hash is computed from the category name; three adapters
on one category exercise the same routing and filter-advance paths
without fanning out to eighteen processes. See
docs/plans/5-jitsurei-and-integration-tests.md Surprises.

Seed: @just seed-jitsurei-partition@ (30 messages spread across
@jitsurei-partition-cat1@ through @jitsurei-partition-cat6@).
Run:  @cabal run multi-partition@.
-}
module Main (main) where

import Control.Concurrent.Async qualified as Async
import Control.Concurrent.STM (
    TVar,
    atomically,
    check,
    modifyTVar',
    newTVarIO,
    readTVar,
    readTVarIO,
 )
import Control.Exception (throwIO)
import Control.Monad (forM_)
import Control.Monad.IO.Class (liftIO)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Text.IO qualified as Text.IO
import Effectful (Eff, IOE, runEff, (:>))
import Effectful.Concurrent (runConcurrent)
import Effectful.Error.Static (runErrorNoCallStack)
import Effectful.Hasql (SessionError, runHasqlWithPool)
import Effectful.Trace qualified as MsgDbTrace
import Hasql.Connection.Settings qualified as ConnSettings
import Hasql.Pool (Pool, UsageError)
import Hasql.Pool qualified as Pool
import Hasql.Pool.Config qualified as PoolConfig
import MessageDb.CheckpointStore.Effectful (runPostgresCheckointStore)
import MessageDb.Effectful (runMessageDb)
import MessageDb.Message qualified as Mdb
import MessageDb.Message.Stream qualified as Stream
import OpenTelemetry.Attributes qualified as OTel
import OpenTelemetry.Trace.Core qualified as OTel
import Shibuya.Adapter (Adapter (..))
import Shibuya.Adapter.MessageDb (
    CategoryStream (..),
    ConsumerGroupConfig (..),
    MessageDbAdapterConfig (..),
    defaultConfig,
    messageDbAdapter,
 )
import Shibuya.Core.Ack (AckDecision (..))
import Shibuya.Core.AckHandle (AckHandle (..))
import Shibuya.Core.Ingested (Ingested (..))
import Shibuya.Core.Types (Envelope (..), MessageId (..))
import Streamly.Data.Fold qualified as Fold
import Streamly.Data.Stream qualified as SStream
import System.Environment (getEnv)
import System.IO (BufferMode (LineBuffering), hSetBuffering, stdout)

category :: Text
category = "jitsurei-partition"

subscriptionBase :: Text
subscriptionBase = "jitsurei-partition"

groupSize :: Int
groupSize = 3

total :: Int
total = 30

main :: IO ()
main = do
    hSetBuffering stdout LineBuffering
    Text.IO.putStrLn "[multi-partition] Starting three adapters, groupSize=3..."
    pool <- acquirePool

    perMember <- newTVarIO (Map.empty :: Map Int [Text])
    totalCount <- newTVarIO (0 :: Int)

    asyncs <-
        mapM
            (spawnMember pool perMember totalCount)
            [0 .. groupSize - 1]

    -- Wait until the owner member has ack'd all messages.
    atomically $ do
        n <- readTVar totalCount
        check (n >= total)

    mapM_ Async.cancel asyncs

    seen <- readTVarIO perMember
    let flatIds = concat (Map.elems seen)
        distinct = Set.size (Set.fromList flatIds)

    forM_ (Map.toList seen) $ \(m, ids) ->
        Text.IO.putStrLn $
            "[multi-partition] member "
                <> Text.pack (show m)
                <> ": "
                <> Text.pack (show (length ids))
                <> " messages"

    Text.IO.putStrLn $
        "[multi-partition] total: "
            <> Text.pack (show (length flatIds))
            <> " messages, "
            <> Text.pack (show (length flatIds - distinct))
            <> " duplicates"

    Pool.release pool
    Text.IO.putStrLn "[multi-partition] Done."

{- | Spawn one adapter instance as an 'Async.Async'. Each instance
streams its filtered view of the shared category, appending every
ack'd message's id to its member-indexed slot in the shared map.
Members whose partition is empty still drive the poll loop so the
background persister can flush their checkpoint.
-}
spawnMember ::
    Pool ->
    TVar (Map Int [Text]) ->
    TVar Int ->
    Int ->
    IO (Async.Async ())
spawnMember pool perMember totalCount memberIx = do
    Async.async (runMember pool perMember totalCount memberIx)

runMember ::
    Pool ->
    TVar (Map Int [Text]) ->
    TVar Int ->
    Int ->
    IO ()
runMember pool perMember totalCount memberIx = do
    tracer <- noopTracer
    let cfg =
            (defaultConfig (CategoryStream category) subscriptionBase)
                { consumerGroup =
                    Just
                        ConsumerGroupConfig
                            { groupSize = groupSize
                            , member = memberIx
                            }
                }
    r <-
        runEff
            . runConcurrent
            . runErrorNoCallStack @UsageError
            . runErrorNoCallStack @SessionError
            . runHasqlWithPool pool
            . MsgDbTrace.runTrace tracer
            . runMessageDb
            . runPostgresCheckointStore
            $ do
                adapter <- messageDbAdapter cfg
                SStream.fold (Fold.drainMapM handle) adapter.source
    case r of
        Left ue -> throwIO (userError ("pool: " <> show ue))
        Right (Left se) -> throwIO (userError ("session: " <> show se))
        Right (Right ()) -> pure ()
  where
    handle ::
        (IOE :> es) =>
        Ingested es Mdb.Message ->
        Eff es ()
    handle
        Ingested
            { envelope = Envelope{messageId = MessageId mid, payload = msg}
            , ack = AckHandle finalize
            } = do
            let streamTxt = Stream.toText msg.stream
            liftIO $
                Text.IO.putStrLn $
                    "[multi-partition] member "
                        <> Text.pack (show memberIx)
                        <> " got "
                        <> streamTxt
            liftIO $ atomically $ do
                modifyTVar' perMember (Map.insertWith (<>) memberIx [mid])
                modifyTVar' totalCount (+ 1)
            finalize AckOk

acquirePool :: IO Pool
acquirePool = do
    connStr <- getEnv "PG_CONNECTION_STRING"
    let settings = ConnSettings.connectionString (Text.pack connStr)
        cfg =
            PoolConfig.settings
                [ PoolConfig.size 6
                , PoolConfig.staticConnectionSettings settings
                ]
    Pool.acquire cfg

noopTracer :: IO OTel.Tracer
noopTracer = do
    tp <- OTel.getGlobalTracerProvider
    let lib =
            OTel.InstrumentationLibrary
                { libraryName = "shibuya-message-db-adapter-jitsurei"
                , libraryVersion = "0.1"
                , librarySchemaUrl = ""
                , libraryAttributes = OTel.emptyAttributes
                }
    pure (OTel.makeTracer tp lib OTel.tracerOptions)
