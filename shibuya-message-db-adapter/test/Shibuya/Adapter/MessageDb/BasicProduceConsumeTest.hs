{- | Integration test: basic produce-and-consume.

Seeds 10 messages into one category, runs the adapter with an AckOk
handler, and asserts the handler observes every message in
global-position order 1..10 before the stream terminates.

Exercises the happy path — conversion, source stream, stubbed
checkpointing — and is the cheapest sanity check that the full
adapter + message-db + checkpoint-store stack is wired up.
-}
module Shibuya.Adapter.MessageDb.BasicProduceConsumeTest (tests) where

import Control.Concurrent.STM (TVar, atomically, modifyTVar', newTVarIO, readTVarIO)
import Control.Exception (throwIO)
import Data.Text (Text)
import Effectful (Eff, IOE, liftIO, runEff, (:>))
import Effectful.Concurrent (runConcurrent)
import Effectful.Error.Static (runErrorNoCallStack)
import Effectful.Hasql (SessionError, runHasqlWithPool)
import Effectful.Trace qualified as MsgDbTrace
import Hasql.Pool (UsageError)
import MessageDb.CheckpointStore.Effectful (runPostgresCheckointStore)
import MessageDb.Effectful (runMessageDb)
import MessageDb.Message qualified as Mdb
import OpenTelemetry.Attributes qualified as OTel
import OpenTelemetry.Trace.Core qualified as OTel
import Shibuya.Adapter (Adapter (..))
import Shibuya.Adapter.MessageDb (
    CategoryStream (..),
    MessageDbAdapterConfig (..),
    defaultConfig,
    messageDbAdapter,
 )
import Shibuya.Adapter.MessageDb.Config (
    BatchSize (..),
    CheckpointInterval (..),
    DrainTimeout (..),
    PollInterval (..),
 )
import Shibuya.Core.Ack (AckDecision (..))
import Shibuya.Core.AckHandle (AckHandle (..))
import Shibuya.Core.Ingested (Ingested (..))
import Shibuya.Core.Types (Envelope (..))
import Streamly.Data.Fold qualified as Fold
import Streamly.Data.Stream qualified as SStream
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertEqual, testCase)
import TestEnv (TestEnv (..), readCheckpointPosition, withTestEnv, writeCategoryMessages)

tests :: TestTree
tests =
    testGroup
        "BasicProduceConsume"
        [ testCase "ten messages produce-and-consume in order" $ withTestEnv $ \env -> do
            -- Category must be a single segment with no dashes: message-db's
            -- `category()` function returns everything before the first `-`,
            -- so `orders-basic-1` has category `orders`, not `orders-basic`.
            let category = "ordersbasic"
                subscription = "orders-basic"
                n = 10
            writeCategoryMessages env category n

            observed <- drainAdapter env category subscription n

            assertEqual
                "every seeded message observed once, in order"
                [1 .. n]
                observed

            chk <- readCheckpointPosition env.pool subscription
            assertEqual
                "checkpoint advanced to the last observed position"
                (Just n)
                chk
        ]

drainAdapter :: TestEnv -> Text -> Text -> Int -> IO [Int]
drainAdapter env category subscription target = do
    observedVar <- newTVarIO ([] :: [Int])
    tracer <- noopTracer
    outer <-
        runEff
            . runConcurrent
            . runErrorNoCallStack @UsageError
            . runErrorNoCallStack @SessionError
            . runHasqlWithPool env.pool
            . MsgDbTrace.runTrace tracer
            . runMessageDb
            . runPostgresCheckointStore
            $ do
                adapter <- messageDbAdapter (testConfig category subscription)
                SStream.fold
                    (Fold.drainMapM (recordAndAck observedVar))
                    (SStream.take target adapter.source)
                adapter.shutdown
    case outer of
        Left ue -> throwIO (userError ("pool: " <> show ue))
        Right (Left se) -> throwIO (userError ("session: " <> show se))
        Right (Right ()) -> pure ()
    reverse <$> readTVarIO observedVar

recordAndAck ::
    (IOE :> es) =>
    TVar [Int] ->
    Ingested es Mdb.Message ->
    Eff es ()
recordAndAck obs Ingested{envelope = Envelope{payload = msg}, ack = AckHandle finalize} = do
    let Mdb.GlobalPosition gp = msg.globalPosition
    liftIO $ atomically $ modifyTVar' obs (fromIntegral gp :)
    finalize AckOk

testConfig :: Text -> Text -> MessageDbAdapterConfig
testConfig category subscription =
    (defaultConfig (CategoryStream category) subscription)
        { batchSize = BatchSize 10
        , pollInterval = PollInterval 0.05
        , drainTimeout = DrainTimeout 2
        , checkpointInterval = CheckpointInterval 0.05
        }

noopTracer :: IO OTel.Tracer
noopTracer = do
    tp <- OTel.getGlobalTracerProvider
    let lib =
            OTel.InstrumentationLibrary
                { libraryName = "shibuya-message-db-adapter-test"
                , libraryVersion = "0.1"
                , librarySchemaUrl = ""
                , libraryAttributes = OTel.emptyAttributes
                }
    pure (OTel.makeTracer tp lib OTel.tracerOptions)
