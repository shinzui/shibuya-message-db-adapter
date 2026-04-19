{- | Integration test: dead-letter skip-and-log.

Seeds three messages, runs the adapter with the default
'DlqSkipAndLog' strategy and a handler that dead-letters message 2
with @AckDeadLetter (PoisonPill ...)@. Asserts:

* All three messages are delivered to the handler exactly once.
* The final checkpoint advances to message 3's global position.
* No rows with stream names containing @"dlq"@ or @"dead"@ are
  written back to message-db (the skip-and-log strategy does not
  produce a durable record).
-}
module Shibuya.Adapter.MessageDb.DeadLetterSkipAndLogTest (tests) where

import Control.Concurrent.STM (TVar, atomically, modifyTVar', newTVarIO, readTVarIO)
import Control.Exception (throwIO)
import Data.Text (Text)
import Effectful (Eff, IOE, liftIO, runEff, (:>))
import Effectful.Concurrent (runConcurrent)
import Effectful.Error.Static (runErrorNoCallStack)
import Effectful.Hasql (SessionError, runHasqlWithPool)
import Effectful.Trace qualified as MsgDbTrace
import Hasql.Decoders qualified as D
import Hasql.Encoders qualified as E
import Hasql.Pool (UsageError)
import Hasql.Pool qualified as Pool
import Hasql.Session qualified as Session
import Hasql.Statement (preparable)
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
import Shibuya.Core.Ack (AckDecision (..), DeadLetterReason (..))
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
        "DeadLetterSkipAndLog"
        [ testCase "DlqSkipAndLog: all three delivered, checkpoint at 3, no DLQ rows" $
            withTestEnv $ \env -> do
                -- message-db's `category()` splits on the first dash, so the
                -- category string cannot contain a dash itself (or else the
                -- category would be parsed as only the first segment).
                let category = "ordersskip"
                    subscription = "orders-skip"
                writeCategoryMessages env category 3

                observed <- drainAdapter env category subscription 3
                assertEqual
                    "each message delivered exactly once in order"
                    [1, 2, 3]
                    observed

                chk <- readCheckpointPosition env.pool subscription
                assertEqual
                    "checkpoint advances past the dead-lettered message"
                    (Just 3)
                    chk

                dlqishCount <- countDlqLikeStreams env
                assertEqual
                    "no dlq-like streams were written"
                    0
                    dlqishCount
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
                    (Fold.drainMapM (handle observedVar))
                    (SStream.take target adapter.source)
                adapter.shutdown
    case outer of
        Left ue -> throwIO (userError ("pool: " <> show ue))
        Right (Left se) -> throwIO (userError ("session: " <> show se))
        Right (Right ()) -> pure ()
    reverse <$> readTVarIO observedVar

handle ::
    (IOE :> es) =>
    TVar [Int] ->
    Ingested es Mdb.Message ->
    Eff es ()
handle obs Ingested{envelope = Envelope{payload = msg}, ack = AckHandle finalize} = do
    let Mdb.GlobalPosition gp = msg.globalPosition
        pos = fromIntegral gp :: Int
    liftIO $ atomically $ modifyTVar' obs (pos :)
    if pos == 2
        then finalize (AckDeadLetter (PoisonPill "skip and log"))
        else finalize AckOk

{- | Count message-db streams whose name looks like a dead-letter
queue. The 'DlqSkipAndLog' strategy is expected to produce zero such
rows; this check guards against accidental DLQ writes.
-}
countDlqLikeStreams :: TestEnv -> IO Int
countDlqLikeStreams env = do
    let sql =
            "SELECT count(*)::int8\
            \ FROM message_store.messages\
            \ WHERE stream_name ILIKE '%dlq%' OR stream_name ILIKE '%dead%'"
        encoder = E.noParams
        decoder = D.singleRow (D.column (D.nonNullable D.int8))
        stmt = preparable sql encoder decoder
    r <- Pool.use env.pool $ Session.statement () stmt
    case r of
        Left e -> throwIO (userError (show e))
        Right n -> pure (fromIntegral n)

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
