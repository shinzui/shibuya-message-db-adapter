{- | Integration tests for EP-3: retry, dead-letter, and halt handling.

All three scenarios share an ephemeral-pg harness and a bootstrapped
message-db schema, seeded with a small run of messages for each test.

* __retry-then-ok__ — handler retries message 3 once with a 100 ms
  delay, succeeds on redelivery. All five messages end as AckOk; the
  persisted checkpoint equals position 5.

* __deadletter-write__ — handler dead-letters message 3 with
  'DlqWriteToStream'. The DLQ stream ends up with exactly one message
  whose metadata correlation equals the original's id.

* __halt__ — handler halts on message 3. Checkpoint pins at 2; a
  fresh adapter resumes at 3.

These tests are sequential: they share the ephemeral database, and
message-db's @global_position@ is serial across all writes, so
concurrent tests interleave positions and break assertions.
-}
module Shibuya.Adapter.MessageDb.RetryDlqHaltResumeTest (tests) where

import Control.Concurrent.STM (TVar, atomically, modifyTVar', newTVarIO, readTVar, readTVarIO)
import Control.Exception (throwIO)
import Control.Monad (forM_)
import Data.Aeson (Value (..))
import Data.Aeson qualified as Aeson
import Data.Aeson.Key qualified as Key
import Data.Aeson.KeyMap qualified as KeyMap
import Data.Functor.Contravariant ((>$<))
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Text.IO qualified as Text.IO
import Data.UUID (UUID)
import Data.UUID qualified as UUID
import Effectful (Eff, IOE, liftIO, runEff, (:>))
import Effectful.Concurrent (runConcurrent)
import Effectful.Error.Static (runErrorNoCallStack)
import Effectful.Hasql (SessionError, runHasqlWithPool)
import Effectful.Trace qualified as MsgDbTrace
import EphemeralPg qualified as Pg
import EphemeralPg.Config (Config (..), defaultPostgresSettings)
import Hasql.Decoders qualified as D
import Hasql.Encoders qualified as E
import Hasql.Pool (Pool, UsageError)
import Hasql.Pool qualified as Pool
import Hasql.Pool.Config qualified as PoolConfig
import Hasql.Session qualified as Session
import Hasql.Statement (preparable)
import MessageDb.CheckpointStore.Effectful (runPostgresCheckointStore)
import MessageDb.Effectful (runMessageDb)
import MessageDb.Message qualified as Mdb
import MessageDb.Message.Stream qualified as Mdb.Stream
import OpenTelemetry.Attributes qualified as OTel
import OpenTelemetry.Trace.Core qualified as OTel
import Shibuya.Adapter (Adapter (..))
import Shibuya.Adapter.MessageDb (
    CategoryStream (..),
    DlqStrategy (..),
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
import Shibuya.Core.Ack (
    AckDecision (..),
    DeadLetterReason (..),
    HaltReason (..),
    RetryDelay (..),
 )
import Shibuya.Core.AckHandle (AckHandle (..))
import Shibuya.Core.Ingested (Ingested (..))
import Shibuya.Core.Types (Envelope (..))
import Streamly.Data.Fold qualified as Fold
import Streamly.Data.Stream qualified as SStream
import Test.Tasty (DependencyType (..), TestTree, sequentialTestGroup, withResource)
import Test.Tasty.HUnit (assertBool, assertEqual, testCase)

messageDbRoot :: FilePath
messageDbRoot =
    "/Users/shinzui/Keikaku/hub/event-sourcing/message-db-project/message-db/database"

checkpointSchemaPath :: FilePath
checkpointSchemaPath =
    "/Users/shinzui/Keikaku/work/libraries/haskell/message-db-hs-master/message-db-checkpoint-store/migrations/scripts/create_checkpoints.sql"

-- * Ephemeral-pg bootstrap

pgConfig :: Pg.Config
pgConfig =
    Pg.defaultConfig
        { postgresSettings =
            defaultPostgresSettings
                <> [("search_path", "'message_store,\"$user\",public'")]
        }

withFixture :: (IO (Pg.Database, Pool) -> TestTree) -> TestTree
withFixture k =
    withResource startDb (\_ -> pure ()) $ \dbIO ->
        withResource (dbIO >>= mkPool) (\(_, p) -> Pool.release p) k
  where
    startDb = Pg.startCached pgConfig Pg.defaultCacheConfig >>= either throwIO pure

    mkPool db = do
        let cfg =
                PoolConfig.settings
                    [ PoolConfig.size 3
                    , PoolConfig.staticConnectionSettings (Pg.connectionSettings db)
                    ]
        pool <- Pool.acquire cfg
        bootstrapMessageDb pool
        pure (db, pool)

bootstrapMessageDb :: Pool -> IO ()
bootstrapMessageDb pool = do
    execSql pool "CREATE EXTENSION IF NOT EXISTS pgcrypto"
    execSqlFile pool (messageDbRoot <> "/schema/message-store.sql")
    execSqlFile pool (messageDbRoot <> "/types/message.sql")
    execSqlFile pool (messageDbRoot <> "/tables/messages.sql")
    forM_ functionFiles $ \f ->
        execSqlFile pool (messageDbRoot <> "/functions/" <> f)
    execSqlFile pool checkpointSchemaPath
  where
    functionFiles =
        [ "message-store-version.sql"
        , "hash-64.sql"
        , "acquire-lock.sql"
        , "category.sql"
        , "is-category.sql"
        , "id.sql"
        , "cardinal-id.sql"
        , "stream-version.sql"
        , "write-message.sql"
        , "get-stream-messages.sql"
        , "get-category-messages.sql"
        , "get-last-stream-message.sql"
        ]

execSqlFile :: Pool -> FilePath -> IO ()
execSqlFile pool path = Text.IO.readFile path >>= execSql pool

execSql :: Pool -> Text -> IO ()
execSql pool stmt = do
    r <- Pool.use pool (Session.script stmt)
    case r of
        Left e -> throwIO (userError (show e))
        Right () -> pure ()

-- * Data access helpers

{- | Seed one message per stream entity, with a deterministic UUID
derived from @n@ so assertions can recover the original id from the
DLQ correlation field.
-}
writeMessage :: Pool -> Text -> Int -> IO ()
writeMessage pool category n = do
    let streamName = category <> "-" <> Text.pack (show n)
        messageType = "OrderPlaced" :: Text
        dataJson = "{\"n\": " <> Text.pack (show n) <> "}"
        metadataJson = "{}" :: Text
        messageId = UUID.toText (uuidFromInt n)
        sql =
            "SELECT write_message(\
            \$1::varchar, $2::varchar, $3::varchar, $4::jsonb, $5::jsonb)"
        encoder =
            ((\(a, _, _, _, _) -> a) >$< E.param (E.nonNullable E.text))
                <> ((\(_, b, _, _, _) -> b) >$< E.param (E.nonNullable E.text))
                <> ((\(_, _, c, _, _) -> c) >$< E.param (E.nonNullable E.text))
                <> ((\(_, _, _, d, _) -> d) >$< E.param (E.nonNullable E.text))
                <> ((\(_, _, _, _, e) -> e) >$< E.param (E.nonNullable E.text))
        decoder = D.singleRow (D.column (D.nonNullable D.int8))
        stmt = preparable sql encoder decoder
    r <-
        Pool.use pool $
            Session.statement
                (messageId, streamName, messageType, dataJson, metadataJson)
                stmt
    case r of
        Left e -> throwIO (userError (show e))
        Right _ -> pure ()

uuidFromInt :: Int -> UUID
uuidFromInt n = UUID.fromWords 0 0 0 (fromIntegral n)

readCheckpointPosition :: Pool -> Text -> IO (Maybe Int)
readCheckpointPosition pool subName = do
    let sql =
            "SELECT last_processed_global_position FROM checkpoints WHERE name = $1"
        encoder = E.param (E.nonNullable E.text)
        decoder = D.rowMaybe (D.column (D.nullable D.int8))
        stmt = preparable sql encoder decoder
    r <- Pool.use pool $ Session.statement subName stmt
    case r of
        Left e -> throwIO (userError (show e))
        Right mRow -> pure (fmap fromIntegral =<< mRow)

-- | Fetch @(message_id, metadata_jsonb)@ rows from a named stream.
readDlqRows :: Pool -> Text -> IO [(Text, Aeson.Value)]
readDlqRows pool streamName = do
    let sql =
            "SELECT id::text, metadata::text\
            \ FROM message_store.messages\
            \ WHERE stream_name = $1\
            \ ORDER BY global_position"
        encoder = E.param (E.nonNullable E.text)
        decoder =
            D.rowList $
                (,)
                    <$> D.column (D.nonNullable D.text)
                    <*> D.column (D.nonNullable D.text)
        stmt = preparable sql encoder decoder
    r <- Pool.use pool $ Session.statement streamName stmt
    case r of
        Left e -> throwIO (userError (show e))
        Right rows ->
            pure
                [ (mid, fromMaybe Aeson.Null (Aeson.decodeStrictText md))
                | (mid, md) <- rows
                ]

resetTables :: Pool -> IO ()
resetTables pool = do
    execSql pool "TRUNCATE message_store.messages RESTART IDENTITY"
    execSql pool "TRUNCATE checkpoints RESTART IDENTITY"

-- * Adapter driver

testConfig ::
    Text ->
    Text ->
    (MessageDbAdapterConfig -> MessageDbAdapterConfig) ->
    MessageDbAdapterConfig
testConfig category subscription tweak =
    tweak $
        (defaultConfig (CategoryStream category) subscription)
            { batchSize = BatchSize 10
            , pollInterval = PollInterval 0.05
            , drainTimeout = DrainTimeout 2
            , checkpointInterval = CheckpointInterval 0.05
            }

{- | Drain the adapter's source stream until either @target@ messages
have been yielded or the adapter's source stream terminates (e.g. via
a halt).

Records every @(globalPosition, deliveryIndex)@ pair the handler
observes, where @deliveryIndex@ increments per-position across
redeliveries.
-}
runDrain ::
    Pool ->
    MessageDbAdapterConfig ->
    Int ->
    (Int -> Int -> AckDecision) ->
    IO [(Int, Int)]
runDrain pool cfg target decide = do
    observedVar <- newTVarIO ([] :: [(Int, Int)])
    -- Per-position delivery counter so decide can distinguish first
    -- vs. later deliveries.
    deliveriesVar <- newTVarIO (mempty :: KeyMap.KeyMap Int)
    tracer <- noopTracer
    outer <-
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
                SStream.fold
                    (Fold.drainMapM (handle observedVar deliveriesVar))
                    (SStream.take target adapter.source)
                adapter.shutdown
    case outer of
        Left ue -> throwIO (userError ("pool usage error: " <> show ue))
        Right (Left se) -> throwIO (userError ("session error: " <> show se))
        Right (Right ()) -> pure ()
    reverse <$> readTVarIO observedVar
  where
    handle ::
        (IOE :> es) =>
        TVar [(Int, Int)] ->
        TVar (KeyMap.KeyMap Int) ->
        Ingested es Mdb.Message ->
        Eff es ()
    handle observedVar deliveriesVar Ingested{envelope = Envelope{payload = msg}, ack = AckHandle finalize} = do
        let Mdb.GlobalPosition posInt = msg.globalPosition
            pos = fromIntegral posInt :: Int
            posKey = Key.fromText (Text.pack (show pos))
        idx <- liftIO . atomically $ do
            current <- KeyMap.lookup posKey <$> readTVar deliveriesVar
            let next = maybe 1 (+ 1) current
            modifyTVar' deliveriesVar (KeyMap.insert posKey next)
            modifyTVar' observedVar ((pos, next) :)
            pure next
        finalize (decide pos idx)

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

-- * Tests

tests :: TestTree
tests =
    withFixture $ \fixtureIO ->
        sequentialTestGroup
            "RetryDlqHaltResume"
            AllFinish
            [ testCase "retry-then-ok: AckRetry on message 3 redelivers and checkpoint reaches 5" $ do
                (_, pool) <- fixtureIO
                resetTables pool
                let category = "orderstestretry"
                    subscription = "orders-test-retry-then-ok"
                forM_ [1 .. 5] (writeMessage pool category)

                observed <-
                    runDrain
                        pool
                        (testConfig category subscription id)
                        6 -- 5 first-time deliveries + 1 retry of message 3
                        ( \pos idx ->
                            if pos == 3 && idx == 1
                                then AckRetry (RetryDelay 0.1)
                                else AckOk
                        )
                -- Five distinct positions, one of them (3) delivered twice.
                let positions = map fst observed
                assertEqual
                    "all five positions appear"
                    [1, 2, 3, 4, 5]
                    (dedup (take 5 positions))
                assertBool
                    "message 3 appears twice"
                    (length (filter (== 3) positions) == 2)
                mCheckpoint <- readCheckpointPosition pool subscription
                assertEqual
                    "checkpoint reaches position 5"
                    (Just 5)
                    mCheckpoint
            , testCase "deadletter-write: DlqWriteToStream writes one row with correlation=original id" $ do
                (_, pool) <- fixtureIO
                resetTables pool
                let category = "orderstestdlq"
                    subscription = "orders-test-dlq-write"
                dlqStreamName <- case Mdb.Stream.parseEither "orders.dlq" of
                    Right s -> pure s
                    Left err -> error ("dlq parse: " <> show err)
                forM_ [1 .. 5] (writeMessage pool category)

                _ <-
                    runDrain
                        pool
                        ( testConfig category subscription $ \c ->
                            c{dlqStrategy = DlqWriteToStream dlqStreamName}
                        )
                        5
                        ( \pos _ ->
                            if pos == 3
                                then AckDeadLetter (PoisonPill "unprocessable")
                                else AckOk
                        )
                dlqRows <- readDlqRows pool "orders.dlq"
                assertEqual
                    "DLQ stream has exactly one row"
                    1
                    (length dlqRows)
                case dlqRows of
                    [(_mid, Object md)] -> do
                        let correlation = KeyMap.lookup (Key.fromText "correlation") md
                            expected = Aeson.String (UUID.toText (uuidFromInt 3))
                        assertEqual
                            "DLQ correlation equals original message 3 id"
                            (Just expected)
                            correlation
                    _ -> error "unexpected DLQ contents"
                mCheckpoint <- readCheckpointPosition pool subscription
                assertEqual
                    "checkpoint reaches position 5 after DLQ"
                    (Just 5)
                    mCheckpoint
            , testCase "halt: AckHalt on message 3 pins checkpoint at 2; fresh adapter resumes at 3" $ do
                (_, pool) <- fixtureIO
                resetTables pool
                let category = "orderstesthalt"
                    subscription = "orders-test-halt"
                forM_ [1 .. 5] (writeMessage pool category)

                _ <-
                    runDrain
                        pool
                        (testConfig category subscription id)
                        5
                        ( \pos _ ->
                            if pos == 3
                                then AckHalt (HaltFatal "manual stop")
                                else AckOk
                        )
                mCheckpoint <- readCheckpointPosition pool subscription
                assertEqual
                    "halt pins the checkpoint at position 2"
                    (Just 2)
                    mCheckpoint

                observed2 <-
                    runDrain
                        pool
                        (testConfig category subscription id)
                        3
                        (\_ _ -> AckOk)
                let positions2 = map fst observed2
                assertEqual
                    "fresh adapter resumes at 3 and advances"
                    [3, 4, 5]
                    positions2
                mCheckpoint2 <- readCheckpointPosition pool subscription
                assertEqual
                    "checkpoint reaches 5 after resume"
                    (Just 5)
                    mCheckpoint2
            ]

-- * Utilities

dedup :: (Eq a) => [a] -> [a]
dedup = \case
    [] -> []
    (x : xs) -> x : dedup (filter (/= x) xs)
