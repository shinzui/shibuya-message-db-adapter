{- | Integration tests for durable-checkpoint resume and AckRetry
contiguous-prefix guarantees.

Spins up a throwaway Postgres via @ephemeral-pg@, installs the
message-db schema plus the @checkpoints@ table, writes ten messages to
a category, and exercises the adapter through two complete
start-drain-shutdown cycles to prove that progress survives restart.

Not a unit test: the server process runs for the duration of the suite.
@ephemeral-pg@ caches its initdb template so the first suite run is
~2 s and later runs are ~500 ms.
-}
module Shibuya.Adapter.MessageDb.CheckpointResumeTest (tests) where

import Control.Concurrent.STM (TVar, atomically, modifyTVar', newTVarIO, readTVarIO)
import Control.Exception (throwIO)
import Control.Monad (forM_)
import Data.Functor.Contravariant ((>$<))
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
import Shibuya.Core.Ack (AckDecision (..), RetryDelay (..))
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
                <> [ ("search_path", "'message_store,\"$user\",public'")
                   ]
        }

{- | @tasty@ resource: @(database, pool)@. The pool is released on
teardown; @ephemeral-pg@ handles the server lifecycle itself
through its cache.
-}
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

{- | Install message-db's schema, functions, and the checkpoint-store
table. Skips role/privilege installation — not needed because the
ephemeral database runs as a single user.
-}
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

{- | Write one message to @\<category\>-\<n\>@. Message ID is a
deterministic UUID derived from @n@ so the test is reproducible.
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
uuidFromInt n =
    UUID.fromWords 0 0 0 (fromIntegral n)

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

resetTables :: Pool -> IO ()
resetTables pool = do
    execSql pool "TRUNCATE message_store.messages RESTART IDENTITY"
    execSql pool "TRUNCATE checkpoints RESTART IDENTITY"

-- * Running the adapter

testConfig :: Text -> Text -> MessageDbAdapterConfig
testConfig category subscription =
    (defaultConfig (CategoryStream category) subscription)
        { batchSize = BatchSize 10
        , pollInterval = PollInterval 0.05
        , drainTimeout = DrainTimeout 2
        , checkpointInterval = CheckpointInterval 0.05
        }

{- | Run the adapter against the ephemeral DB, driving it with
@decide@ until @target@ messages have been yielded, then call
@adapter.shutdown@.

Returns the ordered list of @globalPosition@s the handler observed.
-}
runDrain ::
    Pool ->
    MessageDbAdapterConfig ->
    Int ->
    (Int -> AckDecision) ->
    IO [Int]
runDrain pool cfg target decide = do
    positionsVar <- newTVarIO ([] :: [Int])
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
                    (Fold.drainMapM (handle positionsVar))
                    (SStream.take target adapter.source)
                adapter.shutdown
    case outer of
        Left ue -> throwIO (userError ("pool usage error: " <> show ue))
        Right (Left se) -> throwIO (userError ("session error: " <> show se))
        Right (Right ()) -> pure ()
    reverse <$> readTVarIO positionsVar
  where
    handle ::
        (IOE :> es) =>
        TVar [Int] ->
        Ingested es Mdb.Message ->
        Eff es ()
    handle posVar Ingested{envelope = Envelope{payload = msg}, ack = AckHandle finalize} = do
        let Mdb.GlobalPosition gp = msg.globalPosition
            pos = fromIntegral gp :: Int
        liftIO $ atomically $ modifyTVar' posVar (pos :)
        finalize (decide pos)

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
    -- Both cases share the ephemeral Postgres and write to the same
    -- @messages@ table. They must run sequentially — message-db's
    -- global_position is a serial across all writes, so concurrent
    -- tests interleave positions and break the assertions.
    withFixture $ \fixtureIO ->
        sequentialTestGroup
            "CheckpointResume"
            AllFinish
            [ testCase "resumes from last checkpoint after restart" $ do
                (_, pool) <- fixtureIO
                resetTables pool
                let category = "orderstesta"
                    subscription = "orders-test-resume"
                forM_ [1 .. 10] (writeMessage pool category)

                firstRun <-
                    runDrain
                        pool
                        (testConfig category subscription)
                        5
                        (const AckOk)
                assertEqual "first run yielded five positions" 5 (length firstRun)
                assertEqual
                    "first-run positions are a contiguous prefix"
                    [1, 2, 3, 4, 5]
                    firstRun
                mCheckpoint <- readCheckpointPosition pool subscription
                assertEqual "checkpoint after first shutdown" (Just 5) mCheckpoint

                secondRun <-
                    runDrain
                        pool
                        (testConfig category subscription)
                        5
                        (const AckOk)
                assertEqual "second run yielded five positions" 5 (length secondRun)
                assertEqual
                    "second run resumes at 6..10"
                    [6, 7, 8, 9, 10]
                    secondRun
                mCheckpoint2 <- readCheckpointPosition pool subscription
                assertEqual "checkpoint after second shutdown" (Just 10) mCheckpoint2
            , testCase "AckRetry on position 3 pins the checkpoint at 2" $ do
                (_, pool) <- fixtureIO
                resetTables pool
                let category = "orderstestb"
                    subscription = "orders-test-retry"
                forM_ [1 .. 10] (writeMessage pool category)

                firstRun <-
                    runDrain
                        pool
                        (testConfig category subscription)
                        10
                        ( \p ->
                            if p == 3
                                then AckRetry (RetryDelay 0.1)
                                else AckOk
                        )
                assertEqual "first run yielded ten positions" 10 (length firstRun)
                mCheckpoint <- readCheckpointPosition pool subscription
                assertEqual
                    "AckRetry at position 3 pins checkpoint at 2"
                    (Just 2)
                    mCheckpoint

                secondRun <-
                    runDrain
                        pool
                        (testConfig category subscription)
                        8
                        (const AckOk)
                assertBool
                    "second run replays position 3 first"
                    (take 1 secondRun == [3])
                assertEqual
                    "second run yields 3..10 in order"
                    [3, 4, 5, 6, 7, 8, 9, 10]
                    secondRun
            ]
