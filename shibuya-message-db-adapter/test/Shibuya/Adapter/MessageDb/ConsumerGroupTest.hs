{- | Integration test for consumer-group partitioning.

Writes 30 messages to a single category, launches three adapter
instances cooperating under @ConsumerGroupConfig { groupSize = 3 }@
with @member@ indices 0, 1, and 2, and asserts:

* Exactly 30 handler invocations total across the three members.
* No duplicate message ids across members.
* Every invocation lands on the member whose index equals
  @categoryPartition 3 categoryName@ — the other two members filter
  every message and advance their checkpoint past it without
  processing.
* All three partition-scoped checkpoint rows
  (@\<base\>-\<m\>-of-3@) exist at the last written global position.

See the \"Surprises & Discoveries\" section of the ExecPlan for why
this variant differs from the plan's original six-category design.
Message-db's @get_category_messages@ takes a single category
argument, and the plan's Decision Log pins the partition hash to
@message-db-subscription@'s category-based @getPartitionMurmur@.
Three adapters polling one category prove the routing and
filter-advance paths without fanning out to 18 adapter instances.

Termination: each member's thread is cancelled with 'Async.cancel'
once the owner has drained its messages and all three checkpoint
rows have reached the target position. The non-owner members'
streams never emit (every polled message is filtered), so they can
only be stopped externally — the 'Stream.takeWhileM' inside the
source never re-evaluates its predicate when the stream is empty.
-}
module Shibuya.Adapter.MessageDb.ConsumerGroupTest (tests) where

import Contravariant.Extras (contrazip5)
import Control.Concurrent (threadDelay)
import Control.Concurrent.Async qualified as Async
import Control.Concurrent.STM (
    atomically,
    check,
    modifyTVar',
    newTVarIO,
    readTVar,
 )
import Control.Exception (throwIO)
import Control.Monad (forM_)
import Data.IORef (atomicModifyIORef', newIORef, readIORef)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Set qualified as Set
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
import Hasql.Pool (Pool)
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
    ConsumerGroupConfig (..),
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
import Shibuya.Adapter.MessageDb.Internal (categoryPartition)
import Shibuya.Core.Ack (AckDecision (..))
import Shibuya.Core.AckHandle (AckHandle (..))
import Shibuya.Core.Ingested (Ingested (..))
import Shibuya.Core.Types (Envelope (..), MessageId (..))
import Streamly.Data.Fold qualified as Fold
import Streamly.Data.Stream qualified as SStream
import Test.Tasty (TestTree, withResource)
import Test.Tasty.HUnit (assertBool, assertEqual, testCase)

-- * Paths (shared with CheckpointResumeTest)

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
                    [ PoolConfig.size 6
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
        textParam = E.param (E.nonNullable E.text)
        encoder = contrazip5 textParam textParam textParam textParam textParam
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

resetTables :: Pool -> IO ()
resetTables pool = do
    execSql pool "TRUNCATE message_store.messages RESTART IDENTITY"
    execSql pool "TRUNCATE checkpoints RESTART IDENTITY"

-- * Adapter harness

testConfig :: Text -> Text -> Int -> Int -> MessageDbAdapterConfig
testConfig category subscription groupSize member =
    (defaultConfig (CategoryStream category) subscription)
        { batchSize = BatchSize 10
        , pollInterval = PollInterval 0.05
        , drainTimeout = DrainTimeout 2
        , checkpointInterval = CheckpointInterval 0.05
        , consumerGroup = Just (ConsumerGroupConfig{groupSize = groupSize, member = member})
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

{- | Run one member's adapter indefinitely. The action drains the
source into @handle@ until cancelled by an asynchronous exception
('Async.cancel' from the test body). Members whose partition is empty
still drive the poll loop long enough for the background persister to
flush their checkpoint.
-}
runMemberForever ::
    Pool ->
    MessageDbAdapterConfig ->
    Int ->
    (Int -> Text -> IO ()) ->
    IO ()
runMemberForever pool cfg memberIx recordInvocation = do
    tracer <- noopTracer
    r <-
        runEff
            . runConcurrent
            . runErrorNoCallStack @Pool.UsageError
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
    handle Ingested{envelope = Envelope{messageId = MessageId mid}, ack = AckHandle finalize} = do
        liftIO $ recordInvocation memberIx mid
        finalize AckOk

{- | Poll every member's checkpoint row until each has reached
@target@. Bounded at ~5 s so a persistent stall shows up as an
assertion failure rather than an infinite wait.
-}
waitForAllCheckpoints :: Pool -> Text -> Int -> Int -> IO Bool
waitForAllCheckpoints pool subBase groupSize target = go (100 :: Int)
  where
    go 0 = pure False
    go n = do
        allReached <-
            and
                <$> mapM checkMember [0 .. groupSize - 1]
        if allReached
            then pure True
            else do
                threadDelay 50_000
                go (n - 1)

    checkMember m = do
        chk <- readCheckpointPosition pool (memberSubName subBase groupSize m)
        pure $ maybe False (>= target) chk

memberSubName :: Text -> Int -> Int -> Text
memberSubName subBase groupSize m =
    subBase
        <> "-"
        <> Text.pack (show m)
        <> "-of-"
        <> Text.pack (show groupSize)

-- * Tests

tests :: TestTree
tests =
    withFixture $ \fixtureIO ->
        testCase "three-member group routes messages by category hash" $ do
            (_, pool) <- fixtureIO
            resetTables pool

            let category = "cgtdemo"
                subBase = "cgtdemo-sub"
                groupSize = 3
                expectedMember = categoryPartition groupSize category
                total = 30

            forM_ [1 .. total] (writeMessage pool category)

            totalCount <- newTVarIO (0 :: Int)
            perMemberRef <- newIORef (Map.empty :: Map Int [Text])

            let recordInvocation m mid = do
                    atomicModifyIORef' perMemberRef $ \mp ->
                        (Map.insertWith (<>) m [mid] mp, ())
                    atomically $ modifyTVar' totalCount (+ 1)

            asyncs <-
                mapM
                    ( \m ->
                        Async.async $
                            runMemberForever
                                pool
                                (testConfig category subBase groupSize m)
                                m
                                recordInvocation
                    )
                    [0 .. groupSize - 1]

            -- Wait for the owner to drain all messages.
            atomically $ do
                n <- readTVar totalCount
                check (n >= total)

            -- Wait for every member's checkpoint to reach the target so the
            -- filter-and-advance path has completed on the non-owners.
            allReached <- waitForAllCheckpoints pool subBase groupSize total

            -- Terminate all three runner threads.
            mapM_ Async.cancel asyncs

            assertBool "all members' checkpoints reached the total" allReached

            seen <- readIORef perMemberRef
            let flatIds = concat (Map.elems seen)

            assertEqual "total handler invocations" total (length flatIds)
            assertEqual "no duplicate message ids" total (Set.size (Set.fromList flatIds))

            let ownerInvocations = Map.findWithDefault [] expectedMember seen
            assertEqual
                ("owner member " <> show expectedMember <> " processed every message")
                total
                (length ownerInvocations)

            forM_ [0 .. groupSize - 1] $ \m ->
                case Map.lookup m seen of
                    Just ids
                        | m /= expectedMember ->
                            assertEqual
                                ("non-owner member " <> show m <> " processed no messages")
                                0
                                (length ids)
                    _ -> pure ()

            forM_ [0 .. groupSize - 1] $ \m -> do
                let subName = memberSubName subBase groupSize m
                chk <- readCheckpointPosition pool subName
                assertBool
                    ("checkpoint for " <> Text.unpack subName <> " at or above total")
                    (maybe False (>= total) chk)
