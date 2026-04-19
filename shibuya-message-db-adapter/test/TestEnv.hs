{- | Shared integration-test harness.

@withTestEnv@ spins up an ephemeral Postgres via @ephemeral-pg@,
installs the message-db schema plus the @checkpoints@ table, acquires
a Hasql connection pool, and hands a 'TestEnv' to the caller. The
Postgres instance is automatically torn down when the continuation
returns.

@writeCategoryMessages@ seeds N messages into @\<category\>-1 ..
\<category\>-N@ with deterministic UUIDs derived from the index
(so tests can recover a known id by number). @writeTestMessages@
writes arbitrary payloads to a single stream.

SQL scripts are read from @$MESSAGE_DB_SQL_DIR@; if the variable is
unset the harness falls back to a hard-coded absolute path on the
developer's machine and prints a warning. Set the variable in
@flake.nix@'s @shellHook@ so that @nix develop@ and @direnv@ users
pick it up automatically.
-}
module TestEnv (
    TestEnv (..),
    withTestEnv,
    withTestEnvPool,
    writeCategoryMessages,
    writeTestMessages,
    readCheckpointPosition,
    readDlqRows,
    resetTables,
    execSql,
    uuidFromInt,
) where

import Contravariant.Extras (contrazip5)
import Control.Exception (bracket, throwIO)
import Control.Monad (forM_)
import Data.Aeson qualified as Aeson
import Data.ByteString.Lazy qualified as BSL
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Text.Encoding qualified as Text.Encoding
import Data.Text.IO qualified as Text.IO
import Data.UUID (UUID)
import Data.UUID qualified as UUID
import EphemeralPg qualified as Pg
import EphemeralPg.Config (Config (..), defaultPostgresSettings)
import Hasql.Connection.Settings qualified as ConnSettings
import Hasql.Decoders qualified as D
import Hasql.Encoders qualified as E
import Hasql.Pool (Pool)
import Hasql.Pool qualified as Pool
import Hasql.Pool.Config qualified as PoolConfig
import Hasql.Session qualified as Session
import Hasql.Statement (preparable)
import System.Environment (lookupEnv)
import System.IO (hPutStrLn, stderr)

-- * Types and main fixture

{- | Per-test handle. Integration tests acquire a fresh 'TestEnv'
through 'withTestEnv', which guarantees a clean database and a
pool ready for use with @runHasqlWithPool@.
-}
data TestEnv = TestEnv
    { connSettings :: ConnSettings.Settings
    , pool :: Pool
    , database :: Pg.Database
    }

{- | Default SQL-script path when @MESSAGE_DB_SQL_DIR@ is unset.

This is the absolute path on the developer's machine. CI and packaged
builds should set the env var explicitly; @flake.nix@ sets it to the
same path via @shellHook@ so that @nix develop@ and @direnv@ users
pick it up automatically.
-}
defaultMessageDbSqlDir :: FilePath
defaultMessageDbSqlDir =
    "/Users/shinzui/Keikaku/hub/event-sourcing/message-db-project/message-db/database"

{- | Default path to the checkpoint-store migration. The file is part
of the @message-db-checkpoint-store@ package and lives alongside the
source; we reference it by absolute path because the migration is a
simple @CREATE TABLE IF NOT EXISTS@ script rather than a runtime
dependency.
-}
defaultCheckpointSchemaPath :: FilePath
defaultCheckpointSchemaPath =
    "/Users/shinzui/Keikaku/work/libraries/haskell/message-db-hs-master/message-db-checkpoint-store/migrations/scripts/create_checkpoints.sql"

resolveMessageDbSqlDir :: IO FilePath
resolveMessageDbSqlDir = do
    mDir <- lookupEnv "MESSAGE_DB_SQL_DIR"
    case mDir of
        Just d -> pure d
        Nothing -> do
            hPutStrLn stderr $
                "warning: MESSAGE_DB_SQL_DIR unset; falling back to "
                    <> defaultMessageDbSqlDir
            pure defaultMessageDbSqlDir

pgConfig :: Pg.Config
pgConfig =
    Pg.defaultConfig
        { postgresSettings =
            defaultPostgresSettings
                <> [("search_path", "'message_store,\"$user\",public'")]
        }

{- | Acquire a fresh ephemeral Postgres, install the message-db
schema, and run the continuation with a ready 'TestEnv'. Releases
the pool and (via @ephemeral-pg@'s cache) the server on return.
-}
withTestEnv :: (TestEnv -> IO a) -> IO a
withTestEnv = withTestEnvPool 3

{- | Variant of 'withTestEnv' with a configurable pool size. Tests
that launch multiple adapter instances against the same database
(e.g. the consumer-group exactly-once test) need a larger pool to
avoid head-of-line blocking in the hasql pool.
-}
withTestEnvPool :: Int -> (TestEnv -> IO a) -> IO a
withTestEnvPool poolSize k =
    bracket startDb stopDb (\db -> withPool db k)
  where
    startDb = Pg.startCached pgConfig Pg.defaultCacheConfig >>= either throwIO pure
    stopDb = Pg.stop
    withPool db k' = do
        let settings = Pg.connectionSettings db
            cfg =
                PoolConfig.settings
                    [ PoolConfig.size poolSize
                    , PoolConfig.staticConnectionSettings settings
                    ]
        bracket (Pool.acquire cfg) Pool.release $ \p -> do
            bootstrapMessageDb p
            k' TestEnv{connSettings = settings, pool = p, database = db}

-- * Schema bootstrap

{- | Apply the message-db schema and the checkpoint-store migration.
Idempotent: every underlying script is @CREATE ... IF NOT EXISTS@
or equivalent. Skipped: @roles@ and @privileges@, which assume a
message-db-specific role that doesn't exist in the ephemeral
single-user database.
-}
bootstrapMessageDb :: Pool -> IO ()
bootstrapMessageDb pool' = do
    sqlDir <- resolveMessageDbSqlDir
    execSql pool' "CREATE EXTENSION IF NOT EXISTS pgcrypto"
    execSqlFile pool' (sqlDir <> "/schema/message-store.sql")
    execSqlFile pool' (sqlDir <> "/types/message.sql")
    execSqlFile pool' (sqlDir <> "/tables/messages.sql")
    forM_ functionFiles $ \f ->
        execSqlFile pool' (sqlDir <> "/functions/" <> f)
    execSqlFile pool' defaultCheckpointSchemaPath
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
execSqlFile pool' path = Text.IO.readFile path >>= execSql pool'

-- | Run an SQL script string against the pool, throwing on error.
execSql :: Pool -> Text -> IO ()
execSql pool' stmt = do
    r <- Pool.use pool' (Session.script stmt)
    case r of
        Left e -> throwIO (userError (show e))
        Right () -> pure ()

-- * Seeding helpers

{- | Seed @count@ messages to the given category: one message per
stream @\<category\>-1@ through @\<category\>-count@ with
deterministic UUIDs (derived from the 1-based index via
'uuidFromInt'), messageType @"OrderPlaced"@, and body
@{"n": <index>}@.

Returns nothing; the messages' global positions are guaranteed to be
@[1 .. count]@ by message-db's serial @global_position@ and the
fact that the database is fresh.
-}
writeCategoryMessages :: TestEnv -> Text -> Int -> IO ()
writeCategoryMessages env category count =
    forM_ [1 .. count] $ \n ->
        writeOneMessage env.pool (category <> "-" <> Text.pack (show n)) "OrderPlaced" n

{- | Write arbitrary payloads to a single stream, with deterministic
per-index UUIDs (offset by 1 for each payload) and messageType
@"TestEvent"@.
-}
writeTestMessages :: TestEnv -> Text -> [Aeson.Value] -> IO ()
writeTestMessages env streamName payloads =
    forM_ (zip [1 :: Int ..] payloads) $ \(n, payload) ->
        writeOneMessageJson env.pool streamName "TestEvent" n payload

writeOneMessage :: Pool -> Text -> Text -> Int -> IO ()
writeOneMessage pool' streamName messageType n = do
    let dataJson = "{\"n\": " <> Text.pack (show n) <> "}"
    writeOneMessageRaw pool' streamName messageType n dataJson

writeOneMessageJson :: Pool -> Text -> Text -> Int -> Aeson.Value -> IO ()
writeOneMessageJson pool' streamName messageType n payload =
    writeOneMessageRaw pool' streamName messageType n (encodeJson payload)

encodeJson :: Aeson.Value -> Text
encodeJson = Text.Encoding.decodeUtf8 . BSL.toStrict . Aeson.encode

writeOneMessageRaw :: Pool -> Text -> Text -> Int -> Text -> IO ()
writeOneMessageRaw pool' streamName messageType n dataJson = do
    let metadataJson = "{}" :: Text
        messageId = UUID.toText (uuidFromInt n)
        sql =
            "SELECT write_message(\
            \$1::varchar, $2::varchar, $3::varchar, $4::jsonb, $5::jsonb)"
        textParam = E.param (E.nonNullable E.text)
        encoder = contrazip5 textParam textParam textParam textParam textParam
        decoder = D.singleRow (D.column (D.nonNullable D.int8))
        stmt = preparable sql encoder decoder
    r <-
        Pool.use pool' $
            Session.statement
                (messageId, streamName, messageType, dataJson, metadataJson)
                stmt
    case r of
        Left e -> throwIO (userError (show e))
        Right _ -> pure ()

{- | Produce a deterministic UUID from an integer. The low 32 bits of
the UUID equal @n@.
-}
uuidFromInt :: Int -> UUID
uuidFromInt n = UUID.fromWords 0 0 0 (fromIntegral n)

-- * Query helpers

readCheckpointPosition :: Pool -> Text -> IO (Maybe Int)
readCheckpointPosition pool' subName = do
    let sql =
            "SELECT last_processed_global_position FROM checkpoints WHERE name = $1"
        encoder = E.param (E.nonNullable E.text)
        decoder = D.rowMaybe (D.column (D.nullable D.int8))
        stmt = preparable sql encoder decoder
    r <- Pool.use pool' $ Session.statement subName stmt
    case r of
        Left e -> throwIO (userError (show e))
        Right mRow -> pure (fmap fromIntegral =<< mRow)

-- | Fetch @(message_id, metadata_jsonb)@ rows from a named stream.
readDlqRows :: Pool -> Text -> IO [(Text, Aeson.Value)]
readDlqRows pool' streamName = do
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
    r <- Pool.use pool' $ Session.statement streamName stmt
    case r of
        Left e -> throwIO (userError (show e))
        Right rows ->
            pure
                [ (mid, fromMaybe Aeson.Null (Aeson.decodeStrictText md))
                | (mid, md) <- rows
                ]

resetTables :: Pool -> IO ()
resetTables pool' = do
    execSql pool' "TRUNCATE message_store.messages RESTART IDENTITY"
    execSql pool' "TRUNCATE checkpoints RESTART IDENTITY"
