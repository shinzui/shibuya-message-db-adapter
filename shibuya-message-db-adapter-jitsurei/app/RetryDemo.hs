{- | Retry example: each message is delivered twice.

On the first delivery of a given message UUID the handler returns
@AckRetry (RetryDelay 2)@; on the second it returns @AckOk@. A user
sees each seeded message printed twice, roughly two seconds apart,
proving that the adapter's retry buffer re-emits the same message
without advancing the contiguous-prefix checkpoint.

Seed: @just seed-jitsurei-retry@ (category @jitsurei-retry@).
Run:  @cabal run retry-demo@.
-}
module Main (main) where

import Control.Concurrent.STM (TVar, atomically, modifyTVar', newTVarIO, readTVar)
import Control.Monad.IO.Class (liftIO)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Text qualified as Text
import Data.Text.IO qualified as Text.IO
import Data.UUID (UUID)
import Effectful (Eff, IOE, runEff, (:>))
import Effectful.Concurrent (runConcurrent)
import Effectful.Error.Static (runErrorNoCallStack)
import Effectful.Hasql (SessionError, runHasqlWithPool)
import Effectful.Trace qualified as MsgDbTrace
import Hasql.Connection.Settings qualified as ConnSettings
import Hasql.Pool (UsageError)
import Hasql.Pool qualified as Pool
import Hasql.Pool.Config qualified as PoolConfig
import MessageDb.CheckpointStore.Effectful (runPostgresCheckointStore)
import MessageDb.Effectful (runMessageDb)
import MessageDb.Message qualified as Mdb
import OpenTelemetry.Attributes qualified as OTel
import OpenTelemetry.Trace.Core qualified as OTel
import Shibuya.Adapter (Adapter (..))
import Shibuya.Adapter.MessageDb (
    CategoryStream (..),
    defaultConfig,
    messageDbAdapter,
 )
import Shibuya.Core.Ack (AckDecision (..), RetryDelay (..))
import Shibuya.Core.AckHandle (AckHandle (..))
import Shibuya.Core.Ingested (Ingested (..))
import Shibuya.Core.Types (Envelope (..))
import Streamly.Data.Fold qualified as Fold
import Streamly.Data.Stream qualified as SStream
import System.Environment (getEnv)
import System.IO (BufferMode (LineBuffering), hSetBuffering, stdout)

category :: Text.Text
category = "jitsurei-retry"

subscription :: Text.Text
subscription = "jitsurei-retry"

main :: IO ()
main = do
    hSetBuffering stdout LineBuffering
    Text.IO.putStrLn "[retry-demo] Starting..."
    deliveries <- newTVarIO (Map.empty :: Map UUID Int)
    pool <- acquirePool
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
                adapter <-
                    messageDbAdapter
                        (defaultConfig (CategoryStream category) subscription)
                SStream.fold
                    (Fold.drainMapM (handleRetry deliveries))
                    adapter.source
    Pool.release pool
    case outer of
        Left ue -> Text.IO.putStrLn ("pool usage error: " <> Text.pack (show ue))
        Right (Left se) -> Text.IO.putStrLn ("session error: " <> Text.pack (show se))
        Right (Right ()) -> pure ()

handleRetry ::
    (IOE :> es) =>
    TVar (Map UUID Int) ->
    Ingested es Mdb.Message ->
    Eff es ()
handleRetry deliveries Ingested{envelope = Envelope{payload = msg}, ack = AckHandle finalize} = do
    let Mdb.MessageId uid = msg.messageId
    attempt <- liftIO $ atomically $ do
        modifyTVar' deliveries (Map.insertWith (+) uid 1)
        Map.findWithDefault 0 uid <$> readTVar deliveries
    liftIO $
        Text.IO.putStrLn $
            "[retry-demo] delivery "
                <> Text.pack (show attempt)
                <> " of message "
                <> Text.pack (show uid)
    if attempt < 2
        then finalize (AckRetry (RetryDelay 2))
        else finalize AckOk

acquirePool :: IO Pool.Pool
acquirePool = do
    connStr <- getEnv "PG_CONNECTION_STRING"
    let settings = ConnSettings.connectionString (Text.pack connStr)
        cfg =
            PoolConfig.settings
                [ PoolConfig.size 2
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
