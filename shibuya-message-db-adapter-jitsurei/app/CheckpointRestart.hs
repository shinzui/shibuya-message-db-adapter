{- | Checkpoint-restart example: two sequential pipelines in one
process, same subscription name, prove the durable checkpoint
survives a clean adapter shutdown.

Phase 1 consumes the first five messages (positions 1..5), calls
@shutdown@ (which flushes the final checkpoint), and returns. Phase 2
starts a brand new adapter with the same subscription name and
resumes from the stored checkpoint, consuming positions 6..10.

Seed: @just seed-jitsurei-checkpoint@ (ten messages into category
@jitsurei-checkpoint@).
Run:  @cabal run checkpoint-restart@.
-}
module Main (main) where

import Control.Monad.IO.Class (liftIO)
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
import OpenTelemetry.Attributes qualified as OTel
import OpenTelemetry.Trace.Core qualified as OTel
import Shibuya.Adapter (Adapter (..))
import Shibuya.Adapter.MessageDb (
    CategoryStream (..),
    defaultConfig,
    messageDbAdapter,
 )
import Shibuya.Core.Ack (AckDecision (..))
import Shibuya.Core.AckHandle (AckHandle (..))
import Shibuya.Core.Ingested (Ingested (..))
import Shibuya.Core.Types (Envelope (..))
import Streamly.Data.Fold qualified as Fold
import Streamly.Data.Stream qualified as SStream
import System.Environment (getEnv)
import System.IO (BufferMode (LineBuffering), hSetBuffering, stdout)

category :: Text.Text
category = "jitsurei-checkpoint"

subscription :: Text.Text
subscription = "jitsurei-checkpoint"

main :: IO ()
main = do
    hSetBuffering stdout LineBuffering
    pool <- acquirePool
    tracer <- noopTracer
    Text.IO.putStrLn "[checkpoint-restart] Phase 1: consume 5, stop"
    runPhase pool tracer 5
    Text.IO.putStrLn "[checkpoint-restart] Phase 2: consume 5 more"
    runPhase pool tracer 5
    Text.IO.putStrLn "[checkpoint-restart] Done."
    Pool.release pool

{- | Run a single pipeline that takes @n@ messages, ack-oks each, and
then calls @adapter.shutdown@ so the contiguous-prefix checkpoint
is flushed to the @checkpoints@ table before the function returns.
-}
runPhase :: Pool -> OTel.Tracer -> Int -> IO ()
runPhase pool tracer n = do
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
                    (Fold.drainMapM printAndAck)
                    (SStream.take n adapter.source)
                adapter.shutdown
    case outer of
        Left ue -> Text.IO.putStrLn ("pool usage error: " <> Text.pack (show ue))
        Right (Left se) -> Text.IO.putStrLn ("session error: " <> Text.pack (show se))
        Right (Right ()) -> pure ()

printAndAck ::
    (IOE :> es) =>
    Ingested es Mdb.Message ->
    Eff es ()
printAndAck Ingested{envelope = Envelope{payload = msg}, ack = AckHandle finalize} = do
    let Mdb.GlobalPosition gp = msg.globalPosition
    liftIO $ Text.IO.putStrLn ("[phase] got " <> Text.pack (show gp))
    finalize AckOk

acquirePool :: IO Pool
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
