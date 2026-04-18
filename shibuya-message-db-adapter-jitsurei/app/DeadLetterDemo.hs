{- | Dead-letter example: messages whose @messageType@ begins with
\"Bad\" are dead-lettered; the rest are AckOk'd.

Configured with @dlqStrategy = DlqWriteToStream (Stream \"demo-dlq\")@,
so failed messages are written to the @demo-dlq@ stream. After running,
query psql to confirm:

    psql -c "SELECT stream_name, type FROM message_store.messages \
             WHERE stream_name = 'demo-dlq';"

Seed: @just seed-jitsurei-dlq@ (three messages into category
@jitsurei-dlq@ with alternating @OrderPlaced@ and @BadFormat@ types).
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
import Hasql.Pool (UsageError)
import Hasql.Pool qualified as Pool
import Hasql.Pool.Config qualified as PoolConfig
import MessageDb.CheckpointStore.Effectful (runPostgresCheckointStore)
import MessageDb.Effectful (runMessageDb)
import MessageDb.Message qualified as Mdb
import MessageDb.Message.Stream (Stream)
import MessageDb.Message.Stream qualified as Stream
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
import Shibuya.Core.Ack (AckDecision (..), DeadLetterReason (..))
import Shibuya.Core.AckHandle (AckHandle (..))
import Shibuya.Core.Ingested (Ingested (..))
import Shibuya.Core.Types (Envelope (..))
import Streamly.Data.Fold qualified as Fold
import Streamly.Data.Stream qualified as SStream
import System.Environment (getEnv)
import System.IO (BufferMode (LineBuffering), hSetBuffering, stdout)

category :: Text.Text
category = "jitsurei-dlq"

subscription :: Text.Text
subscription = "jitsurei-dlq"

dlqStream :: Stream
dlqStream =
    case Stream.parseEither "demo-dlq" of
        Left e -> error ("demo-dlq is not a valid message-db stream name: " <> Text.unpack e)
        Right s -> s

main :: IO ()
main = do
    hSetBuffering stdout LineBuffering
    Text.IO.putStrLn "[dead-letter-demo] Starting..."
    let cfg =
            (defaultConfig (CategoryStream category) subscription)
                { dlqStrategy = DlqWriteToStream dlqStream
                }
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
                adapter <- messageDbAdapter cfg
                SStream.fold (Fold.drainMapM handleMaybeBad) adapter.source
    Pool.release pool
    case outer of
        Left ue -> Text.IO.putStrLn ("pool usage error: " <> Text.pack (show ue))
        Right (Left se) -> Text.IO.putStrLn ("session error: " <> Text.pack (show se))
        Right (Right ()) -> pure ()

handleMaybeBad ::
    (IOE :> es) =>
    Ingested es Mdb.Message ->
    Eff es ()
handleMaybeBad Ingested{envelope = Envelope{payload = msg}, ack = AckHandle finalize} = do
    let Mdb.MessageType mt = msg.messageType
        streamTxt = Stream.toText msg.stream
    liftIO $
        Text.IO.putStrLn $
            "[jitsurei-dlq] stream=" <> streamTxt <> " type=" <> mt
    if "Bad" `Text.isPrefixOf` mt
        then do
            liftIO $ Text.IO.putStrLn ("[jitsurei-dlq] -> dead-letter " <> mt)
            finalize (AckDeadLetter (InvalidPayload ("demo: type " <> mt)))
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
