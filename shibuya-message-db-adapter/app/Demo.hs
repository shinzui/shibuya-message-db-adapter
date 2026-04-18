{- | Minimal end-to-end demo for the message-db adapter.

Connects to the Postgres database pointed to by @$PG_CONNECTION_STRING@,
constructs a message-db adapter for the requested category, and drains
the adapter's source stream directly — printing one line per message —
until a @SIGINT@ terminates the process.

This deliberately bypasses 'Shibuya.App.runApp': composing the
framework's @Tracing@ effect with the @runMessageDb@ interpreter's
@Trace@ effect would dwarf the code that actually exercises the
adapter. EP-5's integration tests will do the full composition.
-}
module Main (main) where

import Data.Text (Text)
import Data.Text qualified as Text
import Data.Text.IO qualified as Text.IO
import Effectful (Eff, IOE, liftIO, runEff, (:>))
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
import MessageDb.Message.Stream qualified as Stream
import OpenTelemetry.Attributes qualified as OTel
import OpenTelemetry.Trace.Core qualified as OTel
import Options.Applicative qualified as Opt
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

data Args = Args
    { category :: !Text
    , subscription :: !Text
    , limit :: !(Maybe Int)
    }

argsParser :: Opt.Parser Args
argsParser =
    Args
        <$> Opt.strOption
            ( Opt.long "category"
                <> Opt.metavar "CATEGORY"
                <> Opt.help "message-db category to poll (e.g. orders)"
            )
        <*> Opt.strOption
            ( Opt.long "subscription"
                <> Opt.metavar "NAME"
                <> Opt.value "shibuya-demo"
                <> Opt.showDefault
                <> Opt.help "subscription name for the durable checkpoint row"
            )
        <*> Opt.optional
            ( Opt.option
                Opt.auto
                ( Opt.long "limit"
                    <> Opt.metavar "N"
                    <> Opt.help "stop after N messages (default: poll forever)"
                )
            )

{- | Acquire a connection pool from @$PG_CONNECTION_STRING@.

The direnv-managed Postgres in this repo sets that variable to a URI
with a percent-encoded unix-socket host. Hasql accepts libpq-style
connection strings directly.
-}
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

{- | Build a tracer wired to the default global tracer provider.

'OTel.getGlobalTracerProvider' returns a noop provider when nothing
else has been registered, which is exactly what the demo wants — the
adapter still works, spans just go nowhere.
-}
acquireTracer :: IO OTel.Tracer
acquireTracer = do
    tp <- OTel.getGlobalTracerProvider
    let lib =
            OTel.InstrumentationLibrary
                { libraryName = "shibuya-message-db-adapter-demo"
                , libraryVersion = "0.1"
                , librarySchemaUrl = ""
                , libraryAttributes = OTel.emptyAttributes
                }
    pure (OTel.makeTracer tp lib OTel.tracerOptions)

printMessage ::
    (IOE :> es) =>
    Ingested es Mdb.Message ->
    Eff es ()
printMessage Ingested{envelope, ack = AckHandle finalize} = do
    let msg = envelope.payload
        streamTxt = Stream.toText msg.stream
        Mdb.GlobalPosition gp = msg.globalPosition
        Mdb.MessageType mt = msg.messageType
    liftIO $
        Text.IO.putStrLn $
            "message: "
                <> streamTxt
                <> " type="
                <> mt
                <> " gp="
                <> Text.pack (show gp)
    finalize AckOk

main :: IO ()
main = do
    hSetBuffering stdout LineBuffering
    args <-
        Opt.execParser $
            Opt.info
                (argsParser Opt.<**> Opt.helper)
                ( Opt.fullDesc
                    <> Opt.progDesc
                        "Drain a single message-db category through the Shibuya adapter, printing each message."
                )
    pool <- acquirePool
    tracer <- acquireTracer
    runDemo pool tracer args
    Pool.release pool

{- | Wire up the full effect stack around the adapter's source stream.

Order, outermost to innermost: IOE -> Concurrent -> Error UsageError
-> Error SessionError -> Hasql -> Trace -> MessageDb. The two error
interpreters lift their failures into a @Left@ so a SQL failure does
not crash the process uncontrollably.
-}
runDemo ::
    Pool.Pool ->
    OTel.Tracer ->
    Args ->
    IO ()
runDemo pool tracer args = do
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
                        (defaultConfig (CategoryStream args.category) args.subscription)
                drain args adapter
    case outer of
        Left ue -> Text.IO.putStrLn ("pool usage error: " <> Text.pack (show ue))
        Right (Left se) -> Text.IO.putStrLn ("session error: " <> Text.pack (show se))
        Right (Right ()) -> pure ()

drain ::
    (IOE :> es) =>
    Args ->
    Adapter es Mdb.Message ->
    Eff es ()
drain args adapter =
    case args.limit of
        Nothing ->
            SStream.fold (Fold.drainMapM printMessage) adapter.source
        Just n ->
            SStream.fold
                (Fold.drainMapM printMessage)
                (SStream.take n adapter.source)
