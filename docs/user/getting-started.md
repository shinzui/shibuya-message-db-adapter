# Getting Started

This walks through wiring the adapter into a Haskell program that
consumes one message-db category, prints each message, and acks it.

## Prerequisites

- GHC 9.12.2 (the version this library is tested with).
- A reachable Postgres with the [message-db](https://github.com/message-db/message-db)
  schema installed and the `checkpoints` table from
  [`message-db-checkpoint-store`](https://hackage.haskell.org/package/message-db-checkpoint-store).
- A connection string in `PG_CONNECTION_STRING` (the convention the
  bundled examples use).

The repo's flake + `process-compose` setup gives you a local Postgres
with both schemas already in place — see the top-level
[README](../../README.md) for `direnv allow → just process-up →
just bootstrap-message-db`.

## Add the dependency

```cabal
build-depends:
    , effectful                    ^>=2.6.1.0
    , hasql-effectful              ^>=0.1
    , message-db-checkpoint-store  ^>=0.1
    , message-db-effectful         ^>=0.1
    , shibuya-core                 ^>=0.1
    , shibuya-message-db-adapter   ^>=0.1
    , streamly                     ^>=0.11
```

The library is built on `effectful`. You will run a stack of effect
interpreters around the adapter call.

## Minimal consumer

```haskell
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE OverloadedRecordDot #-}

module Main (main) where

import Control.Monad.IO.Class (liftIO)
import Data.Text qualified as T
import Effectful (runEff, (:>))
import Effectful.Concurrent (runConcurrent)
import Effectful.Error.Static (runErrorNoCallStack)
import Effectful.Hasql (SessionError, runHasqlWithPool)
import Hasql.Connection.Settings qualified as ConnSettings
import Hasql.Pool (UsageError)
import Hasql.Pool qualified as Pool
import Hasql.Pool.Config qualified as PoolConfig
import MessageDb.CheckpointStore.Effectful (runPostgresCheckointStore)
import MessageDb.Effectful (runMessageDb)
import Shibuya.Adapter (Adapter (..))
import Shibuya.Adapter.MessageDb
    ( CategoryStream (..)
    , defaultConfig
    , messageDbAdapter
    )
import Shibuya.Core.Ack (AckDecision (..))
import Shibuya.Core.AckHandle (AckHandle (..))
import Shibuya.Core.Ingested (Ingested (..))
import Shibuya.Core.Types (Envelope (..))
import Streamly.Data.Fold qualified as Fold
import Streamly.Data.Stream qualified as SStream
import System.Environment (getEnv)

main :: IO ()
main = do
    pool <- acquirePool
    _ <-
        runEff
            . runConcurrent
            . runErrorNoCallStack @UsageError
            . runErrorNoCallStack @SessionError
            . runHasqlWithPool pool
            -- message-db-effectful and the checkpoint store both run on
            -- top of the Hasql effect, so they go inside it.
            . runMessageDb
            . runPostgresCheckointStore
            $ do
                adapter <-
                    messageDbAdapter
                        (defaultConfig (CategoryStream "orders") "orders-demo")
                SStream.fold (Fold.drainMapM handle) adapter.source
    Pool.release pool
  where
    handle Ingested{ack = AckHandle finalize} = finalize AckOk

acquirePool :: IO Pool.Pool
acquirePool = do
    cs <- T.pack <$> getEnv "PG_CONNECTION_STRING"
    Pool.acquire $
        PoolConfig.settings
            [ PoolConfig.size 2
            , PoolConfig.staticConnectionSettings (ConnSettings.connectionString cs)
            ]
```

What this does:

1. Acquires a Hasql pool from `PG_CONNECTION_STRING`.
2. Builds an effect stack that provides `Concurrent`, `Hasql`,
   `MessageDb`, and `CheckpointStore`. The adapter requires all four.
3. Asks the adapter for a source over the `orders` category, keyed in
   the `checkpoints` table by the subscription name `orders-demo`.
4. Folds the source with Streamly, acking every message as `AckOk`.

The adapter blocks on its poll loop after draining the messages
written so far — Ctrl-C exits the process.

## Reading the message

`Ingested` is the wrapper Shibuya hands to your handler. Pattern-match
to get at the payload — for this adapter it is the original
`MessageDb.Message`:

```haskell
import MessageDb.Message qualified as Mdb
import MessageDb.Message.Stream qualified as Stream

handle Ingested{envelope = Envelope{payload = msg}, ack = AckHandle finalize} = do
    let Mdb.MessageType ty = msg.messageType
        Mdb.GlobalPosition pos = msg.globalPosition
        streamName = Stream.toText msg.stream
    liftIO . putStrLn $
        "stream=" <> T.unpack streamName
            <> " type=" <> T.unpack ty
            <> " pos="  <> show pos
    finalize AckOk
```

Decode `msg.data` (a JSON `ByteString`) to your domain type with
`Aeson.eitherDecodeStrict` or whatever you already use.

## Clean shutdown

For long-running services, install a signal handler and call
`adapter.shutdown` before exiting. That flips an internal shutdown
signal, drains messages still in flight (bounded by `drainTimeout`),
and writes one last checkpoint so the next start picks up exactly
where the last one left off.

The bundled `CheckpointRestart.hs` example runs two pipelines in one
process to demonstrate this — see [examples.md](examples.md).

## Next steps

- [configuration.md](configuration.md) — every knob and what it does.
- [handler-decisions.md](handler-decisions.md) — what to return from your
  handler and what it costs.
- [checkpointing.md](checkpointing.md) — what survives a crash and what
  doesn't.
