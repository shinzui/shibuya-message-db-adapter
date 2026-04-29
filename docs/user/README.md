# shibuya-message-db-adapter — User Guide

A polling adapter that turns a [message-db](https://github.com/message-db/message-db)
category stream into a Shibuya source. Handlers receive a
`Shibuya.Core.Ingested` carrying the raw `MessageDb.Message` and decide
what to do with each one (`AckOk`, `AckRetry`, `AckDeadLetter`, `AckHalt`).

## What it does

- Polls `message_store.get_category_messages` for one category.
- Converts each `MessageDb.Message` to a `Shibuya.Envelope` and yields
  it as a Streamly stream the rest of your Shibuya app can consume.
- Tracks per-message outcomes in an in-process *inflight ledger* and
  flushes a contiguous-prefix checkpoint to a durable Postgres table
  in the background.
- Optionally splits a category across cooperating processes via
  consumer-group partitioning.
- Supports retries with delay, dead-lettering (skip-and-log or
  write-to-stream), and a hard halt for ordered streams.

## What it is not

- It does not give you Kafka-like partitioned ordering inside one
  category — message-db gives you per-stream ordering, and the adapter
  preserves it at the source.
- It does not auto-decode your message payload. Handlers see the raw
  `MessageDb.Message` and decode the JSON `data` field themselves.
- It does not implement consumer-group rebalancing. Group membership
  is static configuration; a rolling deploy is what changes who owns
  what.

## Where to start

| If you want to…                                | Read                                |
|-------------------------------------------------|-------------------------------------|
| Get a working consumer running locally          | [getting-started.md](getting-started.md) |
| Tune polling, retries, or DLQ behavior          | [configuration.md](configuration.md) |
| Decide what your handler should return          | [handler-decisions.md](handler-decisions.md) |
| Understand restart and at-least-once guarantees | [checkpointing.md](checkpointing.md) |
| Scale one category across N processes           | [consumer-groups.md](consumer-groups.md) |
| Walk through the runnable examples              | [examples.md](examples.md)          |

## Public API

The adapter's surface lives in `Shibuya.Adapter.MessageDb`:

```haskell
messageDbAdapter
    :: ( MessageDb :> es
       , CheckpointStore :> es
       , Concurrent :> es
       , IOE :> es
       , Error SessionError :> es
       )
    => MessageDbAdapterConfig
    -> Eff es (Adapter es MessageDb.Message.Message)
```

`Adapter` exposes a `source :: Stream (Eff es) (Ingested es payload)`
that you fold with Streamly, plus a `shutdown :: Eff es ()` action that
drains in-flight work and flushes a final checkpoint.

Configuration types (`MessageDbAdapterConfig`, `CategoryStream`,
`BatchSize`, `PollInterval`, `DrainTimeout`, `CheckpointInterval`,
`DlqStrategy`, `MaxRetryBufferSize`, `ConsumerGroupConfig`) and the
`defaultConfig` smart constructor are re-exported from the same module.
