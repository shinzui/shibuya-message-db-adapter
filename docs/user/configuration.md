# Configuration Reference

Every adapter is built from a `MessageDbAdapterConfig`. Use
`defaultConfig category subscriptionName` and override the fields you
care about with record-update syntax.

```haskell
import Shibuya.Adapter.MessageDb

cfg :: MessageDbAdapterConfig
cfg =
    (defaultConfig (CategoryStream "orders") "orders-demo")
        { batchSize          = BatchSize 200
        , pollInterval       = PollInterval 1.0
        , dlqStrategy        = DlqWriteToStream "orders-dlq"
        , maxRetryBufferSize = MaxRetryBufferSize 5_000
        }
```

## Required fields

### `category :: CategoryStream`

The message-db category to poll, e.g. `CategoryStream "orders"`. This
is the part of `stream_name` *before* the first `-`. The adapter calls
`get_category_messages` with this value verbatim.

### `subscriptionName :: SubscriptionName`

Keys this adapter's row in the `checkpoints` table. Two adapters that
share a subscription name will overwrite each other's progress —
always pick a name unique to *this* logical consumer.

When `consumerGroup` is set, the effective name written to the
checkpoint store is `<subscriptionName>-<member>` so each member keeps
its own checkpoint.

## Polling

### `batchSize :: BatchSize`  *(default: 100)*

Messages requested per poll. Bigger batches amortize the round-trip
cost; smaller batches lower per-message latency. message-db itself
does not limit this, but Hasql connection memory and your handler's
working set will.

### `pollInterval :: PollInterval`  *(default: 0.5 s)*

Sleep time between *empty* polls. A non-empty poll loops immediately
to drain backlog, so this only kicks in when the stream is idle.
Lower it for low-latency reactive workloads; raise it for archival
streams where minute-scale freshness is fine.

## Shutdown

### `drainTimeout :: DrainTimeout`  *(default: 10 s)*

Upper bound on how long `adapter.shutdown` will wait for in-flight
messages to finalize before flushing the final checkpoint anyway.
Messages that finalize after the timeout still update the inflight
ledger, but their ack will not have been persisted — they will be
re-delivered on the next start.

### `checkpointInterval :: CheckpointInterval`  *(default: 1 s)*

How often the background persister flushes the contiguous-prefix
checkpoint. At 100 msgs/s a 1 s interval bounds worst-case replay
after a crash to ~100 messages. Smaller intervals shrink replay at
the cost of write amplification on the `checkpoints` table; larger
intervals trade durability for fewer writes. The default is a
reasonable middle-ground; see [checkpointing.md](checkpointing.md)
for the mechanics.

## Retry & DLQ

### `dlqStrategy :: DlqStrategy`  *(default: `DlqSkipAndLog`)*

How `AckDeadLetter` decisions are handled. message-db has no native
DLQ primitive, so the adapter implements two strategies in-process:

- **`DlqSkipAndLog`** — log the reason at warning level and advance
  the checkpoint. The original message stays in `message_store`; you
  rely on logs to reconstruct what failed. Good for analytics or
  observability-only deployments.
- **`DlqWriteToStream stream`** — write a deterministic-id copy of
  the failing message to `stream` via `writeStreamMessage`, then
  advance the checkpoint. The DLQ `messageId` is derived from the
  original via UUIDv5 with a fixed namespace, so a crash between the
  DB write and the checkpoint advance is safely retryable —
  message-db rejects the duplicate with a unique-violation, which the
  adapter treats as success. Good for production: you get a durable,
  replayable DLQ.

A custom callback strategy is a deliberate non-goal; if you need
custom routing, wrap the adapter in your own layer.

### `maxRetryBufferSize :: MaxRetryBufferSize`  *(default: 1000)*

Upper bound on the in-process retry buffer. When a handler returns
`AckRetry` and the buffer is already at capacity, the adapter
*downgrades* the decision to
`AckDeadLetter MaxRetriesExceeded` rather than back-pressuring the
poll loop. Back-pressure would let one misbehaving handler stall every
sibling message on the subscription; the downgrade keeps progress
flowing and reuses Shibuya's existing taxonomy.

Raise this if your domain genuinely produces transient errors that
take many seconds to clear; lower it if you would rather fail fast
than tolerate a queue of stuck messages.

## Consumer groups

### `consumerGroup :: Maybe ConsumerGroupConfig`  *(default: `Nothing`)*

Set this to fan one logical category out across cooperating
processes. See [consumer-groups.md](consumer-groups.md) for the model.

```haskell
ConsumerGroupConfig
    { groupSize = 3   -- total members in the group
    , member    = 0   -- this process's index, in [0, groupSize)
    }
```

Validation: `groupSize >= 1`, `0 <= member < groupSize`. Invalid
combinations cause `messageDbAdapter` to throw `userError` at startup
rather than silently dropping messages.

## Defaults at a glance

| Field                | Default                        |
|----------------------|--------------------------------|
| `batchSize`          | `BatchSize 100`                |
| `pollInterval`       | `PollInterval 0.5` s           |
| `drainTimeout`       | `DrainTimeout 10` s            |
| `checkpointInterval` | `CheckpointInterval 1` s       |
| `dlqStrategy`        | `DlqSkipAndLog`                |
| `maxRetryBufferSize` | `MaxRetryBufferSize 1000`      |
| `consumerGroup`      | `Nothing`                      |
