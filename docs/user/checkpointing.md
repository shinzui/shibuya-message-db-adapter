# Checkpointing & Restart

The adapter promises **at-least-once delivery**: every message in the
category will be handed to your handler at least once, and after a
clean restart you will not be flooded with everything you have ever
seen. This page explains the mechanics so you can reason about
duplicates, replay, and shutdown.

## The model

The adapter maintains an in-memory **inflight ledger** keyed on
`globalPosition`. Every fetched message starts as inflight; every
finalized message records its outcome (`AckOk`, `AckDeadLetter`, etc.)
in the ledger. A background **persister** fiber periodically computes
the *longest contiguous prefix* of resolved positions starting at
the current checkpoint, and writes that prefix's tail to the
`checkpoints` row keyed by your `subscriptionName`.

The contiguous-prefix rule is what gives us at-least-once even with
out-of-order completion. If positions 100, 101, 103, 104 have all
resolved but 102 is still inflight, the checkpoint stays at 101 — a
crash at this moment will redeliver 102, 103, and 104, and your
handler will see 103 and 104 again. Your handlers must therefore be
**idempotent** with respect to duplicates.

## The `checkpoints` table

Provided by
[`message-db-checkpoint-store`](https://hackage.haskell.org/package/message-db-checkpoint-store).
The adapter reads and writes one row per *subscription*. `subscriptionName`
is the primary key; a row records the last persisted `globalPosition`
plus its category.

When `consumerGroup` is configured, the adapter writes its row under
`<subscriptionName>-<member>` so each member keeps its own checkpoint
independently. See [consumer-groups.md](consumer-groups.md).

## Startup

1. Read the persisted `globalPosition` for `subscriptionName` (zero if
   no row exists yet).
2. Seed the fetch position at `checkpoint + 1`. message-db's
   `get_category_messages` filters `global_position >= $2`, so this
   skips everything we have already acked.
3. Begin polling.

There is no replay flag. To replay from the beginning, delete the
row for `subscriptionName` in the `checkpoints` table.

## Steady state

The persister wakes every `checkpointInterval` (default 1 s) and
asks the inflight ledger for its current contiguous-prefix tail.
If the tail has advanced since the last write, it `storeCheckpoint`s
the new value. Otherwise it sleeps again.

Tradeoff:

- **Smaller intervals** shrink worst-case replay after a crash but
  multiply writes against the `checkpoints` table.
- **Larger intervals** are cheaper but mean more redelivery after a
  crash. At 1000 msgs/s and a 5 s interval you would replay up to
  5,000 messages.

## Shutdown

`adapter.shutdown` runs four steps in order:

1. Flip a shared `shutdownSignal` `TVar`. The source stream stops
   pulling new batches; the persister exits its sleep loop.
2. Poll the inflight ledger every 10 ms until it is empty, **or**
   `drainTimeout` (default 10 s) elapses, whichever first.
3. Ask the ledger for its final contiguous-prefix tail. If it
   advanced past the last persisted value, `storeCheckpoint` it.
4. Sleep one extra `checkpointInterval` to give the persister fiber
   time to observe the shutdown signal and exit.

If `drainTimeout` fires before the ledger drains, the messages still
in flight do *not* update the checkpoint. They will be redelivered
on the next start. Pick `drainTimeout` to comfortably exceed your
P99 handler latency.

Calling `shutdown` is **strongly recommended** for long-running
services. Without it, a crash will replay roughly
`checkpointInterval`'s worth of messages, and any message whose
handler had not finalized by the crash time will be replayed too.

## What survives a crash

| Event                                                 | After restart                                     |
|-------------------------------------------------------|---------------------------------------------------|
| Clean shutdown via `adapter.shutdown`                  | Resume at `lastAckOk + 1`. No replay.             |
| Crash mid-poll, ledger had nothing inflight           | Resume at last persisted checkpoint. Up to one `checkpointInterval` of messages may replay. |
| Crash mid-handler, `AckOk` not finalized              | That message and everything after it on the stream replays. |
| Out-of-order completions across positions             | Checkpoint stays at the contiguous prefix; everything past the gap replays. |
| `AckRetry` outstanding when the process dies          | Retry buffer is in-memory only. Message replays on restart and your handler decides again. |

## Idempotency: the contract

Because the adapter only guarantees at-least-once, your handler is
the place where exactly-once meaning lives. Practical patterns:

- Write events to your domain DB with the source `messageId` as a
  unique key; on conflict, treat as success.
- Use the `globalPosition` as a monotonic high-water mark in any
  read model you build — skip messages whose position is `<=` the
  last applied one.
- For non-DB side-effects (HTTP calls, emails), persist a
  "have-I-done-this-yet" record before the call so a retry after a
  crash can short-circuit.

## Verifying

The runnable
[`CheckpointRestart.hs`](../../shibuya-message-db-adapter-jitsurei/app/CheckpointRestart.hs)
example runs two adapter pipelines in one process under the same
`subscriptionName`, with a `shutdown` between them, to demonstrate
that phase 2 picks up exactly where phase 1 left off. Read it
alongside this page and run it locally to see the mechanics in
action.
