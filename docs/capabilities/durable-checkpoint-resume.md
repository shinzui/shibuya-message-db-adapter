---
title: "At-least-once delivery with durable checkpoint resume"
type: Capability
description: "Persist a contiguous-prefix checkpoint to a durable store and resume from it after a crash or restart, so every message is delivered at least once even when handlers ack out of order."
generated:
  by: adopt-capabilities/0.9.2
  at: "2026-08-08T00:00:00Z"
capabilityId: CAP-2
provider: mori://shinzui/shibuya-message-db-adapter
status: shipped
stability: experimental
since: unreleased
packages:
  - shibuya-message-db-adapter
interface:
  - Shibuya.Adapter.MessageDb
  - Shibuya.Adapter.MessageDb.Config
requires:
  - CAP-1
evidence:
  - kind: test
    resource: shibuya-message-db-adapter/test/Shibuya/Adapter/MessageDb/InflightStateTest.hs
    proves: The contiguous-prefix ledger advances the checkpoint only across a gap-free run of completed positions and stops at the first pending or missing one.
  - kind: test
    resource: shibuya-message-db-adapter/test/Shibuya/Adapter/MessageDb/CheckpointResumeTest.hs
    proves: Two full start-drain-shutdown cycles over ephemeral Postgres show progress surviving restart and a pending AckRetry pinning the checkpoint at its predecessor.
  - kind: example
    resource: shibuya-message-db-adapter-jitsurei/app/CheckpointRestart.hs
    proves: A runnable program that consumes, shuts down, restarts, and resumes past the persisted checkpoint.
  - kind: guide
    resource: docs/user/checkpointing.md
    proves: Explains at-least-once delivery, checkpoint storage, and restart behavior for operators.
---

# At-least-once delivery with durable checkpoint resume

message-db tracks a subscription's progress with a single `GlobalPosition`, but
Shibuya handlers ack each message independently and may complete positions out
of order. This capability reconciles the two: an in-memory ledger
(`InflightState`) records every ingested position and its outcome, and
`advanceCheckpointTo` computes the longest gap-free run of completed positions
past the last saved checkpoint. A background persister flushes that
contiguous prefix to the durable checkpoint store (via
`message-db-checkpoint-store`) every `checkpointInterval`, and on shutdown the
adapter drains inflight work and does one final flush.

On start the adapter reads the persisted checkpoint for its subscription name
and resumes at `checkpoint + 1`. A crash therefore replays only the messages
that were inflight or unacked at the moment it died — delivery is **at least
once**.

## Shape

```haskell
messageDbAdapter (defaultConfig (CategoryStream "orders") "orders-demo")
-- checkpointInterval = 1 s, drainTimeout = 10 s by default;
-- both are MessageDbAdapterConfig fields.
```

Adopting this capability builds directly on the polling core in
[CAP-1](category-polling.md); the checkpoint accounting is what turns a plain
poll loop into a resumable subscription.

## Limits

- **At-least-once, not exactly-once.** A crash after a handler succeeds but
  before the next checkpoint flush redelivers those messages on restart.
  Handlers must be idempotent.
- **Checkpoint storage is an external Postgres table.** The capability requires
  a `checkpoints` table reachable through `message-db-checkpoint-store`; without
  it there is nothing durable to resume from.
- **Subscription names are the isolation key, and collisions corrupt progress.**
  Two adapters consuming the same category under the same `subscriptionName`
  overwrite each other's checkpoint row. This is an operational footgun with no
  runtime guard.
- **The process-local `ackedRef` high-watermark is diagnostic only** — it is not
  the durable position and must not be read as one.
- **Shutdown durability is bounded by `drainTimeout`.** If inflight work does
  not finalize within the timeout, shutdown proceeds and those positions are
  replayed on the next start rather than waited on indefinitely.
</content>
