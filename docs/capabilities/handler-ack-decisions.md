---
title: "Retry, dead-letter, and halt handler decisions"
type: Capability
description: "Honor a handler's AckRetry, AckDeadLetter, and AckHalt decisions with an in-process retry buffer, an in-DB dead-letter stream, and a checkpoint-pinning halt — on a substrate that has none of these natively."
generated:
  by: adopt-capabilities/0.9.2
  at: "2026-08-08T00:00:00Z"
capabilityId: CAP-3
provider: mori://shinzui/shibuya-message-db-adapter
status: shipped
stability: experimental
since: unreleased
packages:
  - shibuya-message-db-adapter
interface:
  - Shibuya.Adapter.MessageDb.Config
  - Shibuya.Core.Ack
requires:
  - CAP-2
evidence:
  - kind: test
    resource: shibuya-message-db-adapter/test/Shibuya/Adapter/MessageDb/RetryBufferTest.hs
    proves: The bounded retry buffer enqueues, drains FIFO, and refuses at capacity so overflow downgrades to dead-letter rather than blocking the poll loop.
  - kind: test
    resource: shibuya-message-db-adapter/test/Shibuya/Adapter/MessageDb/DlqTest.hs
    proves: DLQ message-id derivation is deterministic (UUIDv5) and DLQ metadata carries correlation, causation, original stream, and reason.
  - kind: test
    resource: shibuya-message-db-adapter/test/Shibuya/Adapter/MessageDb/RetryDlqHaltResumeTest.hs
    proves: Over ephemeral Postgres, retry-then-ok reaches checkpoint 5, dead-letter-to-stream writes exactly one correlated DLQ message, and halt pins the checkpoint at the predecessor and redelivers on restart.
  - kind: test
    resource: shibuya-message-db-adapter/test/Shibuya/Adapter/MessageDb/DeadLetterSkipAndLogTest.hs
    proves: The default DlqSkipAndLog strategy advances past a dead-lettered message and writes no durable DLQ row.
  - kind: example
    resource: shibuya-message-db-adapter-jitsurei/app/RetryDemo.hs
    proves: A runnable program that retries a failing message and then succeeds on redelivery.
  - kind: example
    resource: shibuya-message-db-adapter-jitsurei/app/DeadLetterDemo.hs
    proves: A runnable program that dead-letters a poison message to a named stream.
  - kind: guide
    resource: docs/user/handler-decisions.md
    proves: Documents AckOk / AckRetry / AckDeadLetter / AckHalt semantics as the adapter interprets them.
---

# Retry, dead-letter, and halt handler decisions

message-db is a plain event log: it has no visibility timeout, no redelivery
counter, and no dead-letter primitive. This capability implements the three
non-trivial `AckDecision` outcomes in the adapter itself, so a handler can
return them uniformly regardless of transport.

- **`AckRetry delay`** schedules the message in a bounded in-process buffer; a
  background fiber waits out the delay and re-injects it into the poll stream.
  When the buffer is at capacity the decision downgrades to
  `AckDeadLetter MaxRetriesExceeded` so one slow position never back-pressures
  the whole subscription.
- **`AckDeadLetter reason`** runs the configured `DlqStrategy`: `DlqSkipAndLog`
  logs and advances, while `DlqWriteToStream` writes a copy to a named stream
  with a deterministic UUIDv5 id (so a crash between the DB write and the
  checkpoint advance replays safely — message-db rejects the duplicate and the
  adapter treats that as success).
- **`AckHalt reason`** flips the shutdown signal and deliberately records no ack
  outcome, so the contiguous-prefix checkpoint stops at `haltedPos - 1` and the
  halted message is redelivered on restart.

These outcomes are only meaningful because the checkpoint ledger in
[CAP-2](durable-checkpoint-resume.md) tracks per-position state — retry pins a
position, halt withholds one, dead-letter completes one.

## Shape

```haskell
defaultConfig cat sub
  { dlqStrategy = DlqWriteToStream (Stream "orders:dlq")
  , maxRetryBufferSize = MaxRetryBufferSize 1000
  }
-- handler returns AckRetry / AckDeadLetter / AckHalt from Shibuya.Core.Ack
```

## Limits

- **Retries reorder relative to freshly-polled messages** because the retry
  fiber and poll fiber merge asynchronously. This is safe for `Unordered`
  processors only; for `StrictInOrder` processors set `maxRetryBufferSize = 0`
  (converting every retry to an immediate dead-letter) or avoid `AckRetry`.
- **The retry buffer is in-process and volatile.** Retries still buffered at
  shutdown are dropped; their positions remain unacked in the ledger, so they
  are re-polled from the checkpoint on the next start rather than lost — but the
  in-memory delay state does not survive a restart.
- **Shutdown can lag by up to one retry delay.** The retry fiber sleeps with an
  uninterruptible `threadDelay`, so a shutdown mid-sleep waits out the remaining
  delay before the fiber exits.
- **A non-duplicate DLQ write failure is logged and dropped, not retried.**
  Dropping a DLQ copy under duress is preferred over stalling the subscription,
  so `DlqWriteToStream` is best-effort, not guaranteed.
- **No per-message attempt count is surfaced.** The envelope's `attempt` stays
  `Nothing`; a handler cannot see how many times a message has been retried.
- **`DlqCustom` (a caller-supplied routing callback) is a declared non-goal.**
  Custom routing requires wrapping the adapter in your own layer.
</content>
