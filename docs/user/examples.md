# Runnable Examples (Jitsurei)

The `shibuya-message-db-adapter-jitsurei` package
(*jitsurei* = 実例, "worked examples") ships five end-to-end programs.
Each one is small, self-contained, and exists to make one feature of
the adapter visible.

All examples expect:

- The dev Postgres up (`just process-up` in another terminal).
- The schema installed (`just bootstrap-message-db`, idempotent).
- `PG_CONNECTION_STRING` exported in your shell — the `direnv` setup
  in this repo already does this.

## 1. Basic consumer

**File:** `app/BasicConsumer.hs` — **Run:** `cabal run basic-consumer`

The simplest possible adapter wiring: open a pool, run the effect
stack, build the adapter from `defaultConfig`, drain its source, and
`AckOk` every message. Prints `stream / type / globalPosition` for
each one.

Seed: `just seed-jitsurei-basic` (three messages on category
`jitsurei-basic`).

Read it first if you are new to the adapter — it is the template
every other example builds on.

## 2. Retry

**File:** `app/RetryDemo.hs` — **Run:** `cabal run retry-demo`

Demonstrates the retry buffer. The handler keeps a per-message
delivery count in a `TVar`; on the first delivery it returns
`AckRetry (RetryDelay 2)`, and on the second `AckOk`.

You will see each seeded message printed twice, ~2 s apart. The
adapter's contiguous-prefix checkpoint does not advance past a
message until it ack-oks (or dead-letters), so the retry mechanism
is fully transparent to the durability story.

Seed: `just seed-jitsurei-retry`.

## 3. Dead-letter

**File:** `app/DeadLetterDemo.hs` — **Run:** `cabal run dead-letter-demo`

Configures the adapter with `dlqStrategy = DlqWriteToStream "demo-dlq"`
and dead-letters every message whose `messageType` starts with `"Bad"`.
After running, query psql to confirm the DLQ stream:

```sql
SELECT stream_name, type FROM message_store.messages
WHERE stream_name = 'demo-dlq';
```

You should see one row for the `BadFormat` message that was
dead-lettered. The DLQ message id is derived deterministically from
the original via UUIDv5, so a crash between the DB write and the
checkpoint advance is safely retryable.

Seed: `just seed-jitsurei-dlq` (three messages, alternating
`OrderPlaced` and `BadFormat`).

## 4. Checkpoint restart

**File:** `app/CheckpointRestart.hs` — **Run:** `cabal run checkpoint-restart`

Runs *two* sequential pipelines in the same process under the same
`subscriptionName`:

- Phase 1 takes the first 5 messages, ack-oks each, and calls
  `adapter.shutdown`. The shutdown drains and flushes a final
  checkpoint.
- Phase 2 builds a brand new adapter and resumes from the persisted
  checkpoint, consuming positions 6..10.

This is the visible proof that the durable checkpoint behaves as
[checkpointing.md](checkpointing.md) describes — phase 2 sees only
positions 6..10, not 1..10.

Seed: `just seed-jitsurei-checkpoint` (10 messages).

## 5. Multi-partition (consumer group)

**File:** `app/MultiPartition.hs` — **Run:** `cabal run multi-partition`

Spawns three adapters in one process, all bound to the
`jitsurei-partition` category, with `groupSize = 3` and member
indices 0, 1, 2. The example seeds 30 messages; routing hashes the
*category name*, so every message lands on exactly one member.
Members whose partition is empty still drive the poll loop and
advance their partition-scoped checkpoint.

The summary at the end prints a per-member message count and a
duplicate count (which should be zero — exactly-once routing within
the group).

Seed: `just seed-jitsurei-partition` (30 messages spread across six
sub-categories).

## Reading order

If you are evaluating the library, read in numeric order — each
example builds on the previous one's machinery without re-explaining
it.

If you are looking for a template to copy into your own project,
start with `BasicConsumer.hs` and add features (retry, DLQ, group)
as you need them. Most production wiring ends up looking like a
union of the demos.
