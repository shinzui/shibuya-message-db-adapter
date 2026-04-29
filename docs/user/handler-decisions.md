# Handler Decisions

A handler receives an `Ingested es MessageDb.Message` and finishes by
calling `finalize` on its `AckHandle` with one of four `AckDecision`
values. The decision tells the adapter what to do next — it is *not*
just a "yes/no" reply.

```haskell
import Shibuya.Core.Ack
    ( AckDecision (..)
    , RetryDelay (..)
    , DeadLetterReason (..)
    , HaltReason (..)
    )
import Shibuya.Core.AckHandle (AckHandle (..))
import Shibuya.Core.Ingested (Ingested (..))

handle Ingested{ack = AckHandle finalize} = do
    ...
    finalize AckOk
```

You must call `finalize` exactly once per message. Failing to do so
leaves the message inflight forever — the contiguous-prefix
checkpoint cannot advance past it.

## `AckOk` — success

The message has been processed. The adapter records the outcome in
the inflight ledger; the contiguous-prefix checkpoint advances past
this position the next time the persister runs.

This is the boring path and what most messages should resolve to.

## `AckRetry !RetryDelay` — try again later

The handler hit a transient error (downstream service blip, optimistic
concurrency conflict, etc.) and wants the same message redelivered
after `RetryDelay` seconds.

```haskell
finalize (AckRetry (RetryDelay 2.0))
```

What actually happens:

1. The message is buffered in the in-process retry buffer.
2. After `RetryDelay`, a background fiber re-emits it through the
   adapter's source; your handler runs again.
3. The contiguous-prefix checkpoint does **not** advance past this
   message until it eventually resolves to `AckOk` or
   `AckDeadLetter`.

There is no per-message retry counter. If you need bounded retries,
keep a count in your own state (e.g. an `STM` map keyed on
`messageId`) and switch to `AckDeadLetter` once you have decided to
give up — see `RetryDemo.hs` for the pattern.

**Capacity:** the buffer is bounded by `maxRetryBufferSize`. A retry
that would overflow the buffer is *downgraded* to
`AckDeadLetter MaxRetriesExceeded`. This protects the poll loop from
one stuck handler back-pressuring the whole subscription.

## `AckDeadLetter !DeadLetterReason` — give up on this one

The message is unprocessable and processing of *the rest of the
stream* should continue. The adapter applies whatever
[`DlqStrategy`](configuration.md#dlqstrategy--dlqstrategy--default-dlqskipandlog)
you configured and advances the checkpoint past it.

Reasons (informational; the strategy is the same regardless):

- `PoisonPill !Text` — the message itself is broken in a way that
  makes it permanently unprocessable. Use this when you have
  positively identified the message as the problem.
- `InvalidPayload !Text` — the payload failed schema/parse validation.
  Use this for "we expected JSON shape X and got Y."
- `MaxRetriesExceeded` — your handler kept retrying past whatever
  bound it tracks, or the adapter's retry buffer overflowed and
  forced this. The adapter itself emits this when downgrading an
  `AckRetry` it cannot accept.

```haskell
finalize (AckDeadLetter (InvalidPayload "missing 'order_id'"))
```

## `AckHalt !HaltReason` — stop the world

The handler has decided that *no further message on this subscription
should be processed*. The adapter stops emitting, the source stream
ends, and the application is responsible for deciding what to do —
typically, log loudly and exit so an operator investigates.

Use this **only** when continuing would corrupt state — for example,
an ordered stream where you cannot skip ahead without breaking
downstream invariants.

```haskell
finalize (AckHalt (HaltOrderedStream "schema migration mid-stream"))
finalize (AckHalt (HaltFatal       "DB unique constraint violated; check logs"))
```

`AckHalt` is *not* "I am tired, please pause" — there is no resume.
On the next process start the adapter resumes from the last
checkpoint, which means the halting message will be redelivered. If
you cannot make progress on it, follow the halt with operator
intervention before restarting.

## Choosing between them

| Situation                                                         | Use this                          |
|-------------------------------------------------------------------|-----------------------------------|
| Processed cleanly                                                 | `AckOk`                           |
| Transient downstream error, will probably succeed soon            | `AckRetry`                        |
| The message itself is wrong; rest of the stream is fine           | `AckDeadLetter (InvalidPayload …)`|
| Schema drift, missing migration, unrecoverable                    | `AckHalt (HaltFatal …)`           |
| Ordered stream where skipping this message would corrupt state    | `AckHalt (HaltOrderedStream …)`   |

When in doubt: prefer `AckDeadLetter` over `AckHalt`. Halt is the
nuclear option and it stops the entire subscription, not just one
stream.
