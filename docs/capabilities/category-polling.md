---
title: "Poll a message-db category into Shibuya envelopes"
type: Capability
description: "Continuously poll one message-db category and stream each message to a Shibuya handler as a typed Envelope, with W3C trace context carried across."
generated:
  by: adopt-capabilities/0.9.2
  at: "2026-08-08T00:00:00Z"
capabilityId: CAP-1
provider: mori://shinzui/shibuya-message-db-adapter
status: shipped
stability: experimental
since: unreleased
packages:
  - shibuya-message-db-adapter
interface:
  - Shibuya.Adapter.MessageDb
  - Shibuya.Adapter.MessageDb.Config
  - Shibuya.Adapter.MessageDb.Convert
evidence:
  - kind: test
    resource: shibuya-message-db-adapter/test/Shibuya/Adapter/MessageDb/ConvertTest.hs
    proves: The pure Message -> Envelope mapping — id, cursor, enqueue time, and W3C traceparent/tracestate extraction — is exercised without Postgres.
  - kind: test
    resource: shibuya-message-db-adapter/test/Shibuya/Adapter/MessageDb/BasicProduceConsumeTest.hs
    proves: Ten messages seeded into one category are delivered to an AckOk handler in global-position order before the stream terminates, over the full adapter + message-db stack.
  - kind: example
    resource: shibuya-message-db-adapter-jitsurei/app/BasicConsumer.hs
    proves: A runnable program that wires the adapter into a Shibuya app and drains a category end to end.
  - kind: guide
    resource: docs/user/getting-started.md
    proves: Step-by-step walkthrough of wiring the adapter into a Haskell program that consumes one category.
---

# Poll a message-db category into Shibuya envelopes

The base capability a consumer adopts: build an adapter for a single message-db
category and hand it to a Shibuya app. The adapter polls
[message-db](https://github.com/topagentnetwork/message-db-hs)'s
`get_category_messages` on an interval, converts each `MessageDb.Message` to a
`Shibuya.Core.Types.Envelope`, and streams the envelopes into the handler your
processor defines. The message itself rides along as the envelope `payload`, so
handlers decode the `data` field into a domain type on their own terms.

## Shape

```haskell
import Shibuya.Adapter.MessageDb (messageDbAdapter, defaultConfig, CategoryStream (..))
import MessageDb.Effectful (runMessageDb)
import MessageDb.CheckpointStore.Effectful (runPostgresCheckointStore)

adapter <- messageDbAdapter (defaultConfig (CategoryStream "orders") "orders-demo")
-- hand `adapter` to Shibuya.App.runApp ...
```

`defaultConfig` seeds a 100-message batch size and a 500 ms poll interval; both
are `MessageDbAdapterConfig` fields you can override.

During conversion the adapter extracts W3C `traceparent`/`tracestate` from the
message metadata JSON when present, so downstream telemetry stays linked to the
producing trace without handler involvement.

## Limits

- **Evidence is weaker than the message-body claim.** `BasicProduceConsumeTest`
  proves ordered delivery; it does not, on its own, prove durability across a
  restart — that is [CAP-2](durable-checkpoint-resume.md).
- **The envelope is deliberately sparse.** `attempt` is `Nothing` (message-db
  has no native redelivery counter), `partition` is `Nothing` at this layer
  (set only under [CAP-4](consumer-group-partitioning.md)), and `attributes` is
  empty (message-db contributes no broker-specific OTel attributes). Handlers
  that expect a redelivery count from the transport will not find one here.
- **Category names containing `-` are rejected at startup.** message-db reserves
  `-` as the entity separator; `parseCategoryStream` fails loudly rather than
  polling an unparseable name.
- **The idle poll is a fixed sleep, not a notification.** When a poll returns
  nothing the loop sleeps for `pollInterval`; there is no `LISTEN/NOTIFY`
  wake-up, so end-to-end latency on a quiet category is bounded below by that
  interval.
- **`since` is `unreleased`:** the package is at version `0.1.0.0` with no
  tagged release, so no capability in this catalog can cite a release version.
</content>
</invoke>
