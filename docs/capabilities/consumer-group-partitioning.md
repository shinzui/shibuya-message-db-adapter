---
title: "Consumer-group partitioning across cooperating processes"
type: Capability
description: "Split one category across N cooperating adapter processes by deterministic Murmur3-64 hash of the message category, each member keeping its own partition-scoped checkpoint with no inter-process coordination."
generated:
  by: adopt-capabilities/0.9.2
  at: "2026-08-08T00:00:00Z"
capabilityId: CAP-4
provider: mori://shinzui/shibuya-message-db-adapter
status: shipped
stability: experimental
since: unreleased
packages:
  - shibuya-message-db-adapter
interface:
  - Shibuya.Adapter.MessageDb.Config
requires:
  - CAP-2
evidence:
  - kind: test
    resource: shibuya-message-db-adapter/test/Shibuya/Adapter/MessageDb/PartitionTest.hs
    proves: categoryPartition matches message-db-subscription's private getPartitionMurmur for a pinned set of category names.
  - kind: test
    resource: shibuya-message-db-adapter/test/Shibuya/Adapter/MessageDb/InflightFilterTest.hs
    proves: A filtered-out position marked via recordFilteredCompleted does not block the contiguous-prefix checkpoint from advancing.
  - kind: test
    resource: shibuya-message-db-adapter/test/Shibuya/Adapter/MessageDb/ConsumerGroupTest.hs
    proves: Three members over one category of 30 messages produce exactly 30 non-duplicated deliveries routed by hash, with all three partition-scoped checkpoints at the final position.
  - kind: example
    resource: shibuya-message-db-adapter-jitsurei/app/MultiPartition.hs
    proves: A runnable program running multiple partitioned members over a shared category.
  - kind: guide
    resource: docs/user/consumer-groups.md
    proves: Explains splitting a category across N cooperating adapter processes via partition routing.
---

# Consumer-group partitioning across cooperating processes

A consumer group splits one logical category across `groupSize` cooperating
adapter processes, each addressed by a `member` index in `[0, groupSize)`.
Routing is deterministic: a message whose category hashes (Murmur3-64 over the
UTF-8 category name, modulo `groupSize`) to a member's index is processed by that
member; the others filter it and advance their checkpoint past it. Members share
nothing but configuration — there is no inter-process chatter — and each keeps
its own checkpoint under a partition-scoped subscription name
(`<base>-<member>-of-<groupSize>`). Belonging messages carry a
`<member>-of-<groupSize>` partition label on their envelope.

## Shape

```haskell
defaultConfig cat sub
  { consumerGroup = Just (ConsumerGroupConfig { groupSize = 3, member = 0 }) }
-- run three processes with member = 0, 1, 2
```

This builds on the checkpoint ledger from
[CAP-2](durable-checkpoint-resume.md): filtered-out positions are recorded as
immediately-completed so a non-belonging position never stalls a member's
contiguous prefix.

## Limits

- **Routing is client-side filtering — every member reads every message.** The
  poll query sets `consumerGroupMember`/`consumerGroupSize` to `Nothing`, so each
  member fetches the full category and discards positions it does not own.
  Database read load scales with `groupSize`, not with per-member throughput;
  this is a real cost at high volume, not just an implementation detail.
- **Coordination is configuration-only, and misconfiguration fails silently at
  the routing layer.** If two processes claim the same `member`, or the
  `groupSize` values disagree across processes, no runtime check catches it —
  messages are silently double-processed or dropped from the group's coverage.
  Only per-process `groupSize`/`member` bounds are validated at startup.
- **Correct routing depends on matching an upstream hash that is not imported.**
  `categoryPartition` must stay bit-compatible with `message-db-subscription`'s
  private `getPartitionMurmur`; `PartitionTest` re-implements the computation to
  catch divergence, but a change to the upstream hash would split partitions in
  production and is caught only by that pinned test.
- **Partitioning is by category, not by stream.** All messages in one category
  route to one member; this does not provide per-stream ordering, which is
  unimplemented (see the "Deliberately excluded" section of the catalog index).
</content>
