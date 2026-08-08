---
okf_version: "0.2"
title: "Capabilities of shibuya-message-db-adapter"
---

# Capabilities of shibuya-message-db-adapter

This repository provides a [Shibuya](mori://shinzui/shibuya) adapter for
[message-db](mori://tan/message-db-hs), a Postgres-based event store. A consumer
depends on the `shibuya-message-db-adapter` library to poll a message-db
category and drive it through Shibuya handlers with durable, at-least-once
delivery, honored handler ack decisions, and optional consumer-group
partitioning.

Everything here is what the library does **today**. The package is at version
`0.1.0.0` with no tagged release, so every capability's `since` is `unreleased`
and every `stability` is `experimental` — the compatibility promise is uniform
because the whole surface is pre-1.0, not because the field went unconsidered.

## Capabilities

| Handle | Capability | Since | Stability | Requires |
|---|---|---|---|---|
| [CAP-1](category-polling.md) | Poll a message-db category into Shibuya envelopes | unreleased | experimental | — |
| [CAP-2](durable-checkpoint-resume.md) | At-least-once delivery with durable checkpoint resume | unreleased | experimental | CAP-1 |
| [CAP-3](handler-ack-decisions.md) | Retry, dead-letter, and halt handler decisions | unreleased | experimental | CAP-2 |
| [CAP-4](consumer-group-partitioning.md) | Consumer-group partitioning across cooperating processes | unreleased | experimental | CAP-2 |

## Deliberately excluded

- **Per-stream ordered dispatch (`PartitionedInOrder`)** — planned in
  `docs/plans/7-per-stream-ordered-dispatch.md` but unimplemented (no code, no
  evidence). An unbuilt feature is an improvement request, not a capability, so
  it has no record here.
- **Benchmarks** — planned in `docs/plans/6-benchmarks.md`; no benchmark suite
  exists in the repository, so there is nothing to cite as `benchmark` evidence.
- **The `shibuya-message-db-adapter-jitsurei` examples package** — it is an
  internal, runnable demonstration package, not something a downstream project
  depends on. Its executables appear here only as `example` evidence for the
  capabilities they demonstrate.
- **Consumer-facing features that require another repository to cooperate** —
  for example an end-to-end event-sourced workflow across Shibuya and a domain
  service — are use-case features owned by the consuming repository, not
  capabilities this adapter can assert or prove alone.
</content>
