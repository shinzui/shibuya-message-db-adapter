# Consumer Groups

A consumer group splits one logical category across N cooperating
adapter processes so that each message is handled by exactly one
member. Routing is purely a function of the message's *category
name* — there is no broker, no rebalance protocol, no inter-process
chatter. Membership is static configuration.

## When to use this

- One handler can no longer keep up with one category.
- You want to scale horizontally across machines or processes.
- You can tolerate a static membership model: changing the group
  size is a deploy, not a runtime event.

If your bottleneck is downstream of the handler (a slow DB, a slow
HTTP call), increasing `batchSize` or running concurrent work
*inside* one handler is usually a better first move.

## Routing

```
member_index = murmur3_64(category_name) mod groupSize
```

Each member polls the *same* category and runs every message it
fetches through this filter. Messages that do not belong to it are
skipped — but the member still ack-advances past them so its
checkpoint can move forward.

Two consequences:

- **Routing is per category-stream name, not per individual message.**
  All messages that share a `stream_name` (e.g. `orders-42`) land on
  the same member, which preserves per-stream ordering inside the
  group.
- **Every member sees every message** before filtering. Postgres
  read load scales with `groupSize`. The win is downstream: one
  member's handler sees only ~`1/groupSize` of the traffic.

A future "filter at the SQL level" optimization is possible but not
in scope today.

## Configuration

```haskell
import Shibuya.Adapter.MessageDb (ConsumerGroupConfig (..), MessageDbAdapterConfig (..))

cfg member =
    (defaultConfig (CategoryStream "orders") "orders")
        { consumerGroup =
            Just ConsumerGroupConfig
                { groupSize = 3
                , member    = member  -- 0, 1, or 2
                }
        }
```

Validation, applied at adapter construction:

- `groupSize >= 1` — a single-member "group" is valid (and equivalent
  to no group at all, modulo subscription naming).
- `0 <= member < groupSize` — out-of-range member indices throw
  `userError` immediately rather than silently dropping messages.

## Subscription names and checkpoints

Each member keeps its own checkpoint under a partition-scoped
subscription name: `<subscriptionName>-<member>`. The base name you
pass to `defaultConfig` is the *group* identifier; the suffix
distinguishes members.

This means:

- Members do not overwrite each other's checkpoints.
- Adding a fourth member to a previously 3-member group requires no
  data migration; member 3 starts from `globalPosition 0` and
  re-processes everything that hashes to its slot.
- Resizing the group (changing `groupSize`) re-shuffles the hash
  space. Existing checkpoints are still valid for "where each
  member left off," but every member is now responsible for a
  different subset of streams. **Most messages will replay** until
  each member has caught up. Plan resizes carefully.

## Deployment shape

Run `groupSize` separate processes (or pods, threads, etc.). Each
takes its `member` index from configuration. The processes do not
need to know about each other — they only need to agree on the
group size and pull from the same `message_store`.

A typical Kubernetes deployment uses a `StatefulSet` so each pod
gets a stable index it can pass to the adapter via env var.

## Limitations

- **Static membership.** There is no "join" or "leave" — change the
  configuration and redeploy.
- **Read-side fan-out.** Every member fetches every message. This is
  fine up to a few thousand messages per second; beyond that, look
  at the per-stream ordered-dispatch plan in `docs/plans/7`.
- **No automatic rebalance on failure.** If member 2 dies, its
  partition is unhandled until you bring it back. You can still
  scale by making each member highly available.

## See it in action

The runnable
[`MultiPartition.hs`](../../shibuya-message-db-adapter-jitsurei/app/MultiPartition.hs)
example spawns three adapters in one process against
`jitsurei-partition`, prints which member handled which message, and
asserts every message went to exactly one member. Seed it with
`just seed-jitsurei-partition` and run with
`cabal run multi-partition`.
