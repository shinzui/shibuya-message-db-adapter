# shibuya-message-db-adapter

Polling adapter for [message-db](https://github.com/message-db/message-db): converts `MessageDb.Message` to `Shibuya.Envelope` and streams messages into [Shibuya](https://github.com/shinzui/shibuya) handlers.

## Packages

- **`shibuya-message-db-adapter`** — the library. A Streamly-based poller that reads from `message_store` and hands envelopes off to Shibuya's queue-processing pipeline.
- **`shibuya-message-db-adapter-jitsurei`** — runnable examples (*jitsurei* = 実例, "worked examples"): basic consumer, retry, dead-letter, checkpoint-restart, multi-partition.

## Stack

Haskell · `effectful` · `streamly` · `hasql` · `message-db-hs` · `shibuya`

## Development

The repo ships a Nix flake + direnv + `process-compose` setup for a local Postgres with the message-db schema installed.

```sh
direnv allow                   # load the dev shell
just process-up                # start Postgres (foreground)
just bootstrap-message-db      # create DB + install message-db schema
just build                     # cabal build
just test                      # cabal test
```

`just` with no args lists every recipe (services, db seeding, build, fmt).

The message-db schema is installed from a local checkout; set `MESSAGE_DB_ROOT` if yours lives elsewhere than the default in the `Justfile`.

## Examples

Each jitsurei example has its own seed recipe:

```sh
just seed-jitsurei-basic
just seed-jitsurei-retry
just seed-jitsurei-dlq
just seed-jitsurei-checkpoint
just seed-jitsurei-partition
```

Then run the matching executable from `shibuya-message-db-adapter-jitsurei`.

## Status

Active development. See `docs/masterplans` and `docs/plans` for the roadmap and execution plans.
