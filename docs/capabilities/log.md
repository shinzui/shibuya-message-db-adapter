# Capability catalog log

## 2026-08-08

Adopt the shared OKF capability profile. Authored the initial capability catalog
for `shibuya-message-db-adapter` under the shared `coordination/capabilities`
profile (okf-profiles v0.9.0). Derived four capabilities from source modules,
the tasty test-suite, the `jitsurei` runnable examples, the user guide, and the
EP-structured git history:

- CAP-1 — poll a message-db category into Shibuya envelopes (EP-1).
- CAP-2 — at-least-once delivery with durable checkpoint resume (EP-2).
- CAP-3 — retry, dead-letter, and halt handler decisions (EP-3).
- CAP-4 — consumer-group partitioning across cooperating processes (EP-4).

Registered the bundle in `mori.dhall` under `okfBundles` and wired
`okf validate` into the repository. All `since` values are `unreleased`: the
package is at `0.1.0.0` with no git tags. Per-stream ordered dispatch and
benchmarks were excluded as unimplemented improvement requests.
</content>
