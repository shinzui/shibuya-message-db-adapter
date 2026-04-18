# Release metadata and Hackage prep

MasterPlan: docs/masterplans/1-shibuya-message-db-adapter.md

Intention: intention_01kpgme50se0ranxp41ghfhajf

This ExecPlan is a living document. The sections Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.

This document is maintained in accordance with `.claude/skills/exec-plan/PLANS.md`.


## Purpose / Big Picture

After this plan is complete, the `shibuya-message-db-adapter` repository is ready for
public consumption on Hackage. The repository acquires the release scaffolding any
Hackage package needs: a top-level `LICENSE` (MIT), a user-facing `README.md`, a
`CHANGELOG.md` describing the `0.1.0.0` release, `.cabal` stanzas carrying real
`synopsis`, `description`, `homepage`, `maintainer`, `copyright`, and
`source-repository head` metadata on every package, and Haddock comments on every
exposed module and every exported function in the main library.

Observable outcome. From a `nix develop` shell at the repo root, three commands
succeed: `cabal check` exits 0 inside each package directory; `cabal haddock all`
produces HTML with no `-Wmissing-docs` warnings on the main library; `cabal sdist
all` creates source tarballs under `dist-newstyle/sdist/` for every published
package. Tarballs are not uploaded to Hackage as part of this plan; upload is a
separate human-driven decision.

This is the final ExecPlan in the eight-plan initiative described by
`docs/masterplans/1-shibuya-message-db-adapter.md`. It hard-depends on EP-7
(`docs/plans/7-per-stream-ordered-dispatch.md`) — the last feature plan — because
the release CHANGELOG, README, and Haddock must describe the per-stream dispatch
feature alongside everything else. It soft-depends on EP-2 through EP-6: each adds
modules, fields, examples, or a separate package that must be documented in the
release. If any soft-dep plan has not landed when EP-8 is implemented, its
documentation contribution is omitted and a follow-up release prep is needed when
that plan lands.

The intent is for EP-8 to be the **last thing done before tagging a release**.
Running it before all the feature plans land is permitted but defeats its purpose:
the resulting tarball ships an incomplete README and CHANGELOG.


## Progress

Use a checklist to summarize granular steps. Every stopping point must be documented
here, even if it requires splitting a partially completed task into two ("done" vs.
"remaining"). This section must always reflect the actual current state of the work.

### Milestone 1: LICENSE, README, CHANGELOG

- [ ] Write `LICENSE` at the repo root with the MIT text and the correct copyright year
      and holder.
- [ ] Write `README.md` at the repo root matching the structure of
      `/Users/shinzui/Keikaku/bokuno/shibuya-project/shibuya-kafka-adapter/README.md`.
- [ ] Write `CHANGELOG.md` at the repo root describing the `0.1.0.0` release.
- [ ] Cross-check the README's quick-start against the demo from EP-1 and the jitsurei
      BasicConsumer from EP-5 (if EP-5 has landed) to ensure it compiles mentally.
- [ ] Insert the per-stream ordering README section verbatim from EP-7's
      *Documentation Fragment* section.

### Milestone 2: .cabal polish

- [ ] Fill in `synopsis`, `description`, `category`, `author`, `maintainer`,
      `copyright`, `homepage`, `bug-reports`, `license`, `license-file`,
      `extra-doc-files`, and `source-repository head` for
      `shibuya-message-db-adapter/shibuya-message-db-adapter.cabal`.
- [ ] Do the same for
      `shibuya-message-db-adapter-jitsurei/shibuya-message-db-adapter-jitsurei.cabal`
      if that package exists (EP-5).
- [ ] Do the same for
      `shibuya-message-db-adapter-bench/shibuya-message-db-adapter-bench.cabal`
      (EP-6).
- [ ] Set `version: 0.1.0.0` on every `.cabal` file.
- [ ] Run `cabal check` in each package directory and fix any warnings.

### Milestone 3: Haddock sweep

- [ ] Add a substantial module-level Haddock header to
      `src/Shibuya/Adapter/MessageDb.hs` describing the adapter's model (what
      `CategoryStream` is, how polling interacts with acks, how checkpoints survive
      restarts, what the consumer-group partition does, what per-stream ordering
      provides).
- [ ] Add a module-level Haddock header to every exposed module
      (`.Config`, `.Convert`, `.Internal`, and any helper modules added by
      EP-2/3/4/7).
- [ ] Add a function-level Haddock comment to every exported function, type, and
      constructor in the exposed modules.
- [ ] Turn on `-Wmissing-docs` for the main library in the cabal file (scoped to the
      library only, not tests or bench).
- [ ] Run `cabal haddock all` and verify zero warnings on the main library; verify
      HTML is produced at
      `dist-newstyle/build/<arch>/<ghc>/shibuya-message-db-adapter-0.1.0.0/doc/html/`.

### Milestone 4: Release dry-run

- [ ] Run `cabal check` in every package and fix remaining issues.
- [ ] Run `cabal sdist all` and verify tarballs appear under `dist-newstyle/sdist/`.
- [ ] Open one tarball with `tar -tzf` and confirm contents include cabal file,
      LICENSE, README, CHANGELOG, and source tree.
- [ ] Do **not** upload to Hackage. Record the tarball paths in the transcript for
      posterity.


## Surprises & Discoveries

(None yet.)


## Decision Log

- Decision: The plan performs a `cabal sdist` dry run but does **not** upload to
  Hackage.
  Rationale: Hackage upload is a release act with real side effects (irreversible
  publication of a package version). EP-8's job is to get the repository ready for
  upload, not to perform the upload. Uploading is a separate, human-driven decision.
  Date: 2026-04-18

- Decision: `-Wmissing-docs` is enabled only for the main library, not for the
  jitsurei examples, test suite, or bench package.
  Rationale: Executables and test code are read as source, not as generated docs. Their
  internals do not need Haddock coverage. The main library is what Hackage readers see
  and is the public surface area where missing docs actually hurt.
  Date: 2026-04-18

- Decision: Release metadata is its own ExecPlan (EP-8), separate from EP-6's
  benchmarks.
  Rationale: Benchmarks are a continuous regression-tracking concern that lands as
  soon as the main library exists; release metadata is a single end-of-line act that
  must describe every shipped feature. Bundling them forced one of two bad choices —
  either run benchmarks late so the release notes could mention them, or ship release
  metadata that ignored later-landing features. Splitting lets benchmarks land early
  (right after EP-1) and lets release metadata describe the complete feature set
  including benchmarks themselves.
  Date: 2026-04-18

- Decision: EP-8 hard-depends on EP-7 only, with soft-deps on EP-2 through EP-6.
  Rationale: EP-7 is the last *feature* plan. Its landing is the natural signal that
  the codebase is feature-complete for `0.1.0.0`. Soft-depending on the rest means
  EP-8 can technically run earlier, but the resulting release would be incomplete —
  the soft-dep label communicates "wait for these in practice, but the build won't
  break if you don't."
  Date: 2026-04-18


## Outcomes & Retrospective

(To be filled during and after implementation.)


## Context and Orientation

The reader knows Haskell and Cabal but need not have seen this repository. What
follows is enough to pick up EP-8 cold.

**What the adapter is.** `shibuya-message-db-adapter` plugs a `MessageDb.Message`
source into Shibuya's queue-processing framework. A user's application defines a
`Handler es MessageDb.Message`, passes it and `messageDbAdapter cfg` to
`Shibuya.App.runApp`, and Shibuya runs a supervised polling loop that reads messages
from a message-db category, wraps each in a `Shibuya.Core.Types.Envelope`, dispatches
to the user's handler, and acks the result. "message-db" is the PostgreSQL-backed
event store at `/Users/shinzui/Keikaku/hub/event-sourcing/message-db-project/message-db/`,
accessed via `message-db-hs` and `message-db-effectful`. A "CategoryStream" is the
prefix of a stream name before the first hyphen (e.g., `orders` in `orders-123`).

**Repository layout at the start of EP-8.** Assuming EP-1 through EP-7 are complete,
the repository at `/Users/shinzui/Keikaku/work/libraries/haskell/shibuya-message-db-adapter`
contains `cabal.project`, `flake.nix`, `flake.lock`, `process-compose.yaml`,
`Justfile`, `mori.dhall`, `treefmt.nix`, `db/`, `docs/masterplans/`, `docs/plans/1-*`
through `docs/plans/8-release-metadata.md` (this file),
`shibuya-message-db-adapter/` (library sources, tests, `app/Demo.hs`),
`shibuya-message-db-adapter-jitsurei/` (examples + integration tests, from EP-5),
and `shibuya-message-db-adapter-bench/` (benchmarks, from EP-6). If any of EP-5,
EP-6, or EP-7 has not landed, the corresponding documentation contributions are
omitted; the README will reference fewer examples and the CHANGELOG will list
fewer features.

**Why release metadata matters.** Hackage's readers see the README rendered on the
package landing page, the synopsis on the search results page, and Haddock-generated
HTML for every exposed module. Without those, a package looks abandoned and is
unusable. Cabal's own `cabal check` enforces a minimum bar for publishable packages
(synopsis, license, version, source-repository); failing those checks blocks
upload. This plan brings the repo over that bar.


## Plan of Work

### Milestone 1: LICENSE, README, CHANGELOG

At the end of this milestone, the repository root contains `LICENSE`, `README.md`,
and `CHANGELOG.md`.

Write `LICENSE` with the standard MIT template, matching
`/Users/shinzui/Keikaku/bokuno/shibuya-project/shibuya-kafka-adapter/LICENSE` with
`Copyright (c) 2026 Nadeem Bitar` on line 3.

Write `README.md` matching the structure of
`/Users/shinzui/Keikaku/bokuno/shibuya-project/shibuya-kafka-adapter/README.md`. The
sections, in order:

- **Title and one-sentence pitch**: "message-db adapter for the Shibuya
  queue-processing framework. Integrates with the PostgreSQL-backed event store via
  `message-db-effectful` for polling and `message-db-checkpoint-store` for durable
  subscription checkpoints; provides retry, dead-letter, consumer-group partitioning,
  per-stream ordered dispatch, and graceful shutdown."
- **Packages**: bullet list of `shibuya-message-db-adapter`,
  `shibuya-message-db-adapter-jitsurei`, `shibuya-message-db-adapter-bench`.
- **Installation**: add `shibuya-message-db-adapter` to `build-depends`; note the
  runtime dependency on a message-db-schema-installed Postgres at
  `/Users/shinzui/Keikaku/hub/event-sourcing/message-db-project/message-db/database/`.
- **Quick-start**: the complete demo from EP-1 (`app/Demo.hs`) as a four-space-indented
  block. Must be runnable as-written against a seeded database.
- **Configuration reference**: a table of `MessageDbAdapterConfig` fields (type,
  default, meaning) covering `category`, `batchSize`, `pollInterval`, `drainTimeout`,
  `subscriptionName`, `checkpointInterval`, `dlqStrategy`, `consumerGroup`,
  `streamOrdering`.
- **Ack semantics**: table mapping each `AckDecision` to adapter behavior. `AckOk`
  advances the checkpoint when contiguous; `AckRetry d` schedules in-process
  re-delivery after delay `d`; `AckDeadLetter r` runs the configured DLQ strategy then
  advances the checkpoint; `AckHalt r` stops the adapter and leaves the checkpoint as
  it was.
- **DLQ strategies**: `DlqSkipAndLog` (drop with structured log; default) and
  `DlqWriteToStream Stream` (write a `DeadLettered`-type message into the given
  message-db stream). If EP-3 shipped a `DlqCustom` variant, mention and point to its
  Haddock.
- **Consumer group note**: when `consumerGroup` is set, the adapter's subscription
  name is suffixed with the partition number and messages are filtered by Murmur3-64
  hash of the category, consistent with `message-db-subscription`'s Consumer
  partitioning.
- **Per-stream ordering**: insert verbatim the *Documentation Fragment* section from
  `docs/plans/7-per-stream-ordered-dispatch.md`. EP-7 wrote that fragment expecting
  EP-8 to copy it here.
- **Development workflow**: `direnv allow && nix develop`, then `just process-up`,
  then `cabal test` / `cabal bench` / `cabal run <example>`. Reference
  `mori show --full` for dependency inspection.
- **License**: "MIT. See `LICENSE` for details."

Keep the README concise. The detailed developer guide lives in the plan files; the
README is the Hackage landing page.

Write `CHANGELOG.md` at the repo root with two sections. First is an `## Unreleased`
header with an empty body (Shibuya convention, per
`/Users/shinzui/Keikaku/bokuno/shibuya-project/shibuya/CHANGELOG.md`). Second is
`## 0.1.0.0 — <release-date>` with an `### Added` body grouping features by
ExecPlan: core adapter and conversion (EP-1), durable checkpoints and InflightState
(EP-2), retry / DLQ / halt (EP-3), consumer-group partitioning (EP-4), examples and
integration tests (EP-5), benchmarks (EP-6), per-stream ordered dispatch and
`PartitionedInOrder` contract enforcement (EP-7), release metadata (EP-8 — this
plan).

### Milestone 2: .cabal polish

At the end of this milestone, every `.cabal` file in the repository carries uniform
release metadata: `version: 0.1.0.0`, matching `author`, `maintainer`, `copyright`,
`license: MIT`, `license-file: LICENSE`, `homepage` pointing at the GitHub repo,
`bug-reports` pointing at issues, and a `source-repository head` stanza. Every file
`cabal check`s cleanly.

The fields to fill in on
`shibuya-message-db-adapter/shibuya-message-db-adapter.cabal`:

- `synopsis:` "message-db adapter for the Shibuya queue processing framework"
- `description:` two to three sentences describing polling, checkpointing, retry/DLQ,
  consumer groups, per-stream ordering, and graceful shutdown.
- `category:` `Concurrency, Streaming, Database`
- `author:` `Nadeem Bitar`
- `maintainer:` `nadeem@gmail.com`
- `copyright:` `2026 Nadeem Bitar`
- `homepage:` `https://github.com/shinzui/shibuya-message-db-adapter`
- `bug-reports:` `https://github.com/shinzui/shibuya-message-db-adapter/issues`
- `license:` `MIT`
- `license-file:` `LICENSE`
- `extra-doc-files:` `CHANGELOG.md README.md`
- `source-repository head` block with `type: git` and
  `location: https://github.com/shinzui/shibuya-message-db-adapter.git`.

Do the same on
`shibuya-message-db-adapter-jitsurei/shibuya-message-db-adapter-jitsurei.cabal`
(synopsis: "Runnable examples for shibuya-message-db-adapter") and
`shibuya-message-db-adapter-bench/shibuya-message-db-adapter-bench.cabal` (synopsis:
"Benchmarks for shibuya-message-db-adapter conversion and ack-accounting functions").

Note: because the main library, jitsurei, and bench packages all live at the repository
root, each package's `LICENSE` reference points to its own directory. Place a copy of
`LICENSE` at the repo root (per Milestone 1) and symlinks or copies inside each
package directory, and have each cabal's `license-file:` point to the package-local
`LICENSE`.

For each package, run `cabal check` from inside the package directory. Expected
output: `The package is fine.` (or, on older cabals, no warnings). Fix any warnings —
common ones are missing `extra-doc-files:`, non-Canonical `version:`, and unversioned
dependencies.

Finally, set `version: 0.1.0.0` uniformly. Search all three `.cabal` files for the
`version:` line and set them together to avoid drift.

### Milestone 3: Haddock sweep

At the end of this milestone, `cabal haddock all` completes with zero
`-Wmissing-docs` warnings on the main library and produces HTML documentation.

For every exposed module, add a file-top comment of the form

    -- |
    -- Module      : Shibuya.Adapter.MessageDb.Convert
    -- Description : Conversion from MessageDb.Message to Shibuya.Envelope.
    --
    -- Short prose explaining what this module does and a pointer to the
    -- adapter-level overview in Shibuya.Adapter.MessageDb.

before the `module` keyword, and a one- to three-sentence Haddock comment on every
identifier in the module's explicit export list.

The root `Shibuya.Adapter.MessageDb` module's header is the adapter's front-door
documentation. It must cover:

- What `messageDbAdapter` plugs into (Shibuya's `Adapter` record) and what it produces
  (`Ingested es MessageDb.Message` on a Streamly stream).
- The polling model: `batchSize` messages per call via `getCategoryMessages`,
  starting from the last persisted checkpoint, sleeping `pollInterval` on empty
  batches.
- The ack contract: `AckOk` marks the message complete; `InflightState` advances the
  checkpoint only through contiguous `AckOk | AckDeadLetter`; `AckRetry` leaves a
  hole until the retry resolves.
- The DLQ story: `DlqSkipAndLog` drops-with-log; `DlqWriteToStream` produces a
  dead-letter event back into message-db. Both advance the checkpoint.
- The consumer-group story: Murmur3-64 of the category mod `groupSize` determines
  which partition processes each message.
- The per-stream ordering story: `PerStreamInOrder` mode keeps at most one in-flight
  message per `MessageDb.Stream`, fulfilling `shibuya-core`'s `PartitionedInOrder`
  contract; cross-stream concurrency is bounded by Shibuya's `Async N`.
- The shutdown semantics: `shutdown` drains inflight work up to `drainTimeout`,
  persists the final checkpoint, exits cleanly.

For each of the modules below, add function-level Haddock to every exported binding:

- `Shibuya.Adapter.MessageDb`: `messageDbAdapter`.
- `Shibuya.Adapter.MessageDb.Config`: `MessageDbAdapterConfig` and each field, every
  newtype (`CategoryStream`, `BatchSize`, `PollInterval`, `DrainTimeout`,
  `SubscriptionName`, `DlqStrategy`, `ConsumerGroupConfig`, `StreamOrderingMode`,
  `PerStreamConfig`), `defaultConfig`, `defaultPerStreamConfig`.
- `Shibuya.Adapter.MessageDb.Convert`: `messageToEnvelope`, `extractTraceContext`.
- `Shibuya.Adapter.MessageDb.Internal`: `messageDbSource`, `InflightState`,
  `newInflightState`, `recordIngested`, `recordAckResult`, `advanceCheckpointTo`,
  and any partition helpers added by EP-4.
- `Shibuya.Adapter.MessageDb.Internal.PerStreamDispatch` (EP-7): the module-level
  Haddock written by EP-7 should already be present; verify and extend if needed.

The rule: if a function or type is in an `exposed-modules` module's explicit export
list, it must have at least one Haddock comment line. If it is internal detail, remove
it from the export list.

Enable `-Wmissing-docs` in the library stanza's `ghc-options` (only the library; not
tests, bench, or executables). Run `cabal haddock all`; expected trailing output:

    Documentation created:
    .../doc/html/shibuya-message-db-adapter/index.html
    .../doc/html/shibuya-message-db-adapter-jitsurei/index.html
    .../doc/html/shibuya-message-db-adapter-bench/index.html

Fix every `-Wmissing-docs` warning. Errors (not warnings) indicate compile failure;
the module must compile before docs can be produced.

### Milestone 4: Release dry-run

Run `cabal check` in every package directory. Expected: `The package is fine.`

Run `cabal sdist all` from the repo root. Expected output:

    Wrote tarball sdist to .../dist-newstyle/sdist/shibuya-message-db-adapter-0.1.0.0.tar.gz
    Wrote tarball sdist to .../dist-newstyle/sdist/shibuya-message-db-adapter-jitsurei-0.1.0.0.tar.gz
    Wrote tarball sdist to .../dist-newstyle/sdist/shibuya-message-db-adapter-bench-0.1.0.0.tar.gz

The tarballs must open (`tar -tzf <path> | head`) and include the cabal file, the
sources, LICENSE, README, and CHANGELOG.

Do not run `cabal upload` or `cabal upload --publish`. Uploading is a separate human
decision.


## Concrete Steps

Run all commands from
`/Users/shinzui/Keikaku/work/libraries/haskell/shibuya-message-db-adapter` inside a
`nix develop` shell.

Write `LICENSE`, `README.md`, `CHANGELOG.md` at the repo root (Milestone 1).

Fill in cabal metadata on all three packages (Milestone 2) and verify:

    cabal check

run inside each package directory. Expected: `The package is fine.` each time.

Perform the Haddock sweep (Milestone 3), then:

    cabal haddock all

Expected: exit 0, `Documentation created:` lines for each package, no
`-Wmissing-docs` warnings.

Run the release dry-run (Milestone 4):

    cabal check
    cabal sdist all

Expected: three tarballs under `dist-newstyle/sdist/`. Inspect one:

    tar -tzf dist-newstyle/sdist/shibuya-message-db-adapter-0.1.0.0.tar.gz | head

Expected contents include `shibuya-message-db-adapter.cabal`, `LICENSE`,
`README.md`, `CHANGELOG.md`, and the `src/` tree.


## Validation and Acceptance

The plan is complete when all of the following are observable in the working tree.

1. The repository root contains `LICENSE`, `README.md`, and `CHANGELOG.md`. The
   README has the sections listed in Milestone 1; the CHANGELOG lists the `0.1.0.0`
   release with the per-ExecPlan feature grouping.

2. `cabal check` exits 0 inside each of the three package directories. No warnings
   about missing synopsis, missing category, unversioned dependencies, or missing
   source-repository.

3. `cabal haddock all` exits 0 and produces HTML at
   `dist-newstyle/build/<arch>/<ghc>/shibuya-message-db-adapter-0.1.0.0/doc/html/shibuya-message-db-adapter/index.html`
   and equivalent paths for the other two packages. Zero `-Wmissing-docs` warnings
   on the main library.

4. `cabal sdist all` writes three tarballs
   (`shibuya-message-db-adapter-0.1.0.0.tar.gz`,
   `shibuya-message-db-adapter-jitsurei-0.1.0.0.tar.gz`,
   `shibuya-message-db-adapter-bench-0.1.0.0.tar.gz`) to
   `dist-newstyle/sdist/`. Each tarball opens with `tar -tzf` and contains the cabal
   file, LICENSE, README (if applicable), and the package's source tree.

5. A fresh reader visiting the Haddock HTML for `Shibuya.Adapter.MessageDb` can read
   a substantial module-level description and then click through to every exported
   function and type and see at least one sentence of documentation on each.

Non-goals (explicit): uploading to Hackage, producing Git release tags, running
`cabal format` or `fourmolu` over the entire tree, making the benchmarks reproducible
across machines. Any of these that become necessary can be tracked in a follow-up
plan.


## Idempotence and Recovery

Every editing step in this plan is idempotent. Writing the cabal file, the README, the
CHANGELOG, and the LICENSE overwrites the previous contents. `cabal sdist` and
`cabal haddock` produce independent reports each time — no state is persisted.

If `cabal haddock all` fails with a parse error in a Haddock comment, the comment's
opening marker is `-- |` (pipe, with a space) on a line by itself, followed by prose
that may continue with `--` without the pipe on subsequent lines. A common mistake is
omitting the pipe, in which case Haddock ignores the comment and `-Wmissing-docs`
still fires. A second common mistake is placing the comment **after** the declaration
instead of before it; `-- ^` post-position docs only work for constructor arguments
and record fields.

If `cabal sdist all` fails with "no source-repository head" warnings, the fix is to
add or correct the `source-repository head` stanza in the offending cabal file.

If `cabal check` reports unversioned dependencies, add a `^>=` upper bound to each
dependency in the offending stanza. The Shibuya convention is `^>=<major>.<minor>`
to allow patch-level updates without requiring a re-release.

Rolling back the plan is straightforward: `git reset --hard <pre-EP8-commit>` restores
the repository to its pre-plan state. No external state changes are made by this plan.


## Interfaces and Dependencies

This plan does not introduce new Haskell modules, signatures, or library
dependencies. It edits documentation and metadata in existing files:

- `LICENSE` (new file at repo root)
- `README.md` (new file at repo root)
- `CHANGELOG.md` (new file at repo root)
- `shibuya-message-db-adapter/shibuya-message-db-adapter.cabal` (metadata fields,
  `-Wmissing-docs` for library stanza)
- `shibuya-message-db-adapter-jitsurei/shibuya-message-db-adapter-jitsurei.cabal`
  (metadata fields, if EP-5 has landed)
- `shibuya-message-db-adapter-bench/shibuya-message-db-adapter-bench.cabal`
  (metadata fields, if EP-6 has landed)
- Every Haskell module under `shibuya-message-db-adapter/src/` (Haddock comments)

The main library exposed-module list at the end of EP-8 reads:

    exposed-modules:
      Shibuya.Adapter.MessageDb
      Shibuya.Adapter.MessageDb.Config
      Shibuya.Adapter.MessageDb.Convert
      Shibuya.Adapter.MessageDb.Internal

plus any helper modules added by EP-2, EP-3, EP-4, or EP-7 (notably
`Shibuya.Adapter.MessageDb.Internal.PerStreamDispatch` from EP-7). The MasterPlan's
additive rule applies: EP-8 does not itself add or remove modules; it documents
whatever is present.
