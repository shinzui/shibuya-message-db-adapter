# Benchmarks for shibuya-message-db-adapter

MasterPlan: docs/masterplans/1-shibuya-message-db-adapter.md

Intention: intention_01kpgme50se0ranxp41ghfhajf

This ExecPlan is a living document. The sections Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.

This document is maintained in accordance with `.claude/skills/exec-plan/PLANS.md`.


## Purpose / Big Picture

After this plan is complete, a new Cabal sub-package `shibuya-message-db-adapter-bench`
at the repository root runs a collection of `tasty-bench` micro-benchmarks that measure
the adapter's pure hot paths: `Message`-to-`Envelope` conversion, W3C trace-context
extraction, STM inflight-set bookkeeping (EP-2), and category-partition hashing (EP-4).
The goal is regression detection, not absolute performance characterisation.

Observable outcome. From a `nix develop` shell at the repo root,
`cabal bench shibuya-message-db-adapter-bench` exits 0 and prints a tasty-bench table
with nanosecond-level timings for each micro-benchmark. The bench package compiles in
isolation (`cabal build shibuya-message-db-adapter-bench`) and is listed in
`cabal.project` and `mori.dhall` alongside the existing packages.

This plan focuses **only on benchmarking**. Release metadata (LICENSE, README,
CHANGELOG, .cabal polish, Haddock sweep, sdist dry-run) is the responsibility of EP-8
(`docs/plans/8-release-metadata.md`), which runs after every feature plan has landed
and prepares the repository for Hackage upload. Splitting the two concerns lets EP-6
land as soon as the main library exists, without waiting for the full feature set, and
keeps EP-8's release CHANGELOG aware of every shipped feature including the bench
package.

This plan hard-depends on EP-1 (`docs/plans/1-scaffold-and-minimal-adapter.md`)
because the bench package depends on the main library. Conditional bench milestones
gate on EP-2 (inflight-set benchmarks) and EP-4 (category-partition benchmarks); if
either has not landed when EP-6 is implemented, the milestone is skipped and the
Decision Log records the skip. The skipped milestone can be revisited as a follow-up
once the gating plan lands.


## Progress

Use a checklist to summarize granular steps. Every stopping point must be documented
here, even if it requires splitting a partially completed task into two ("done" vs.
"remaining"). This section must always reflect the actual current state of the work.

### Milestone 1: Scaffold the bench package

- [ ] Create the directory `shibuya-message-db-adapter-bench/` at the repo root with
      subdirectories `bench/` and `src/`.
- [ ] Write `shibuya-message-db-adapter-bench/shibuya-message-db-adapter-bench.cabal`
      with a single `benchmark` stanza plus a small `library` stanza for `Fixtures`.
- [ ] Write `shibuya-message-db-adapter-bench/bench/Main.hs` containing only a
      `defaultMain []` stub so the package compiles before any benchmarks are wired up.
- [ ] Write `shibuya-message-db-adapter-bench/src/Fixtures.hs` with the three sample
      `Message` values documented under *Interfaces and Dependencies* and their
      supporting `NFData` orphan instances.
- [ ] Update `cabal.project` to list `./shibuya-message-db-adapter-bench` under
      `packages:` after the existing entries.
- [ ] Update `mori.dhall` to add a `shibuya-message-db-adapter-bench` entry mirroring
      the shape of the `shibuya-message-db-adapter` entry.
- [ ] Confirm `cabal build shibuya-message-db-adapter-bench` compiles the empty
      skeleton.

### Milestone 2: Conversion benchmarks

- [ ] Wire the three `messageToEnvelope` variants (empty metadata, traceparent only,
      traceparent+tracestate) into the `bgroup "messageToEnvelope"` in `bench/Main.hs`.
- [ ] Wire the four `extractTraceContext` variants (empty object, unrelated keys,
      traceparent only, both headers, deeply nested object) into
      `bgroup "extractTraceContext"`.
- [ ] Run `cabal bench shibuya-message-db-adapter-bench` and confirm every benchmark
      reports a timing with non-`NaN` mean.
- [ ] Paste the transcript under *Concrete Steps* so later runs can be compared.

### Milestone 3: InflightState benchmarks (conditional on EP-2 complete)

- [ ] If `Shibuya.Adapter.MessageDb.Internal.InflightState` exists, add
      `bgroup "InflightState"` with `recordIngested` throughput, `recordAckResult`
      throughput, and `advanceCheckpointTo` on three inflight-set shapes.
- [ ] If EP-2 is not complete, note the skip in the Decision Log and move on.
- [ ] Run `cabal bench shibuya-message-db-adapter-bench` and update the transcript.

### Milestone 4: categoryPartition benchmark (conditional on EP-4 complete)

- [ ] If `Shibuya.Adapter.MessageDb.Internal.categoryPartition` (or equivalent from
      EP-4) exists, add `bgroup "categoryPartition"` benchmarking the hash at
      `groupSize` values `1`, `3`, `8`, `32`.
- [ ] If EP-4 is not complete, note the skip in the Decision Log and move on.
- [ ] Run `cabal bench shibuya-message-db-adapter-bench` and update the transcript.


## Surprises & Discoveries

(None yet.)


## Decision Log

- Decision: The bench package is a standalone Cabal package at the repo root
  (`shibuya-message-db-adapter-bench/`), not a `benchmark` stanza inside the main
  library's cabal file.
  Rationale: This mirrors the sibling `shibuya-kafka-adapter` layout at
  `/Users/shinzui/Keikaku/bokuno/shibuya-project/shibuya-kafka-adapter/shibuya-kafka-adapter-bench/`.
  Keeping benches separate means the main library does not build `tasty-bench` for
  ordinary users, and users installing from Hackage do not pull benchmark-only
  dependencies. It also lets the bench package depend on a private `Fixtures` library
  that exposes test-only helpers without polluting the main library's API.
  Date: 2026-04-18

- Decision: Use `Bodigrim/tasty-bench` (not `criterion`).
  Rationale: `tasty-bench` ships with shibuya's other sub-projects (core, pgmq, kafka
  adapters), integrates with `tasty` test discovery, prints readable plain-text tables,
  and has no `statistics` / `ieee754` transitive-dependency footprint. Picking the same
  tool as the sibling adapters keeps the bench transcripts comparable across the
  Shibuya family.
  Date: 2026-04-18

- Decision: Fixtures live in a small `library` stanza inside the bench package
  (`shibuya-message-db-adapter-bench/src/Fixtures.hs`), not in the bench executable
  itself.
  Rationale: Exposing fixtures as a library lets future tests (integration or property)
  reuse the same `Message` samples without copy-paste. The library is not on Hackage —
  it is only visible to in-tree consumers — so it costs nothing to publish.
  Date: 2026-04-18

- Decision: Release metadata is split out into EP-8.
  Rationale: Benchmarks are a continuous regression-tracking concern that lands as
  soon as the main library exists. Release metadata (LICENSE, README, CHANGELOG,
  cabal polish, Haddock, sdist) is a single end-of-line act that should describe
  every shipped feature including the bench package itself. Bundling them in one
  ExecPlan forced EP-6 to either wait for every feature plan or ship release metadata
  without describing later plans. Separating them lets benchmarks land early and
  release metadata describe the complete feature set.
  Date: 2026-04-18


## Outcomes & Retrospective

(To be filled during and after implementation.)


## Context and Orientation

The reader knows Haskell and Cabal but need not have seen this repository. What
follows is enough to pick up EP-6 cold.

**What the adapter is.** `shibuya-message-db-adapter` plugs a `MessageDb.Message`
source into Shibuya's queue-processing framework. A user's application defines a
`Handler es MessageDb.Message`, passes it and `messageDbAdapter cfg` to
`Shibuya.App.runApp`, and Shibuya runs a supervised polling loop that reads messages
from a message-db category, wraps each in a `Shibuya.Core.Types.Envelope`, dispatches
to the user's handler, and acks the result. "message-db" is the PostgreSQL-backed
event store at `/Users/shinzui/Keikaku/hub/event-sourcing/message-db-project/message-db/`,
accessed via `message-db-hs` and `message-db-effectful`. A "CategoryStream" is the
prefix of a stream name before the first hyphen (e.g., `orders` in `orders-123`).
Typical users are event-sourcing applications that already run message-db and want
Shibuya's supervised handler runtime instead of the raw `message-db-subscription`
consumer loop.

**Repository layout at the start of EP-6.** Assuming EP-1 has landed, the repository
at `/Users/shinzui/Keikaku/work/libraries/haskell/shibuya-message-db-adapter`
contains `cabal.project`, `flake.nix`, `flake.lock`, `process-compose.yaml`,
`Justfile`, `mori.dhall`, `treefmt.nix`, `db/`, `docs/masterplans/`, `docs/plans/1-*`
through `docs/plans/8-release-metadata.md`, and `shibuya-message-db-adapter/` (library
sources, tests, `app/Demo.hs`). Subsequent feature plans (EP-2 through EP-5, EP-7) may
or may not have landed when EP-6 is implemented; only EP-1 is required. EP-2 and EP-4
gate conditional benchmark milestones.

**Why benchmarks matter.** The adapter's two pure hot paths run once per message:
`Shibuya.Adapter.MessageDb.Convert.messageToEnvelope` (wraps into `Envelope`) and
`Shibuya.Adapter.MessageDb.Convert.extractTraceContext` (reads W3C trace headers
from `messageMetadata`). A regression in either — for example, accidentally
traversing the metadata object twice — would silently halve throughput.
Micro-benchmarks catch that kind of regression before it reaches users. The
InflightState operations (EP-2) and `categoryPartition` (EP-4) sit on the same
per-message path.

**What `tasty-bench` is.** `tasty-bench` is a `tasty`-integrated benchmark runner
(on Hackage as `tasty-bench`). Its API is small: `bench "label" $ nf function input`
measures pure evaluation to normal form, `bench "label" $ nfIO action` measures an
`IO` action, and `bgroup "name" [children]` nests groups. `main` calls
`defaultMain [topLevelBenchmark]`. A sample run looks like

    shibuya-message-db-adapter-bench
      messageToEnvelope
        empty metadata:          OK (0.88s)
          15.3 ns ± 1.2 ns, 152 B allocated, 0 B copied
        traceparent only:        OK (0.90s)
          42.7 ns ± 3.1 ns, 376 B allocated, 0 B copied

Each row reports mean wall-clock time ± standard deviation, bytes allocated per
iteration, and bytes copied. `tasty-bench` auto-sizes iteration counts. For this
adapter we want nanosecond per-message numbers with allocations in the low hundreds
of bytes; if a benchmark reports microseconds or kilobytes, something is wrong and
Surprises & Discoveries must record it. The `--csv FILE.csv` flag emits
machine-readable output; comparing two transcripts visually is sufficient here.

**NoFieldSelectors caveat.** Both `shibuya-core` and `shibuya-message-db-adapter`
enable `NoFieldSelectors` by default. Record field names (`payload`, `envelope`,
`messageMetadata`, `globalPosition`) are **not** automatic accessors. Inside the
library they are reached via `OverloadedRecordDot` (`env.payload`) or record pattern
matching (`Envelope{payload} = env`). In the bench package the same rules apply: do
**not** write `payload env` as if the field were a function. In `Fixtures.hs` most
values are built with record construction, which sidesteps the issue:

    sampleMessage :: Mdb.Message
    sampleMessage =
        Mdb.Message
            { Mdb.messageId       = Mdb.MessageId fixedUUID
            , Mdb.stream          = Mdb.Stream "orders-123"
            , ...
            }

The simplest discipline for the benchmark body is to measure functions, not to
inspect their results; `nf conversionFunction input` is sufficient.

**NFData orphans.** `tasty-bench`'s `nf` forces its result to normal form via
`NFData`. `shibuya-core` 0.1.0.0 ships `NFData` instances for `MessageId`, `Cursor`,
and `Envelope a` (see `/Users/shinzui/Keikaku/bokuno/shibuya-project/shibuya/CHANGELOG.md`).
If the locally-pinned `shibuya-core` has these, the bench package needs no extra
derives. Otherwise the bench package declares orphans in `Main.hs` behind
`{-# OPTIONS_GHC -Wno-orphans #-}`, exactly as the kafka adapter does at
`/Users/shinzui/Keikaku/bokuno/shibuya-project/shibuya-kafka-adapter/shibuya-kafka-adapter-bench/bench/Main.hs`
lines 20–33. `MessageDb.Message` itself needs an `NFData` instance derived via
`Generic`; this is also an orphan in the bench package.


## Plan of Work

### Milestone 1: Scaffold the bench package

At the end of this milestone, the directory
`shibuya-message-db-adapter-bench/` exists with a working but empty benchmark
executable, a `Fixtures` library with three sample messages, an entry in
`cabal.project`, and an entry in `mori.dhall`. Running
`cabal build shibuya-message-db-adapter-bench` succeeds.

Create the directory tree

    shibuya-message-db-adapter-bench/
      shibuya-message-db-adapter-bench.cabal
      bench/Main.hs
      src/Fixtures.hs

Write `shibuya-message-db-adapter-bench.cabal` with two stanzas. The `library` stanza
exposes `Fixtures` with `hs-source-dirs: src`; it depends on `base`, `aeson`,
`bytestring`, `containers`, `deepseq`, `text`, `time`, `uuid-types`, `vector`,
`message-db-hs`, `shibuya-core`, `shibuya-message-db-adapter`. The `benchmark` stanza
has `type: exitcode-stdio-1.0`, `main-is: Main.hs`, `hs-source-dirs: bench`, depends
on `base`, `deepseq`, `tasty-bench`, and the in-tree `Fixtures` library plus the main
adapter library.

The cabal file's common warning stanza should match the kafka adapter bench's at
`/Users/shinzui/Keikaku/bokuno/shibuya-project/shibuya-kafka-adapter/shibuya-kafka-adapter-bench/shibuya-kafka-adapter-bench.cabal`:

    common warnings
      ghc-options:
        -Wall -Wcompat -Widentities -Wincomplete-uni-patterns
        -Wincomplete-record-updates -Wredundant-constraints
        -fhide-source-paths -Wmissing-export-lists -Wpartial-fields
        -Wmissing-deriving-strategies

Benchmark-specific ghc-options should include
`-Wno-orphans -rtsopts -with-rtsopts=-A32m -fproc-alignment=64` so allocation-heavy
benchmarks don't thrash the nursery and alignment-sensitive timings are stable.

Default extensions for the benchmark stanza: `DeriveAnyClass DerivingStrategies
DuplicateRecordFields LambdaCase NoFieldSelectors OverloadedLabels OverloadedRecordDot
OverloadedStrings StandaloneDeriving`. The `Fixtures` library need not enable
`NoFieldSelectors` — it is easier to write fixtures with ordinary record construction.

Write `bench/Main.hs` initially as

    {-# OPTIONS_GHC -Wno-orphans #-}

    module Main (main) where

    import Test.Tasty.Bench (defaultMain)

    main :: IO ()
    main = defaultMain []

and `src/Fixtures.hs` with the three sample messages described under *Interfaces and
Dependencies*.

Edit `cabal.project` at the repo root. The `packages:` stanza after EP-1 (and EP-5
if it landed) reads

    packages:
      ./shibuya-message-db-adapter
      -- optionally: ./shibuya-message-db-adapter-jitsurei

Append the new package line:

    packages:
      ./shibuya-message-db-adapter
      -- optionally: ./shibuya-message-db-adapter-jitsurei
      ./shibuya-message-db-adapter-bench

Edit `mori.dhall`. The file will contain a `packages` list with entries for the main
library (and the jitsurei package if EP-5 has landed). Append a new entry whose
fields mirror the main library's, adjusting `name` to
`shibuya-message-db-adapter-bench`, `description` to "Benchmarks for
shibuya-message-db-adapter", and the modules list to `[ "Fixtures" ]`. The top-level
`dependencies` list must already include `tasty-bench` (add it here if missing).

Verify compilation:

    cabal build shibuya-message-db-adapter-bench

Expected final line: `Linking .../shibuya-message-db-adapter-bench ...` or
`Registering library ...`, exit 0.

### Milestone 2: Conversion benchmarks

At the end of this milestone, `cabal bench shibuya-message-db-adapter-bench` prints a
table containing at least seven rows: three for `messageToEnvelope` and four for
`extractTraceContext`. A transcript is pasted into *Concrete Steps*.

Replace the stub `main` in `bench/Main.hs` with a `defaultMain` that composes two
`bgroup` values.

The `messageToEnvelope` group has three `bench` nodes, one per metadata shape:

    bgroup "messageToEnvelope"
      [ bench "empty metadata" $
          nf Shibuya.Adapter.MessageDb.Convert.messageToEnvelope
             Fixtures.messageEmptyMetadata
      , bench "traceparent only" $
          nf Shibuya.Adapter.MessageDb.Convert.messageToEnvelope
             Fixtures.messageTraceparentOnly
      , bench "both traceparent + tracestate" $
          nf Shibuya.Adapter.MessageDb.Convert.messageToEnvelope
             Fixtures.messageBothHeaders
      ]

The `extractTraceContext` group has four `bench` nodes. Each passes a
`MessageDb.MessageMetadata` value to
`Shibuya.Adapter.MessageDb.Convert.extractTraceContext`:

    bgroup "extractTraceContext"
      [ bench "empty object"               $ nf extractTraceContext Fixtures.metaEmpty
      , bench "unrelated keys only"        $ nf extractTraceContext Fixtures.metaUnrelatedKeys
      , bench "traceparent only"           $ nf extractTraceContext Fixtures.metaTraceparentOnly
      , bench "both headers"               $ nf extractTraceContext Fixtures.metaBothHeaders
      , bench "deeply nested object"       $ nf extractTraceContext Fixtures.metaDeeplyNested
      ]

`Fixtures.metaDeeplyNested` is a metadata object whose top-level `traceparent` is
absent but which contains a five-level-deep nested object with unrelated keys. It
probes the conversion's fast-path for "traceparent not found" without short-circuiting
on the first key.

Run `cabal bench shibuya-message-db-adapter-bench`. Expected output:

    All
      messageToEnvelope
        empty metadata:                   OK
          xx.x ns ± x.x ns, NNN B allocated
        traceparent only:                 OK
          xx.x ns ± x.x ns, NNN B allocated
        both traceparent + tracestate:    OK
          xx.x ns ± x.x ns, NNN B allocated
      extractTraceContext
        empty object:                     OK
          ...
        unrelated keys only:              OK
          ...
        traceparent only:                 OK
          ...
        both headers:                     OK
          ...
        deeply nested object:             OK
          ...

    All ... tests passed

Capture this transcript verbatim in *Concrete Steps* as the "Milestone 2 baseline".

### Milestone 3: InflightState benchmarks (conditional on EP-2)

This milestone is conditional. If EP-2 has not produced
`Shibuya.Adapter.MessageDb.Internal.InflightState`, skip the milestone, record the
skip in the Decision Log, and proceed to Milestone 4. If EP-2 is complete, read the
module to discover the actual constructor and operation names — the MasterPlan
sketches `newInflightState`, `recordIngested`, `recordAckResult`, and
`advanceCheckpointTo`, but EP-2 may have refined them.

Add a third top-level `bgroup "InflightState"` node to `main`:

    bgroup "InflightState"
      [ bench "recordIngested x 1000"      $ nfIO (runRecordIngested 1000)
      , bench "recordAckResult x 1000"     $ nfIO (runRecordAckResult 1000)
      , bgroup "advanceCheckpointTo"
          [ bench "fully AckComplete / 1000"  $ nfIO (runAdvance full1000)
          , bench "AckRetry hole in middle"   $ nfIO (runAdvance holeMiddle)
          , bench "scattered AckRetry"        $ nfIO (runAdvance scattered)
          ]
      ]

The `InflightState` operations are in `STM` or `IO`, so they use `nfIO`, not `nf`.
Each helper constructs a fresh `InflightState` per run, populates it with the 1000
entries, and returns the unit result forced through `evaluate`:

    runRecordIngested :: Int -> IO ()
    runRecordIngested n = do
      s <- newInflightState
      mapM_ (recordIngested s) [1 .. fromIntegral n]

The three `advanceCheckpointTo` fixtures differ in the pre-populated ack pattern:
`full1000` has every position acked `AckComplete`; `holeMiddle` has every position
except a single middle position acked; `scattered` has alternating `AckComplete` and
`AckRetry` (placeholder values) to stress the contiguous-prefix walk.

Each fixture's construction lives in `Fixtures.hs` under the
`{- InflightState samples -}` section. Re-run `cabal bench`, confirm new rows appear,
and append the transcript to *Concrete Steps* as "Milestone 3 baseline".

### Milestone 4: categoryPartition benchmark (conditional on EP-4)

Also conditional. If EP-4 is complete, the main library exposes a function that hashes
a category name and reduces modulo a group size; the MasterPlan refers to it as
`categoryPartition` or equivalent, implemented via Murmur3-64. Add:

    bgroup "categoryPartition"
      [ bench "groupSize=1"  $ nf (`categoryPartition` 1)  sampleCategory
      , bench "groupSize=3"  $ nf (`categoryPartition` 3)  sampleCategory
      , bench "groupSize=8"  $ nf (`categoryPartition` 8)  sampleCategory
      , bench "groupSize=32" $ nf (`categoryPartition` 32) sampleCategory
      ]

where `sampleCategory :: CategoryStream` is `Fixtures.sampleCategory = CategoryStream
"orders"`. Because the hash function is total and cheap, the benchmark primarily
measures Murmur3 throughput; timings should land in the tens of nanoseconds.

Re-run `cabal bench` and append the transcript as "Milestone 4 baseline".


## Concrete Steps

Run all commands from
`/Users/shinzui/Keikaku/work/libraries/haskell/shibuya-message-db-adapter` inside a
`nix develop` shell.

Create the bench package tree:

    mkdir -p shibuya-message-db-adapter-bench/bench
    mkdir -p shibuya-message-db-adapter-bench/src

Write `shibuya-message-db-adapter-bench/shibuya-message-db-adapter-bench.cabal`,
`bench/Main.hs`, and `src/Fixtures.hs` per *Plan of Work*.

Edit `cabal.project` and `mori.dhall` per *Plan of Work*.

Verify the scaffold compiles:

    cabal build shibuya-message-db-adapter-bench

Expected: exit 0, final line `Linking` or `Registering`.

Wire conversion benchmarks (Milestone 2) and run:

    cabal bench shibuya-message-db-adapter-bench

Paste the transcript here as the Milestone 2 baseline once observed.

Wire InflightState benchmarks (Milestone 3) and re-run `cabal bench`. Append the new
transcript lines.

Wire categoryPartition benchmark (Milestone 4) and re-run `cabal bench`. Append the
new transcript lines.


## Validation and Acceptance

The plan is complete when all of the following are observable in the working tree.

1. `cabal bench shibuya-message-db-adapter-bench` exits 0 and reports at least seven
   benchmark rows (Milestones 2 through 4 combined; skipped conditional milestones
   reduce this count but the Decision Log records the skip).

2. Every row in the benchmark output has a non-`NaN` mean and a standard deviation
   less than or equal to the mean; if any row is extremely noisy, the transcript must
   include a note in Surprises & Discoveries.

3. `cabal build shibuya-message-db-adapter-bench` exits 0 from a clean
   `dist-newstyle/`.

4. `mori show --full` lists the bench package with the correct dependency set.

5. The bench package's `cabal.project` and `mori.dhall` entries are present.

Non-goals for this plan (handled by EP-8): LICENSE, README, CHANGELOG, .cabal
metadata polish, Haddock sweep, `-Wmissing-docs`, `cabal sdist`, Hackage upload.


## Idempotence and Recovery

Every editing step in this plan is idempotent. Writing the cabal file overwrites the
previous contents. Running `cabal bench` any number of times produces independent
reports — no state is persisted.

If `cabal build shibuya-message-db-adapter-bench` fails with a dependency-resolution
error, run `cabal update` and retry. If it fails with a compile error inside
`Fixtures.hs`, the most likely cause is a mismatch between the field names this plan
assumes (`messageId`, `stream`, `messageType`, `position`, `globalPosition`,
`messageData`, `messageMetadata`, `time`) and the actual export list of
`MessageDb.Message`. Read
`/Users/shinzui/Keikaku/work/libraries/haskell/message-db-hs-master/message-db-hs/src/MessageDb/Message.hs`
and adjust.

If a benchmark reports wildly unstable timings (standard deviation larger than mean),
the cause is usually system noise. Retry with a quieter system, or increase
`-with-rtsopts=-A` in the benchmark stanza. Record the observation in
Surprises & Discoveries regardless.

Rolling back the plan is straightforward: `git reset --hard <pre-EP6-commit>` restores
the repository to its pre-plan state. No external state changes are made by this plan.


## Interfaces and Dependencies

### Bench package cabal stanza

`shibuya-message-db-adapter-bench/shibuya-message-db-adapter-bench.cabal` at the end
of Milestone 1 (note: release-related fields like `synopsis`, `description`,
`homepage`, `bug-reports`, `source-repository head` are filled in by EP-8, not here):

    cabal-version:   3.12
    name:            shibuya-message-db-adapter-bench
    version:         0.1.0.0
    synopsis:        Benchmarks for shibuya-message-db-adapter
    license:         MIT
    build-type:      Simple
    tested-with:     GHC ==9.12.2

    common warnings
      ghc-options:
        -Wall -Wcompat -Widentities -Wincomplete-uni-patterns
        -Wincomplete-record-updates -Wredundant-constraints
        -fhide-source-paths -Wmissing-export-lists -Wpartial-fields
        -Wmissing-deriving-strategies

    library
      import:           warnings
      exposed-modules:  Fixtures
      hs-source-dirs:   src
      default-language: GHC2024
      build-depends:
        , aeson
        , base                        ^>=4.21.0.0
        , bytestring
        , containers
        , deepseq
        , message-db-hs
        , shibuya-core                ^>=0.1.0.0
        , shibuya-message-db-adapter
        , text
        , time
        , uuid-types

    benchmark shibuya-message-db-adapter-bench
      import:           warnings
      type:             exitcode-stdio-1.0
      main-is:          Main.hs
      hs-source-dirs:   bench
      default-language: GHC2024
      ghc-options:
        -Wno-orphans -rtsopts -with-rtsopts=-A32m -fproc-alignment=64
      default-extensions:
        DeriveAnyClass
        DerivingStrategies
        DuplicateRecordFields
        LambdaCase
        NoFieldSelectors
        OverloadedLabels
        OverloadedRecordDot
        OverloadedStrings
        StandaloneDeriving
      build-depends:
        , base                              ^>=4.21.0.0
        , deepseq
        , message-db-hs
        , shibuya-core                      ^>=0.1.0.0
        , shibuya-message-db-adapter
        , shibuya-message-db-adapter-bench
        , tasty-bench

### Public fixtures module

`shibuya-message-db-adapter-bench/src/Fixtures.hs` exposes the message and metadata
samples used by every benchmark. Skeleton:

    module Fixtures
        ( messageEmptyMetadata
        , messageTraceparentOnly
        , messageBothHeaders
        , metaEmpty
        , metaUnrelatedKeys
        , metaTraceparentOnly
        , metaBothHeaders
        , metaDeeplyNested
        , sampleCategory
          -- InflightState fixtures (EP-2 gated)
        , runRecordIngested
        , runRecordAckResult
        , fullComplete1000
        , holeMiddle1000
        , scattered1000
        ) where

    mkMessage :: Mdb.MessageMetadata -> Mdb.Message
    mkMessage md =
        Mdb.Message
            { Mdb.messageId       = Mdb.MessageId fixedUUID
            , Mdb.stream          = Mdb.Stream "orders-123"
            , Mdb.messageType     = Mdb.MessageType "OrderPlaced"
            , Mdb.position        = Mdb.MessagePosition 7
            , Mdb.globalPosition  = Mdb.GlobalPosition 12345
            , Mdb.messageData     = Mdb.MessageData (Aeson.Object mempty)
            , Mdb.messageMetadata = md
            , Mdb.time            = fixedTime
            }

Each metadata fixture is a `Mdb.MessageMetadata` built from an `Aeson.Object`:
`metaEmpty` wraps `Aeson.Object mempty`; `metaUnrelatedKeys` has
`correlationId`/`causationId`/`tenant` but no trace keys; `metaTraceparentOnly` and
`metaBothHeaders` carry the W3C header strings; `metaDeeplyNested` is a recursive
five-level nested object with unrelated keys (stress test for
`extractTraceContext`'s fast-path for missing `traceparent`). The three `message*`
fixtures compose `mkMessage` with the appropriate metadata value. `sampleCategory =
CategoryStream "orders"`. InflightState fixtures are added in Milestone 3 once EP-2
is complete; see that milestone for details.

### Dependencies

The bench package introduces exactly two new dependencies that were not already
present in the main library or jitsurei package: `tasty-bench` and `deepseq`. Both
are widely used and `ghc-boot-compatible`. No new system libraries, no FFI.

### Signatures to exist at milestone ends

At the end of Milestone 1, the bench package exposes:

    module Fixtures where

      messageEmptyMetadata    :: MessageDb.Message
      messageTraceparentOnly  :: MessageDb.Message
      messageBothHeaders      :: MessageDb.Message

      metaEmpty               :: MessageDb.MessageMetadata
      metaUnrelatedKeys       :: MessageDb.MessageMetadata
      metaTraceparentOnly     :: MessageDb.MessageMetadata
      metaBothHeaders         :: MessageDb.MessageMetadata
      metaDeeplyNested        :: MessageDb.MessageMetadata

      sampleCategory          :: CategoryStream

At the end of Milestone 2, `bench/Main.hs` exposes:

    main :: IO ()

with `defaultMain [conversionBenchmarks, traceContextBenchmarks]` where both named
groups are top-level functions of type `Benchmark`.

At the end of Milestone 3 (if EP-2 is done):

    inflightStateBenchmarks :: Benchmark

appears in `bench/Main.hs` and `Fixtures.hs` exports `runRecordIngested`,
`runRecordAckResult`, `fullComplete1000`, `holeMiddle1000`, `scattered1000`.

At the end of Milestone 4 (if EP-4 is done):

    categoryPartitionBenchmarks :: Benchmark

appears in `bench/Main.hs`.
