# Jitsurei examples and integration test suite

MasterPlan: docs/masterplans/1-shibuya-message-db-adapter.md

Intention: intention_01kpgme50se0ranxp41ghfhajf

This ExecPlan is a living document. The sections Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.

This document is maintained in accordance with `.claude/skills/exec-plan/PLANS.md`.


## Purpose / Big Picture

After this plan is complete, two things exist that did not before. First, a new Cabal
package `shibuya-message-db-adapter-jitsurei` sits at the repo root containing a small
collection of stand-alone example executables that demonstrate every feature of
`shibuya-message-db-adapter` end-to-end, against a real Postgres instance. A novice who
has never used the adapter can `cabal run basic-consumer`, `cabal run retry-demo`,
`cabal run dead-letter-demo`, `cabal run checkpoint-restart`, and (if consumer groups
are available) `cabal run multi-partition`, and see the adapter's behavior working
exactly as documented in the MasterPlan.

Second, the main `shibuya-message-db-adapter` package gains a proper integration test
suite, driven by `tasty` and backed by the `shinzui/ephemeral-pg` library, which spins
up a throw-away PostgreSQL instance per test run, installs the message-db schema,
applies the checkpoint-store migrations, and exercises the adapter against it. Seven
tests codify the adapter's contract: basic produce-and-consume, checkpoint resume
across an adapter stop/start, retry re-delivery with checkpoint safety, dead-letter
skip-and-log, dead-letter write-to-stream, halt preserving checkpoint, and (if EP-4 is
complete) consumer-group exactly-once across three in-process adapters.

"Jitsurei" (実例) means "real example" and names a directory convention already used
in the neighboring repository
`/Users/shinzui/Keikaku/bokuno/shibuya-project/shibuya-kafka-adapter/`, whose
`shibuya-kafka-adapter-jitsurei/` package contains analogous runnable demos. This plan
mirrors that layout exactly so the two adapters feel interchangeable to a user.

Observable outcome: from the repo root, inside a `nix develop` shell with Postgres
running via `just process-up`, the commands listed in *Validation and Acceptance* all
succeed, each producing the transcripts shown there. `cabal test
shibuya-message-db-adapter` reports all integration tests passing.

This is ExecPlan 5 of six in the MasterPlan initiative. EP-1 scaffolds the adapter and
provides a minimal polling source. EP-2 adds durable checkpointing. EP-3 layers retry
and dead-letter semantics on top. EP-4 adds consumer-group partitioning. EP-5 (this
plan) ships runnable examples and integration tests. EP-6 is benchmarks and docs
polish. EP-5 has a hard dependency on EP-3 — RetryDemo, DeadLetterDemo, and five of
seven integration tests exercise ack decisions beyond `AckOk`. EP-5 has a soft
dependency on EP-4: the MultiPartition example and the `consumerGroupExactlyOnce` test
are stubbed with a TODO if EP-4 has not landed yet.


## Progress

Use a checklist to summarize granular steps. Every stopping point must be documented
here, even if it requires splitting a partially completed task into two ("done" vs.
"remaining"). This section must always reflect the actual current state of the work.

### Milestone 1: Scaffold shibuya-message-db-adapter-jitsurei

- [ ] Create directory `shibuya-message-db-adapter-jitsurei/` at the repo root with
      `app/` subdirectory and `LICENSE` copied from the main package.
- [ ] Write `shibuya-message-db-adapter-jitsurei/shibuya-message-db-adapter-jitsurei.cabal`
      with one `common common-deps` stanza and one initial `executable basic-consumer`
      stanza; additional stanzas are added as examples are implemented.
- [ ] Update `cabal.project` at the repo root to include
      `./shibuya-message-db-adapter-jitsurei` in `packages:`.
- [ ] Update `mori.dhall` at the repo root to register both packages (library and
      jitsurei) following the kafka adapter's structure.
- [ ] Confirm `cabal build shibuya-message-db-adapter-jitsurei` compiles a placeholder
      `BasicConsumer.hs` that just prints a TODO.

### Milestone 2: BasicConsumer, RetryDemo, DeadLetterDemo

- [ ] Replace the `app/BasicConsumer.hs` placeholder with a real implementation that
      subscribes to `jitsurei-basic` and prints every message.
- [ ] Implement `app/RetryDemo.hs` whose handler returns `AckRetry (RetryDelay 2)` on
      first delivery and `AckOk` on second, with a log line per attempt.
- [ ] Implement `app/DeadLetterDemo.hs` whose handler returns
      `AckDeadLetter (InvalidPayload "demo")` for any message whose `messageType`
      starts with `"Bad"`, configured with `DlqWriteToStream (Stream "demo-dlq")`.
- [ ] Add cabal `executable` stanzas for `retry-demo` and `dead-letter-demo`.
- [ ] Verify each example runs end-to-end against a seeded dev database per the
      transcripts in *Validation and Acceptance*.

### Milestone 3: CheckpointRestart example

- [ ] Implement `app/CheckpointRestart.hs` running two sequential pipelines in one
      process with the same subscription name; phase 1 consumes 5 and stops, phase 2
      resumes and consumes the next 5.
- [ ] Add executable stanza `checkpoint-restart` and verify.

### Milestone 4: MultiPartition example (or stub)

- [ ] If EP-4 is complete: implement `app/MultiPartition.hs` running three adapter
      instances with `ConsumerGroupConfig { groupSize = 3, member = 0..2 }` inside one
      `runApp` call. Seed 30 messages across 6 categories; print which member
      processed which message; print a summary asserting exactly-once.
- [ ] If EP-4 is not complete: write `app/MultiPartition.hs` as a stub that prints a
      TODO explaining the missing dependency, and add a note here with the date the
      stub was created.

### Milestone 5: Integration test harness

- [ ] Add `ephemeral-pg`, `tasty`, `tasty-hunit`, `hasql`, `hasql-transaction`,
      `hasql-pool`, `directory`, `filepath` to the test-suite `build-depends` in
      `shibuya-message-db-adapter.cabal`.
- [ ] Add `test/TestEnv.hs` implementing `withTestEnv :: (TestEnv -> IO a) -> IO a`
      and `writeTestMessages`, applying message-db SQL and checkpoint-store migrations.
- [ ] Expose `MESSAGE_DB_SQL_DIR` via `flake.nix` (either via `env` attribute or
      `shellHook`).
- [ ] Verify `cabal test shibuya-message-db-adapter` still passes the EP-1 convert
      unit tests and now also reports `TestEnv bootstrap: OK`.

### Milestone 6: Integration tests

- [ ] `basicProduceConsume`
- [ ] `checkpointResume`
- [ ] `retryReDelivery`
- [ ] `deadLetterSkipAndLog`
- [ ] `deadLetterWriteToStream`
- [ ] `haltPreservesCheckpoint`
- [ ] `consumerGroupExactlyOnce` (gated on EP-4; if EP-4 incomplete, mark as skipped
      with a dated note here and in `test/Main.hs`)

### Milestone 7: Optional ShibuyaAppMultiProcessor example

- [ ] Implement `app/ShibuyaAppMultiProcessor.hs` showing two adapters with distinct
      `ProcessorId`s under one `runApp`. Optional; skip if time runs out and record
      the decision here.


## Surprises & Discoveries

(None yet.)


## Decision Log

- Decision: The jitsurei package lives in a sibling directory
  `shibuya-message-db-adapter-jitsurei/` as its own Cabal package, rather than as
  extra `executable` stanzas inside the main library package.
  Rationale: Matches the convention established by
  `/Users/shinzui/Keikaku/bokuno/shibuya-project/shibuya-kafka-adapter/shibuya-kafka-adapter-jitsurei/`
  and keeps the main library's dependency footprint small. Users installing
  `shibuya-message-db-adapter` do not drag in example-only deps.
  Date: 2026-04-18

- Decision: Integration tests live in the main `shibuya-message-db-adapter` package's
  `test/` directory, either as additional modules inside the existing test-suite or
  as a second test-suite stanza (preferred: extend the existing one).
  Rationale: Tests assert invariants of the library itself and should travel with
  the library. A single `cabal test shibuya-message-db-adapter` runs both the EP-1
  pure unit tests and the integration tests. The jitsurei package is demonstration
  only.
  Date: 2026-04-18

- Decision: Integration tests use `ephemeral-pg` rather than the direnv-managed
  Postgres used for running the jitsurei examples.
  Rationale: ephemeral-pg gives each test-run an isolated cluster with no shared
  state, and its `withCached` variant holds startup cost to ~400ms. The dev-loop
  Postgres remains for running the jitsurei examples and manual inspection.
  Date: 2026-04-18

- Decision: Tests find the message-db SQL scripts via `MESSAGE_DB_SQL_DIR`, defaulting
  to a hard-coded absolute path if unset, with a warning printed.
  Rationale: The SQL scripts are not bundled with message-db-hs (they live in an
  upstream JavaScript/SQL project). CI or packaged builds should point
  `MESSAGE_DB_SQL_DIR` at a vendored copy; local dev reads from the corpus directory
  directly. An env var keeps the test harness portable without pulling the scripts
  into the repo.
  Date: 2026-04-18

- Decision: MultiPartition example and `consumerGroupExactlyOnce` test are gated on
  EP-4. If EP-5 begins before EP-4 completes, those deliverables are stubbed or
  skipped with a visible note here.
  Rationale: Soft dependency; the rest of EP-5's value — four examples plus six
  integration tests — is realizable on EP-3 alone. Blocking on EP-4 would delay a
  lot of observable progress.
  Date: 2026-04-18


## Outcomes & Retrospective

(To be filled during and after implementation.)


## Context and Orientation

The reader is assumed to know Haskell and Cabal but not this repository or the
surrounding projects. Read this section in full before making changes.

### What the adapter does

`shibuya-message-db-adapter` is a Haskell library at the repo root
(`/Users/shinzui/Keikaku/work/libraries/haskell/shibuya-message-db-adapter`) that
implements the `Adapter es msg` contract from the Shibuya queue-processing framework
on top of message-db, a PostgreSQL-backed event store. A user of Shibuya writes a
handler `Handler es MessageDb.Message` (a function taking an
`Ingested es MessageDb.Message` and returning an `AckDecision`), pairs the handler
with `messageDbAdapter` inside `Shibuya.App.runApp`, and Shibuya supervises a
pipeline that polls message-db, wraps each `Message` in an `Envelope`, invokes the
handler, and acts on the handler's `AckDecision`.

The key types come from
`/Users/shinzui/Keikaku/bokuno/shibuya-project/shibuya/shibuya-core/`:

- `Adapter es msg` with fields `adapterName :: Text`,
  `source :: Stream (Eff es) (Ingested es msg)`, and `shutdown :: Eff es ()`.
- `Envelope msg` with fields `messageId`, `cursor :: Maybe Cursor`,
  `partition :: Maybe Text`, `enqueuedAt :: Maybe UTCTime`,
  `traceContext :: Maybe TraceHeaders`, `payload :: msg`.
- `Ingested es msg` with fields `envelope :: Envelope msg` and
  `ack :: AckHandle es`.
- `AckHandle es = AckHandle { finalize :: AckDecision -> Eff es () }`.
- `AckDecision = AckOk | AckRetry RetryDelay | AckDeadLetter DeadLetterReason |
  AckHalt HaltReason`.

As of EP-1 the adapter has a `MessageDbAdapterConfig` with fields `category`,
`batchSize`, `pollInterval`, `drainTimeout`. EP-2 adds a `checkpointStore` field of
type `CheckpointStore` (effectful, backed by `message-db-checkpoint-store`). EP-3
adds `retryStrategy`, `maxRetries`, and `deadLetter :: DlqStrategy` with variants
`DlqSkipAndLog`, `DlqWriteToStream !Stream`, and `DlqWriteToStreamAndPause !Stream`.
EP-4 adds `group :: ConsumerGroupConfig` with fields `groupSize :: Word` and
`member :: Word`.

### NoFieldSelectors caveat

`shibuya-core` is compiled with `-XNoFieldSelectors`. Record-dot syntax does not
work across package boundaries because field selectors are simply not exported. All
access to `Envelope`, `Ingested`, `AckHandle`, etc. must go through pattern
matching. The kafka adapter's BasicConsumer at
`/Users/shinzui/Keikaku/bokuno/shibuya-project/shibuya-kafka-adapter/shibuya-kafka-adapter-jitsurei/app/BasicConsumer.hs`
demonstrates the expected style: destructure with
`Ingested{envelope = Envelope{messageId = MessageId msgId, cursor, payload},
ack = AckHandle finalize}` in a `Stream.mapM` lambda. Always destructure this way
in examples and tests.

### What the jitsurei package looks like

The existing sibling at
`/Users/shinzui/Keikaku/bokuno/shibuya-project/shibuya-kafka-adapter/shibuya-kafka-adapter-jitsurei/`
has one Cabal file (`shibuya-kafka-adapter-jitsurei.cabal`) with many `executable`
stanzas (basic-consumer, multi-topic, offset-management, multi-partition,
fatal-error-demo, otel-demo, otel-upstream-probe, otel-producer-demo), each with
`hs-source-dirs: app` and its own `main-is` file. A single `common common-deps`
stanza groups shared dependencies; every executable `import`s it plus a `common
warnings` stanza. Each executable uses `ghc-options: -threaded -rtsopts
-with-rtsopts=-N`. `default-language` is `GHC2024`. `default-extensions` includes
`NoFieldSelectors`, `OverloadedStrings`, `OverloadedRecordDot`,
`DuplicateRecordFields`, `DerivingStrategies`, `DeriveAnyClass`, `LambdaCase`.
Mirror this layout exactly.

### What ephemeral-pg does

`ephemeral-pg` is a Haskell library at
`/Users/shinzui/Keikaku/bokuno/ephemeral-pg-project/ephemeral-pg/` that creates
temporary PostgreSQL clusters for tests. It spawns `initdb`, starts `postgres` on a
free port, gives you a `Database` handle, and tears everything down on exit. It
caches the `initdb` output so second and subsequent runs are fast.

The top-level module is `EphemeralPg`, conventionally imported as
`EphemeralPg qualified as Pg`. Relevant exports (verified against
`/Users/shinzui/Keikaku/bokuno/ephemeral-pg-project/ephemeral-pg/src/EphemeralPg.hs`):

    with        :: (Database -> IO a) -> IO (Either StartError a)
    withConfig  :: Config -> (Database -> IO a) -> IO (Either StartError a)
    withCached  :: (Database -> IO a) -> IO (Either StartError a)
    start       :: Config -> IO (Either StartError Database)
    stop        :: Database -> IO ()
    connectionSettings :: Database -> Hasql.Connection.Settings
    connectionString   :: Database -> Text

`Database` has fields `port :: Word16`, `databaseName :: Text`, `user :: Text`,
`dataDirectory :: FilePath`, `socketDirectory :: FilePath`. Using dot syntax
requires `OverloadedRecordDot`. ephemeral-pg does not apply any schema — it gives
you an empty database. Applying the message-db schema is the caller's job.

### Bootstrapping the message-db schema

message-db's PostgreSQL schema lives in a standalone project at
`/Users/shinzui/Keikaku/hub/event-sourcing/message-db-project/message-db/database/`.
The install order, derived from `database/install.sh`, is: `schema/message-store.sql`,
`extensions/pgcrypto.sql`, `tables/messages.sql`, then every `.sql` in `functions/`,
`indexes/`, and `views/` in sorted order. Skip `roles/` and `privileges/` for tests
(ephemeral-pg runs as a single user; those are role-dependent and unnecessary).

The test harness reads these files and runs them via `Hasql.Session.sql`. To avoid
hard-coding the path into Haskell source, the harness consults an environment
variable `MESSAGE_DB_SQL_DIR`; if unset, it falls back to the absolute path noted
above and prints a warning. The Nix flake at the repo root should set this variable
automatically (see *Interfaces and Dependencies*).

### Bootstrapping the checkpoint-store schema

EP-2 introduces a module (likely `MessageDb.CheckpointStore.Db` from the
`message-db-checkpoint-store` upstream package, TBD by EP-2) that contains SQL
migrations for a `checkpoints` table. The test harness must apply those migrations
after the message-db schema is installed. The expected migration entry point is
roughly:

    MessageDb.CheckpointStore.Db.migrate :: Hasql.Connection.Connection -> IO ()

If EP-2 ships a different migration entry point, update the *Interfaces and
Dependencies* section to match.

### What each example is for

- **BasicConsumer** shows the minimum wiring: one category subscription, one
  handler that prints and AckOks. This is the "hello world" of the adapter.
- **RetryDemo** exercises EP-3's retry semantics. The handler tracks per-message
  delivery counts in a `TVar (Map UUID Int)` and returns `AckRetry (RetryDelay 2)`
  on the first delivery of each message, `AckOk` on the second. A user sees each
  message logged twice, roughly 2 seconds apart.
- **DeadLetterDemo** exercises EP-3's dead-letter-to-stream strategy. The handler
  checks `messageType` and returns `AckDeadLetter (InvalidPayload "demo")` for any
  type starting with `"Bad"`; otherwise AckOk. After running with
  `DlqWriteToStream (Stream "demo-dlq")`, the user queries `demo-dlq` via psql to
  confirm the DLQ messages.
- **CheckpointRestart** exercises EP-2's durable checkpoint. Two pipelines run
  sequentially in the same process under the same subscription name. The first
  consumes 5 and stops; the second starts fresh and should consume only messages
  6..10.
- **MultiPartition** exercises EP-4's consumer-group partitioning. Three adapter
  instances with `groupSize = 3` and members 0, 1, 2 run inside one `runApp` call.
  Thirty messages across six categories are written; the example prints which
  member handled which message and prints a summary asserting the total is 30 with
  no duplicates.
- **(Optional) ShibuyaAppMultiProcessor** shows the adapter participating in
  Shibuya's multi-processor orchestration: two `messageDbAdapter` instances with
  distinct `ProcessorId`s under one `runApp`, identically to how the kafka or pgmq
  adapters would be composed.

### What each integration test asserts

- `basicProduceConsume`: write 10 messages to a single category, run the adapter
  with a handler that pushes every `Envelope` payload into an `MVar [Message]`,
  wait until length is 10, assert order matches insertion order.
- `checkpointResume`: write 10 messages, run the adapter with AckOk until 5 have
  been processed, call `shutdown`, start again with the same subscription name and
  checkpoint store, assert the next 5 positions are 6..10 with no duplicates of
  1..5.
- `retryReDelivery`: write 3 messages. Handler returns
  `AckRetry (RetryDelay 0.1)` exactly once for message #2, else AckOk. Assert:
  message #1 seen once, message #2 seen exactly twice, message #3 seen once, final
  checkpoint equals message #3's global position.
- `deadLetterSkipAndLog`: write 3 messages, handler AckDeadLetters message #2,
  default `DlqSkipAndLog` strategy, assert checkpoint advances past #3 and no entry
  is written to any dead-letter stream.
- `deadLetterWriteToStream`: same as above but with
  `DlqWriteToStream (Stream "test-dlq")`. Assert checkpoint advances past #3 and a
  row exists in `message_store.messages` with `stream_name = 'test-dlq'` whose
  metadata references the original message id.
- `haltPreservesCheckpoint`: write 5 messages, handler returns `AckHalt
  (Unrecoverable "test")` on message #3. Assert the adapter's `shutdown` runs, the
  checkpoint equals message #2's position (not #3), and messages #1 and #2 are
  acknowledged.
- `consumerGroupExactlyOnce` (EP-4 only): write 30 messages across 6 categories
  (5 per category). Run 3 adapter instances with `groupSize = 3`, members 0..2,
  each pushing into its own `MVar`. Assert the union contains exactly 30 distinct
  messages and same-category messages always land in the same MVar.


## Plan of Work

### Milestone 1: Scaffold shibuya-message-db-adapter-jitsurei

At the end of this milestone, `cabal build shibuya-message-db-adapter-jitsurei`
compiles a placeholder executable. The repository gains a new top-level directory
`shibuya-message-db-adapter-jitsurei/` with `app/`, a `.cabal` file, and a copy of
`LICENSE` from the main package.

Create the directory tree (`app/` empty, `LICENSE` copied). Write
`shibuya-message-db-adapter-jitsurei.cabal` by adapting the kafka adapter's
equivalent at
`/Users/shinzui/Keikaku/bokuno/shibuya-project/shibuya-kafka-adapter/shibuya-kafka-adapter-jitsurei/shibuya-kafka-adapter-jitsurei.cabal`.
The `common common-deps` stanza should list `base`, `bytestring`, `containers`,
`effectful`, `effectful-core`, `hasql`, `message-db-hs`, `message-db-effectful`,
`shibuya-core`, `shibuya-message-db-adapter`, `streamly`, `streamly-core`, `stm`,
`text`, `time`, `aeson`, `uuid-types`. Include the same `default-extensions` and
`default-language: GHC2024` as the kafka adapter.

Add an initial `executable basic-consumer` stanza with `main-is: BasicConsumer.hs`.
Write a placeholder `app/BasicConsumer.hs` that prints `"basic-consumer: TODO next
milestone"` and exits; this keeps the build green while later milestones flesh out
the real code.

Update `cabal.project` at the repo root to add the new package directory under
`packages:`. Update `mori.dhall` at the repo root to register both packages
(library and jitsurei) — follow the kafka adapter's mori.dhall shape for the
two-package case. Run `mori register` to push the updated metadata and confirm via
`mori registry show shinzui/shibuya-message-db-adapter --full`.

Confirm: `cabal build shibuya-message-db-adapter-jitsurei` — expected to end with
`Linking .../basic-consumer ...`.

### Milestone 2: BasicConsumer, RetryDemo, DeadLetterDemo

At the end of this milestone, three examples run end-to-end against a local
Postgres seeded with test messages, each producing the transcript described in
*Validation and Acceptance*.

Replace `app/BasicConsumer.hs` with a real implementation. The module imports
`Shibuya.Adapter.MessageDb.messageDbAdapter`, `Shibuya.Adapter.MessageDb.Config
(defaultConfig, CategoryStream (..))`, the Shibuya core types, and
`MessageDb.Effectful` (for the Hasql runner). `main` runs `runEff .
MdbEff.runMessageDbHasql connSettings` where `connSettings` points at the dev
database `shibuya_message_db_adapter_dev`. Inside the runner it calls
`messageDbAdapter (defaultConfig (CategoryStream "jitsurei-basic"))` to obtain
`Adapter{source}`, then uses `Stream.fold Fold.drain $ Stream.mapM handlePrint
source` where `handlePrint` destructures the `Ingested`, prints a line of the form
`"[jitsurei-basic] id=<uuid> type=<T> pos=<N>"`, and calls `finalize AckOk`. The
exact Hasql settings constructor is whatever `MessageDb.Effectful` exports — read
`/Users/shinzui/Keikaku/work/libraries/haskell/message-db-hs-master/message-db-effectful/src/MessageDb/Effectful.hs`
before writing.

Implement `app/RetryDemo.hs`. Create a `TVar (Map UUID Int)` in `main`. The handler
atomically increments the count for the incoming message's UUID, prints a log line
`"[retry-demo] delivery <N> of message <uuid>"`, and returns `AckRetry (RetryDelay
2)` if the new count is `< 2`, else `AckOk`. Otherwise the wiring is identical to
BasicConsumer but targets category `jitsurei-retry`.

Implement `app/DeadLetterDemo.hs`. The handler inspects `Mdb.messageType` on the
payload, and if its text starts with `"Bad"` returns `AckDeadLetter (InvalidPayload
"demo: type starts with Bad")`; otherwise `AckOk`. The config passed to
`messageDbAdapter` is `(defaultConfig (CategoryStream "jitsurei-dlq")) {
deadLetter = DlqWriteToStream (Stream "demo-dlq") }`. Import `DlqStrategy (..)`,
`InvalidPayload`, and the `Stream` newtype from whichever modules EP-3 chose — if
EP-3's type names differ from these guesses, update the imports to match EP-3
exactly.

Add the cabal stanzas `retry-demo` and `dead-letter-demo`, identical in shape to
`basic-consumer`. `retry-demo` should add `containers, stm, uuid` to
`build-depends`.

Verify each example runs correctly per the Concrete Steps transcripts.

### Milestone 3: CheckpointRestart example

At the end of this milestone, `cabal run checkpoint-restart` runs two pipelines in
one process, proving EP-2's durable checkpoint.

`app/CheckpointRestart.hs` defines a local helper `runPhase :: Int -> Eff es ()`
that builds an adapter under a fixed subscription name (e.g., `SubscriptionName
"checkpoint-restart-demo"`), wraps `source` in `Stream.takeWhileM` gated on a
counter incremented per-message, drains the stream, then calls `shutdown` to force
a clean checkpoint flush. `main` calls `runEff . MdbEff.runMessageDbHasql
settings $ runPhase 5` twice in sequence, printing `"[checkpoint-restart] Phase 1:
consume 5, stop"` before the first call and `"Phase 2: consume 5 more"` before the
second. The crucial detail is that both phases use the same subscription name so
phase 2's checkpoint lookup finds the row phase 1 wrote.

Add the cabal stanza `checkpoint-restart` with `build-depends: stm` in addition to
the common deps.

### Milestone 4: MultiPartition example (or stub)

If EP-4 is not complete when this milestone begins, write `app/MultiPartition.hs`
as a stub:

    module Main (main) where
    main = putStrLn "[multi-partition] TODO: waiting on EP-4 (consumer groups)."

and record the date of stubbing in Progress.

If EP-4 is complete: `main` seeds 30 messages (6 categories, 5 messages each) via
psql or a Haskell helper; creates three `TVar [Message]`s `m0`, `m1`, `m2`; builds
three adapters `mkAdapter memberIdx = messageDbAdapter (defaultConfig
(CategoryStream "*")) { group = ConsumerGroupConfig { groupSize = 3, member =
memberIdx } }` (the exact `group` field and `ConsumerGroupConfig` constructor come
from EP-4); and runs all three under one `Shibuya.App.runApp` call, each processor
having a handler that appends to its member's TVar. After shutdown, the example
prints a summary: the categories each member saw, and the total count with
duplicate detection. Read
`/Users/shinzui/Keikaku/bokuno/shibuya-project/shibuya/shibuya-core/src/Shibuya/App.hs`
for the exact `runApp` signature and `Processor` constructor.

Add the cabal stanza `multi-partition`.

### Milestone 5: Integration test harness

At the end of this milestone, `test/TestEnv.hs` exists and a smoke test using it
passes.

Edit `shibuya-message-db-adapter/shibuya-message-db-adapter.cabal`. Extend the
existing test-suite stanza: add `other-modules` entries for `TestEnv` and each of
the `IntegrationTest.*` modules to be written in Milestone 6. Add to
`build-depends`: `ephemeral-pg`, `hasql`, `hasql-transaction`, `hasql-pool`,
`directory`, `filepath`, `stm`, `time`, `uuid-types`, `aeson`, `containers`,
`effectful`, `effectful-core`, `streamly`, `streamly-core`.

Write `test/TestEnv.hs` exporting:

    data TestEnv = TestEnv
      { connSettings :: Hasql.Connection.Settings
      , connection   :: Hasql.Connection.Connection
      }

    withTestEnv       :: (TestEnv -> IO a) -> IO a
    writeTestMessages :: TestEnv -> Text -> [Aeson.Value] -> IO ()

`withTestEnv` resolves `MESSAGE_DB_SQL_DIR` via `System.Environment.lookupEnv`
(defaulting to the corpus absolute path with a warning), calls `Pg.with` to get a
`Database`, acquires a Hasql `Connection` against `Pg.connectionSettings db`,
applies the message-db SQL files (schema, extension, tables, then sorted contents
of `functions/`, `indexes/`, `views/`) via `Hasql.Session.sql` against the
connection, calls `MessageDb.CheckpointStore.Db.migrate conn` (or the EP-2
equivalent), constructs `TestEnv`, and runs the user action. It handles
`Pg.with`'s `Either StartError a` return by rendering the error via
`Pg.renderStartError` and throwing.

`writeTestMessages env streamName payloads` runs `SELECT write_message(...)`
through Hasql for each payload, generating a fresh UUID and a fixed message type
`"TestEvent"` per row unless callers need otherwise. The implementation uses
`Hasql.Session` with a prepared statement taking six parameters (id, stream,
type, data, metadata, expected_version) matching message-db's `write_message`
function signature.

Expose `MESSAGE_DB_SQL_DIR` in `flake.nix`. Add an `env` attribute to
`devShells.default`:

    devShells.default = pkgs.mkShell {
      nativeBuildInputs = [ ... ];
      env = {
        MESSAGE_DB_SQL_DIR =
          "/Users/shinzui/Keikaku/hub/event-sourcing/message-db-project/message-db/database";
      };
    };

Equivalently, add a `shellHook = "export MESSAGE_DB_SQL_DIR=...";`. Either makes
the variable visible to `cabal test` (which inherits the shell environment).

Update `test/Main.hs` to add a `testGroup "integration"` containing a single
smoke test `testCase "TestEnv bootstrap" $ withTestEnv $ \_ -> pure ()`, plus
placeholders for Milestone 6 tests. Verify:

    cabal test shibuya-message-db-adapter

Expected: the four EP-1 convert tests plus `TestEnv bootstrap: OK`.

### Milestone 6: Integration tests

At the end of this milestone, all seven (or six if EP-4 incomplete) integration
tests pass.

Create one module per test under `test/IntegrationTest/<TestName>.hs`, each
exposing `test :: TestTree`. Each module follows a common shape: call
`withTestEnv`, seed messages via `writeTestMessages`, wire an adapter with a
handler that records observations into an `MVar` or `TVar`, run the adapter's
stream (possibly gated by `Stream.take` or a timeout), inspect the observations,
and assert with HUnit's `@?=` operator.

`BasicProduceConsume`: seed 10 messages to `orders-1`, collect payloads into an
`MVar [Message]`, run until length is 10, assert `map Mdb.globalPosition (reverse
received) == [1..10]`.

`CheckpointResume`: seed 10 messages, run phase 1 under subscription name
`"ckpt-test"` until 5 messages received, call `shutdown`, run phase 2 under the
same name until 5 more received. Assert positions of phase 1 are `[1..5]` and
phase 2 are `[6..10]`.

`RetryReDelivery`: seed 3 messages. The handler maintains a delivery-count
`MVar (Map UUID Int)` and a "has retried message #2?" flag; it returns `AckRetry
(RetryDelay 0.1)` exactly once for message #2, else `AckOk`. Wait for stable
state (e.g., no new messages for 2s). Assert the final map records `[1, 2, 1]`
counts for the three messages in order, and query the checkpoint store via Hasql
to confirm it equals message #3's position.

`DeadLetterSkipAndLog`: seed 3 messages, handler AckDeadLetters message #2 with
default strategy. Assert: all three messages were delivered to the handler
exactly once, checkpoint is at message #3's position, no rows exist with a
`stream_name` containing `"dlq"` or `"dead-letter"`.

`DeadLetterWriteToStream`: same seeding, but with `deadLetter = DlqWriteToStream
(Stream "test-dlq")`. Assert: checkpoint at message #3's position, and exactly
one row exists in `message_store.messages` with `stream_name = 'test-dlq'` whose
metadata JSON contains the original message #2's id. Query via Hasql with a
statement like `SELECT metadata FROM message_store.messages WHERE stream_name =
$1`.

`HaltPreservesCheckpoint`: seed 5 messages, handler returns `AckHalt
(Unrecoverable "test")` on message #3. Assert the stream terminates, the
checkpoint equals message #2's position (not #3 and not #4).

`ConsumerGroupExactlyOnce` (EP-4 only): seed 30 messages across 6 categories (5
per category). Run 3 in-process adapters under one `runApp` or three parallel
stream drains, each appending to its own `MVar [Message]`. Assert: the union has
exactly 30 distinct UUIDs; for each category, all 5 messages land in the same
`MVar`. If EP-4 is not complete, `test/Main.hs` should include this test as
`pendingIO "EP-4 not complete"` or omit it and include a `testCase "EP-4
consumer group test" (assertFailure "skipped: EP-4 pending")` with a clear
`PENDING` label.

Update `test/Main.hs` to include every integration test. Run `cabal test
shibuya-message-db-adapter` and confirm the transcript in *Validation and
Acceptance*.

### Milestone 7: Optional ShibuyaAppMultiProcessor example

`app/ShibuyaAppMultiProcessor.hs`: seed messages into two categories (e.g.,
`orders` and `shipments`); build two adapters via `messageDbAdapter`; run them
under one `Shibuya.App.runApp` as two `Processor` values with distinct
`ProcessorId`s `"orders-proc"` and `"shipments-proc"`; each processor has its own
handler printing `"[orders] <msg>"` or `"[shipments] <msg>"`. Consult
`/Users/shinzui/Keikaku/bokuno/shibuya-project/shibuya/shibuya-core/src/Shibuya/App.hs`
for the exact `Processor` record shape.

Add the cabal stanza `shibuya-app-multi-processor`. Verify `cabal run
shibuya-app-multi-processor` prints both prefixes interleaved as expected.


## Concrete Steps

All commands assume the working directory is
`/Users/shinzui/Keikaku/work/libraries/haskell/shibuya-message-db-adapter` unless
stated otherwise, run inside a `nix develop` shell.

Scaffolding (Milestone 1):

    mkdir -p shibuya-message-db-adapter-jitsurei/app
    cp shibuya-message-db-adapter/LICENSE shibuya-message-db-adapter-jitsurei/LICENSE

Then create the `.cabal` file per *Plan of Work*, update `cabal.project`, update
`mori.dhall`. Verify:

    cabal build shibuya-message-db-adapter-jitsurei

Expected (truncated):

    Configuring executable 'basic-consumer' for shibuya-message-db-adapter-jitsurei-0.1.0.0..
    Building executable 'basic-consumer' ...
    Linking .../basic-consumer ...

Running examples (Milestones 2-4). In shell 1:

    just process-up

In shell 2, seed and run. Add a `Justfile` recipe per example for reproducibility;
the recipe body is `psql -d shibuya_message_db_adapter_dev -c "SELECT
write_message(gen_random_uuid()::text, '<stream>', '<type>', '<json>', NULL,
NULL);"` repeated per seed message.

For `basic-consumer`:

    just seed-jitsurei-basic
    cabal run basic-consumer

Expected transcript (first ~6 lines):

    [basic-consumer] Starting...
    [jitsurei-basic] id=<uuid1> type=OrderPlaced pos=1
    [jitsurei-basic] id=<uuid2> type=OrderPlaced pos=2
    [jitsurei-basic] id=<uuid3> type=OrderPaid   pos=3
    (blocks on poll loop; Ctrl-C to exit)

For `retry-demo` with 2 seed messages:

    just seed-jitsurei-retry
    cabal run retry-demo

Expected transcript:

    [retry-demo] Starting...
    [retry-demo] delivery 1 of message <uuid1>
    [retry-demo] delivery 1 of message <uuid2>
    (~2s later)
    [retry-demo] delivery 2 of message <uuid1>
    [retry-demo] delivery 2 of message <uuid2>
    (blocks on poll loop)

For `dead-letter-demo` with 3 seed messages (Good, BadFormat, Good):

    just seed-jitsurei-dlq
    cabal run dead-letter-demo
    # in another shell:
    psql -d shibuya_message_db_adapter_dev -c \
      "SELECT stream_name, type FROM message_store.messages WHERE stream_name = 'demo-dlq';"

Expected psql output:

     stream_name |  type
    -------------+-----------
     demo-dlq    | DeadLetter
    (1 row)

For `checkpoint-restart` with 10 seed messages:

    just seed-jitsurei-checkpoint
    cabal run checkpoint-restart

Expected transcript:

    [checkpoint-restart] Phase 1: consume 5, stop
    [phase] got 1
    [phase] got 2
    [phase] got 3
    [phase] got 4
    [phase] got 5
    [checkpoint-restart] Phase 2: consume 5 more
    [phase] got 6
    [phase] got 7
    [phase] got 8
    [phase] got 9
    [phase] got 10
    [checkpoint-restart] Done.

For `multi-partition` (EP-4 complete) with 30 seed messages:

    just seed-jitsurei-partition
    cabal run multi-partition

Expected transcript (30 data lines omitted, followed by):

    [multi-partition] member 0: [cat1-*, cat4-*]
    [multi-partition] member 1: [cat2-*, cat5-*]
    [multi-partition] member 2: [cat3-*, cat6-*]
    [multi-partition] total: 30 messages, 0 duplicates

Running the integration tests (Milestones 5-6):

    cabal test shibuya-message-db-adapter

Expected transcript:

    Running 1 test suites...
    Test suite shibuya-message-db-adapter: RUNNING...
    shibuya-message-db-adapter
      ConvertTest
        empty metadata yields Nothing traceContext:    OK
        only traceparent yields one header:            OK
        traceparent + tracestate yields two headers:   OK
        non-object metadata yields Nothing:            OK
      integration
        TestEnv bootstrap:                             OK (0.45s)
        basicProduceConsume:                           OK (0.92s)
        checkpointResume:                              OK (1.10s)
        retryReDelivery:                               OK (1.03s)
        deadLetterSkipAndLog:                          OK (0.98s)
        deadLetterWriteToStream:                       OK (1.05s)
        haltPreservesCheckpoint:                       OK (0.87s)
        consumerGroupExactlyOnce:                      OK (2.10s)

    All 12 tests passed (9.50s)
    Test suite shibuya-message-db-adapter: PASS


## Validation and Acceptance

The plan is complete when, inside a `nix develop` shell with
`MESSAGE_DB_SQL_DIR` set and Postgres running via `just process-up`:

1. `cabal build all` succeeds with zero warnings attributable to this plan's
   changes.
2. Each of the five (or six if the optional seventh is pursued) jitsurei
   executables builds and produces the transcript shown in *Concrete Steps*.
   Specifically: `basic-consumer` prints one line per seeded message and then
   polls; `retry-demo` shows each message appearing twice with a ~2s gap;
   `dead-letter-demo` skips Good messages and writes Bad ones to the `demo-dlq`
   stream (verifiable via psql); `checkpoint-restart` prints a 1..5 then 6..10
   split across its two phases with no overlap; `multi-partition` (if EP-4
   complete) shows all 30 messages handled exactly once with sticky category
   assignment.
3. `cabal test shibuya-message-db-adapter` reports either 12 passed (EP-4
   complete) or 11 passed with `consumerGroupExactlyOnce` marked PENDING (EP-4
   not complete). Every other test passes outright.
4. `mori registry show shinzui/shibuya-message-db-adapter --full` returns metadata
   that includes both packages.

Non-goals: benchmarks, Hackage README polish, CI configuration. Those belong to
EP-6.


## Idempotence and Recovery

All scaffolding steps (writing cabal files, mori.dhall, Justfile recipes) are
idempotent — re-running them overwrites the same files with the same content.
`mori register` is safe to re-run.

Each jitsurei example is safe to run repeatedly: seeding writes new messages each
time (message-db messages are append-only), and each example either uses a fresh
subscription name per run or consumes idempotently.

The integration tests are fully isolated. Each test acquires a fresh ephemeral
Postgres via `Pg.with`, so there is no shared state between tests or between
runs. If a test hangs, wrap it in `Test.Tasty.localOption (Timeout 30_000_000
"30s")` to kill it cleanly.

If `cabal test` fails at the `TestEnv bootstrap` step because the SQL scripts at
`$MESSAGE_DB_SQL_DIR` are missing or malformed, the harness prints the exact file
path that failed to load. Set `MESSAGE_DB_SQL_DIR` to a valid directory (the
default vendored one at the absolute path noted above) and retry.

If `ephemeral-pg` fails to start because `initdb` is not on the PATH, ensure the
devShell in `flake.nix` includes `pkgs.postgresql` in `nativeBuildInputs` (it
already should from EP-1).

Rolling back the scaffold is simply `git reset --hard` to the commit before
Milestone 1; no external state is mutated (ephemeral-pg cleans up after itself,
and the jitsurei examples write only to the dev database, which can be dropped via
`just drop-database`).


## Interfaces and Dependencies

Libraries used:

- `ephemeral-pg` (from `shinzui/ephemeral-pg` at
  `/Users/shinzui/Keikaku/bokuno/ephemeral-pg-project/ephemeral-pg`) — temporary
  Postgres harness. Imported qualified as `EphemeralPg qualified as Pg`. Key
  functions: `Pg.with`, `Pg.withCached`, `Pg.connectionSettings`, `Pg.Database`,
  `Pg.renderStartError`.
- `hasql`, `hasql-transaction`, `hasql-pool` — SQL execution and connection
  management in the test harness.
- `tasty`, `tasty-hunit` — test runner.
- `message-db-hs`, `message-db-effectful` — already depended on by the adapter;
  used in tests and examples.
- `shibuya-core`, `shibuya-message-db-adapter` — the adapter under test.
- `aeson`, `uuid-types`, `containers`, `stm`, `streamly`, `effectful`,
  `directory`, `filepath` — example and test plumbing.

### New cabal packages

At the end of Milestone 1, the repository has two packages listed in
`cabal.project`: `./shibuya-message-db-adapter` (from EP-1) and
`./shibuya-message-db-adapter-jitsurei` (new).

### New cabal executables (in shibuya-message-db-adapter-jitsurei)

- `basic-consumer` → `app/BasicConsumer.hs`
- `retry-demo` → `app/RetryDemo.hs`
- `dead-letter-demo` → `app/DeadLetterDemo.hs`
- `checkpoint-restart` → `app/CheckpointRestart.hs`
- `multi-partition` → `app/MultiPartition.hs` (stubbed if EP-4 incomplete)
- `shibuya-app-multi-processor` (optional) → `app/ShibuyaAppMultiProcessor.hs`

### New test modules (in shibuya-message-db-adapter/test)

At the end of Milestone 5, the following must exist:

    module TestEnv where
      data TestEnv = TestEnv
        { connSettings :: Hasql.Connection.Settings
        , connection   :: Hasql.Connection.Connection
        }
      withTestEnv       :: (TestEnv -> IO a) -> IO a
      writeTestMessages :: TestEnv -> Text -> [Aeson.Value] -> IO ()

At the end of Milestone 6, the following exist, each exposing `test :: TestTree`:

    module IntegrationTest.BasicProduceConsume      where test :: TestTree
    module IntegrationTest.CheckpointResume         where test :: TestTree
    module IntegrationTest.RetryReDelivery          where test :: TestTree
    module IntegrationTest.DeadLetterSkipAndLog     where test :: TestTree
    module IntegrationTest.DeadLetterWriteToStream  where test :: TestTree
    module IntegrationTest.HaltPreservesCheckpoint  where test :: TestTree
    module IntegrationTest.ConsumerGroupExactlyOnce where test :: TestTree  -- EP-4

### Nix flake change

Edit `flake.nix` to expose `MESSAGE_DB_SQL_DIR` to every shell and to the test
environment. The simplest working form is an `env` attribute on the devShell:

    devShells.default = pkgs.mkShell {
      nativeBuildInputs = [ pkgs.postgresql pkgs.just pkgs.cabal-install
                            pkgs.pkg-config pkgs.process-compose pkgs.zlib
                            haskellPackages.haskell-language-server ];
      env = {
        MESSAGE_DB_SQL_DIR =
          "/Users/shinzui/Keikaku/hub/event-sourcing/message-db-project/message-db/database";
      };
    };

Equivalently, use `shellHook = "export MESSAGE_DB_SQL_DIR=...";`. Either sets the
variable whenever a user runs `nix develop` or direnv activates the shell. `cabal
test` inherits the shell environment, so tests see the value automatically.

### mori.dhall change

Extend the `packages` list in `mori.dhall` at the repo root to include a second
entry describing `shibuya-message-db-adapter-jitsurei`: its executables list and
its dependencies (the common-deps set above). After editing, run:

    mori register
    mori registry show shinzui/shibuya-message-db-adapter --full

and confirm both packages appear in the output.
