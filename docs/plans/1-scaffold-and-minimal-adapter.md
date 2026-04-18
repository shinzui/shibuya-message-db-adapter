# Scaffold shibuya-message-db-adapter and implement minimal polling adapter

MasterPlan: docs/masterplans/1-shibuya-message-db-adapter.md

Intention: intention_01kpgme50se0ranxp41ghfhajf

This ExecPlan is a living document. The sections Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.

This document is maintained in accordance with `.claude/skills/exec-plan/PLANS.md`.


## Purpose / Big Picture

After this plan is complete, the repository at
`/Users/shinzui/Keikaku/work/libraries/haskell/shibuya-message-db-adapter` builds a
Cabal package called `shibuya-message-db-adapter` that exposes a function
`messageDbAdapter` satisfying Shibuya's `Adapter` interface. A user can write a
`Handler es MessageDb.Message`, pair it with `messageDbAdapter` inside
`Shibuya.App.runApp`, and watch Shibuya print messages that the adapter is polling from
a local Postgres instance running message-db.

This first plan covers three things: scaffolding (cabal project, Haskell package layout,
Nix flake updates, Justfile, mori.dhall), a pure conversion function from
`MessageDb.Message` to `Shibuya.Envelope MessageDb.Message`, and a minimal polling
stream that uses the `message-db-effectful` effect to fetch category messages in
batches. Ack handling is stubbed at this stage — every `AckDecision` logs and moves a
**process-local** position forward. Durable checkpointing, retry, DLQ, partitioning,
and benchmarks are all explicit non-goals of this plan; see sibling plans in
`docs/plans/` for those concerns.

Observable outcome: from the repo root, `cabal build all` succeeds and
`cabal run shibuya-message-db-adapter-demo -- --category orders` prints a line for
each message in the `orders` category of a pre-seeded message-db database, then hangs
waiting for new messages on the configured poll interval.


## Progress

Use a checklist to summarize granular steps. Every stopping point must be documented
here, even if it requires splitting a partially completed task into two ("done" vs.
"remaining"). This section must always reflect the actual current state of the work.

### Milestone 1: Repository scaffolding

- [x] Verify nothing of value exists in the current project directory aside from
      `flake.nix`, `process-compose.yaml`, `db/`, and the seihou/claude skill tree.
      (Confirmed 2026-04-18: only Nix, seihou, claude, and `docs/plans/` exist.)
- [x] Write `cabal.project` at the repo root listing `./shibuya-message-db-adapter` and
      `packages:` entries for local dependencies (see Decision Log for why local paths
      were chosen over `source-repository-package`). (2026-04-18)
- [x] Create the directory `shibuya-message-db-adapter/` with `src/Shibuya/Adapter/MessageDb`
      and `test/` subdirectories. (2026-04-18)
- [x] Write `shibuya-message-db-adapter/shibuya-message-db-adapter.cabal` with one
      library stanza (exposing four modules, three of which are empty stubs) and one
      test-suite stanza. (2026-04-18)
- [x] Confirm `cabal build shibuya-message-db-adapter` compiles the empty skeleton.
      (2026-04-18: all four modules compile.)

### Milestone 2: mori.dhall and Justfile

- [x] Write `mori.dhall` at the repo root registering the project with mori under the
      name `shinzui/shibuya-message-db-adapter`. (2026-04-18)
- [x] Write a `Justfile` at the repo root with recipes `process-up`, `process-down`,
      `psql`, `create-database`, `drop-database`, `bootstrap-message-db`,
      `seed-messages`, `build`, `test`, `clean`, `fmt`. (2026-04-18)
- [x] Run `mori register` and confirm
      `mori registry show shinzui/shibuya-message-db-adapter --full` returns the project.
      (2026-04-18)

### Milestone 3: Flake updates and dev loop

- [x] `flake.nix` already includes `zlib`, `postgresql`, `process-compose`, `just`,
      `cabal-install`, `pkg-config`, and `haskell-language-server` as
      `nativeBuildInputs` (scaffolded previously by the `nix-haskell-flake` seihou
      module). Rename `$PGDATABASE` from `shibuya-message-db-adapter` (dashes need
      quoting in psql and message-db install scripts) to
      `shibuya_message_db_adapter_dev`. (2026-04-18)
- [x] `process-compose.yaml`'s `create_schema` process already runs
      `just create-database`, which now chains to `bootstrap-message-db` and applies
      the message-db schema from `$MESSAGE_DB_ROOT/database/install.sh` with
      `CREATE_DATABASE=off`. (2026-04-18)
- [x] Verify `nix develop` yields `psql`, `just`, `cabal`, `process-compose`.
      (2026-04-18: all four resolve to nix store paths.)
- [x] Verify bootstrap works: `pg_ctl start` + `just bootstrap-message-db` installs
      the schema and `psql -c '\dt message_store.*'` lists the `messages` table.
      (2026-04-18: confirmed against message-db 1.3.0.)

### Milestone 4: Conversion module

- [x] Implement `Shibuya.Adapter.MessageDb.Convert.messageToEnvelope` with the exact
      signature and field mapping specified under *Interfaces and Dependencies*.
      (2026-04-18) Uses OverloadedRecordDot for accessor syntax, mirroring
      shibuya-kafka-adapter's convention.
- [x] Write pure unit tests in `test/Shibuya/Adapter/MessageDb/ConvertTest.hs`
      exercising empty metadata, traceparent-only, traceparent+tracestate, and
      non-object metadata. (2026-04-18)
- [x] Run `cabal test shibuya-message-db-adapter` and verify 4 tests pass.
      (2026-04-18: `All 4 tests passed (0.00s)`.)

### Milestone 5: Config module and adapter skeleton

- [ ] Implement `Shibuya.Adapter.MessageDb.Config` with `MessageDbAdapterConfig`,
      `defaultConfig`, and the newtype wrappers listed under *Interfaces and Dependencies*.
- [ ] Implement `Shibuya.Adapter.MessageDb.Internal.messageDbSource` — a Streamly stream
      that repeatedly calls `getCategoryMessages` and emits `Ingested es MessageDb.Message`
      values with a stub ack handle.
- [ ] Implement `Shibuya.Adapter.MessageDb.messageDbAdapter` returning
      `Adapter es MessageDb.Message`.
- [ ] Compile with `cabal build shibuya-message-db-adapter`.

### Milestone 6: Demo executable and end-to-end verification

- [ ] Add an `executable shibuya-message-db-adapter-demo` stanza to the cabal file.
- [ ] Write `app/Demo.hs` that runs the adapter under `runApp` with a print handler.
- [ ] From a `nix develop` shell with `just process-up` running, seed the `orders`
      category with three test messages via `psql` (exact commands below).
- [ ] Run `cabal run shibuya-message-db-adapter-demo -- --category orders` and confirm
      three lines print to stdout, followed by the process waiting on the poll loop.
- [ ] Press Ctrl-C and observe graceful shutdown (no stack trace, exit code 0 or 130).


## Surprises & Discoveries

- **2026-04-18 — Dependency resolution required several `allow-newer` and one
  `allow-older` entry.** message-db-effectful pins `containers ^>=0.6` but GHC
  9.12.2 ships `containers 0.7`; `tan-event-source` pins `generic-lens ^>=2.2`
  while `shibuya-core` needs `^>=2.3`; `tan-hasql` pins `hasql ^>=1.5` while
  `shibuya-core` needs `hasql 1.10`; and `shibuya-core` pins `text ^>=2.1.3`
  while GHC 9.12.2's boot text is 2.1.2. Each mismatch is a cosmetic upper
  bound in the dependency; relaxing them in `cabal.project` via
  `allow-newer`/`allow-older` is the standard fix. The full list lives in the
  repo's `cabal.project`. Evidence: `cabal build shibuya-message-db-adapter`
  failed with `[Cabal-7107]` on each constraint in turn until the bound was
  relaxed.

- **2026-04-18 — `Shibuya.Core.Ingested.Ingested` has three fields, not two.**
  The plan's Context & Orientation section described it as
  `Ingested { envelope, ack }`, but the real record also has
  `lease :: !(Maybe (Lease es))`. The kafka adapter passes `lease = Nothing`.
  We will do the same — message-db has no visibility-timeout concept.


## Decision Log

- Decision: The adapter's payload type is `MessageDb.Message` (from `message-db-hs`),
  not `Aeson.Value` or a decoded domain type.
  Rationale: message-db messages carry their stream name, position, global position,
  message type, and metadata — users' handlers need that to route by `messageType`.
  Decoding the message's `data` field into a domain type is a handler-level concern.
  Date: 2026-04-18

- Decision: Ack handling is a stub in EP-1 (a process-local `IORef MessagePosition` that
  advances on `AckOk`, logs on any other decision). Durable checkpointing is deferred to
  EP-2.
  Rationale: Keeps EP-1 small enough to verify end-to-end in one sitting. Splitting
  stubbed ack from durable ack keeps the diffs reviewable and the user can see progress
  with a working adapter before EP-2 layers persistence on top.
  Date: 2026-04-18

- Decision: `cabal.project` references shibuya-core, message-db-hs,
  message-db-effectful, and their transitive dependencies (hasql-effectful,
  tan-event-source, tan-commons-*, tan-hasql) via absolute local paths rather
  than `source-repository-package` entries.
  Rationale: the required versions are not all on Hackage (hasql 1.10
  ecosystem, tan-* family, hs-opentelemetry for GHC 9.12), and the repository
  has no stable tag to pin. Mixing local paths for working-tree packages with
  `source-repository-package` entries for published commits (hs-opentelemetry,
  hasql, hasql-pool, hasql-transaction, hasql-migration) keeps the build
  reproducible without duplicating checkouts. When publishing this package to
  Hackage, swap to `source-repository-package` entries that pin release
  commits.
  Date: 2026-04-18

- Decision: Integration tests (against ephemeral-pg) are a non-goal of EP-1. Unit tests
  cover the pure conversion; end-to-end verification is manual via the demo executable
  in this plan. EP-5 introduces the integration test harness.
  Rationale: Setting up `ephemeral-pg` plus the message-db schema bootstrap is a chunk
  of work that naturally belongs with the jitsurei examples (EP-5), which also need
  this harness. Doing it twice would be waste.
  Date: 2026-04-18


## Outcomes & Retrospective

(To be filled during and after implementation.)


## Context and Orientation

The reader is assumed to know Haskell and Cabal but not this repository. Orient
yourself as follows.

**The repository under work** is at
`/Users/shinzui/Keikaku/work/libraries/haskell/shibuya-message-db-adapter`. It is
currently almost empty: a Nix `flake.nix`, a `process-compose.yaml` that starts a
direnv-managed Postgres, a `db/` directory where Postgres stores its cluster files, a
`.claude/` skill tree, and a `.seihou/` config. No Haskell sources exist yet.

**Shibuya** is a queue-processing framework at
`/Users/shinzui/Keikaku/bokuno/shibuya-project/shibuya`. Its core package
`shibuya-core` defines the types every adapter implements:

- `Shibuya.Adapter.Adapter es msg` — the record the framework consumes:

        data Adapter es msg = Adapter
          { adapterName :: !Text
          , source      :: Stream (Eff es) (Ingested es msg)
          , shutdown    :: Eff es ()
          }

- `Shibuya.Core.Types.Envelope msg` — the message wrapper:

        data Envelope msg = Envelope
          { messageId    :: !MessageId
          , cursor       :: !(Maybe Cursor)
          , partition    :: !(Maybe Text)
          , enqueuedAt   :: !(Maybe UTCTime)
          , traceContext :: !(Maybe TraceHeaders)
          , payload      :: !msg
          }

  where `MessageId = MessageId Text`, `Cursor = CursorInt Int | CursorText Text`, and
  `TraceHeaders = [(ByteString, ByteString)]`.

- `Shibuya.Core.AckHandle.AckHandle es` — a callback the adapter attaches to each
  `Ingested`:

        newtype AckHandle es = AckHandle
          { finalize :: AckDecision -> Eff es ()
          }

- `Shibuya.Core.Ack.AckDecision` — what the user's handler returns:

        data AckDecision
          = AckOk
          | AckRetry !RetryDelay
          | AckDeadLetter !DeadLetterReason
          | AckHalt !HaltReason

- `Shibuya.Core.Ingested es msg` — what the source yields:

        data Ingested es msg = Ingested
          { envelope :: Envelope msg
          , ack      :: AckHandle es
          }

Because `shibuya-core` defines its types with `NoFieldSelectors`, **do not** use record
dot syntax (`x.field`) across package boundaries; use explicit record pattern matching
(`Ingested{envelope, ack = AckHandle finalize}`).

**message-db-hs** is at `/Users/shinzui/Keikaku/work/libraries/haskell/message-db-hs-master`.
Its sub-packages relevant here:

- `message-db-hs` (library) — defines the `Message` record (full fields listed below).
- `message-db-effectful` (library) — defines a `MessageDb` Effectful effect with a
  `getCategoryMessages :: GetCategoryMessagesQuery -> MessageDb (Eff es) (Vector Message)`
  operation and a Postgres interpreter `runMessageDbHasql`.

The `MessageDb.Message.Message` record has these fields:

    data Message = Message
      { messageId       :: !MessageId         -- newtype over UUID
      , stream          :: !Stream            -- category-entity, or category-only
      , messageType     :: !MessageType       -- Text
      , position        :: !MessagePosition   -- Int64 stream-local ordinal
      , globalPosition  :: !GlobalPosition    -- Int64 store-wide
      , messageData     :: !MessageData       -- JSONB payload
      , messageMetadata :: !MessageMetadata   -- JSONB (may contain W3C headers)
      , time            :: !UTCTime
      }

The conversion from `MessageDb.Message` to `Envelope MessageDb.Message` is the hot path
this plan introduces. Global position is monotonic across the whole store and is the
right value to put in `cursor`.

**Local sibling package** (not a dependency of this plan but referenced for context):
`/Users/shinzui/Keikaku/bokuno/shibuya-project/shibuya-kafka-adapter/` is the other
existing adapter. Its module layout (`Shibuya.Adapter.Kafka`, `.Kafka.Config`,
`.Kafka.Convert`, `.Kafka.Internal`) is the pattern to mirror.


## Plan of Work

### Milestone 1: Repository scaffolding

At the end of this milestone, `cabal build shibuya-message-db-adapter` compiles an empty
skeleton and the repository layout matches the other shibuya adapters.

Starting state: the repository has only Nix, seihou, and `db/` artefacts. There is no
`cabal.project`, no `.cabal` file, no `src/`.

Write `cabal.project` at the repo root containing:

    packages:
      ./shibuya-message-db-adapter

    source-repository-package
      type: git
      location: https://github.com/shinzui/shibuya
      subdir: shibuya-core
      tag: master

    source-repository-package
      type: git
      location: https://github.com/topagentnetwork/message-db-hs
      subdir: message-db-hs
      tag: master

    source-repository-package
      type: git
      location: https://github.com/topagentnetwork/message-db-hs
      subdir: message-db-effectful
      tag: master

(If the local copies of `shibuya-core` and `message-db-hs` are preferred for
development, use `../bokuno/shibuya-project/shibuya/shibuya-core` and the
`message-db-hs-master/message-db-hs` directory as `packages:` entries instead. Document
whichever choice is made in the Decision Log.)

Create the directory tree:

    shibuya-message-db-adapter/
      shibuya-message-db-adapter.cabal
      src/Shibuya/Adapter/MessageDb.hs
      src/Shibuya/Adapter/MessageDb/Config.hs
      src/Shibuya/Adapter/MessageDb/Convert.hs
      src/Shibuya/Adapter/MessageDb/Internal.hs
      test/Main.hs
      test/ConvertTest.hs

Write `shibuya-message-db-adapter.cabal` with a library stanza listing the four modules
under `exposed-modules`, with `build-depends` including `base`, `text`, `bytestring`,
`containers`, `aeson`, `time`, `uuid-types`, `vector`, `streamly`, `effectful-core`,
`effectful`, `shibuya-core`, `message-db-hs`, `message-db-effectful`. Default
language `GHC2021`; common warning flags `-Wall -Wcompat -Wincomplete-record-updates
-Wincomplete-uni-patterns -Wredundant-constraints -Wmissing-export-lists
-Wpartial-fields`.

Write a test-suite stanza of type `exitcode-stdio-1.0` with `test/Main.hs` as its main
and `ConvertTest` as `other-modules`, depending on `base`, `tasty`, `tasty-hunit`,
`shibuya-message-db-adapter`, `message-db-hs`.

Each of the three implementation modules begins as an empty stub:

    module Shibuya.Adapter.MessageDb.Convert () where

The root module `Shibuya.Adapter.MessageDb` re-exports nothing yet.

### Milestone 2: mori.dhall and Justfile

At the end of this milestone, `mori registry show shinzui/shibuya-message-db-adapter --full`
returns the project metadata and `just --list` shows the expected recipes.

Copy the structure of
`/Users/shinzui/Keikaku/bokuno/shibuya-project/shibuya-kafka-adapter/mori.dhall` and
adapt it. Name the project `shinzui/shibuya-message-db-adapter`, description
"message-db adapter for the Shibuya queue processing framework", domains
`["concurrency", "queue-processing", "event-sourcing"]`. The package stanza should list
the four exposed modules and dependencies
`effectful/effectful`, `composewell/streamly`, `shinzui/shibuya`, `tan/message-db-hs`,
`hasql/hasql`. Top-level `dependencies` list should mirror the package's `dependencies`.

Register with mori. The exact command is

    cd /Users/shinzui/Keikaku/work/libraries/haskell/shibuya-message-db-adapter
    mori register

Verify with

    mori registry show shinzui/shibuya-message-db-adapter --full

Write a `Justfile` at the repo root mirroring the kafka adapter's
`/Users/shinzui/Keikaku/bokuno/shibuya-project/shibuya-kafka-adapter/Justfile`, with
these recipe groups:

- `services`: `process-up`, `process-down`, `psql` (convenience to open a psql session).
- `db`: `create-database`, `drop-database`, `seed-messages CATEGORY` (inserts a few
  sample messages via the message-db `write_message` SQL function).
- `build`: `build`, `test`, `clean`, `fmt` (using `nix fmt`).

### Milestone 3: Flake updates and dev loop

At the end of this milestone, `nix develop` gives a working shell with the toolchain
and `just process-up` starts Postgres with the message-db schema installed.

Edit `flake.nix` to add (as `nativeBuildInputs`): `pkgs.zlib`, `pkgs.postgresql`,
`pkgs.just`, `pkgs.cabal-install`, `pkgs.pkg-config`, `pkgs.process-compose`,
`haskellPackages.ghcWithPackages (ps: [ ps.haskell-language-server ])`.

Edit `process-compose.yaml`:

- Keep the existing `postgres` process.
- Modify the `create_schema` process to run `just bootstrap-message-db` which:
  1. Creates the database `shibuya_message_db_adapter_dev`.
  2. Runs every `.sql` file in
     `/Users/shinzui/Keikaku/hub/event-sourcing/message-db-project/message-db/database`
     in the order documented in that project's install instructions.

Document the bootstrap recipe in the Justfile:

    bootstrap-message-db:
        createdb $PGDATABASE || true
        for f in /Users/shinzui/Keikaku/hub/event-sourcing/message-db-project/message-db/database/*.sql; do \
          psql -f "$f"; \
        done

Run `direnv allow && nix develop`, then `just process-up` and confirm
`psql -c '\dt message_store.*'` lists the `messages` table and related message-db objects.

### Milestone 4: Conversion module

At the end of this milestone, `Shibuya.Adapter.MessageDb.Convert` is implemented and
covered by four unit tests that all pass.

In `src/Shibuya/Adapter/MessageDb/Convert.hs`, define:

    module Shibuya.Adapter.MessageDb.Convert
      ( messageToEnvelope
      , extractTraceContext
      ) where

    import Shibuya.Core.Types (Envelope (..), MessageId (..), Cursor (..), TraceHeaders)
    import qualified MessageDb.Message as Mdb
    import qualified Data.Aeson as Aeson
    import qualified Data.Aeson.KeyMap as KeyMap
    import qualified Data.ByteString.Char8 as BS8
    import qualified Data.Text.Encoding as TE
    import qualified Data.UUID as UUID

    messageToEnvelope :: Mdb.Message -> Envelope Mdb.Message
    messageToEnvelope m =
      Envelope
        { messageId    = MessageId (UUID.toText (Mdb.unMessageId (Mdb.messageId m)))
        , cursor       = Just (CursorInt (fromIntegral (Mdb.unGlobalPosition (Mdb.globalPosition m))))
        , partition    = Nothing
        , enqueuedAt   = Just (Mdb.time m)
        , traceContext = extractTraceContext (Mdb.messageMetadata m)
        , payload      = m
        }

    extractTraceContext :: Mdb.MessageMetadata -> Maybe TraceHeaders
    extractTraceContext md =
      case Mdb.unMessageMetadata md of
        Aeson.Object obj ->
          let lookupStr k = case KeyMap.lookup k obj of
                Just (Aeson.String t) -> Just (BS8.pack (show k), TE.encodeUtf8 t)
                _ -> Nothing
              traceparent = lookupStr "traceparent"
              tracestate  = lookupStr "tracestate"
          in case traceparent of
               Nothing -> Nothing
               Just tp -> Just (tp : maybe [] pure tracestate)
        _ -> Nothing

Note: the exact constructor names for `MessageId`, `GlobalPosition`, and
`MessageMetadata` must match what `message-db-hs` exports. If the actual module
exposes smart constructors or accessor functions instead of record selectors, adjust
the code above accordingly. Read the module at
`/Users/shinzui/Keikaku/work/libraries/haskell/message-db-hs-master/message-db-hs/src/MessageDb/Message.hs`
before writing this code and update the `import qualified MessageDb.Message as Mdb`
imports to match its export list.

In `test/ConvertTest.hs`, define four `testCase`s:

1. *"empty metadata yields Nothing traceContext"* — construct a `Message` whose
   `messageMetadata` is `Aeson.Object KeyMap.empty`, convert, assert
   `traceContext result == Nothing`.
2. *"only traceparent yields one header"* — metadata `{"traceparent": "00-..."}`, assert
   `traceContext result == Just [("traceparent", "00-...")]`.
3. *"traceparent + tracestate yields two headers in order"* — metadata with both, assert
   two headers with `traceparent` first.
4. *"non-object metadata yields Nothing"* — metadata `Aeson.String "oops"`, assert
   `traceContext result == Nothing` and the call does not throw.

In `test/Main.hs`:

    module Main (main) where

    import qualified ConvertTest
    import Test.Tasty

    main :: IO ()
    main = defaultMain $ testGroup "shibuya-message-db-adapter" [ConvertTest.tests]

Run `cabal test shibuya-message-db-adapter` and confirm all four tests pass.

### Milestone 5: Config module and adapter skeleton

At the end of this milestone, `Shibuya.Adapter.MessageDb.messageDbAdapter` compiles and
returns an `Adapter` whose `source` is a working polling stream with stub ack handling.

In `src/Shibuya/Adapter/MessageDb/Config.hs`:

    module Shibuya.Adapter.MessageDb.Config
      ( MessageDbAdapterConfig (..)
      , CategoryStream (..)
      , BatchSize (..)
      , PollInterval (..)
      , DrainTimeout (..)
      , defaultConfig
      ) where

    import Data.Text (Text)
    import Data.Time.Clock (NominalDiffTime)

    newtype CategoryStream = CategoryStream { unCategoryStream :: Text }
      deriving (Eq, Show)

    newtype BatchSize = BatchSize { unBatchSize :: Int }
      deriving (Eq, Show)

    newtype PollInterval = PollInterval { unPollInterval :: NominalDiffTime }
      deriving (Eq, Show)

    newtype DrainTimeout = DrainTimeout { unDrainTimeout :: NominalDiffTime }
      deriving (Eq, Show)

    data MessageDbAdapterConfig = MessageDbAdapterConfig
      { category     :: !CategoryStream
      , batchSize    :: !BatchSize
      , pollInterval :: !PollInterval
      , drainTimeout :: !DrainTimeout
      }

    defaultConfig :: CategoryStream -> MessageDbAdapterConfig
    defaultConfig cat = MessageDbAdapterConfig
      { category     = cat
      , batchSize    = BatchSize 100
      , pollInterval = PollInterval 0.5
      , drainTimeout = DrainTimeout 10
      }

In `src/Shibuya/Adapter/MessageDb/Internal.hs`, implement `messageDbSource` as a
Streamly stream that:

1. Reads `IORef MessagePosition` as the "next position to fetch" (seeded to `0`).
2. Repeatedly invokes `MessageDb.getCategoryMessages` for the configured category,
   batch size, and current position.
3. For each returned `Message`, yields `Ingested` with
   `envelope = messageToEnvelope m` and a stub `AckHandle` whose `finalize`
   logs the decision and, on `AckOk`, advances a separate `IORef` holding the highest
   acknowledged position.
4. When `getCategoryMessages` returns empty, sleeps for `pollInterval` then retries.
5. Uses `Control.Concurrent.STM.TVar Bool` as a shutdown signal; `takeWhileM` over the
   TVar gates the stream.

Exact module skeleton:

    module Shibuya.Adapter.MessageDb.Internal
      ( messageDbSource
      , mkStubAckHandle
      ) where

    -- imports ...

    messageDbSource ::
      (MessageDb :> es, Concurrent :> es, IOE :> es) =>
      TVar Bool ->                      -- ^ shutdown signal
      IORef MessagePosition ->          -- ^ next position cursor
      MessageDbAdapterConfig ->
      Stream (Eff es) (Ingested es Mdb.Message)

    mkStubAckHandle ::
      (IOE :> es) =>
      Mdb.Message -> AckHandle es

In `src/Shibuya/Adapter/MessageDb.hs` export the public constructor:

    module Shibuya.Adapter.MessageDb
      ( messageDbAdapter
      , module Shibuya.Adapter.MessageDb.Config
      ) where

    messageDbAdapter ::
      (MessageDb :> es, Concurrent :> es, IOE :> es) =>
      MessageDbAdapterConfig ->
      Eff es (Adapter es Mdb.Message)

The implementation creates the shutdown `TVar`, the position `IORef`, builds the
stream, and returns:

    pure Adapter
      { adapterName = "message-db"
      , source      = messageDbSource shutdownSignal positionRef cfg
      , shutdown    = atomically $ writeTVar shutdownSignal True
      }

Confirm compilation with `cabal build shibuya-message-db-adapter`.

### Milestone 6: Demo executable and end-to-end verification

At the end of this milestone, a user can run the demo executable and watch it print
messages from a seeded local database.

Add an executable stanza to the cabal file:

    executable shibuya-message-db-adapter-demo
      hs-source-dirs:   app
      main-is:          Demo.hs
      build-depends:    base, shibuya-message-db-adapter, shibuya-core,
                        message-db-hs, message-db-effectful, effectful,
                        optparse-applicative, text, aeson
      default-language: GHC2021
      ghc-options:      -Wall -threaded

Write `app/Demo.hs`:

    module Main (main) where

    import Shibuya.App (runApp, SupervisionStrategy (..))
    import Shibuya.Adapter.MessageDb (messageDbAdapter, defaultConfig, CategoryStream (..))
    import Shibuya.Core.Ack (AckDecision (..))
    import Shibuya.Core.Ingested (Ingested (..))
    import qualified MessageDb.Message as Mdb
    import qualified MessageDb.Effectful as MdbEff
    import Effectful (Eff, IOE, runEff)
    import Effectful.Concurrent (runConcurrent)
    import qualified Options.Applicative as Opt
    import qualified Data.Text as Text

    data Args = Args { category :: Text }

    argsParser :: Opt.Parser Args
    argsParser = Args <$> Opt.strOption (Opt.long "category" <> Opt.metavar "CATEGORY")

    handlePrint :: IOE :> es => Ingested es Mdb.Message -> Eff es AckDecision
    handlePrint Ingested{envelope} = do
      liftIO $ putStrLn $ "message: " <> show (Mdb.stream (payload envelope))
                         <> " pos " <> show (Mdb.globalPosition (payload envelope))
      pure AckOk

    main :: IO ()
    main = do
      Args{category} <- Opt.execParser (Opt.info (argsParser <**> Opt.helper) Opt.fullDesc)
      runEff . runConcurrent . MdbEff.runMessageDbHasql myConnectionSettings $ do
        adapter <- messageDbAdapter (defaultConfig (CategoryStream category))
        -- simplified direct stream consumption for the demo
        Stream.fold (Fold.drainMapM handlePrint) (source adapter)

(The exact effect-runner stack for `message-db-effectful` is whatever
`MessageDb.Effectful` exports for a Hasql-backed Postgres interpreter. Read
`/Users/shinzui/Keikaku/work/libraries/haskell/message-db-hs-master/message-db-effectful/src/MessageDb/Effectful.hs`
for the right runner name.)

Verify end-to-end:

1. In one shell: `nix develop`, then `just process-up`.
2. In another shell: `nix develop`, then

        just seed-messages orders

   where `seed-messages CATEGORY` writes three messages via `SELECT write_message(...)`.
   Implement this recipe to call:

        psql -c "SELECT write_message(gen_random_uuid()::text, 'orders-1', 'OrderPlaced', '{\"id\": 1}'::jsonb, NULL, NULL);"
        psql -c "SELECT write_message(gen_random_uuid()::text, 'orders-1', 'OrderPlaced', '{\"id\": 2}'::jsonb, NULL, NULL);"
        psql -c "SELECT write_message(gen_random_uuid()::text, 'orders-2', 'OrderPlaced', '{\"id\": 3}'::jsonb, NULL, NULL);"

3. Run the demo:

        cabal run shibuya-message-db-adapter-demo -- --category orders

4. Expected output — three `message: orders-...` lines, then the process blocks on the
   poll loop. Ctrl-C to exit.


## Concrete Steps

Run from `/Users/shinzui/Keikaku/work/libraries/haskell/shibuya-message-db-adapter`.

Scaffolding:

    mkdir -p shibuya-message-db-adapter/src/Shibuya/Adapter/MessageDb
    mkdir -p shibuya-message-db-adapter/test
    mkdir -p shibuya-message-db-adapter/app

Create `cabal.project`, `shibuya-message-db-adapter/shibuya-message-db-adapter.cabal`,
`mori.dhall`, `Justfile` using the content in *Plan of Work*.

Verify compile:

    cabal build shibuya-message-db-adapter

Expected output ends with `Linking ...` or `Registering library ...`, exit 0.

Run the unit tests:

    cabal test shibuya-message-db-adapter

Expected output:

    shibuya-message-db-adapter-tests
      empty metadata yields Nothing traceContext: OK
      only traceparent yields one header:         OK
      traceparent + tracestate yields two:        OK
      non-object metadata yields Nothing:         OK

    All 4 tests passed

Register mori:

    mori register
    mori registry show shinzui/shibuya-message-db-adapter --full

End-to-end demo run (shell 1):

    nix develop
    just process-up

Shell 2:

    nix develop
    just seed-messages orders
    cabal run shibuya-message-db-adapter-demo -- --category orders


## Validation and Acceptance

The plan is complete when:

1. `cabal build all` succeeds with zero warnings relevant to this package.
2. `cabal test shibuya-message-db-adapter` reports all 4 tests passing.
3. `mori registry show shinzui/shibuya-message-db-adapter --full` returns the project.
4. Given a seeded database with three `orders-*` messages, the demo executable prints
   three lines and then blocks on the poll loop; Ctrl-C terminates cleanly.
5. `shibuya-message-db-adapter/src/Shibuya/Adapter/MessageDb/` contains exactly four
   `.hs` files matching the names above.

Non-goals (explicit): checkpoint persistence, retry, DLQ, consumer groups, integration
tests, benchmarks, Hackage README polish. Any of those attempted here should be pushed
to the appropriate sibling plan.


## Idempotence and Recovery

Scaffolding steps are idempotent: writing `cabal.project`, `.cabal`, and `.dhall` files
overwrites whatever was there. `mori register` is safe to re-run; it updates the
existing registration.

`just bootstrap-message-db` is safe to re-run: `createdb` is guarded by `|| true`, and
the message-db SQL scripts use `CREATE SCHEMA IF NOT EXISTS` / `CREATE OR REPLACE
FUNCTION`.

If `cabal build` fails after a checkout, `cabal clean && cabal update && cabal build`
is the safe retry. If `just process-up` fails due to a stale `.dev/process-compose.sock`,
`rm -f .dev/process-compose.sock` and retry.

Rolling back the scaffold is simply `git reset --hard` on the feature branch; no
external state changes are made outside the direnv-managed Postgres, which can be
dropped with `just drop-database`.


## Interfaces and Dependencies

Libraries used:

- `shibuya-core` (from `github: shinzui/shibuya`) — provides `Adapter`, `Envelope`,
  `AckDecision`, `Ingested`. Import via
  `Shibuya.Adapter` (the `Adapter` type), `Shibuya.Core.Types` (Envelope, MessageId,
  Cursor, TraceHeaders), `Shibuya.Core.Ack`, `Shibuya.Core.AckHandle`, `Shibuya.Core.Ingested`.
- `message-db-hs` — the `MessageDb.Message` type and its field accessors.
- `message-db-effectful` — the `MessageDb` Effectful effect and its Postgres interpreter.
- `streamly` — stream construction (`Streamly.Data.Stream`).
- `effectful-core`, `effectful` — effect system.
- `aeson`, `uuid-types`, `time`, `text`, `bytestring` — for the conversion and metadata
  handling.
- `tasty`, `tasty-hunit` (test only).
- `optparse-applicative` (demo only).

At the end of Milestone 4, the following must exist:

    module Shibuya.Adapter.MessageDb.Convert where
      messageToEnvelope :: MessageDb.Message -> Shibuya.Core.Types.Envelope MessageDb.Message
      extractTraceContext :: MessageDb.MessageMetadata -> Maybe Shibuya.Core.Types.TraceHeaders

At the end of Milestone 5, the following must exist:

    module Shibuya.Adapter.MessageDb.Config where
      data MessageDbAdapterConfig = MessageDbAdapterConfig
        { category     :: !CategoryStream
        , batchSize    :: !BatchSize
        , pollInterval :: !PollInterval
        , drainTimeout :: !DrainTimeout
        }
      defaultConfig :: CategoryStream -> MessageDbAdapterConfig

    module Shibuya.Adapter.MessageDb.Internal where
      messageDbSource ::
        (MessageDb :> es, Concurrent :> es, IOE :> es) =>
        TVar Bool ->
        IORef MessagePosition ->
        MessageDbAdapterConfig ->
        Stream (Eff es) (Ingested es MessageDb.Message)

    module Shibuya.Adapter.MessageDb where
      messageDbAdapter ::
        (MessageDb :> es, Concurrent :> es, IOE :> es) =>
        MessageDbAdapterConfig ->
        Eff es (Adapter es MessageDb.Message)
