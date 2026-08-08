# Justfile for shibuya-message-db-adapter

# Default recipe to display help
default:
    @just --list


# --- Services ---

# Start Postgres via process-compose (foreground; Ctrl-C to stop)
[group("services")]
process-up:
    process-compose --tui=false --unix-socket .dev/process-compose.sock up

# Stop Postgres
[group("services")]
process-down:
    process-compose --unix-socket .dev/process-compose.sock down || true

# Open a psql session to the development database
[group("services")]
psql:
    psql


# --- Database ---

# Create the dev database and apply the message-db schema.
# `createdb` is idempotent via `|| true`; message-db's SQL scripts are
# themselves idempotent (CREATE SCHEMA IF NOT EXISTS / CREATE OR REPLACE).
[group("db")]
create-database: bootstrap-message-db

# Drop the dev database (use when you want a clean slate).
[group("db")]
drop-database:
    dropdb --if-exists $PGDATABASE

# Create the database (if missing) and install the message-db schema from the
# local checkout under $MESSAGE_DB_ROOT. This recipe is the glue used by the
# `create_schema` process in process-compose.yaml.
#
# We `createdb` ourselves (idempotently) and set CREATE_DATABASE=off so the
# message-db install script installs only the schema/extensions/table/
# functions/indexes/views/privileges into the existing database.
[group("db")]
bootstrap-message-db:
    createdb $PGDATABASE || true
    MESSAGE_DB_ROOT=${MESSAGE_DB_ROOT:-/Users/shinzui/Keikaku/hub/event-sourcing/message-db-project/message-db} \
      && DATABASE_NAME=$PGDATABASE CREATE_DATABASE=off $MESSAGE_DB_ROOT/database/install.sh
    psql -v ON_ERROR_STOP=1 -c "ALTER DATABASE $PGDATABASE SET search_path = message_store, public;"

# Seed three sample messages into a category stream.
# Usage: just seed-messages orders
#
# message_store.write_message takes 4 required args (id, stream_name, type,
# data) plus two optional (metadata, expected_version). All Text args must
# be explicitly cast to varchar so Postgres can resolve the overload.
[group("db")]
seed-messages CATEGORY:
    psql -v ON_ERROR_STOP=1 -c "SET search_path = message_store, public; SELECT write_message(gen_random_uuid()::varchar, '{{CATEGORY}}-1'::varchar, 'OrderPlaced'::varchar, '{\"id\": 1}'::jsonb);"
    psql -v ON_ERROR_STOP=1 -c "SET search_path = message_store, public; SELECT write_message(gen_random_uuid()::varchar, '{{CATEGORY}}-1'::varchar, 'OrderPlaced'::varchar, '{\"id\": 2}'::jsonb);"
    psql -v ON_ERROR_STOP=1 -c "SET search_path = message_store, public; SELECT write_message(gen_random_uuid()::varchar, '{{CATEGORY}}-2'::varchar, 'OrderPlaced'::varchar, '{\"id\": 3}'::jsonb);"

# Seed the jitsurei-basic category with three OrderPlaced / OrderPaid messages.
[group("db")]
seed-jitsurei-basic:
    psql -v ON_ERROR_STOP=1 -c "SET search_path = message_store, public; SELECT write_message(gen_random_uuid()::varchar, 'jitsurei-basic-1'::varchar, 'OrderPlaced'::varchar, '{\"id\": 1}'::jsonb);"
    psql -v ON_ERROR_STOP=1 -c "SET search_path = message_store, public; SELECT write_message(gen_random_uuid()::varchar, 'jitsurei-basic-2'::varchar, 'OrderPlaced'::varchar, '{\"id\": 2}'::jsonb);"
    psql -v ON_ERROR_STOP=1 -c "SET search_path = message_store, public; SELECT write_message(gen_random_uuid()::varchar, 'jitsurei-basic-3'::varchar, 'OrderPaid'::varchar, '{\"id\": 3}'::jsonb);"

# Seed two messages for the retry demo.
[group("db")]
seed-jitsurei-retry:
    psql -v ON_ERROR_STOP=1 -c "SET search_path = message_store, public; SELECT write_message(gen_random_uuid()::varchar, 'jitsurei-retry-1'::varchar, 'OrderPlaced'::varchar, '{\"id\": 1}'::jsonb);"
    psql -v ON_ERROR_STOP=1 -c "SET search_path = message_store, public; SELECT write_message(gen_random_uuid()::varchar, 'jitsurei-retry-2'::varchar, 'OrderPlaced'::varchar, '{\"id\": 2}'::jsonb);"

# Seed three messages for the DLQ demo: Good, Bad, Good.
[group("db")]
seed-jitsurei-dlq:
    psql -v ON_ERROR_STOP=1 -c "SET search_path = message_store, public; SELECT write_message(gen_random_uuid()::varchar, 'jitsurei-dlq-1'::varchar, 'OrderPlaced'::varchar, '{\"id\": 1}'::jsonb);"
    psql -v ON_ERROR_STOP=1 -c "SET search_path = message_store, public; SELECT write_message(gen_random_uuid()::varchar, 'jitsurei-dlq-2'::varchar, 'BadFormat'::varchar, '{\"id\": 2}'::jsonb);"
    psql -v ON_ERROR_STOP=1 -c "SET search_path = message_store, public; SELECT write_message(gen_random_uuid()::varchar, 'jitsurei-dlq-3'::varchar, 'OrderPlaced'::varchar, '{\"id\": 3}'::jsonb);"

# Seed ten messages for the checkpoint-restart demo.
[group("db")]
seed-jitsurei-checkpoint:
    #!/usr/bin/env bash
    set -euo pipefail
    for n in 1 2 3 4 5 6 7 8 9 10; do
      psql -v ON_ERROR_STOP=1 -c "SET search_path = message_store, public; SELECT write_message(gen_random_uuid()::varchar, 'jitsurei-checkpoint-${n}'::varchar, 'OrderPlaced'::varchar, ('{\"id\": ' || ${n} || '}')::jsonb);"
    done

# Seed thirty messages across six category streams for the multi-partition demo.
[group("db")]
seed-jitsurei-partition:
    #!/usr/bin/env bash
    set -euo pipefail
    for cat in cat1 cat2 cat3 cat4 cat5 cat6; do
      for n in 1 2 3 4 5; do
        psql -v ON_ERROR_STOP=1 -c "SET search_path = message_store, public; SELECT write_message(gen_random_uuid()::varchar, 'jitsurei-${cat}-${n}'::varchar, 'OrderPlaced'::varchar, ('{\"id\": ' || ${n} || '}')::jsonb);"
      done
    done


# --- Build ---

# Build the library (and all local packages pulled in by cabal.project)
[group("build")]
build:
    cabal build shibuya-message-db-adapter

# Run the test suite
[group("build")]
test:
    cabal test shibuya-message-db-adapter

# Validate the capability catalog: mori config plus the profile-governed
# okf bundle under docs/capabilities (evidence, log, and profile enforced).
[group("build")]
check-capabilities:
    mori validate
    okf validate docs/capabilities --profile docs/capabilities/profile.dhall --profile-enforce --log-enforce

# Clean cabal build artifacts
[group("build")]
clean:
    cabal clean

# Format the tree (treefmt via nix fmt)
[group("build")]
fmt:
    nix fmt
