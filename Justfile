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

# Seed three sample messages into a category stream.
# Usage: just seed-messages orders
[group("db")]
seed-messages CATEGORY:
    psql -v ON_ERROR_STOP=1 -c "SELECT write_message(gen_random_uuid()::varchar, '{{CATEGORY}}-1', 'OrderPlaced', '{\"id\": 1}'::jsonb, NULL);"
    psql -v ON_ERROR_STOP=1 -c "SELECT write_message(gen_random_uuid()::varchar, '{{CATEGORY}}-1', 'OrderPlaced', '{\"id\": 2}'::jsonb, NULL);"
    psql -v ON_ERROR_STOP=1 -c "SELECT write_message(gen_random_uuid()::varchar, '{{CATEGORY}}-2', 'OrderPlaced', '{\"id\": 3}'::jsonb, NULL);"


# --- Build ---

# Build the library (and all local packages pulled in by cabal.project)
[group("build")]
build:
    cabal build shibuya-message-db-adapter

# Run the test suite
[group("build")]
test:
    cabal test shibuya-message-db-adapter

# Clean cabal build artifacts
[group("build")]
clean:
    cabal clean

# Format the tree (treefmt via nix fmt)
[group("build")]
fmt:
    nix fmt
