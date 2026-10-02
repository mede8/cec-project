#!/bin/bash
# Creates a read-only login for the REST API (principle of least privilege).
# Postgres runs this automatically on first start, right after init.sql,
# because both files are mounted into /docker-entrypoint-initdb.d/.
#
# The password comes from API_DB_PASSWORD, never from this file.
set -euo pipefail

: "${API_DB_USER:=api_reader}"
: "${API_DB_PASSWORD:?API_DB_PASSWORD must be set}"

psql -v ON_ERROR_STOP=1 \
     --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
     -v api_user="$API_DB_USER" -v api_password="$API_DB_PASSWORD" <<'SQL'
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'api_user', :'api_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'api_user')
\gexec

GRANT CONNECT ON DATABASE :"DBNAME" TO :"api_user";
GRANT USAGE ON SCHEMA public TO :"api_user";
GRANT SELECT ON ALL TABLES IN SCHEMA public TO :"api_user";
-- Tables created later (by a migration) are readable too.
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO :"api_user";
SQL
