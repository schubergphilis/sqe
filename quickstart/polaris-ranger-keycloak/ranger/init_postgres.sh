#!/bin/bash
# Postgres init for Ranger Admin. Runs once via docker-entrypoint-initdb.d on a
# stock postgres image. Creates the application DB user + database that
# install.properties (db_user=rangeradmin, db_name=ranger) expects. The password
# comes from RANGER_DB_PASSWORD (docker-compose.yml, same value ranger-admin gets). The
# superuser (db_root_user=postgres) is the image default and runs the schema DDL
# during Ranger Admin first-boot setup.
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE USER rangeradmin WITH PASSWORD '${RANGER_DB_PASSWORD:-rangerR0cks!}';
    CREATE DATABASE ranger;
    GRANT ALL PRIVILEGES ON DATABASE ranger TO rangeradmin;
EOSQL
