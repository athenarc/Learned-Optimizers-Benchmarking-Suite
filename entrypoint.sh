#!/bin/bash
set -e

# Define configuration file paths
PG_HBA_CONF="/var/lib/postgresql/data/pg_hba.conf"
PG_CONF="/var/lib/postgresql/data/postgresql.conf"
PG_DATA_DIR="/var/lib/postgresql/data"

# Initialize database if the data directory is empty
if [ ! -d "$PG_DATA_DIR/PG_VERSION" ]; then
    echo "Initializing database..."
    /usr/lib/postgresql/12/bin/initdb -D "$PG_DATA_DIR"

    # Configure PostgreSQL to allow remote access
    echo "host all all 0.0.0.0/0 md5" >> "$PG_HBA_CONF"
    echo "listen_addresses = '*'" >> "$PG_CONF"

    echo "Database initialized and configured for remote access."

    # Start PostgreSQL to run SQL commands
    /usr/lib/postgresql/12/bin/pg_ctl -D "$PG_DATA_DIR" -w start

    # Create the new user and database as the 'postgres' user
    psql --username=postgres <<EOSQL
    CREATE USER ${POSTGRES_USER} WITH PASSWORD '${POSTGRES_PASSWORD}'
        SUPERUSER 
        CREATEDB 
        CREATEROLE 
        REPLICATION 
        BYPASSRLS;

    CREATE DATABASE ${POSTGRES_DB} OWNER ${POSTGRES_USER};    
EOSQL

    # Drop the postgres user after reassignment
    psql -h localhost -U ${POSTGRES_USER} -d ${POSTGRES_DB} <<EOSQL
	ALTER ROLE postgres NOLOGIN;
EOSQL

    # Stop PostgreSQL after setup
    /usr/lib/postgresql/12/bin/pg_ctl -D "$PG_DATA_DIR" -m fast -w stop
fi

# Start the PostgreSQL server
exec /usr/lib/postgresql/12/bin/postgres -D "$PG_DATA_DIR"
