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

    # Change ownership of all schemas to the new superuser
    psql -h localhost -U ${POSTGRES_USER} -d ${POSTGRES_DB} <<EOSQL
    DO \$\$
    DECLARE
        r RECORD;
    BEGIN
        -- Change ownership of all schemas
        FOR r IN 
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema') -- Exclude system schemas
        LOOP
            EXECUTE 'ALTER SCHEMA ' || quote_ident(r.schema_name) || ' OWNER TO ${POSTGRES_USER}';
        END LOOP;

        -- Change ownership of all tables
        FOR r IN 
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
            AND table_schema NOT IN ('pg_catalog', 'information_schema')
        LOOP
            EXECUTE 'ALTER TABLE ' || quote_ident(r.table_schema) || '.' || quote_ident(r.table_name) || ' OWNER TO ${POSTGRES_USER}';
        END LOOP;

        -- Change ownership of all views
        FOR r IN 
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'VIEW'
            AND table_schema NOT IN ('pg_catalog', 'information_schema')
        LOOP
            EXECUTE 'ALTER VIEW ' || quote_ident(r.table_schema) || '.' || quote_ident(r.table_name) || ' OWNER TO ${POSTGRES_USER}';
        END LOOP;

        -- Change ownership of all sequences
        FOR r IN 
            SELECT sequence_schema, sequence_name
            FROM information_schema.sequences
            WHERE sequence_schema NOT IN ('pg_catalog', 'information_schema')
        LOOP
            EXECUTE 'ALTER SEQUENCE ' || quote_ident(r.sequence_schema) || '.' || quote_ident(r.sequence_name) || ' OWNER TO ${POSTGRES_USER}';
        END LOOP;

        -- Change ownership of all functions
        FOR r IN 
            SELECT routine_schema, routine_name
            FROM information_schema.routines
            WHERE routine_schema NOT IN ('pg_catalog', 'information_schema')
        LOOP
            EXECUTE 'ALTER FUNCTION ' || quote_ident(r.routine_schema) || '.' || quote_ident(r.routine_name) || ' OWNER TO ${POSTGRES_USER}';
        END LOOP;
    END \$\$;
EOSQL

    # Change ownership of the system databases (postgres, template0, template1)
    psql -h localhost -U ${POSTGRES_USER} -d ${POSTGRES_DB} <<EOSQL
    ALTER DATABASE postgres OWNER TO ${POSTGRES_USER};
    ALTER DATABASE template0 OWNER TO ${POSTGRES_USER};
    ALTER DATABASE template1 OWNER TO ${POSTGRES_USER};
EOSQL

    # Revoke all privileges from the postgres user
    psql -h localhost -U ${POSTGRES_USER} -d ${POSTGRES_DB} <<EOSQL
    REVOKE CONNECT ON DATABASE ${POSTGRES_DB} FROM postgres;
    REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM postgres;
    REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM postgres;
    REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public FROM postgres;
    REVOKE ALL PRIVILEGES ON SCHEMA public FROM postgres;

    -- Revoke all other global privileges if any
    REVOKE ALL PRIVILEGES ON DATABASE postgres FROM postgres;
    REVOKE ALL PRIVILEGES ON DATABASE template0 FROM postgres;
    REVOKE ALL PRIVILEGES ON DATABASE template1 FROM postgres;

    -- Revoke specific administrative privileges
    ALTER ROLE postgres NOLOGIN;    
    ALTER ROLE postgres NOSUPERUSER;
    ALTER ROLE postgres NOCREATEDB;
    ALTER ROLE postgres NOCREATEROLE;
    ALTER ROLE postgres NOREPLICATION;
    ALTER ROLE postgres NOBYPASSRLS;    
EOSQL

    # Stop PostgreSQL after setup
    /usr/lib/postgresql/12/bin/pg_ctl -D "$PG_DATA_DIR" -m fast -w stop
fi

# Start the PostgreSQL server
exec /usr/lib/postgresql/12/bin/postgres -D "$PG_DATA_DIR"
