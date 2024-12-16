#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Initialize database if the data directory is empty
if [ ! -d "$DB_CLUSTER_DIR/PG_VERSION" ]; then
    # Create the new user and database as the 'postgres' user
    $PSQL <<EOSQL
    CREATE USER ${POSTGRES_USER} WITH PASSWORD '${POSTGRES_PASSWORD}'
        SUPERUSER 
        CREATEDB 
        CREATEROLE 
        REPLICATION 
        BYPASSRLS;
EOSQL

    # Change ownership of all schemas to the new superuser
    $PSQL <<EOSQL
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
    $PSQL <<EOSQL
    ALTER DATABASE postgres OWNER TO ${POSTGRES_USER};
    ALTER DATABASE template0 OWNER TO ${POSTGRES_USER};
    ALTER DATABASE template1 OWNER TO ${POSTGRES_USER};
    ALTER DATABASE imdbload OWNER TO ${POSTGRES_USER};
    ALTER DATABASE ssb OWNER TO ${POSTGRES_USER};
    ALTER DATABASE stats OWNER TO ${POSTGRES_USER};
    ALTER DATABASE tpch OWNER TO ${POSTGRES_USER};
    ALTER DATABASE tpcds OWNER TO ${POSTGRES_USER};
EOSQL

    # Revoke all privileges from the postgres user
    $PSQL <<EOSQL
    REVOKE CONNECT ON DATABASE imdbload, ssb, stats, tpch, tpcds FROM postgres;
    REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM postgres;
    REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM postgres;
    REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public FROM postgres;
    REVOKE ALL PRIVILEGES ON SCHEMA public FROM postgres;

    -- Revoke all other global privileges if any
    REVOKE ALL PRIVILEGES ON DATABASE postgres, template0, template1, imdbload, ssb, stats, tpch, tpcds FROM postgres;

    -- Revoke specific administrative privileges
    ALTER ROLE postgres NOLOGIN;
    ALTER ROLE postgres NOCREATEDB;
    ALTER ROLE postgres NOCREATEROLE;
    ALTER ROLE postgres NOREPLICATION;
    ALTER ROLE postgres NOBYPASSRLS;    
    ALTER ROLE postgres NOSUPERUSER;
EOSQL

fi
