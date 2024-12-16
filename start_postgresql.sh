#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Initialize PostgreSQL database cluster if it doesn't already exist
if [ ! -f ${DB_CLUSTER_DIR}/PG_VERSION ]; then
    echo 'Initializing database cluster...'
    $PG_CTL -D "$DB_CLUSTER_DIR" initdb
fi

# Run the database configuration script
echo 'Running database configurations setup...'
/app/installation_scripts/psql_configurations.sh

# Start PostgreSQL in the background
echo 'Starting PostgreSQL...'
$PG_CTL -D "$DB_CLUSTER_DIR" start &

# Wait for PostgreSQL to be ready
echo 'Waiting for PostgreSQL to be ready...'
sleep 5

# Load the benchmarks datasets and their workloads into the database
echo 'Running benchmark loaders...'
/app/installation_scripts/benchmark_loader.sh

# Run the database configuration script
echo 'Revokinf database privileges...'
/app/installation_scripts/psql_privileges.sh

# Keep the container running
tail -f /dev/null
