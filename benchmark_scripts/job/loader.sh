#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

DBNAME="imdbload"

# Check if DATA_DIR and DB_CLUSTER_DIR are provided
if [ -z "$DATA_DIR" ] || [ -z "$DB_CLUSTER_DIR" ]; then
    echo "Error: DATA_DIR and DB_CLUSTER_DIR are required."
    exit 1
fi

# Define job directory
JOB_DIR="${DATA_DIR}/job"

# Create JOB_DIR if it doesn't exist and navigate into it
mkdir -p "$JOB_DIR" || { echo "Error: Failed to create directory $JOB_DIR"; exit 1; }
cd "$JOB_DIR" || { echo "Error: Failed to navigate to directory $JOB_DIR"; exit 1; }

# Download the IMDb dataset
IMDB_TAR="imdb.tgz"
if ! wget -c "https://event.cwi.nl/da/job/$IMDB_TAR"; then
    echo "Error: Failed to download $IMDB_TAR"
    exit 1
fi

# Extract the tar file
if ! tar -xvzf "$IMDB_TAR"; then
    echo "Error: Failed to extract $IMDB_TAR"
    exit 1
fi

# Return to the original directory
cd - || { echo "Error: Failed to return to the original directory"; exit 1; }

# Get the script directory (the location of this script)
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Run the header prepending script
if ! bash "$SCRIPT_DIR/utility/prepend_headers.sh" "$JOB_DIR"; then
    echo "Error: Failed to prepend headers"
    exit 1
fi

# Create the PostgreSQL database
if ! $CREATE_DB "$DBNAME"; then
    echo "Error: Failed to create database $DBNAME"
    exit 1
fi

# Load the dataset into PostgreSQL
if ! bash "$SCRIPT_DIR/utility/load_job_postgres.sh" "$JOB_DIR" "$DBNAME"; then
    echo "Error: Failed to load data into PostgreSQL"
    exit 1
fi

# Change to workload directory
pushd "$WORKLOAD_DIR" || { echo "Error: Failed to navigate to workload directory"; exit 1; }
mkdir -p join-order-benchmark || { echo "Error: Failed to create directory join-order-benchmark"; exit 1; }
cp "$BENCHMARKS_DIR"/job/workload/* join-order-benchmark/ || { echo "Error: Failed to copy workload.sql to join-order-benchmark"; exit 1; }
popd || { echo "Error: Failed to return to the original directory"; exit 1; }


echo "IMDB dataset successfully loaded into $DBNAME database."
