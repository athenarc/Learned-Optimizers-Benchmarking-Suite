#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

DBNAME="ssb"

# Check if DATA_DIR and DB_CLUSTER_DIR are provided
if [ -z "$DATA_DIR" ] || [ -z "$DB_CLUSTER_DIR" ]; then
    echo "Error: DATA_DIR and DB_CLUSTER_DIR are required."
    exit 1
fi

# Define ssb directory
SSB_DIR="${DATA_DIR}/ssb"

# Create SSB_DIR if it doesn't exist and navigate into it
mkdir -p "$SSB_DIR" || { echo "Error: Failed to create directory $SSB_DIR"; exit 1; }
cd "$SSB_DIR" || { echo "Error: Failed to navigate to directory $SSB_DIR"; exit 1; }

# Define the repository URL and directory
REPO_URL="https://github.com/lemire/StarSchemaBenchmark.git"
REPO_DIR="StarSchemaBenchmark"

# Clone the repository if it doesn't exist, or pull the latest changes if it does
if [ -d "$REPO_DIR" ]; then
    echo "Repository already exists. Pulling the latest changes..."
    cd "$REPO_DIR" || exit
    if ! git pull; then
        echo "Error: Failed to pull the latest changes."
        exit 1
    fi
    cd ..
else
    echo "Cloning the Star Schema Benchmark repository..."
    if ! git clone "$REPO_URL"; then
        echo "Error: Failed to clone StarSchemaBenchmark repository."
        exit 1
    fi
fi

cd StarSchemaBenchmark || { echo "Error: Failed to navigate into StarSchemaBenchmark directory"; exit 1; }

# Compile the benchmark tools
if ! make; then
    echo "Error: Failed to compile StarSchemaBenchmark"
    exit 1
fi

# Generate the data
if ! ./dbgen -s 1 -T a -f; then
    echo "Error: Failed to generate the data"
    exit 1
fi

# Move the generated tables to the SSB_DIR
mv *.tbl "$SSB_DIR" || { echo "Error: Failed to move *.tbl files to $SSB_DIR"; exit 1; }

# Return to the SSB_DIR
cd "$SSB_DIR" || { echo "Error: Failed to return to $SSB_DIR"; exit 1; }

# Get the script directory (the location of this script)
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Run the header prepending script
if ! bash "$SCRIPT_DIR/utility/prepend_headers.sh" "$SSB_DIR"; then
    echo "Error: Failed to prepend headers"
    exit 1
fi

# Create the PostgreSQL database
if ! $CREATE_DB "$DBNAME"; then
    echo "Error: Failed to create database $DBNAME"
    exit 1
fi

# Load the data into the PostgreSQL database
if ! bash "$SCRIPT_DIR/utility/load_ssb_postgres.sh" "$SSB_DIR" "$DBNAME"; then
    echo "Error: Failed to load data into PostgreSQL"
    exit 1
fi

# Change to workload directory
pushd "$WORKLOAD_DIR" || { echo "Error: Failed to navigate to workload directory"; exit 1; }
mkdir -p star-schema-benchmark || { echo "Error: Failed to create directory star-schema-benchmark"; exit 1; }
cp "$BENCHMARKS_DIR"/ssb/utility/workload.sql star-schema-benchmark/ssb.sql || { echo "Error: Failed to copy workload.sql to star-schema-benchmark"; exit 1; }
popd || { echo "Error: Failed to return to the original directory"; exit 1; }

echo "SSB dataset successfully loaded into $DBNAME database."
