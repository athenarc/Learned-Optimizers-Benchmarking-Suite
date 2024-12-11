#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

DBNAME="tpch"

# Check if DATA_DIR and DB_CLUSTER_DIR are provided
if [ -z "$DATA_DIR" ] || [ -z "$DB_CLUSTER_DIR" ]; then
    echo "Error: DATA_DIR and DB_CLUSTER_DIR are required."
    exit 1
fi

# Define tpc-h directory
TPCH_DIR="${DATA_DIR}/tpc-h"

# Create TPCH_DIR if it doesn't exist and navigate into it
mkdir -p "$TPCH_DIR" || { echo "Error: Failed to create directory $TPCH_DIR"; exit 1; }
cd "$TPCH_DIR" || { echo "Error: Failed to navigate to directory $TPCH_DIR"; exit 1; }

# Return to the TPCH_DIR
cd "$TPCH_DIR" || { echo "Error: Failed to return to $TPCH_DIR"; exit 1; }

# Get the script directory (the location of this script)
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
TOOL_DIR="$SCRIPT_DIR/tpc-h-tool/dbgen"

# Create the PostgreSQL database
if ! $CREATE_DB "$DBNAME"; then
    echo "Error: Failed to create database $DBNAME"
    exit 1
fi

# Load the dataset into PostgreSQL
if ! bash "$SCRIPT_DIR/utility/load_tpch_postgres.sh" "$TPCH_DIR" "$TOOL_DIR" "$DBNAME" ; then
    echo "Error: Failed to load data into PostgreSQL"
    exit 1
fi

echo "TPCH dataset successfully loaded into $DBNAME database."
