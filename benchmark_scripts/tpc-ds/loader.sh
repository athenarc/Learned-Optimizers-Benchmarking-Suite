#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

DBNAME="tpcds"

# Check if DATA_DIR and DB_CLUSTER_DIR are provided
if [ -z "$DATA_DIR" ] || [ -z "$DB_CLUSTER_DIR" ]; then
    echo "Error: DATA_DIR and DB_CLUSTER_DIR are required."
    exit 1
fi

# Define tpc-h directory
TPCDS_DIR="${DATA_DIR}/tpc-ds"

# Create TPCDS_DIR if it doesn't exist and navigate into it
mkdir -p "$TPCDS_DIR" || { echo "Error: Failed to create directory $TPCDS_DIR"; exit 1; }
cd "$TPCDS_DIR" || { echo "Error: Failed to navigate to directory $TPCDS_DIR"; exit 1; }

# Return to the TPCDS_DIR
cd "$TPCDS_DIR" || { echo "Error: Failed to return to $TPCDS_DIR"; exit 1; }

# Get the script directory (the location of this script)
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
TOOL_DIR="$SCRIPT_DIR/tpc-ds-tool/tools"

# Create the PostgreSQL database
if ! $CREATE_DB "$DBNAME"; then
    echo "Error: Failed to create database $DBNAME"
    exit 1
fi

# Load the dataset into PostgreSQL
if ! bash "$SCRIPT_DIR/utility/load_tpcds_postgres.sh" "$TPCDS_DIR" "$TOOL_DIR" "$DBNAME" ; then
    echo "Error: Failed to load data into PostgreSQL"
    exit 1
fi

echo "TPCDS dataset successfully loaded into $DBNAME database."
