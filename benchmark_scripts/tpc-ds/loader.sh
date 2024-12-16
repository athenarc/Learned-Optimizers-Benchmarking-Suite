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

BENCHMARK_DIR="${BENCHMARKS_DIR}/tpc-ds/tpc-ds-tool/tools"
TEMPLATES_DIR="${BENCHMARKS_DIR}/tpc-ds/tpc-ds-tool/query_templates"

# Change to workload directory
pushd "$WORKLOAD_DIR" || { echo "Error: Failed to navigate to workload directory"; exit 1; }
mkdir -p tpc-ds-benchmark || { echo "Error: Failed to create directory tpc-ds-benchmark"; exit 1; }
popd || { echo "Error: Failed to return to the original directory"; exit 1; }

# Known issue: TPC-DS qgen tool does not work with PostgreSQL
# pushd "$TEMPLATES_DIR" || { echo "Error: Failed to navigate to tpc-ds-tool/query_templates directory"; exit 1; }
# bash "$BENCHMARKS_DIR"/tpc-ds/utility/fix_templates.sh
# popd || { echo "Error: Failed to return to the original directory"; exit 1; }

# pushd "$BENCHMARK_DIR" || { echo "Error: Failed to navigate to tpc-ds-tool/tools directory"; exit 1; }
# # make
# ./dsqgen -DIRECTORY ../query_templates -INPUT ../query_templates/templates.lst -VERBOSE Y -QUALIFY Y -SCALE 10 -DIALECT netezza -OUTPUT_DIR "$WORKLOAD_DIR"/tpc-ds-benchmark
# popd || { echo "Error: Failed to return to the original directory"; exit 1; }

echo "TPCDS dataset successfully loaded into $DBNAME database."
