#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

DBNAME="stats"

# Check if DATA_DIR and DB_CLUSTER_DIR are provided
if [ -z "$DATA_DIR" ] || [ -z "$DB_CLUSTER_DIR" ]; then
    echo "Error: DATA_DIR and DB_CLUSTER_DIR are required."
    exit 1
fi

# Define ssb directory
STATS_DIR="${DATA_DIR}/stats"

# Create STATS_DIR if it doesn't exist and navigate into it
mkdir -p "$STATS_DIR" || { echo "Error: Failed to create directory $STATS_DIR"; exit 1; }
cd "$STATS_DIR" || { echo "Error: Failed to navigate to directory $STATS_DIR"; exit 1; }

# Define the repository URL and directory
REPO_URL="https://github.com/Nathaniel-Han/End-to-End-CardEst-Benchmark.git"
REPO_DIR="End-to-End-CardEst-Benchmark"

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
    echo "Cloning the End-to-End-CardEst-Benchmark repository..."
    if ! git clone "$REPO_URL"; then
        echo "Error: Failed to clone End-to-End-CardEst-Benchmark repository."
        exit 1
    fi
fi

cd End-to-End-CardEst-Benchmark || { echo "Error: Failed to navigate into End-to-End-CardEst-Benchmark directory"; exit 1; }

# Create the PostgreSQL database
if ! $CREATE_DB "$DBNAME"; then
    echo "Error: Failed to create database $DBNAME"
    exit 1
fi

BENCHMARK_DIR="$STATS_DIR/End-to-End-CardEst-Benchmark"

$PSQL $DBNAME -f "$BENCHMARK_DIR/datasets/stats_simplified/stats.sql"
$PSQL $DBNAME -f "$BENCHMARK_DIR/scripts/sql/stats_load.sql"
$PSQL $DBNAME -f "$BENCHMARK_DIR/scripts/sql/stats_index.sql"

echo "Stats dataset successfully loaded into $DBNAME database."

