#!/bin/bash
set -e  # Exit on any error
set -x  # Print each command before executing

# Arguments
DATA_DIR=$1
TOOL_DIR=$2
DBNAME=${3:-tpch}

# Directory of the script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

SCHEMA_SQL="$DIR/schema.sql"
$PSQL "$DBNAME" -f "$SCHEMA_SQL"

cd "$TOOL_DIR" || { echo "Error: Failed to navigate to directory $TOOL_DIR"; exit 1; }
make

# Generate the data
./dbgen -s 10
mv *.tbl "$DATA_DIR"

# List of tables to load
declare -a TABLES=(
    "region"
    "nation"
    "customer"
    "supplier"
    "part"
    "partsupp"
    "orders"
    "lineitem"
)

# Change to the data directory
pushd "$DATA_DIR"

# Load each table
for TABLE in "${TABLES[@]}"; do
    FILE="$DATA_DIR/${TABLE}.tbl"
    if [[ -f "$FILE" ]]; then
        $PSQL "$DBNAME" -c "\copy $TABLE from '$FILE' DELIMITER '|' csv header" &
    else
        echo "Warning: File $FILE not found, skipping."
    fi
done

# Wait for all background tasks to complete
wait

# Return to the original directory
popd

cd "$DIR" || { echo "Error: Failed to return to directory $DIR"; exit 1; }