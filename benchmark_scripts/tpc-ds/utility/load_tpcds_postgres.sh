#!/bin/bash
set -e  # Exit on any error
set -x  # Print each command before executing

# Arguments
DATA_DIR=$1
TOOL_DIR=$2
DBNAME=${3:-tpcds}

# Directory of the script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

cd "$TOOL_DIR" || { echo "Error: Failed to navigate to directory $TOOL_DIR"; exit 1; }
make CC=gcc-9 OS=LINUX

./dsdgen -scale 10
mv *.dat "$DATA_DIR"

# Create the database schema
$PSQL $DBNAME -f tpcds.sql

# Change to the data directory
pushd "$DATA_DIR"

for i in `ls *.dat`; do
  table=${i/.dat/}
  echo "Loading $table..."
  sed 's/|$//' $i > /tmp/$i
  $PSQL $DBNAME -c "TRUNCATE $table"
  $PSQL $DBNAME -c "\\copy $table FROM '/tmp/$i' CSV DELIMITER '|'"
done

# Wait for all background tasks to complete
wait

# Return to the original directory
popd

cd "$DIR" || { echo "Error: Failed to return to directory $DIR"; exit 1; }