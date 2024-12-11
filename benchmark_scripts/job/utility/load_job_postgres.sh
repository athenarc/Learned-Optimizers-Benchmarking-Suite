#!/bin/bash
set -e  # Exit on any error
set -x  # Print each command before executing

# Arguments
DATA_DIR=$1
DBNAME=${2:-imdbload}

# Directory of the script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"


# SQL files for schema and foreign keys
SCHEMA_SQL="$DIR/schema.sql"
FK_SQL="$DIR/fkindexes.sql"
ADD_FKS_SQL="$DIR/add_fks.sql"

# List of tables to load
declare -a TABLES=(
    "name"
    "char_name"
    "comp_cast_type"
    "company_name"
    "company_type"
    "info_type"
    "keyword"
    "kind_type"
    "link_type"
    "role_type"
    "title"
    "aka_name"
    "aka_title"
    "cast_info"
    "complete_cast"
    "movie_companies"
    "movie_info"
    "movie_info_idx"
    "movie_keyword"
    "movie_link"
    "person_info"
)

# Apply schema and foreign key definitions
$PSQL "$DBNAME" -f "$SCHEMA_SQL"
$PSQL "$DBNAME" -f "$FK_SQL"

# Change to the data directory
pushd "$DATA_DIR"

# Load each table
for TABLE in "${TABLES[@]}"; do
    FILE="$DATA_DIR/${TABLE}.csv"
    if [[ -f "$FILE" ]]; then
        $PSQL "$DBNAME" -c "\copy $TABLE from '$FILE' escape '\' csv header" &
    else
        echo "Warning: File $FILE not found, skipping."
    fi
done

# Wait for all background tasks to complete
wait

# Return to the original directory
popd

# Apply foreign key constraints
$PSQL "$DBNAME" -f "$ADD_FKS_SQL"

# Create histograms for query optimization
$PSQL "$DBNAME" -c "ANALYZE VERBOSE;"
