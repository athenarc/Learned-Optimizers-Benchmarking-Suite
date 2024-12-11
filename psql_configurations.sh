#!/bin/bash

# Exit immediately if any command fails
set -e

PATCH_FILE="$PATCH_DIR/postgresql_conf.patch"

# Ensure the PostgreSQL configuration file exists
if [[ ! -f "$PG_CONF" ]]; then
    echo "Error: PostgreSQL configuration file '$PG_CONF' does not exist."
    exit 1
fi

# Check if the patch file exists
if [[ ! -f "$PATCH_FILE" ]]; then
    echo "Error: Patch file '$PATCH_FILE' does not exist."
    exit 1
fi

# Navigate to the database cluster directory
pushd "$DB_CLUSTER_DIR" > /dev/null

# Apply the patch to the PostgreSQL configuration file
if patch -s -p0 < "$PATCH_FILE"; then
    echo "Patch applied successfully to $PG_CONF."
else
    echo "Error: Failed to apply patch."
    popd > /dev/null
    exit 1
fi

# Return to the previous directory
popd > /dev/null

echo "host all all 0.0.0.0/0 md5" >> "$PG_HBA"

# Script completed successfully
echo "Script executed successfully."