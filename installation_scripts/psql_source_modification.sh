#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Define the directory where the patch should be applied
TARGET_DIR="/tmp/postgresql-${POSTGRES_VERSION}"
PATCH_FILE="$PATCH_DIR/stats_benchmark.patch"

# Check if the patch file exists
if [[ ! -f "$PATCH_FILE" ]]; then
    echo "Error: Patch file '$PATCH_FILE' does not exist."
    exit 1
fi

# Navigate to the target directory
pushd "$TARGET_DIR" > /dev/null

# Apply the patch
if patch -s -p1 < "$PATCH_FILE"; then
    echo "Patch applied successfully."
else
    echo "Error: Failed to apply patch."
    popd > /dev/null
    exit 1
fi

# Return to the original directory
popd > /dev/null

# Script completed successfully
echo "Script executed successfully."