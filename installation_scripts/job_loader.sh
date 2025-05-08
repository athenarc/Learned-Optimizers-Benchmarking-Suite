#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

LOADER="$BENCHMARKS_DIR/job/loader.sh"
bash "$LOADER"

# Completion message
echo "Setup complete."
