#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Run dataset loader scripts for all benchmarks
for BENCHMARK in "$BENCHMARKS_DIR"/*; do
    if [[ -d "$BENCHMARK" ]]; then
        LOADER="$BENCHMARK/loader.sh"
        if [[ -f "$LOADER" ]]; then
            echo "Running dataset loader script for $(basename "$BENCHMARK")"
            bash "$LOADER"
        fi
    fi
done

# Completion message
echo "Setup complete."
