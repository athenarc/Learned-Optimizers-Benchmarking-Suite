#!/bin/bash

# Directory where the .tpl files are located
TARGET_DIR="."

# Iterate over all files named query*.tpl in the specified directory
for file in "$TARGET_DIR"/query*.tpl; do
    # Check if the file exists
    if [[ -f "$file" ]]; then
        echo "Processing $file..."

        # Add the line define _END = ""; to the end of the file
        echo 'define _END = "";' >> "$file"

        echo "Added define _END = \"\"; to $file"
    else
        echo "No query*.tpl files found."
    fi
done

echo "Script completed."