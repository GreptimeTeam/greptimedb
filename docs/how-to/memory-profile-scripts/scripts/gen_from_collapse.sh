#!/bin/bash

# Generate flame graphs from .collapse files
# Argument: Path to directory containing collapse files
# Requires `flamegraph.pl` in current directory

# Check if flamegraph.pl exists
if [ ! -f "./flamegraph.pl" ]; then
    echo "Error: flamegraph.pl not found in the current directory."
    exit 1
fi

# Check if directory argument is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <collapse_directory>"
    exit 1
fi

COLLAPSE_DIR=$1

# Check if the provided argument is a directory
if [ ! -d "$COLLAPSE_DIR" ]; then
    echo "Error: '$COLLAPSE_DIR' is not a valid directory."
    exit 1
fi

echo "Generating flame graphs from collapse files in '$COLLAPSE_DIR'..."

# Find and process each .collapse file
find "$COLLAPSE_DIR" -maxdepth 1 -name "*.collapse" -print0 | while IFS= read -r -d $'\0' collapse_file; do
    if [ -f "$collapse_file" ]; then
        # Construct the output SVG filename
        svg_file="${collapse_file%.collapse}.svg"
        echo "Generating $svg_file from $collapse_file..."
        ./flamegraph.pl "$collapse_file" > "$svg_file"
        if [ $? -ne 0 ]; then
            echo "Error generating flame graph for $collapse_file"
        else
            echo "Successfully generated $svg_file"
        fi
    fi
done

echo "Flame graph generation complete."
