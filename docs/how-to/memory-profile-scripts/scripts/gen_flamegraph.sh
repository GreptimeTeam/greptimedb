#!/bin/bash

# Generate flame graphs from a series of `.gprof` files
# First argument: Path to the binary executable
# Second argument: Path to directory containing gprof files
# Requires `jeprof` and `flamegraph.pl` in current directory
# What this script essentially does is:
# ./jeprof <binary> <gprof> --collapse | ./flamegraph.pl > <output>
# For differential analysis between consecutive profiles:
# ./jeprof <binary> --base <gprof1> <gprof2> --collapse | ./flamegraph.pl > <output_diff>

set -e # Exit immediately if a command exits with a non-zero status.

# Check for required tools
if [ ! -f "./jeprof" ]; then
    echo "Error: jeprof not found in the current directory."
    exit 1
fi

if [ ! -f "./flamegraph.pl" ]; then
    echo "Error: flamegraph.pl not found in the current directory."
    exit 1
fi

# Check arguments
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <binary_path> <gprof_directory>"
    exit 1
fi

BINARY_PATH=$1
GPROF_DIR=$2
OUTPUT_DIR="${GPROF_DIR}/flamegraphs" # Store outputs in a subdirectory

if [ ! -f "$BINARY_PATH" ]; then
    echo "Error: Binary file not found at $BINARY_PATH"
    exit 1
fi

if [ ! -d "$GPROF_DIR" ]; then
    echo "Error: gprof directory not found at $GPROF_DIR"
    exit 1
fi

mkdir -p "$OUTPUT_DIR"
echo "Generating flamegraphs in $OUTPUT_DIR"

# Find and sort gprof files
# Use find + sort -V for natural sort of version numbers if present in filenames
# Use null-terminated strings for safety with find/xargs/sort
mapfile -d $'\0' gprof_files < <(find "$GPROF_DIR" -maxdepth 1 -name '*.gprof' -print0 | sort -zV)

if [ ${#gprof_files[@]} -eq 0 ]; then
    echo "No .gprof files found in $GPROF_DIR"
    exit 0
fi

prev_gprof=""

# Generate flamegraphs
for gprof_file in "${gprof_files[@]}"; do
    # Skip empty entries if any
    if [ -z "$gprof_file" ]; then
        continue
    fi

    filename=$(basename "$gprof_file" .gprof)
    output_collapse="${OUTPUT_DIR}/${filename}.collapse"
    output_svg="${OUTPUT_DIR}/${filename}.svg"
    echo "Generating collapse file for $gprof_file -> $output_collapse"
    ./jeprof "$BINARY_PATH" "$gprof_file" --collapse > "$output_collapse"
    echo "Generating flamegraph for $gprof_file -> $output_svg"
    ./flamegraph.pl "$output_collapse" > "$output_svg" || true

    # Generate diff flamegraph if not the first file
    if [ -n "$prev_gprof" ]; then
        prev_filename=$(basename "$prev_gprof" .gprof)
        diff_output_collapse="${OUTPUT_DIR}/${prev_filename}_vs_${filename}_diff.collapse"
        diff_output_svg="${OUTPUT_DIR}/${prev_filename}_vs_${filename}_diff.svg"
        echo "Generating diff collapse file for $prev_gprof vs $gprof_file -> $diff_output_collapse"
        ./jeprof "$BINARY_PATH" --base "$prev_gprof" "$gprof_file" --collapse > "$diff_output_collapse"
        echo "Generating diff flamegraph for $prev_gprof vs $gprof_file -> $diff_output_svg"
        ./flamegraph.pl "$diff_output_collapse" > "$diff_output_svg" || true
    fi

    prev_gprof="$gprof_file"
done

echo "Flamegraph generation complete."
