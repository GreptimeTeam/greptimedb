#!/bin/bash

# Monitors greptime process memory usage every 10 minutes
# Triggers memory profile capture via `curl -X POST localhost:4000/debug/prof/mem > greptime-{timestamp}.gprof`
# when memory increases by more than 20MB since last check
# Generated profiles can be analyzed using flame graphs as described in `how-to-profile-memory.md`
# (jeprof is compiled with the database - see documentation)
# Alternative: Share binaries + profiles for analysis (Docker images preferred)

# Threshold in Kilobytes (20 MB)
threshold_kb=$((20 * 1024))
sleep_interval=$((10 * 60))

# Variable to store the last measured memory usage in KB
last_mem_kb=0

echo "Starting memory monitoring for 'greptime' process..."

while true; do

    # Check if PID is provided as an argument
    if [ -z "$1" ]; then
        echo "$(date): PID must be provided as a command-line argument."
        exit 1
    fi

    pid="$1"

    # Validate that the PID is a number
    if ! [[ "$pid" =~ ^[0-9]+$ ]]; then
        echo "$(date): Invalid PID: '$pid'. PID must be a number."
        exit 1
    fi

    # Get the current Resident Set Size (RSS) in Kilobytes
    current_mem_kb=$(ps -o rss= -p "$pid")

    # Check if ps command was successful and returned a number
    if ! [[ "$current_mem_kb" =~ ^[0-9]+$ ]]; then
        echo "$(date): Failed to get memory usage for PID $pid. Skipping check."
        # Keep last_mem_kb to avoid false positives if the process briefly becomes unreadable.
        continue
    fi

    echo "$(date): Current memory usage for PID $pid: ${current_mem_kb} KB"

    # Compare with the last measurement
    # if it's the first run, also do a baseline dump just to make sure we can dump
    
    diff_kb=$((current_mem_kb - last_mem_kb))
    echo "$(date): Memory usage change since last check: ${diff_kb} KB"

    if [ "$diff_kb" -gt "$threshold_kb" ]; then
        echo "$(date): Memory increase (${diff_kb} KB) exceeded threshold (${threshold_kb} KB). Dumping profile..."
        timestamp=$(date +%Y%m%d%H%M%S)
        profile_file="greptime-${timestamp}.gprof"
        # Execute curl and capture output to file
        if curl -sf -X POST localhost:4000/debug/prof/mem > "$profile_file"; then
            echo "$(date): Memory profile saved to $profile_file"
        else
            echo "$(date): Failed to dump memory profile (curl exit code: $?)."
            # Remove the potentially empty/failed profile file
            rm -f "$profile_file"
        fi
    else
            echo "$(date): Memory increase (${diff_kb} KB) is within the threshold (${threshold_kb} KB)."
    fi
    

    # Update the last memory usage
    last_mem_kb=$current_mem_kb
    
    # Wait for 5 minutes
    echo "$(date): Sleeping for $sleep_interval seconds..."
    sleep $sleep_interval
done

echo "Memory monitoring script stopped." # This line might not be reached in normal operation
