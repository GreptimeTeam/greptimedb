#!/bin/bash

# Locates compiled jeprof binary (memory analysis tool) after cargo build
# Copies it to current directory from target/ build directories

JPROF_PATH=$(find . -name 'jeprof' -print -quit)
if [ -n "$JPROF_PATH" ]; then
  echo "Found jeprof at $JPROF_PATH"
  cp "$JPROF_PATH" .
  chmod +x jeprof
  echo "Copied jeprof to current directory and made it executable."
else
  echo "jeprof not found"
  exit 1
fi
