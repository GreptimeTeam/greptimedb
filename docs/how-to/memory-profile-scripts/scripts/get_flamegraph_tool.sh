#!/bin/bash

# Download flamegraph.pl to current directory - this is the flame graph generation tool script

curl https://raw.githubusercontent.com/brendangregg/FlameGraph/master/flamegraph.pl > ./flamegraph.pl
chmod +x ./flamegraph.pl
