#!/usr/bin/env bash
# profile-with-async-profiler.sh — capture a CPU flame graph of a Flink
# job using async-profiler.
#
# Prereq: download async-profiler from https://github.com/async-profiler/async-profiler/releases
# and unpack to /opt/async-profiler.
#
# Usage:
#   ./profile-with-async-profiler.sh com.training.flink.exercises.Microbench
#
# This runs the job with the profiler agent and writes flame.html.

set -euo pipefail

MAIN_CLASS=${1:?Usage: $0 <main-class>}
PROFILER=${PROFILER:-/opt/async-profiler/build/libasyncProfiler.so}
OUTPUT=${OUTPUT:-flame.html}
DURATION=${DURATION:-30}     # seconds

if [ ! -f "$PROFILER" ]; then
    echo "async-profiler not found at $PROFILER. Set PROFILER env var."
    exit 1
fi

# Build the project so target/classes is up to date.
mvn -q compile

# Run via mvn exec, attaching the agent.
mvn -q exec:exec \
    -Dexec.mainClass="$MAIN_CLASS" \
    -Dexec.args="" \
    -Dexec.executable=java \
    -Dexec.argsLine="-agentpath:${PROFILER}=start,event=cpu,file=${OUTPUT},flamegraph,duration=${DURATION}"

echo "Wrote $OUTPUT — open in a browser to view the flame graph."
