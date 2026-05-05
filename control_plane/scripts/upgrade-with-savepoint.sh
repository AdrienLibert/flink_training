#!/usr/bin/env bash
# upgrade-with-savepoint.sh — graceful job upgrade via REST API.
#
# This is what the Flink Kubernetes Operator does internally for an
# `upgradeMode: savepoint` upgrade. Knowing the steps lets you build
# your own control plane (e.g., a CI/CD pipeline that bypasses the
# operator).
#
# Usage:
#   FLINK=http://flink-jobmanager:8081 \
#   JOB_ID=abc123 \
#   NEW_JAR=/path/to/new-job.jar \
#   ENTRY_CLASS=com.example.JobMain \
#   ./upgrade-with-savepoint.sh

set -euo pipefail

FLINK=${FLINK:-http://localhost:8081}
JOB_ID=${JOB_ID:?JOB_ID required}
NEW_JAR=${NEW_JAR:?NEW_JAR required}
ENTRY_CLASS=${ENTRY_CLASS:?ENTRY_CLASS required}
SAVEPOINT_DIR=${SAVEPOINT_DIR:-s3://flink-savepoints/upgrades}

echo "==> 1. Stop with savepoint"
TRIGGER_ID=$(curl -sS -X POST "$FLINK/jobs/$JOB_ID/stop" \
    -H 'Content-Type: application/json' \
    -d "{\"targetDirectory\":\"$SAVEPOINT_DIR\",\"drain\":false}" \
    | jq -r .request-id)
echo "    triggerId=$TRIGGER_ID"

echo "==> 2. Wait for savepoint completion"
while true; do
    STATUS=$(curl -sS "$FLINK/jobs/$JOB_ID/savepoints/$TRIGGER_ID")
    STATE=$(echo "$STATUS" | jq -r '.status.id')
    if [ "$STATE" = "COMPLETED" ]; then
        SAVEPOINT_PATH=$(echo "$STATUS" | jq -r '.operation.location')
        echo "    savepoint=$SAVEPOINT_PATH"
        break
    fi
    if [ "$STATE" = "FAILED" ]; then
        echo "    savepoint FAILED: $(echo "$STATUS" | jq -r '.operation.failure-cause.stack-trace')"
        exit 2
    fi
    sleep 2
done

echo "==> 3. Upload new jar"
JAR_ID=$(curl -sS -X POST "$FLINK/jars/upload" \
    -F "jarfile=@$NEW_JAR" \
    | jq -r .filename | xargs -n1 basename)
echo "    jarId=$JAR_ID"

echo "==> 4. Submit job from savepoint"
NEW_JOB=$(curl -sS -X POST "$FLINK/jars/$JAR_ID/run" \
    -H 'Content-Type: application/json' \
    -d "{\"entryClass\":\"$ENTRY_CLASS\",\"savepointPath\":\"$SAVEPOINT_PATH\"}" \
    | jq -r .jobid)
echo "    new jobId=$NEW_JOB"

echo "==> Done. Old job is stopped, new job is running from savepoint."
