
#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-localhost:9092}"

echo "================================="
echo "       KAFKA TOPIC LIST"
echo "================================="

kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --list

echo ""
echo "================================="
echo "      KAFKA TOPIC DETAILS"
echo "================================="

kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --describe

echo ""
echo "Topic inspection completed."
