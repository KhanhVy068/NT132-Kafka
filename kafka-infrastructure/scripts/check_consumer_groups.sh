
#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-localhost:9092}"

echo "======================================"
echo "       KAFKA CONSUMER GROUPS"
echo "======================================"

echo ""
echo "=== Available Consumer Groups ==="

kafka-consumer-groups.sh \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --list

echo ""
echo "=== Consumer Group Offset and Lag ==="

kafka-consumer-groups.sh \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --describe \
    --all-groups

echo ""
echo "=== Consumer Group Members ==="

kafka-consumer-groups.sh \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --describe \
    --all-groups \
    --members \
    --verbose

echo ""
echo "Consumer Group inspection completed."
