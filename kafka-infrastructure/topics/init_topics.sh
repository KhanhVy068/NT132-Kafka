
#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-localhost:9092}"

TOPICS=(
    "orders.created"
    "inventory.results"
    "payments.results"
    "inventory.updated"
)

PARTITIONS=2
REPLICATION_FACTOR=1

echo "=== Initializing Kafka Topics ==="

for topic in "${TOPICS[@]}"; do
    echo ""
    echo "Creating or checking topic: $topic"

    kafka-topics.sh \
        --bootstrap-server "$BOOTSTRAP_SERVER" \
        --create \
        --if-not-exists \
        --topic "$topic" \
        --partitions "$PARTITIONS" \
        --replication-factor "$REPLICATION_FACTOR"

    echo "Topic configuration: $topic"

    description=$(kafka-topics.sh \
        --bootstrap-server "$BOOTSTRAP_SERVER" \
        --describe \
        --topic "$topic")

    echo "$description"

    topic_line=$(printf '%s\n' "$description" |
        grep -m1 'PartitionCount:' || true)

    if [[ "$topic_line" != *"PartitionCount: $PARTITIONS"* ]] ||
       [[ "$topic_line" != *"ReplicationFactor: $REPLICATION_FACTOR"* ]]; then
        echo "ERROR: Topic configuration mismatch: $topic" >&2
        exit 1
    fi

    echo "Verified topic: $topic"
done

echo ""
echo "=== Topic initialization completed ==="
