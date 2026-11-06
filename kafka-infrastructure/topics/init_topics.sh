#!/usr/bin/env bash
set -euo pipefail

verify_description() {
    local description="$1" expected_partitions="$2" expected_rf="$3"
    local line actual_partitions actual_rf
    while IFS= read -r line; do
        if [[ "$line" =~ PartitionCount:[[:space:]]*([0-9]+)([[:space:]]|$) ]]; then
            actual_partitions="${BASH_REMATCH[1]}"
            if [[ ! "$line" =~ ReplicationFactor:[[:space:]]*([0-9]+)([[:space:]]|$) ]]; then
                return 1
            fi
            actual_rf="${BASH_REMATCH[1]}"
            [[ "$actual_partitions" == "$expected_partitions" &&
               "$actual_rf" == "$expected_rf" ]]
            return
        fi
    done <<< "$description"
    return 1
}

main() {
    local bootstrap="${BOOTSTRAP_SERVER:-localhost:9092}"
    local topic description
    local partitions=2 rf=1
    local topics=(orders.created inventory.results payments.results inventory.updated)
    for topic in "${topics[@]}"; do
        kafka-topics.sh --bootstrap-server "$bootstrap" --create --if-not-exists \
            --topic "$topic" --partitions "$partitions" --replication-factor "$rf"
        description=$(kafka-topics.sh --bootstrap-server "$bootstrap" \
            --describe --topic "$topic")
        printf '%s\n' "$description"
        if ! verify_description "$description" "$partitions" "$rf"; then
            printf 'ERROR: invalid topic configuration: %s\n' "$topic" >&2
            return 1
        fi
    done
    echo "TOPICS VERIFIED"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    main "$@"
fi
