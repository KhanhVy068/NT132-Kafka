
#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-localhost:9092}"
TOPIC="${BENCHMARK_TOPIC:-orders.created}"

RECORD_SIZE="${RECORD_SIZE:-256}"
THROUGHPUT="${THROUGHPUT:--1}"
RESULT_DIR="${RESULT_DIR:-benchmark-results}"

TEST_CASES=(1000 10000 100000)

mkdir -p "$RESULT_DIR"

echo "===================================="
echo "       KAFKA BENCHMARK TEST"
echo "===================================="

echo "Bootstrap server: $BOOTSTRAP_SERVER"
echo "Topic: $TOPIC"
echo "Record size: $RECORD_SIZE bytes"
echo "Throughput limit: $THROUGHPUT"
echo "Results directory: $RESULT_DIR"

echo ""
echo "WARNING: Use an isolated test environment."
echo "Benchmark events may be consumed by application services."

for COUNT in "${TEST_CASES[@]}"; do
    echo ""
    echo "Running benchmark: $COUNT events"

    OUTPUT_FILE="$RESULT_DIR/benchmark_${COUNT}.log"

    kafka-producer-perf-test.sh \
        --topic "$TOPIC" \
        --num-records "$COUNT" \
        --record-size "$RECORD_SIZE" \
        --throughput "$THROUGHPUT" \
        --producer-props \
        bootstrap.servers="$BOOTSTRAP_SERVER" \
        acks=all \
        2>&1 | tee "$OUTPUT_FILE"

    echo "Results saved: $OUTPUT_FILE"
done

echo ""
echo "All benchmark tests completed."
