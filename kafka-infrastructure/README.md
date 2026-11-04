
# Kafka Infrastructure - NT132

## 1. Overview

This directory contains Kafka administration
scripts for the NT132 e-commerce project.

Responsibilities:
- Initialize Kafka topics
- Inspect topics and partitions
- Monitor consumer groups and lag
- Benchmark Kafka producer performance

## 2. Kafka Topics

| Topic | Partitions | Replication Factor |
|---|---|---|
| orders.created | 2 | 1 |
| inventory.results | 2 | 1 |
| payments.results | 2 | 1 |
| inventory.updated | 2 | 1 |

## 3. Requirements

- Running Kafka Broker
- Kafka CLI tools available
- Network access to the Broker
- Bash shell

Set BOOTSTRAP_SERVER to the address reachable
from the environment executing the scripts.

Example:

```bash
export BOOTSTRAP_SERVER=localhost:9092
```

Replace localhost with the actual Broker address
when running remotely.

## 4. Initialize Topics

```bash
bash topics/init_topics.sh
```

The script creates missing topics and verifies
their partition and replication settings.

## 5. List Topics

```bash
bash scripts/list_topics.sh
```

Displays:
- Topic names
- Partitions
- Replication information

## 6. Check Consumer Groups

```bash
bash scripts/check_consumer_groups.sh
```

Displays:
- Consumer group list
- Current offsets
- Log end offsets
- Consumer lag
- Consumer group members

## 7. Benchmark

Run only in an isolated Kafka test environment.

```bash
bash scripts/benchmark_kafka.sh
```

Test sizes:
- 1,000 records
- 10,000 records
- 100,000 records

Default record size: 256 bytes.

Benchmark output is saved under
benchmark-results/.

The script measures producer performance.
Consumer lag must be monitored separately.

## 8. Integration

Kafka infrastructure is deployed by TV1.

Kafka application services are implemented
by other team members.

The actual bootstrap server address and
Kafka CLI execution method must be agreed
upon with TV1 before deployment.

## 9. Project Status

Phase 1:
- Scripts prepared
- Bash syntax validation required
- Runtime Kafka testing pending

Phase 2:
- Kafka Broker deployment
- Topic initialization
- Consumer group monitoring
- Benchmark execution
- Result collection
