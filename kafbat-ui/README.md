
# Kafbat UI - NT132 Kafka Project

## 1. Overview

Kafbat UI provides a web interface for monitoring
and managing the Kafka infrastructure.

Main features:
- View Kafka Brokers and Topics
- Inspect Partitions and Messages
- Monitor Consumer Groups
- Check Committed Offsets and Consumer Lag
- Observe Consumer status

## 2. Requirements

- Docker Engine and Docker Compose
- Kafka Broker deployed by TV1
- Network connectivity to Kafka Broker

## 3. Configuration

Copy the environment template:

```bash
cp .env.example .env
```

Configure the following variables:

- KAFBAT_VERSION: Kafbat UI image version
- KAFBAT_UI_PORT: Web interface port
- KAFKA_BOOTSTRAP_SERVER: Kafka Broker address

The Kafka address must be reachable from VM2.

## 4. Deployment

Start Kafbat UI:

```bash
docker compose up -d
```

Check container status:

```bash
docker compose ps
```

View logs:

```bash
docker compose logs -f
```

Stop Kafbat UI:

```bash
docker compose down
```

## 5. Access

Open the browser:

http://localhost:8080

When accessing from another machine, replace
localhost with the IP address of VM2.

Do not expose the administration interface
directly to the public Internet.

## 6. Monitoring

Use the Kafbat UI interface to inspect:

- orders.created
- inventory.results
- payments.results
- inventory.updated

Check Consumer Group offsets and lag during UC1.

Observe Kafka metrics during UC2.

## 7. Troubleshooting

### Cannot connect to Kafka

Check:
- Kafka Broker is running
- Bootstrap Server IP and port
- Kafka advertised.listeners configuration
- Firewall and VM network connectivity

### Web UI is unavailable

Check:
- Docker container status
- Port 8080 availability
- Container logs

## 8. Current Status

Configuration prepared for Phase 1.

Runtime deployment and integration testing
will be performed in Phase 2.
