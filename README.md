# RabbitMQ Retry Checker

Detect and handle infinite retry loops in RabbitMQ Dead Letter Queues.

**Navigation:** [How It Works](#how-it-works) · [Quick Start](#quick-start) · [Configuration](#configuration) · [Wildcard Support](#wildcard-support) · [Architecture](#architecture) · [Scheduling](#scheduling)

---

## How It Works

```mermaid
flowchart LR
    A[DLQ Message] --> B{x-death > max?}
    B -->|Yes| C[Move to Failure Queue]
    B -->|No| D[Requeue]
```

The tool checks `x-death` count on messages and moves those exceeding the retry limit to a permanent failure queue. Messages within the limit are left in the DLQ for retry.

---

## Quick Start

```bash
pip install -r requirements.txt
python rmq_retry_checker.py config.yaml
```

---

## Configuration

Create `config.yaml`:

```yaml
rabbitmq:
  host: localhost
  port: 5672
  username: guest
  password: guest
  mgmt_port: 15672      # Required for wildcard support
  vhost: /              # Optional, defaults to /
  use_ssl: false        # Optional, enable for TLS
  ssl_verify: true      # Optional, set false for self-signed certs

queues:
  dlq_name: my_dlq                        # Supports wildcards: dlq.*
  target_queue: permanent_failure_queue   # Supports wildcards: dead.*
  max_retry_count: 3
```

---

## Wildcard Support

Process multiple queues at once using wildcards:

```yaml
queues:
  dlq_name: "dlq.*"
  target_queue: "dead.*"
```

This matches `dlq.orders`, `dlq.users`, etc. and creates corresponding `dead.orders`, `dead.users` targets.

```mermaid
flowchart TD
    A[Pattern: dlq.*] --> B[Query Management API]
    B --> C[Get All Queues]
    C --> D{Match Pattern}
    D --> E[dlq.orders]
    D --> F[dlq.users]
    D --> G[dlq.payments]
    E --> H[Target: dead.orders]
    F --> I[Target: dead.users]
    G --> J[Target: dead.payments]
```

> **Note:** Wildcards require the RabbitMQ Management API (default port 15672).

---

## Architecture

### Message Flow

```mermaid
flowchart TD
    A[Message Published] --> B[Main Queue]
    B -->|Processing Fails| C[Dead Letter Queue]
    C --> D{Retry Checker}
    D -->|x-death count ≤ max| E[Leave in DLQ]
    D -->|x-death count > max| F[permanent_failure_queue]
    F --> G[Manual Review]
```

> **Note:** Messages within the retry limit are left in the DLQ (via nack with requeue). The retry mechanism depends on your RabbitMQ setup—typically the DLQ is configured to dead-letter back to the main queue after a TTL.

### Processing Logic

```mermaid
flowchart LR
    subgraph Initialization
        A1[Load Config] --> A2[Connect to RabbitMQ]
        A2 --> A3[Resolve Queue Patterns]
    end
    
    subgraph Processing
        B1[Get Message from DLQ] --> B2{Message Exists?}
        B2 -->|No| B4[Done]
        B2 -->|Yes| B3[Check x-death Count]
        B3 --> B5{Exceeds Limit?}
        B5 -->|Yes| B6[Move to Failure Queue]
        B5 -->|No| B7[Nack & Requeue]
        B6 --> B1
        B7 --> B1
    end
    
    A3 --> B1
```

### Message Safety

The tool uses RabbitMQ's publisher confirms to ensure no messages are lost:

```mermaid
flowchart TD
    A[Get Message from DLQ] --> B[Publish to Failure Queue]
    B --> C{Broker Confirms?}
    C -->|Yes| D[Ack Original Message]
    C -->|No/Error| E[Nack & Requeue]
    D --> F[Message Safely Moved]
    E --> G[Message Stays in DLQ]
```

| Scenario | Outcome |
|----------|---------|
| Script dies before publish | Message remains in DLQ (no loss) |
| Script dies after publish, before ack | Message may be duplicated on restart |
| Broker rejects publish | Message remains in DLQ, error logged |
| Network failure during publish | Message remains in DLQ (no loss) |

---

## Scheduling

### Cron

Run every 5 minutes:

```bash
*/5 * * * * /usr/bin/python3 /opt/rmq_retry_checker.py /etc/rmq/config.yaml
```

### Systemd Timer

**Service file** (`/etc/systemd/system/rmq-checker.service`):

```ini
[Unit]
Description=RabbitMQ DLQ Retry Checker

[Service]
Type=oneshot
ExecStart=/usr/bin/python3 /opt/rmq_retry_checker.py /etc/rmq/config.yaml
```

**Timer file** (`/etc/systemd/system/rmq-checker.timer`):

```ini
[Unit]
Description=Run RMQ Retry Checker every 5 minutes

[Timer]
OnBootSec=5min
OnUnitActiveSec=5min

[Install]
WantedBy=timers.target
```

**Enable the timer:**

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now rmq-checker.timer
```

**Check status:**

```bash
systemctl status rmq-checker.timer
systemctl list-timers --all
```
