# RabbitMQ Retry Checker

A Python script to detect and handle infinite retry loops in RabbitMQ Dead Letter Queues (DLQ).

## What It Does

- Connects to RabbitMQ
- Checks messages in the DLQ for their `x-death` count
- Moves messages exceeding the retry limit to a permanent failure queue
- Supports wildcard patterns to process multiple queues (e.g., `dlq.*` → `dead.*`)

## Installation

```bash
pip install pika pyyaml python-dotenv
```

## Configuration

### Option 1: Config File (Recommended)

Create `config.yaml`:

```yaml
rabbitmq:
  host: localhost
  port: 5672
  mgmt_port: 15672  # Required for wildcard support
  username: guest
  password: guest
  vhost: /

queues:
  dlq_name: my_dlq           # Or use wildcards: dlq.*
  target_queue: failed_queue  # Or use wildcards: dead.*
  max_retry_count: 3
```

Run:
```bash
python rmq_retry_checker.py --config config.yaml
```

### Option 2: Environment Variables

Create `.env` file:

```ini
RMQ_HOST=localhost
RMQ_PORT=5672
RMQ_USERNAME=guest
RMQ_PASSWORD=guest
RMQ_VHOST=/
DLQ_NAME=my_dlq
TARGET_QUEUE=failed_queue
MAX_RETRY_COUNT=3
```

Run:
```bash
python rmq_retry_checker.py
```

### Option 3: Command Line

```bash
python rmq_retry_checker.py --host localhost --dlq my_dlq --target-queue failed_queue --max-retries 3
```

## Wildcard Support

Process multiple queues with pattern matching:

```bash
# DLQs: dlq.queue1, dlq.queue2 → Targets: dead.queue1, dead.queue2
python rmq_retry_checker.py --dlq "dlq.*" --target-queue "dead.*"
```

**Note:** Wildcards require the RabbitMQ Management API (port 15672).

## Output

### Text (default)
```bash
python rmq_retry_checker.py
```

### JSON (for automation)
```bash
python rmq_retry_checker.py --output-format json --quiet
```

## Options

| Option | Description |
|--------|-------------|
| `--config FILE` | YAML config file path |
| `--host` | RabbitMQ host |
| `--port` | RabbitMQ port (default: 5672) |
| `--mgmt-port` | Management API port (default: 15672) |
| `--username` | RabbitMQ username |
| `--password` | RabbitMQ password |
| `--vhost` | Virtual host (default: /) |
| `--ssl` | Enable SSL/TLS |
| `--no-ssl-verify` | Disable SSL certificate verification (for self-signed certs) |
| `--dlq` | DLQ name (supports wildcards) |
| `--target-queue` | Target queue for failed messages |
| `--max-retries` | Max retry count (default: 3) |
| `--output-format` | `text` or `json` |
| `--quiet` | Suppress logs |
| `--verbose` | Verbose logging |

## Scheduling

### Cron
```bash
*/5 * * * * /usr/bin/python3 /path/to/rmq_retry_checker.py --config /etc/rmq/config.yaml --quiet
```

### Systemd Timer

Service file (`/etc/systemd/system/rmq-checker.service`):
```ini
[Unit]
Description=RabbitMQ DLQ Retry Checker

[Service]
Type=oneshot
ExecStart=/usr/bin/python3 /opt/rmq_retry_checker.py --config /etc/rmq/config.yaml --quiet
```

Timer file (`/etc/systemd/system/rmq-checker.timer`):
```ini
[Unit]
Description=Run RMQ Retry Checker every 5 minutes

[Timer]
OnBootSec=5min
OnUnitActiveSec=5min

[Install]
WantedBy=timers.target
```

Enable:
```bash
sudo systemctl enable --now rmq-checker.timer
```
