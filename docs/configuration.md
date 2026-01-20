# Configuration

The tool supports three configuration methods (in order of precedence):

1. Command-line arguments (highest)
2. YAML config file
3. Environment variables (lowest)

## Config File

Create `config.yaml`:

```yaml
rabbitmq:
  host: localhost
  port: 5672
  mgmt_port: 15672    # Required for wildcard support
  username: guest
  password: guest
  vhost: /
  use_ssl: false
  ssl_verify: true    # Set false for self-signed certs

queues:
  dlq_name: my_dlq                        # Supports wildcards: dlq.*
  target_queue: permanent_failure_queue   # Supports wildcards: dead.*
  max_retry_count: 3
  dedup_file: .rmq_processed_ids          # File to track processed messages
```

## Environment Variables

Create a `.env` file or export variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `RMQ_HOST` | localhost | RabbitMQ host |
| `RMQ_PORT` | 5672 | AMQP port |
| `RMQ_MGMT_PORT` | 15672 | Management API port |
| `RMQ_USERNAME` | guest | Username |
| `RMQ_PASSWORD` | guest | Password |
| `RMQ_VHOST` | / | Virtual host |
| `RMQ_USE_SSL` | false | Enable SSL/TLS |
| `RMQ_SSL_VERIFY` | true | Verify SSL certificates |
| `DLQ_NAME` | my_dlq | DLQ name or pattern |
| `TARGET_QUEUE` | permanent_failure_queue | Target queue or pattern |
| `MAX_RETRY_COUNT` | 3 | Retry threshold |
| `DEDUP_FILE` | .rmq_processed_ids | File to track processed message IDs |

## Command-Line Options

| Option | Description |
|--------|-------------|
| `--config FILE` | YAML config file path |
| `--host` | RabbitMQ host |
| `--port` | RabbitMQ port |
| `--mgmt-port` | Management API port |
| `--username` | RabbitMQ username |
| `--password` | RabbitMQ password |
| `--vhost` | Virtual host |
| `--ssl` | Enable SSL/TLS |
| `--no-ssl-verify` | Disable SSL certificate verification |
| `--dlq` | DLQ name (supports wildcards) |
| `--target-queue` | Target queue for failed messages |
| `--max-retries` | Max retry count |
| `--dedup-file` | File to store processed message IDs |
| `--output-format` | `text` or `json` |
| `--quiet` | Suppress logs |
| `--verbose` | Verbose logging |

## Wildcard Patterns

Use `*` and `?` to match multiple queues:

```bash
# Matches: dlq.orders, dlq.users, dlq.payments
# Creates: dead.orders, dead.users, dead.payments
python rmq_retry_checker.py --dlq "dlq.*" --target-queue "dead.*"
```

**Note:** Wildcards require the RabbitMQ Management API (default port 15672).
