# RabbitMQ Retry Checker

Detect and handle infinite retry loops in RabbitMQ Dead Letter Queues.

## How It Works

```mermaid
flowchart LR
    A[DLQ Message] --> B{x-death > max?}
    B -->|Yes| C[Move to Failure Queue]
    B -->|No| D[Requeue]
```

The tool checks `x-death` count on messages and moves those exceeding the retry limit to a permanent failure queue.

## Quick Start

```bash
# Install dependencies (pika, pyyaml, python-dotenv)
pip install -r requirements.txt

# Run with config file
python rmq_retry_checker.py --config config.yaml

# Or with command line options
python rmq_retry_checker.py --dlq my_dlq --target-queue failed_queue --max-retries 3
```

## Configuration

Create `config.yaml` (minimal example):

```yaml
rabbitmq:
  host: localhost
  port: 5672
  username: guest
  password: guest
  # vhost: /              # Optional, defaults to /
  # use_ssl: false        # Optional, enable for TLS
  # ssl_verify: true      # Optional, set false for self-signed certs

queues:
  dlq_name: my_dlq                        # Supports wildcards: dlq.*
  target_queue: permanent_failure_queue   # Supports wildcards: dead.*
  max_retry_count: 3
```

See [Configuration Guide](docs/configuration.md) for all options.

## Wildcard Support

Process multiple queues at once:

```bash
python rmq_retry_checker.py --dlq "dlq.*" --target-queue "dead.*"
```

This matches `dlq.orders`, `dlq.users`, etc. and creates corresponding `dead.orders`, `dead.users` targets.

## Output Formats

```bash
# Human-readable (default)
python rmq_retry_checker.py

# JSON (for automation)
python rmq_retry_checker.py --output-format json --quiet
```

## Documentation

- [Architecture & Flowcharts](docs/architecture.md)
- [Configuration Reference](docs/configuration.md)
- [Scheduling (Cron/Systemd)](docs/scheduling.md)
