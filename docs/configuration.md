# Configuration

All configuration is done through a YAML config file.

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
```

## Usage

```bash
python rmq_retry_checker.py config.yaml
```

## Wildcard Patterns

Use `*` and `?` to match multiple queues in your config file:

```yaml
queues:
  dlq_name: "dlq.*"        # Matches: dlq.orders, dlq.users, dlq.payments
  target_queue: "dead.*"   # Creates: dead.orders, dead.users, dead.payments
```

**Note:** Wildcards require the RabbitMQ Management API (default port 15672).
