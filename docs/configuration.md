# Configuration

Configuration is done through a YAML file.

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
  dedup_max_age_hours: 168                # Auto-cleanup entries older than N hours
```

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `rabbitmq.host` | localhost | RabbitMQ host |
| `rabbitmq.port` | 5672 | AMQP port |
| `rabbitmq.mgmt_port` | 15672 | Management API port (for wildcards) |
| `rabbitmq.username` | guest | Username |
| `rabbitmq.password` | guest | Password |
| `rabbitmq.vhost` | / | Virtual host |
| `rabbitmq.use_ssl` | false | Enable SSL/TLS |
| `rabbitmq.ssl_verify` | true | Verify SSL certificates |
| `queues.dlq_name` | my_dlq | DLQ name or pattern |
| `queues.target_queue` | permanent_failure_queue | Target queue or pattern |
| `queues.max_retry_count` | 3 | Retry threshold |
| `queues.dedup_file` | .rmq_processed_ids | File to track processed message IDs |
| `queues.dedup_max_age_hours` | 168 | Hours to keep dedup entries before cleanup |

## Wildcard Patterns

Use `*` and `?` to match multiple queues:

```yaml
queues:
  dlq_name: "dlq.*"
  target_queue: "dead.*"
```

This matches `dlq.orders`, `dlq.users`, `dlq.payments` and creates corresponding `dead.orders`, `dead.users`, `dead.payments` targets.

**Note:** Wildcards require the RabbitMQ Management API (default port 15672).
