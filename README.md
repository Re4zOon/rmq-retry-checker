# RabbitMQ Retry Checker

A single-file Python script to detect and handle infinite retry loops in RabbitMQ Dead Letter Queues (DLQ).

## Problem Statement

When using RabbitMQ with quorum queues:
- If a consumer can't process a message, it nacks the message back to the DLQ
- Adding TTL on the DLQ to replay messages can create an infinite loop
- Messages that continuously fail need to be identified and moved to a permanent failure queue

## Solution

This script:
1. Connects to RabbitMQ
2. Inspects messages in the DLQ
3. Checks the `x-death` count for each message
4. Moves messages exceeding a configurable retry threshold to a permanent failure queue

## Features

- ✅ **Single-file script** - Easy to deploy in automation environments
- ✅ **Multiple configuration methods** - Command-line args, config file (.ini), or environment variables (.env)
- ✅ **Human and machine-readable output** - Text format for humans, JSON format for automation
- ✅ Connects to RabbitMQ (supports quorum queues)
- ✅ Inspects DLQ messages without removing them
- ✅ Checks `x-death.count` header to detect retry loops
- ✅ Moves messages exceeding retry limit to a third queue (permanent failure queue)
- ✅ Configurable retry threshold
- ✅ SSL/TLS support
- ✅ Comprehensive logging with quiet/verbose modes

## Requirements

- Python 3.7+
- RabbitMQ server (tested with quorum queues)
- Access to RabbitMQ management interface or connection credentials

## Installation

1. Download the script:
```bash
# Clone the repository
git clone https://github.com/Re4zOon/rmq-retry-checker.git
cd rmq-retry-checker

# Or just download the single file
curl -O https://raw.githubusercontent.com/Re4zOon/rmq-retry-checker/main/rmq_retry_checker.py
```

2. Install dependencies:
```bash
pip install pika python-dotenv
```

That's it! The script is ready to use.

## Configuration

The script supports three configuration methods (in order of precedence):

### 1. Command-Line Arguments (Highest Priority)

```bash
python rmq_retry_checker.py --host localhost --dlq my_dlq --target-queue failed_queue --max-retries 3
```

### 2. Configuration File (.ini format)

Create a `config.ini` file:

```ini
[rabbitmq]
host = localhost
port = 5672
username = guest
password = guest
vhost = /
use_ssl = false

[queues]
dlq_name = my_dlq
target_queue = permanent_failure_queue
max_retry_count = 3
```

Run with:
```bash
python rmq_retry_checker.py --config config.ini
```

### 3. Environment Variables (.env file)

Create a `.env` file:

```ini
RMQ_HOST=localhost
RMQ_PORT=5672
RMQ_USERNAME=guest
RMQ_PASSWORD=guest
RMQ_VHOST=/
DLQ_NAME=my_dlq
TARGET_QUEUE=permanent_failure_queue
MAX_RETRY_COUNT=3
```

Run with:
```bash
python rmq_retry_checker.py
```

### Configuration Parameters

| Parameter | CLI Argument | Config File | Environment Variable | Default |
|-----------|--------------|-------------|---------------------|---------|
| RabbitMQ Host | `--host` | `[rabbitmq] host` | `RMQ_HOST` | `localhost` |
| RabbitMQ Port | `--port` | `[rabbitmq] port` | `RMQ_PORT` | `5672` |
| Username | `--username` | `[rabbitmq] username` | `RMQ_USERNAME` | `guest` |
| Password | `--password` | `[rabbitmq] password` | `RMQ_PASSWORD` | `guest` |
| Virtual Host | `--vhost` | `[rabbitmq] vhost` | `RMQ_VHOST` | `/` |
| Use SSL | `--ssl` | `[rabbitmq] use_ssl` | `RMQ_USE_SSL` | `false` |
| DLQ Name | `--dlq` | `[queues] dlq_name` | `DLQ_NAME` | `my_dlq` |
| Target Queue | `--target-queue` | `[queues] target_queue` | `TARGET_QUEUE` | `permanent_failure_queue` |
| Max Retries | `--max-retries` | `[queues] max_retry_count` | `MAX_RETRY_COUNT` | `3` |

## Usage

### Basic Usage

```bash
# Using default configuration
python rmq_retry_checker.py

# With specific parameters
python rmq_retry_checker.py --dlq my_dlq --max-retries 5

# Using config file
python rmq_retry_checker.py --config /path/to/config.ini
```

### Output Formats

**Human-readable output (default):**
```bash
python rmq_retry_checker.py

# Output:
# ============================================================
# RabbitMQ Retry Checker - Execution Summary
# ============================================================
# Status: SUCCESS
# DLQ: my_dlq
# Target Queue: permanent_failure_queue
# Max Retry Count: 3
# ------------------------------------------------------------
# Messages Processed: 5
# Messages Moved to Failure Queue: 2
# Messages Requeued: 3
# Duration: 0.45 seconds
# ============================================================
```

**Machine-readable JSON output (for automation):**
```bash
python rmq_retry_checker.py --output-format json

# Output:
# {
#   "status": "success",
#   "timestamp": "2026-01-19T18:15:28.456986",
#   "config": {
#     "rmq_host": "localhost",
#     "rmq_port": 5672,
#     "dlq_name": "my_dlq",
#     "target_queue": "permanent_failure_queue",
#     "max_retry_count": 3
#   },
#   "results": {
#     "messages_processed": 5,
#     "messages_moved": 2,
#     "messages_requeued": 3,
#     "duration_seconds": 0.45
#   }
# }
```

### Logging Options

```bash
# Quiet mode (errors only, show summary)
python rmq_retry_checker.py --quiet

# Verbose mode (detailed logging)
python rmq_retry_checker.py --verbose

# JSON output with quiet mode (for automation)
python rmq_retry_checker.py --output-format json --quiet
```

### Complete Examples

```bash
# Production environment with custom config
python rmq_retry_checker.py \
  --host prod-rmq.example.com \
  --port 5671 \
  --ssl \
  --username prod_user \
  --password 'secret' \
  --dlq production_dlq \
  --target-queue failed_messages \
  --max-retries 5 \
  --output-format json

# Using config file for automation
python rmq_retry_checker.py --config /etc/rmq-checker/prod.ini --output-format json --quiet
```

### Get Help

```bash
python rmq_retry_checker.py --help
```

## How the Script Works

The script will:
1. Connect to RabbitMQ using the configured credentials
2. Check all messages in the specified DLQ
3. For each message:
   - Extract the `x-death` count from message headers
   - If count > `MAX_RETRY_COUNT`: move to permanent failure queue
   - If count ≤ `MAX_RETRY_COUNT`: requeue the message in DLQ
4. Log all actions and provide a summary

### Example Output

```
2026-01-19 17:52:00,123 - __main__ - INFO - Starting RabbitMQ Retry Checker
2026-01-19 17:52:00,124 - __main__ - INFO - Configuration: DLQ='my_dlq', Target Queue='permanent_failure_queue', Max Retry Count=3
2026-01-19 17:52:00,150 - __main__ - INFO - Connected to RabbitMQ at localhost:5672
2026-01-19 17:52:00,155 - __main__ - INFO - Target queue 'permanent_failure_queue' is ready
2026-01-19 17:52:00,160 - __main__ - INFO - DLQ 'my_dlq' has 5 messages
2026-01-19 17:52:00,165 - __main__ - INFO - Processing message 1: delivery_tag=1, x-death count=2
2026-01-19 17:52:00,166 - __main__ - INFO - Message retry count (2) is within limit. Requeuing.
2026-01-19 17:52:00,170 - __main__ - INFO - Processing message 2: delivery_tag=2, x-death count=5
2026-01-19 17:52:00,171 - __main__ - WARNING - Message exceeded retry limit (5 > 3). Moving to permanent failure queue: permanent_failure_queue
2026-01-19 17:52:00,175 - __main__ - INFO - Successfully moved message to permanent_failure_queue
2026-01-19 17:52:00,180 - __main__ - INFO - Processing complete. Processed: 5, Moved to permanent failure queue: 2, Requeued: 3
2026-01-19 17:52:00,185 - __main__ - INFO - Connection closed
2026-01-19 17:52:00,186 - __main__ - INFO - RabbitMQ Retry Checker completed successfully

============================================================
RabbitMQ Retry Checker - Execution Summary
============================================================
Status: SUCCESS
DLQ: my_dlq
Target Queue: permanent_failure_queue
Max Retry Count: 3
------------------------------------------------------------
Messages Processed: 5
Messages Moved to Failure Queue: 2
Messages Requeued: 3
Duration: 0.45 seconds
============================================================
```

## How It Works

### x-death Header

When a message is dead-lettered in RabbitMQ, it receives an `x-death` header that tracks:
- The queue it was dead-lettered from
- The reason for dead-lettering
- A count of how many times it has been dead-lettered
- Timestamps

Example `x-death` header structure:
```python
{
    'x-death': [
        {
            'count': 5,
            'reason': 'rejected',
            'queue': 'original_queue',
            'time': datetime,
            'exchange': '',
            'routing-keys': ['original_queue']
        }
    ]
}
```

### Processing Logic

1. **Connect**: Establish connection to RabbitMQ
2. **Inspect**: Retrieve messages from DLQ without auto-acknowledgment
3. **Check**: Extract `x-death[0].count` from message headers
4. **Decide**:
   - If `count > MAX_RETRY_COUNT`: Move to permanent failure queue + ACK
   - If `count ≤ MAX_RETRY_COUNT`: NACK with requeue=True
5. **Log**: Record all actions for monitoring

## Automation & Scheduling

The script is designed to run in automation environments. Use `--output-format json` and `--quiet` for clean, parseable output.

### Cron (Linux/Mac)

```bash
# Run every 5 minutes with JSON output
*/5 * * * * /usr/bin/python3 /path/to/rmq_retry_checker.py --output-format json --quiet >> /var/log/rmq-checker.log 2>&1

# Run every hour with config file
0 * * * * /usr/bin/python3 /path/to/rmq_retry_checker.py --config /etc/rmq/prod.ini --quiet
```

### Systemd Timer (Linux)

**Service file** (`/etc/systemd/system/rmq-checker.service`):
```ini
[Unit]
Description=RabbitMQ DLQ Retry Checker
After=network.target

[Service]
Type=oneshot
ExecStart=/usr/bin/python3 /opt/rmq_retry_checker.py --config /etc/rmq/prod.ini --output-format json --quiet
StandardOutput=journal
StandardError=journal
```

**Timer file** (`/etc/systemd/system/rmq-checker.timer`):
```ini
[Unit]
Description=Run RabbitMQ Retry Checker every 5 minutes

[Timer]
OnBootSec=5min
OnUnitActiveSec=5min

[Install]
WantedBy=timers.target
```

Enable with:
```bash
sudo systemctl enable rmq-checker.timer
sudo systemctl start rmq-checker.timer
```

### Task Scheduler (Windows)

```powershell
# Create a scheduled task
schtasks /create /tn "RMQ Retry Checker" /tr "python C:\scripts\rmq_retry_checker.py --output-format json --quiet" /sc minute /mo 5
```

### Docker/Kubernetes CronJob

**Dockerfile:**
```dockerfile
FROM python:3.9-slim
RUN pip install pika python-dotenv
COPY rmq_retry_checker.py /app/
WORKDIR /app
ENTRYPOINT ["python", "rmq_retry_checker.py"]
```

**Kubernetes CronJob:**
```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: rmq-retry-checker
spec:
  schedule: "*/5 * * * *"
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: checker
            image: rmq-retry-checker:latest
            args: ["--output-format", "json", "--quiet"]
            env:
            - name: RMQ_HOST
              value: "rabbitmq.default.svc.cluster.local"
            - name: DLQ_NAME
              value: "my_dlq"
          restartPolicy: OnFailure
```

### CI/CD Integration

The JSON output format makes it easy to integrate with CI/CD pipelines:

```bash
#!/bin/bash
# Run checker and parse results
result=$(python rmq_retry_checker.py --output-format json --quiet 2>&1)
status=$(echo "$result" | jq -r '.status')
moved=$(echo "$result" | jq -r '.results.messages_moved')

if [ "$moved" -gt 10 ]; then
  echo "WARNING: $moved messages moved to failure queue"
  # Send alert, create ticket, etc.
fi
```

Use Windows Task Scheduler to run the script at regular intervals.

## Error Handling

- **Connection Failures**: Script logs error and exits gracefully
- **Queue Not Found**: Script logs error and exits
- **Message Processing Errors**: Failed messages are NACKed and requeued
- **Interrupted Execution**: Proper cleanup and connection closure

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is open-source and available under the MIT License.

## Support

For issues, questions, or contributions, please open an issue on GitHub.