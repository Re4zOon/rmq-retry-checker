# RabbitMQ Retry Checker

A Python script to detect and handle infinite retry loops in RabbitMQ Dead Letter Queues (DLQ).

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

- ✅ Connects to RabbitMQ (supports quorum queues)
- ✅ Inspects DLQ messages without removing them
- ✅ Checks `x-death.count` header to detect retry loops
- ✅ Moves messages exceeding retry limit to a third queue (permanent failure queue)
- ✅ Configurable retry threshold
- ✅ SSL/TLS support
- ✅ Comprehensive logging

## Requirements

- Python 3.7+
- RabbitMQ server (tested with quorum queues)
- Access to RabbitMQ management interface or connection credentials

## Installation

1. Clone the repository:
```bash
git clone https://github.com/Re4zOon/rmq-retry-checker.git
cd rmq-retry-checker
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure the script:
```bash
cp .env.example .env
# Edit .env with your RabbitMQ connection details
```

## Configuration

Create a `.env` file in the project directory with the following settings:

```ini
# RabbitMQ Connection Settings
RMQ_HOST=localhost
RMQ_PORT=5672
RMQ_USERNAME=guest
RMQ_PASSWORD=guest
RMQ_VHOST=/

# Queue Settings
DLQ_NAME=my_dlq                          # Name of your Dead Letter Queue
TARGET_QUEUE=permanent_failure_queue      # Queue for messages exceeding retry limit
MAX_RETRY_COUNT=3                         # Maximum number of retries before moving to permanent failure queue

# Optional: Use SSL/TLS
# RMQ_USE_SSL=true
```

### Configuration Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `RMQ_HOST` | RabbitMQ server hostname | `localhost` |
| `RMQ_PORT` | RabbitMQ server port | `5672` |
| `RMQ_USERNAME` | RabbitMQ username | `guest` |
| `RMQ_PASSWORD` | RabbitMQ password | `guest` |
| `RMQ_VHOST` | RabbitMQ virtual host | `/` |
| `DLQ_NAME` | Dead Letter Queue name | `my_dlq` |
| `TARGET_QUEUE` | Permanent failure queue name | `permanent_failure_queue` |
| `MAX_RETRY_COUNT` | Maximum retry attempts before moving message | `3` |
| `RMQ_USE_SSL` | Enable SSL/TLS connection | `false` |

## Usage

Run the script:

```bash
python rmq_retry_checker.py
```

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
2026-01-19 17:52:00,180 - __main__ - INFO - Processing complete. Processed: 5, Moved to permanent failure queue: 2
2026-01-19 17:52:00,185 - __main__ - INFO - Connection closed
2026-01-19 17:52:00,186 - __main__ - INFO - RabbitMQ Retry Checker completed successfully
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

## Scheduling

To run this script periodically, you can use:

### Cron (Linux/Mac)

```bash
# Run every 5 minutes
*/5 * * * * cd /path/to/rmq-retry-checker && /usr/bin/python3 rmq_retry_checker.py >> /var/log/rmq-checker.log 2>&1
```

### Systemd Timer (Linux)

Create a service and timer unit for systemd.

### Task Scheduler (Windows)

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