#!/usr/bin/env python3
"""
RabbitMQ Retry Checker - Detect and handle infinite retry loops in DLQs.

Usage:
    python rmq_retry_checker.py config.yaml

Moves messages that have exceeded the retry limit to a failure queue.
Messages within the limit are requeued for retry.
"""
import sys
import logging
import json
import fnmatch
import re
import urllib.request
import urllib.parse
import base64
import ssl
import fcntl
import atexit
import os

import pika
import yaml

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- Counters (global for simplicity) ---
messages_processed = 0
messages_moved = 0
messages_requeued = 0
queue_stats = {}  # Per-queue statistics: {queue_name: {'processed': N, 'moved': N, 'requeued': N}}


# =============================================================================
# Lock File (Prevent Parallel Execution)
# =============================================================================

LOCK_FILE_PATH = '/tmp/rmq_retry_checker.lock'
_lock_file_handle = None


def acquire_lock():
    """
    Acquire an exclusive lock to prevent parallel script execution.

    Uses fcntl.flock() which automatically releases the lock when the
    process exits (even on crash). Returns True if lock acquired,
    False if another instance is already running.
    """
    global _lock_file_handle
    try:
        _lock_file_handle = open(LOCK_FILE_PATH, 'w')
        fcntl.flock(_lock_file_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        # Write PID to lock file for debugging purposes
        _lock_file_handle.write(str(os.getpid()))
        _lock_file_handle.flush()
        logger.info(f"Lock acquired: {LOCK_FILE_PATH}")
        return True
    except BlockingIOError:
        # Lock is held by another process
        if _lock_file_handle:
            _lock_file_handle.close()
            _lock_file_handle = None
        logger.error("Cannot acquire lock - another instance is already running")
        return False
    except (IOError, OSError) as e:
        if _lock_file_handle:
            _lock_file_handle.close()
            _lock_file_handle = None
        logger.error(f"Cannot acquire lock: {e}")
        return False


def release_lock():
    """Release the lock file."""
    global _lock_file_handle
    if _lock_file_handle:
        try:
            _lock_file_handle.close()
            logger.info("Lock released")
        except Exception as e:
            logger.warning(f"Error releasing lock: {e}")
        finally:
            _lock_file_handle = None


# =============================================================================
# Configuration
# =============================================================================

def load_config(config_file):
    """Load configuration from YAML file. Returns a dict with all settings."""
    # Default values
    config = {
        'host': 'localhost',
        'port': 5672,
        'mgmt_port': 15672,
        'username': 'guest',
        'password': 'guest',
        'vhost': '/',
        'use_ssl': False,
        'ssl_verify': True,
        'dlq_name': 'my_dlq',
        'target_queue': 'permanent_failure_queue',
        'max_retry_count': 3,
    }

    try:
        with open(config_file, 'r') as f:
            data = yaml.safe_load(f)
    except FileNotFoundError:
        logger.error(f"Config file not found: {config_file}")
        raise
    except yaml.YAMLError as e:
        logger.error(f"Invalid YAML in config file: {e}")
        raise

    if data is None:
        return config

    # Load RabbitMQ settings
    if 'rabbitmq' in data:
        rmq = data['rabbitmq']
        config['host'] = rmq.get('host', config['host'])
        config['port'] = int(rmq.get('port', config['port']))
        config['mgmt_port'] = int(rmq.get('mgmt_port', config['mgmt_port']))
        config['username'] = rmq.get('username', config['username'])
        config['password'] = rmq.get('password', config['password'])
        config['vhost'] = rmq.get('vhost', config['vhost'])
        use_ssl = rmq.get('use_ssl', config['use_ssl'])
        config['use_ssl'] = use_ssl.lower() == 'true' if isinstance(use_ssl, str) else use_ssl
        ssl_verify = rmq.get('ssl_verify', config['ssl_verify'])
        config['ssl_verify'] = ssl_verify.lower() == 'true' if isinstance(ssl_verify, str) else ssl_verify

    # Load queue settings
    if 'queues' in data:
        queues = data['queues']
        config['dlq_name'] = queues.get('dlq_name', config['dlq_name'])
        config['target_queue'] = queues.get('target_queue', config['target_queue'])
        config['max_retry_count'] = int(queues.get('max_retry_count', config['max_retry_count']))

    return config


# =============================================================================
# RabbitMQ Connection
# =============================================================================

def connect_to_rabbitmq(config):
    """Connect to RabbitMQ and return (connection, channel). Returns (None, None) on failure."""
    try:
        credentials = pika.PlainCredentials(config['username'], config['password'])
        parameters = pika.ConnectionParameters(
            host=config['host'],
            port=config['port'],
            virtual_host=config['vhost'],
            credentials=credentials,
            heartbeat=600,
            blocked_connection_timeout=300
        )

        if config['use_ssl']:
            ssl_context = ssl.create_default_context()
            if not config['ssl_verify']:
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
            parameters.ssl_options = pika.SSLOptions(context=ssl_context)

        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.confirm_delivery()  # Enable publisher confirms for safety
        logger.info("Connected to RabbitMQ with publisher confirms enabled")
        return connection, channel
    except Exception as e:
        logger.error(f"Failed to connect to RabbitMQ: {e}")
        return None, None


def close_connection(connection):
    """Close the RabbitMQ connection."""
    if connection and not connection.is_closed:
        connection.close()
        logger.info("Connection closed")


# =============================================================================
# Wildcard Support
# =============================================================================

def has_wildcard(pattern):
    """Check if pattern contains wildcards."""
    return '*' in pattern or '?' in pattern


def list_queues_from_api(config):
    """List all queues from RabbitMQ Management API."""
    protocol = 'https' if config['use_ssl'] else 'http'
    vhost_encoded = urllib.parse.quote(config['vhost'], safe='')
    url = f"{protocol}://{config['host']}:{config['mgmt_port']}/api/queues/{vhost_encoded}"

    auth_string = f"{config['username']}:{config['password']}"
    auth_b64 = base64.b64encode(auth_string.encode('ascii')).decode('ascii')

    request = urllib.request.Request(url)
    request.add_header('Authorization', f'Basic {auth_b64}')

    context = None
    if config['use_ssl']:
        context = ssl.create_default_context()
        if not config['ssl_verify']:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE

    try:
        with urllib.request.urlopen(request, timeout=10, context=context) as response:
            data = json.loads(response.read().decode('utf-8'))
    except Exception as e:
        logger.error(f"Failed to list queues from Management API: {e}")
        raise

    return [q['name'] for q in data if 'name' in q]


def derive_target_queue(dlq_name, dlq_pattern, target_pattern):
    """Derive target queue name from DLQ name using wildcard patterns."""
    dlq_regex = re.escape(dlq_pattern)
    dlq_regex = dlq_regex.replace(r'\*', '(.*?)').replace(r'\?', '(.)')
    dlq_regex = f'^{dlq_regex}$'

    match = re.match(dlq_regex, dlq_name)
    if not match:
        raise ValueError(f"Unable to derive target queue for '{dlq_name}'")

    captured_parts = list(match.groups())
    parts_iter = iter(captured_parts)

    # Count wildcards in target pattern to validate
    target_wildcard_count = target_pattern.count('*') + target_pattern.count('?')
    if target_wildcard_count != len(captured_parts):
        raise ValueError(
            f"Wildcard mismatch: target pattern '{target_pattern}' has {target_wildcard_count} wildcards "
            f"but DLQ pattern '{dlq_pattern}' has {len(captured_parts)}. "
            "Ensure both patterns have the same number of wildcards."
        )

    def replace_wildcard(match_obj):
        return next(parts_iter)

    return re.sub(r'[*?]', replace_wildcard, target_pattern)


def get_queue_pairs(config):
    """Get list of (dlq_name, target_queue) pairs, supporting wildcard patterns."""
    dlq_pattern = config['dlq_name']
    target_pattern = config['target_queue']

    dlq_has_wildcard = has_wildcard(dlq_pattern)
    target_has_wildcard = has_wildcard(target_pattern)

    # No wildcards - just return the single pair
    if not dlq_has_wildcard and not target_has_wildcard:
        return [(dlq_pattern, target_pattern)]

    # Wildcards found - query Management API
    logger.info("Wildcard pattern detected, querying RabbitMQ Management API")
    all_queues = list_queues_from_api(config)
    matching_dlqs = [q for q in all_queues if fnmatch.fnmatch(q, dlq_pattern)]

    if not matching_dlqs:
        logger.warning(f"No queues found matching DLQ pattern: {dlq_pattern}")
        return []

    logger.info(f"Found {len(matching_dlqs)} DLQ(s) matching '{dlq_pattern}'")

    queue_pairs = []
    for dlq_name in matching_dlqs:
        if target_has_wildcard:
            target_queue = derive_target_queue(dlq_name, dlq_pattern, target_pattern)
        else:
            target_queue = target_pattern
        queue_pairs.append((dlq_name, target_queue))

    return queue_pairs


# =============================================================================
# Message Processing
# =============================================================================

def get_death_count(headers):
    """Extract the x-death count from message headers."""
    if not headers:
        return 0
    x_death = headers.get('x-death')
    if not x_death or not isinstance(x_death, list) or len(x_death) == 0:
        return 0
    first_death = x_death[0]
    if isinstance(first_death, dict):
        count = first_death.get('count', 0)
        return int(count) if count else 0
    return 0


def ensure_target_queue_exists(channel, target_queue):
    """Ensure the target queue exists (tries quorum first, falls back to classic)."""
    try:
        channel.queue_declare(queue=target_queue, durable=True, arguments={'x-queue-type': 'quorum'})
        logger.info(f"Target queue '{target_queue}' is ready (quorum)")
    except pika.exceptions.ChannelClosedByBroker:
        # The broker has closed the channel; it is no longer usable, so we cannot
        # safely fall back to declaring a classic queue on this channel.
        logger.error(
            f"Failed to declare quorum queue '{target_queue}': channel closed by broker; "
            "cannot fall back to classic queue on a closed channel."
        )
        raise
    except Exception:
        # For other errors where the channel remains open, fall back to a classic queue.
        channel.queue_declare(queue=target_queue, durable=True)
        logger.info(f"Target queue '{target_queue}' is ready (classic)")


def process_message(channel, method, properties, body, target_queue, max_retry_count, dlq_name):
    """
    Process a single message from the DLQ.

    Safety: Messages are only acked AFTER successful publish to target queue.
    If publish fails, message is nacked and requeued (no loss).
    """
    global messages_processed, messages_moved, messages_requeued, queue_stats
    messages_processed += 1

    # Track per-queue statistics
    if dlq_name not in queue_stats:
        queue_stats[dlq_name] = {'processed': 0, 'moved': 0, 'requeued': 0}
    queue_stats[dlq_name]['processed'] += 1

    death_count = get_death_count(properties.headers or {})
    logger.info(f"Message {messages_processed}: x-death count={death_count}")

    if death_count > max_retry_count:
        # Message exceeded retry limit - move to failure queue
        logger.warning(f"Message exceeded retry limit ({death_count} > {max_retry_count}), moving to {target_queue}")
        try:
            # Copy properties and ensure persistence
            props_dict = {
                attr: getattr(properties, attr, None)
                for attr in ['content_type', 'content_encoding', 'headers', 'priority',
                             'correlation_id', 'reply_to', 'expiration', 'message_id',
                             'timestamp', 'type', 'user_id', 'app_id', 'cluster_id']
            }
            # Filter out attributes that are not present (None) to avoid passing them explicitly
            props_dict = {k: v for k, v in props_dict.items() if v is not None}
            props_dict['delivery_mode'] = 2  # persistent
            persistent_properties = pika.BasicProperties(**props_dict)

            # With confirm_delivery enabled, basic_publish blocks until broker confirms
            # mandatory=True ensures the message is routed to a queue
            channel.basic_publish(
                exchange='',
                routing_key=target_queue,
                body=body,
                properties=persistent_properties,
                mandatory=True
            )
            # Only ack after successful publish
            channel.basic_ack(delivery_tag=method.delivery_tag)
            messages_moved += 1
            queue_stats[dlq_name]['moved'] += 1
        except pika.exceptions.UnroutableError as e:
            logger.error(f"Message unroutable to {target_queue}: {e}")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        except Exception as e:
            logger.error(f"Failed to move message: {e}")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    else:
        # Message within retry limit - requeue it
        logger.info(f"Message retry count ({death_count}) within limit, requeuing")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        messages_requeued += 1
        queue_stats[dlq_name]['requeued'] += 1


def process_dlq(channel, dlq_name, target_queue, max_retry_count):
    """Process all messages in a single DLQ."""
    ensure_target_queue_exists(channel, target_queue)

    try:
        queue_info = channel.queue_declare(queue=dlq_name, passive=True)
    except pika.exceptions.ChannelClosedByBroker:
        logger.error(f"DLQ '{dlq_name}' does not exist")
        raise

    message_count = queue_info.method.message_count
    logger.info(f"DLQ '{dlq_name}' has {message_count} messages")

    if message_count == 0:
        return

    # Process only the messages that were in the queue when we started.
    # This prevents an infinite loop when messages are requeued (nack with requeue=True)
    # back to the same DLQ - we won't process them again in this run.
    for _ in range(message_count):
        method_frame, properties, body = channel.basic_get(queue=dlq_name, auto_ack=False)
        if method_frame is None:
            break
        process_message(channel, method_frame, properties, body, target_queue, max_retry_count, dlq_name)


# =============================================================================
# Main
# =============================================================================

def run(config):
    """Main execution function. Returns True on success, False on error."""
    global messages_processed, messages_moved, messages_requeued, queue_stats
    messages_processed = 0
    messages_moved = 0
    messages_requeued = 0
    queue_stats = {}

    connection, channel = connect_to_rabbitmq(config)
    if not connection:
        return False

    try:
        queue_pairs = get_queue_pairs(config)

        if not queue_pairs:
            logger.warning("No queues to process")
            return True

        logger.info(f"Processing {len(queue_pairs)} queue pair(s)")

        for dlq_name, target_queue in queue_pairs:
            logger.info(f"Processing DLQ: '{dlq_name}' -> Target: '{target_queue}'")
            process_dlq(channel, dlq_name, target_queue, config['max_retry_count'])

        # Log per-queue statistics
        for q_name, stats in queue_stats.items():
            logger.info(f"  {q_name}: Processed: {stats['processed']}, Moved: {stats['moved']}, Requeued: {stats['requeued']}")
        logger.info(f"Total - Processed: {messages_processed}, Moved: {messages_moved}, Requeued: {messages_requeued}")
        return True
    except KeyboardInterrupt:
        logger.info("Interrupted")
        return False
    except Exception as e:
        logger.error(f"Error: {e}")
        return False
    finally:
        close_connection(connection)


def main():
    """Main entry point."""
    if len(sys.argv) != 2:
        print("Error: Expected exactly one argument (config file path)")
        print("Usage: python rmq_retry_checker.py <config.yaml>")
        sys.exit(1)

    config_file = sys.argv[1]
    logger.info(f"Loading configuration from {config_file}")
    config = load_config(config_file)

    # Acquire lock to prevent parallel execution
    if not acquire_lock():
        print("\nStatus: ERROR")
        print("Another instance is already running")
        sys.exit(1)

    # Register lock release on exit
    atexit.register(release_lock)

    logger.info("Starting RabbitMQ Retry Checker")
    success = run(config)

    # Print summary
    print(f"\nStatus: {'SUCCESS' if success else 'ERROR'}")
    print(f"Processed: {messages_processed}, Moved: {messages_moved}, Requeued: {messages_requeued}")

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
