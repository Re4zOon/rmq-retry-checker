#!/usr/bin/env python3
"""
RabbitMQ Retry Checker - Detect and handle infinite retry loops in DLQs.

Usage:
    python rmq_retry_checker.py config.yaml
"""
import sys
import os
import logging
import json
import fnmatch
import re
import urllib.request
import urllib.parse
import base64
import ssl
import hashlib
from datetime import datetime, timedelta

import pika
import yaml

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Global state
config = {}
connection = None
channel = None
processed_ids = set()
stats = {'processed': 0, 'moved': 0, 'requeued': 0, 'skipped': 0}
queue_stats = {}


def reset_state():
    """Reset global mutable state (primarily intended for testing)."""
    global config, connection, channel, processed_ids, stats, queue_stats
    config = {}
    connection = None
    channel = None
    processed_ids = set()
    stats = {'processed': 0, 'moved': 0, 'requeued': 0, 'skipped': 0}
    queue_stats = {}


def has_wildcard(pattern):
    """Check if a pattern contains wildcard characters."""
    return '*' in pattern or '?' in pattern


def load_config(config_file):
    """Load configuration from YAML file."""
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f) or {}
    except FileNotFoundError:
        logger.error("Configuration file '%s' not found.", config_file)
        sys.exit(1)
    except PermissionError:
        logger.error("Permission denied when reading configuration file '%s'.", config_file)
        sys.exit(1)
    except yaml.YAMLError as e:
        logger.error("Invalid YAML syntax in configuration file '%s': %s", config_file, e)
        sys.exit(1)
    
    rmq = data.get('rabbitmq', {})
    queues = data.get('queues', {})
    
    cfg = {
        'host': rmq.get('host', 'localhost'),
        'port': int(rmq.get('port', 5672)),
        'mgmt_port': int(rmq.get('mgmt_port', 15672)),
        'username': rmq.get('username', 'guest'),
        'password': rmq.get('password', 'guest'),
        'vhost': rmq.get('vhost', '/'),
        'use_ssl': rmq.get('use_ssl', False),
        'ssl_verify': rmq.get('ssl_verify', True),
        'dlq_name': queues.get('dlq_name', 'my_dlq'),
        'target_queue': queues.get('target_queue', 'permanent_failure_queue'),
        'max_retry_count': int(queues.get('max_retry_count', 3)),
        'dedup_file': queues.get('dedup_file', '.rmq_processed_ids'),
        'dedup_max_age_hours': int(queues.get('dedup_max_age_hours', 168)),
    }
    
    # Handle string booleans
    if isinstance(cfg['use_ssl'], str):
        cfg['use_ssl'] = cfg['use_ssl'].lower() == 'true'
    if isinstance(cfg['ssl_verify'], str):
        cfg['ssl_verify'] = cfg['ssl_verify'].lower() == 'true'

    # Validate that critical configuration values are explicitly provided.
    missing_critical = [key for key in ('host', 'username', 'password') if key not in rmq]
    if missing_critical:
        logger.warning(
            "Missing critical RabbitMQ configuration keys in YAML: %s. "
            "Using default values; consider specifying them explicitly.",
            ", ".join(missing_critical),
        )
    
    return cfg


def load_processed_ids():
    """Load previously processed message IDs from dedup file."""
    dedup_file = config.get('dedup_file')
    if not dedup_file or not os.path.exists(dedup_file):
        return
    
    try:
        cutoff = datetime.now() - timedelta(hours=config['dedup_max_age_hours'])
        valid = []
        
        with open(dedup_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                if ':' in line and line.count(':') >= 2:
                    parts = line.rsplit(':', 1)
                    fingerprint = parts[0]
                    try:
                        ts = datetime.fromisoformat(parts[1])
                        if ts >= cutoff:
                            valid.append((fingerprint, ts))
                            processed_ids.add(fingerprint)
                    except ValueError:
                        processed_ids.add(line)
                        valid.append((line, datetime.now()))
                else:
                    processed_ids.add(line)
                    valid.append((line, datetime.now()))
        
        with open(dedup_file, 'w', encoding='utf-8') as f:
            for fp, ts in valid:
                f.write(f"{fp}:{ts.isoformat()}\n")
        
        logger.info(f"Loaded {len(processed_ids)} processed IDs from {dedup_file}")
    except Exception as e:
        logger.warning(f"Failed to load dedup file: {e}")


def save_processed_id(fingerprint):
    """Save processed message ID to dedup file."""
    dedup_file = config.get('dedup_file')
    if dedup_file:
        try:
            with open(dedup_file, 'a', encoding='utf-8') as f:
                f.write(f"{fingerprint}:{datetime.now().isoformat()}\n")
                f.flush()
            processed_ids.add(fingerprint)
        except Exception as e:
            logger.warning(f"Failed to save dedup: {e}")


def get_fingerprint(properties, body):
    """Generate fingerprint for message deduplication."""
    if properties and getattr(properties, 'message_id', None):
        return f"id:{properties.message_id}"
    return f"hash:{hashlib.sha256(body).hexdigest()}"


def connect():
    """Connect to RabbitMQ."""
    global connection, channel
    
    try:
        creds = pika.PlainCredentials(config['username'], config['password'])
        params = pika.ConnectionParameters(
            host=config['host'],
            port=config['port'],
            virtual_host=config['vhost'],
            credentials=creds,
            heartbeat=600,
            blocked_connection_timeout=300
        )
        
        if config['use_ssl']:
            ctx = ssl.create_default_context()
            if not config['ssl_verify']:
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
            params.ssl_options = pika.SSLOptions(context=ctx)
        
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.confirm_delivery()
        logger.info("Connected to RabbitMQ")
        return True
    except Exception as e:
        logger.error(f"Connection failed: {e}")
        return False


def close():
    """Close RabbitMQ connection."""
    global connection
    if connection and not connection.is_closed:
        connection.close()
        logger.info("Connection closed")


def list_queues():
    """List queues from Management API."""
    protocol = 'https' if config['use_ssl'] else 'http'
    vhost = urllib.parse.quote(config['vhost'], safe='')
    url = f"{protocol}://{config['host']}:{config['mgmt_port']}/api/queues/{vhost}"
    
    auth = base64.b64encode(f"{config['username']}:{config['password']}".encode()).decode()
    req = urllib.request.Request(url)
    req.add_header('Authorization', f'Basic {auth}')
    
    ctx = None
    if config['use_ssl']:
        ctx = ssl.create_default_context()
        if not config['ssl_verify']:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
    
    try:
        with urllib.request.urlopen(req, timeout=10, context=ctx) as resp:
            return [q['name'] for q in json.loads(resp.read()) if 'name' in q]
    except Exception as e:
        logger.error(f"Failed to list queues: {e}")
        raise


def get_queue_pairs():
    """Get DLQ/target queue pairs, supporting wildcards."""
    dlq = config['dlq_name']
    target = config['target_queue']
    
    if not has_wildcard(dlq) and not has_wildcard(target):
        return [(dlq, target)]
    
    logger.info("Wildcard detected, querying Management API")
    all_queues = list_queues()
    matches = [q for q in all_queues if fnmatch.fnmatch(q, dlq)]
    
    if not matches:
        logger.warning(f"No queues match: {dlq}")
        return []
    
    logger.info(f"Found {len(matches)} DLQ(s)")
    
    pairs = []
    # Pre-calculate wildcard count for target pattern
    target_wildcard_count = target.count('*') + target.count('?')
    
    for dlq_name in matches:
        if has_wildcard(target):
            # Derive target from DLQ name
            regex = re.escape(dlq).replace(r'\*', '(.*?)').replace(r'\?', '(.)')
            m = re.match(f'^{regex}$', dlq_name)
            if m:
                groups = m.groups()
                if target_wildcard_count != len(groups):
                    logger.error(
                        "Mismatched wildcard/group count when deriving target queue "
                        "from DLQ name. dlq_pattern=%r target_pattern=%r dlq_name=%r "
                        "wildcards=%d groups=%d",
                        dlq, target, dlq_name, target_wildcard_count, len(groups),
                    )
                    # Skip this DLQ to avoid creating invalid queue names
                    continue
                else:
                    parts = iter(groups)
                    tgt = re.sub(r'[*?]', lambda _: next(parts), target)
            else:
                logger.error(
                    "Failed to resolve wildcard target pattern '%s' for DLQ '%s' "
                    "using regex '%s'.",
                    target,
                    dlq_name,
                    regex,
                )
                raise ValueError(
                    f"Cannot resolve wildcard target pattern '{target}' "
                    f"from DLQ '{dlq_name}' with pattern '{dlq}'."
                )
        else:
            tgt = target
        pairs.append((dlq_name, tgt))
    
    return pairs


def get_death_count(headers):
    """Get x-death count from headers."""
    if not headers:
        return 0
    x_death = headers.get('x-death')
    if x_death and isinstance(x_death, list) and x_death:
        if isinstance(x_death[0], dict):
            return int(x_death[0].get('count', 0) or 0)
    return 0


def ensure_queue(queue_name):
    """Ensure target queue exists."""
    try:
        channel.queue_declare(queue=queue_name, durable=True, arguments={'x-queue-type': 'quorum'})
        logger.info(f"Queue '{queue_name}' ready (quorum)")
    except Exception:
        channel.queue_declare(queue=queue_name, durable=True)
        logger.info(f"Queue '{queue_name}' ready (classic)")


def process_message(method, properties, body, target_queue, dlq_name):
    """Process a single DLQ message."""
    stats['processed'] += 1
    if dlq_name not in queue_stats:
        queue_stats[dlq_name] = {'processed': 0, 'moved': 0, 'requeued': 0, 'skipped': 0}
    queue_stats[dlq_name]['processed'] += 1
    
    death_count = get_death_count(properties.headers or {})
    logger.info(f"Message {stats['processed']}: x-death={death_count}")
    
    if death_count > config['max_retry_count']:
        fp = get_fingerprint(properties, body)
        if fp in processed_ids:
            logger.info(f"Duplicate, skipping: {fp}")
            channel.basic_ack(delivery_tag=method.delivery_tag)
            stats['skipped'] += 1
            queue_stats[dlq_name]['skipped'] += 1
            return
        
        logger.warning(f"Exceeded limit ({death_count}>{config['max_retry_count']}), moving to {target_queue}")
        try:
            props = pika.BasicProperties(
                content_type=getattr(properties, 'content_type', None),
                content_encoding=getattr(properties, 'content_encoding', None),
                headers=getattr(properties, 'headers', None),
                priority=getattr(properties, 'priority', None),
                correlation_id=getattr(properties, 'correlation_id', None),
                reply_to=getattr(properties, 'reply_to', None),
                expiration=getattr(properties, 'expiration', None),
                message_id=getattr(properties, 'message_id', None),
                timestamp=getattr(properties, 'timestamp', None),
                type=getattr(properties, 'type', None),
                user_id=getattr(properties, 'user_id', None),
                app_id=getattr(properties, 'app_id', None),
                cluster_id=getattr(properties, 'cluster_id', None),
                delivery_mode=2
            )
            channel.basic_publish(exchange='', routing_key=target_queue, body=body, properties=props, mandatory=True)
            save_processed_id(fp)
            channel.basic_ack(delivery_tag=method.delivery_tag)
            stats['moved'] += 1
            queue_stats[dlq_name]['moved'] += 1
        except pika.exceptions.UnroutableError as e:
            logger.error(f"Unroutable: {e}")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        except Exception as e:
            logger.error(f"Move failed: {e}")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    else:
        logger.info(f"Within limit ({death_count}), requeuing")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        stats['requeued'] += 1
        queue_stats[dlq_name]['requeued'] += 1


def process_dlq(dlq_name, target_queue):
    """Process all messages in a DLQ."""
    ensure_queue(target_queue)
    
    try:
        info = channel.queue_declare(queue=dlq_name, passive=True)
    except pika.exceptions.ChannelClosedByBroker:
        logger.error(f"DLQ '{dlq_name}' does not exist")
        raise
    
    count = info.method.message_count
    logger.info(f"DLQ '{dlq_name}' has {count} messages")
    
    while True:
        method, properties, body = channel.basic_get(queue=dlq_name, auto_ack=False)
        if method is None:
            break
        process_message(method, properties, body, target_queue, dlq_name)


def run():
    """Main execution."""
    load_processed_ids()
    
    if not connect():
        return False
    
    try:
        pairs = get_queue_pairs()
        if not pairs:
            logger.warning("No queues to process")
            return True
        
        logger.info(f"Processing {len(pairs)} queue pair(s)")
        for dlq, target in pairs:
            logger.info(f"Processing: '{dlq}' -> '{target}'")
            process_dlq(dlq, target)
        
        logger.info(f"Total - Processed: {stats['processed']}, Moved: {stats['moved']}, Requeued: {stats['requeued']}, Skipped: {stats['skipped']}")
        return True
    except KeyboardInterrupt:
        logger.info("Interrupted")
        return False
    except Exception as e:
        logger.error(f"Error: {e}")
        return False
    finally:
        close()


def main():
    """Entry point."""
    global config
    
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} config.yaml", file=sys.stderr)
        sys.exit(1)
    
    config_file = sys.argv[1]
    
    logger.info(f"Loading config from {config_file}")
    config = load_config(config_file)
    
    logger.info("Starting RabbitMQ Retry Checker")
    success = run()
    
    print(f"\nStatus: {'SUCCESS' if success else 'ERROR'}")
    print(f"Processed: {stats['processed']}, Moved: {stats['moved']}, Requeued: {stats['requeued']}, Skipped: {stats['skipped']}")
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
