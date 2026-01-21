#!/usr/bin/env python3
"""
RabbitMQ Retry Checker - Detect and handle infinite retry loops in DLQs.

Usage:
    python rmq_retry_checker.py config.yaml
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
from datetime import datetime

import pika
import yaml

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class Config:
    """Configuration for RabbitMQ connection and queue settings."""
    
    def __init__(self, config_file):
        # Default values
        self.RMQ_HOST = 'localhost'
        self.RMQ_PORT = 5672
        self.RMQ_MGMT_PORT = 15672
        self.RMQ_USERNAME = 'guest'
        self.RMQ_PASSWORD = 'guest'
        self.RMQ_VHOST = '/'
        self.RMQ_USE_SSL = False
        self.RMQ_SSL_VERIFY = True
        self.DLQ_NAME = 'my_dlq'
        self.TARGET_QUEUE = 'permanent_failure_queue'
        self.MAX_RETRY_COUNT = 3
        
        self._load_from_file(config_file)
    
    @staticmethod
    def has_wildcard(pattern):
        return '*' in pattern or '?' in pattern
    
    def _load_from_file(self, config_file):
        try:
            with open(config_file, 'r') as f:
                config_data = yaml.safe_load(f)
        except FileNotFoundError:
            logger.error(f"Config file not found: {config_file}")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Invalid YAML in config file: {e}")
            raise
        
        if config_data is None:
            return
        
        if 'rabbitmq' in config_data:
            rmq = config_data['rabbitmq']
            self.RMQ_HOST = rmq.get('host', self.RMQ_HOST)
            self.RMQ_PORT = int(rmq.get('port', self.RMQ_PORT))
            self.RMQ_MGMT_PORT = int(rmq.get('mgmt_port', self.RMQ_MGMT_PORT))
            self.RMQ_USERNAME = rmq.get('username', self.RMQ_USERNAME)
            self.RMQ_PASSWORD = rmq.get('password', self.RMQ_PASSWORD)
            self.RMQ_VHOST = rmq.get('vhost', self.RMQ_VHOST)
            self.RMQ_USE_SSL = rmq.get('use_ssl', self.RMQ_USE_SSL)
            if isinstance(self.RMQ_USE_SSL, str):
                self.RMQ_USE_SSL = self.RMQ_USE_SSL.lower() == 'true'
            self.RMQ_SSL_VERIFY = rmq.get('ssl_verify', self.RMQ_SSL_VERIFY)
            if isinstance(self.RMQ_SSL_VERIFY, str):
                self.RMQ_SSL_VERIFY = self.RMQ_SSL_VERIFY.lower() == 'true'
        
        if 'queues' in config_data:
            queues = config_data['queues']
            self.DLQ_NAME = queues.get('dlq_name', self.DLQ_NAME)
            self.TARGET_QUEUE = queues.get('target_queue', self.TARGET_QUEUE)
            self.MAX_RETRY_COUNT = int(queues.get('max_retry_count', self.MAX_RETRY_COUNT))


class RMQRetryChecker:
    """RabbitMQ Retry Checker - moves messages exceeding retry threshold to failure queue."""
    
    def __init__(self, config):
        self.config = config
        self.connection = None
        self.channel = None
        self.messages_processed = 0
        self.messages_moved = 0
        self.messages_requeued = 0
        self.start_time = None
        self.end_time = None
        self.success = False
        self.queue_stats = {}
    
    def list_queues_from_api(self):
        """List all queues from RabbitMQ Management API."""
        mgmt_port = self.config.RMQ_MGMT_PORT
        protocol = 'https' if self.config.RMQ_USE_SSL else 'http'
        host = self.config.RMQ_HOST
        
        vhost_encoded = urllib.parse.quote(self.config.RMQ_VHOST, safe='')
        url = f"{protocol}://{host}:{mgmt_port}/api/queues/{vhost_encoded}"
        
        auth_string = f"{self.config.RMQ_USERNAME}:{self.config.RMQ_PASSWORD}"
        auth_b64 = base64.b64encode(auth_string.encode('ascii')).decode('ascii')
        
        request = urllib.request.Request(url)
        request.add_header('Authorization', f'Basic {auth_b64}')
        
        context = None
        if self.config.RMQ_USE_SSL:
            context = ssl.create_default_context()
            if not self.config.RMQ_SSL_VERIFY:
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE
        
        try:
            with urllib.request.urlopen(request, timeout=10, context=context) as response:
                data = json.loads(response.read().decode('utf-8'))
        except Exception as e:
            logger.error(f"Failed to list queues from Management API ({host}:{mgmt_port}): {e}")
            raise
        
        return [q['name'] for q in data if 'name' in q]
    
    def get_matching_queue_pairs(self):
        """Get list of (dlq_name, target_queue) pairs, supporting wildcard patterns."""
        dlq_pattern = self.config.DLQ_NAME
        target_pattern = self.config.TARGET_QUEUE
        
        dlq_has_wildcard = Config.has_wildcard(dlq_pattern)
        target_has_wildcard = Config.has_wildcard(target_pattern)
        
        if not dlq_has_wildcard and not target_has_wildcard:
            return [(dlq_pattern, target_pattern)]
        
        logger.info("Wildcard pattern detected, querying RabbitMQ Management API")
        all_queues = self.list_queues_from_api()
        matching_dlqs = [q for q in all_queues if fnmatch.fnmatch(q, dlq_pattern)]
        
        if not matching_dlqs:
            logger.warning(f"No queues found matching DLQ pattern: {dlq_pattern}")
            return []
        
        logger.info(f"Found {len(matching_dlqs)} DLQ(s) matching '{dlq_pattern}'")
        
        queue_pairs = []
        for dlq_name in matching_dlqs:
            if target_has_wildcard:
                target_queue = self._derive_target_queue(dlq_name, dlq_pattern, target_pattern)
            else:
                target_queue = target_pattern
            queue_pairs.append((dlq_name, target_queue))
        
        return queue_pairs
    
    def _derive_target_queue(self, dlq_name, dlq_pattern, target_pattern):
        """Derive target queue name from DLQ name using wildcard patterns."""
        dlq_regex = re.escape(dlq_pattern)
        dlq_regex = dlq_regex.replace(r'\*', '(.*?)').replace(r'\?', '(.)')
        dlq_regex = f'^{dlq_regex}$'
        
        match = re.match(dlq_regex, dlq_name)
        if not match:
            raise ValueError(f"Unable to derive target queue for '{dlq_name}'")
        
        captured_parts = list(match.groups())
        parts_iter = iter(captured_parts)
        
        def replace_wildcard(match_obj):
            return next(parts_iter)
        
        return re.sub(r'[*?]', replace_wildcard, target_pattern)
        
    def connect(self):
        """Establish connection to RabbitMQ."""
        try:
            credentials = pika.PlainCredentials(self.config.RMQ_USERNAME, self.config.RMQ_PASSWORD)
            parameters = pika.ConnectionParameters(
                host=self.config.RMQ_HOST,
                port=self.config.RMQ_PORT,
                virtual_host=self.config.RMQ_VHOST,
                credentials=credentials,
                heartbeat=600,
                blocked_connection_timeout=300
            )
            
            if self.config.RMQ_USE_SSL:
                ssl_context = ssl.create_default_context()
                if not self.config.RMQ_SSL_VERIFY:
                    ssl_context.check_hostname = False
                    ssl_context.verify_mode = ssl.CERT_NONE
                parameters.ssl_options = pika.SSLOptions(context=ssl_context)
            
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            self.channel.confirm_delivery()
            logger.info("Connected to RabbitMQ with publisher confirms enabled")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            return False
    
    def ensure_target_queue_exists(self, target_queue):
        """Ensure the target queue exists."""
        try:
            self.channel.queue_declare(queue=target_queue, durable=True, arguments={'x-queue-type': 'quorum'})
            logger.info(f"Target queue '{target_queue}' is ready (quorum)")
        except Exception:
            self.channel.queue_declare(queue=target_queue, durable=True)
            logger.info(f"Target queue '{target_queue}' is ready (classic)")
    
    def get_death_count(self, headers):
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
    
    def process_message(self, ch, method, properties, body, target_queue, dlq_name):
        """Process a single message from the DLQ."""
        self.messages_processed += 1
        
        if dlq_name not in self.queue_stats:
            self.queue_stats[dlq_name] = {'processed': 0, 'moved': 0, 'requeued': 0}
        self.queue_stats[dlq_name]['processed'] += 1
        
        death_count = self.get_death_count(properties.headers or {})
        logger.info(f"Message {self.messages_processed}: x-death count={death_count}")
        
        if death_count > self.config.MAX_RETRY_COUNT:
            logger.warning(f"Message exceeded retry limit ({death_count} > {self.config.MAX_RETRY_COUNT}), moving to {target_queue}")
            try:
                # Copy properties and ensure persistence with delivery_mode=2
                props_dict = {
                    attr: getattr(properties, attr, None)
                    for attr in ['content_type', 'content_encoding', 'headers', 'priority',
                                 'correlation_id', 'reply_to', 'expiration', 'message_id',
                                 'timestamp', 'type', 'user_id', 'app_id', 'cluster_id']
                }
                props_dict['delivery_mode'] = 2  # persistent
                persistent_properties = pika.BasicProperties(**props_dict)
                # With confirm_delivery enabled, basic_publish blocks until broker confirms
                # mandatory=True ensures the message is routed to a queue
                self.channel.basic_publish(
                    exchange='',
                    routing_key=target_queue,
                    body=body,
                    properties=persistent_properties,
                    mandatory=True
                )
                ch.basic_ack(delivery_tag=method.delivery_tag)
                self.messages_moved += 1
                self.queue_stats[dlq_name]['moved'] += 1
            except pika.exceptions.UnroutableError as e:
                logger.error(f"Message unroutable to {target_queue}: {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            except Exception as e:
                logger.error(f"Failed to move message: {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        else:
            logger.info(f"Message retry count ({death_count}) within limit, requeuing")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            self.messages_requeued += 1
            self.queue_stats[dlq_name]['requeued'] += 1
    
    def check_dlq(self):
        """Check all messages in the DLQ(s) and process them."""
        queue_pairs = self.get_matching_queue_pairs()
        
        if not queue_pairs:
            logger.warning("No queues to process")
            return
        
        logger.info(f"Processing {len(queue_pairs)} queue pair(s)")
        
        for dlq_name, target_queue in queue_pairs:
            logger.info(f"Processing DLQ: '{dlq_name}' -> Target: '{target_queue}'")
            self._process_single_dlq(dlq_name, target_queue)
        
        logger.info(f"Total - Processed: {self.messages_processed}, Moved: {self.messages_moved}, Requeued: {self.messages_requeued}")
    
    def _process_single_dlq(self, dlq_name, target_queue):
        """Process a single DLQ."""
        self.ensure_target_queue_exists(target_queue)
        
        try:
            queue_info = self.channel.queue_declare(queue=dlq_name, passive=True)
        except pika.exceptions.ChannelClosedByBroker:
            logger.error(f"DLQ '{dlq_name}' does not exist")
            raise
        
        message_count = queue_info.method.message_count
        
        logger.info(f"DLQ '{dlq_name}' has {message_count} messages")
        
        if message_count == 0:
            return
        
        while True:
            method_frame, properties, body = self.channel.basic_get(queue=dlq_name, auto_ack=False)
            if method_frame is None:
                break
            self.process_message(self.channel, method_frame, properties, body, target_queue, dlq_name)
    
    def close(self):
        """Close the RabbitMQ connection."""
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            logger.info("Connection closed")
    
    def output_results(self):
        """Output results."""
        print(f"\nStatus: {'SUCCESS' if self.success else 'ERROR'}")
        print(f"Processed: {self.messages_processed}, Moved: {self.messages_moved}, Requeued: {self.messages_requeued}")
    
    def run(self):
        """Main execution method."""
        self.start_time = datetime.now()
        
        try:
            if not self.connect():
                self.success = False
                return False
            
            self.check_dlq()
            self.success = True
            return True
        except KeyboardInterrupt:
            logger.info("Interrupted")
            self.success = False
            return False
        except Exception as e:
            logger.error(f"Error: {e}")
            self.success = False
            return False
        finally:
            self.end_time = datetime.now()
            self.close()
            self.output_results()


def main():
    """Main entry point."""
    if len(sys.argv) != 2:
        print("Error: Expected exactly one argument (config file path)")
        print("Usage: python rmq_retry_checker.py <config.yaml>")
        sys.exit(1)
    
    config_file = sys.argv[1]
    logger.info(f"Loading configuration from {config_file}")
    config = Config(config_file)
    
    logger.info("Starting RabbitMQ Retry Checker")
    
    checker = RMQRetryChecker(config)
    success = checker.run()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
