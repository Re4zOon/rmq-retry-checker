#!/usr/bin/env python3
"""
RabbitMQ Retry Checker

This script connects to RabbitMQ and checks messages in a Dead Letter Queue (DLQ)
for infinite retry loops. If a message's x-death count exceeds a threshold,
it is moved to a permanent failure queue.

Usage:
    python rmq_retry_checker.py --dlq my_dlq --target-queue permanent_failure_queue --max-retries 3
    python rmq_retry_checker.py --config /path/to/config.yaml
    python rmq_retry_checker.py --output-format json
"""
import sys
import os
import logging
import argparse
import json
import fnmatch
import re
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime

try:
    import pika
except ImportError:
    print("ERROR: pika library not found. Install with: pip install pika")
    sys.exit(1)

try:
    import yaml
except ImportError:
    print("ERROR: PyYAML library not found. Install with: pip install pyyaml")
    sys.exit(1)

try:
    from dotenv import load_dotenv
except ImportError:
    # dotenv is optional
    load_dotenv = None

try:
    import urllib.request
    import urllib.parse
    import base64
except ImportError:
    # These should be available in standard library
    pass


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class Config:
    """Configuration class for RabbitMQ connection and queue settings"""
    
    def __init__(self):
        """Initialize configuration with defaults"""
        # Load .env file if dotenv is available
        if load_dotenv:
            load_dotenv()
        
        # RabbitMQ Connection Settings
        self.RMQ_HOST = os.getenv('RMQ_HOST', 'localhost')
        self.RMQ_PORT = int(os.getenv('RMQ_PORT', 5672))
        self.RMQ_MGMT_PORT = int(os.getenv('RMQ_MGMT_PORT', 15672))
        self.RMQ_USERNAME = os.getenv('RMQ_USERNAME', 'guest')
        self.RMQ_PASSWORD = os.getenv('RMQ_PASSWORD', 'guest')
        self.RMQ_VHOST = os.getenv('RMQ_VHOST', '/')
        self.RMQ_USE_SSL = os.getenv('RMQ_USE_SSL', 'false').lower() == 'true'
        
        # Queue Settings
        self.DLQ_NAME = os.getenv('DLQ_NAME', 'my_dlq')
        self.TARGET_QUEUE = os.getenv('TARGET_QUEUE', 'permanent_failure_queue')
        max_retry_raw = os.getenv('MAX_RETRY_COUNT', '3')
        try:
            self.MAX_RETRY_COUNT = int(max_retry_raw)
        except ValueError as e:
            raise ValueError(f"Invalid MAX_RETRY_COUNT value: {max_retry_raw!r}. Must be an integer.") from e
    
    @staticmethod
    def has_wildcard(pattern: str) -> bool:
        """
        Check if a string contains wildcard characters
        
        Args:
            pattern: String to check for wildcards
            
        Returns:
            bool: True if pattern contains * or ?, False otherwise
        """
        return '*' in pattern or '?' in pattern
    
    def validate(self):
        """
        Validate configuration values
        
        Raises:
            ValueError: If configuration is invalid
        """
        if not self.DLQ_NAME or not self.DLQ_NAME.strip():
            raise ValueError("DLQ_NAME must be a non-empty string")
        if not self.TARGET_QUEUE or not self.TARGET_QUEUE.strip():
            raise ValueError("TARGET_QUEUE must be a non-empty string")
    
    def load_from_file(self, config_file: str):
        """
        Load configuration from YAML file
        
        Args:
            config_file: Path to YAML configuration file
        
        Raises:
            FileNotFoundError: If config file doesn't exist
            yaml.YAMLError: If YAML is invalid
        """
        try:
            with open(config_file, 'r') as f:
                config_data = yaml.safe_load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found: {config_file}")
        except yaml.YAMLError as e:
            logger.error("Invalid YAML in configuration file '%s'", config_file, exc_info=True)
            raise yaml.YAMLError(f"Invalid YAML in configuration file: {config_file}") from e
        
        # Empty YAML files return None - use defaults in this case
        if config_data is None:
            return
        
        # Load RabbitMQ settings
        if 'rabbitmq' in config_data:
            rmq = config_data['rabbitmq']
            self.RMQ_HOST = rmq.get('host', self.RMQ_HOST)
            self.RMQ_PORT = int(rmq.get('port', self.RMQ_PORT))
            self.RMQ_MGMT_PORT = int(rmq.get('mgmt_port', self.RMQ_MGMT_PORT))
            self.RMQ_USERNAME = rmq.get('username', self.RMQ_USERNAME)
            self.RMQ_PASSWORD = rmq.get('password', self.RMQ_PASSWORD)
            self.RMQ_VHOST = rmq.get('vhost', self.RMQ_VHOST)
            self.RMQ_USE_SSL = self._parse_bool(rmq.get('use_ssl', self.RMQ_USE_SSL))
        
        # Load queue settings
        if 'queues' in config_data:
            queues = config_data['queues']
            self.DLQ_NAME = queues.get('dlq_name', self.DLQ_NAME)
            self.TARGET_QUEUE = queues.get('target_queue', self.TARGET_QUEUE)
            self.MAX_RETRY_COUNT = int(queues.get('max_retry_count', self.MAX_RETRY_COUNT))
    
    @staticmethod
    def _parse_bool(value) -> bool:
        """
        Parse a value as a boolean
        
        Args:
            value: Value to parse (bool, str, or other)
        
        Returns:
            Boolean value
        """
        if isinstance(value, bool):
            return value
        return str(value).lower() == 'true'
    
    def update_from_args(self, args: argparse.Namespace):
        """
        Update configuration from command-line arguments
        
        Args:
            args: Parsed command-line arguments
        """
        if args.host:
            self.RMQ_HOST = args.host
        if args.port:
            self.RMQ_PORT = args.port
        if args.mgmt_port:
            self.RMQ_MGMT_PORT = args.mgmt_port
        if args.username:
            self.RMQ_USERNAME = args.username
        if args.password:
            self.RMQ_PASSWORD = args.password
        if args.vhost:
            self.RMQ_VHOST = args.vhost
        if args.ssl:
            self.RMQ_USE_SSL = True
        if args.dlq:
            self.DLQ_NAME = args.dlq
        if args.target_queue:
            self.TARGET_QUEUE = args.target_queue
        if args.max_retries is not None:
            self.MAX_RETRY_COUNT = args.max_retries


class RMQRetryChecker:
    """
    RabbitMQ Retry Checker class
    
    Connects to RabbitMQ, inspects DLQ messages for infinite retry loops,
    and moves messages exceeding the retry threshold to a permanent failure queue.
    """
    
    def __init__(self, config: Config, output_format: str = 'text'):
        """
        Initialize the RMQ Retry Checker
        
        Args:
            config: Configuration object with RabbitMQ connection and queue settings
            output_format: Output format ('text' or 'json')
        """
        self.config = config
        self.output_format = output_format
        self.connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[pika.channel.Channel] = None
        self.messages_processed = 0
        self.messages_moved = 0
        self.messages_requeued = 0
        self.start_time = None
        self.end_time = None
        self.success = False  # Track overall success status
        self.queue_stats: Dict[str, Dict[str, int]] = {}  # Per-queue statistics
    
    def list_queues_from_api(self) -> List[str]:
        """
        List all queues from RabbitMQ Management API
        
        Returns:
            List of queue names
            
        Raises:
            Exception: If unable to connect to Management API
        """
        # RabbitMQ Management API port from configuration
        mgmt_port = self.config.RMQ_MGMT_PORT
        protocol = 'https' if self.config.RMQ_USE_SSL else 'http'
        
        # URL encode the vhost (/ becomes %2F)
        vhost_encoded = urllib.parse.quote(self.config.RMQ_VHOST, safe='')
        url = f"{protocol}://{self.config.RMQ_HOST}:{mgmt_port}/api/queues/{vhost_encoded}"
        
        try:
            # Create request with basic auth
            auth_string = f"{self.config.RMQ_USERNAME}:{self.config.RMQ_PASSWORD}"
            auth_bytes = auth_string.encode('ascii')
            auth_b64 = base64.b64encode(auth_bytes).decode('ascii')
            
            request = urllib.request.Request(url)
            request.add_header('Authorization', f'Basic {auth_b64}')
            
            # Make request
            with urllib.request.urlopen(request, timeout=10) as response:
                data = json.loads(response.read().decode('utf-8'))
                
            # Extract queue names
            queue_names = [q['name'] for q in data if 'name' in q]
            logger.debug(f"Found {len(queue_names)} queues via Management API")
            return queue_names
            
        except Exception as e:
            logger.error(f"Failed to list queues from Management API: {e}")
            raise Exception(
                f"Unable to list queues from Management API. "
                f"Ensure RabbitMQ Management plugin is enabled and accessible at {url}. "
                f"Error: {e}"
            ) from e
    
    def get_matching_queue_pairs(self) -> List[Tuple[str, str]]:
        """
        Get list of (dlq_name, target_queue) pairs based on configuration.
        Supports wildcard patterns in queue names.
        
        Returns:
            List of tuples (dlq_name, target_queue_name)
        """
        dlq_pattern = self.config.DLQ_NAME
        target_pattern = self.config.TARGET_QUEUE
        
        # Check if patterns contain wildcards
        dlq_has_wildcard = Config.has_wildcard(dlq_pattern)
        target_has_wildcard = Config.has_wildcard(target_pattern)
        
        if not dlq_has_wildcard and not target_has_wildcard:
            # No wildcards, return single pair
            return [(dlq_pattern, target_pattern)]
        
        # Wildcards present, need to list queues and match
        logger.info("Wildcard pattern detected, querying RabbitMQ Management API for queue list")
        all_queues = self.list_queues_from_api()
        
        # Match DLQ pattern
        matching_dlqs = [q for q in all_queues if fnmatch.fnmatch(q, dlq_pattern)]
        
        if not matching_dlqs:
            logger.warning(f"No queues found matching DLQ pattern: {dlq_pattern}")
            return []
        
        logger.info(f"Found {len(matching_dlqs)} DLQ(s) matching pattern '{dlq_pattern}': {matching_dlqs}")
        
        # Build queue pairs
        queue_pairs = []
        
        for dlq_name in matching_dlqs:
            if target_has_wildcard:
                # Derive target queue name from DLQ name
                # Strategy: replace the wildcard portion in the target pattern
                # with the corresponding portion from the DLQ name
                target_queue = self._derive_target_queue(dlq_name, dlq_pattern, target_pattern)
            else:
                # Target is fixed, same for all DLQs
                target_queue = target_pattern
            
            queue_pairs.append((dlq_name, target_queue))
            logger.debug(f"Queue pair: {dlq_name} -> {target_queue}")
        
        return queue_pairs
    
    def _derive_target_queue(self, dlq_name: str, dlq_pattern: str, target_pattern: str) -> str:
        """
        Derive target queue name from DLQ name using patterns.
        
        For example:
        - dlq_name: "service1_dlq"
        - dlq_pattern: "*_dlq"
        - target_pattern: "*_failed"
        - Result: "service1_failed"
        
        Args:
            dlq_name: Actual DLQ name
            dlq_pattern: Pattern with wildcards that matched the DLQ
            target_pattern: Target pattern with wildcards
            
        Returns:
            Derived target queue name
        """
        # Convert wildcard pattern to regex to extract the matched parts
        # We need to handle * and ? separately
        # * matches any sequence of characters
        # ? matches exactly one character
        
        # Build a regex from the dlq_pattern
        # Escape special regex characters except * and ?
        dlq_regex = re.escape(dlq_pattern)
        # Replace escaped wildcards with appropriate regex groups
        # Use non-greedy matching for * to handle multiple wildcards correctly
        dlq_regex = dlq_regex.replace(r'\*', '(.*?)')
        dlq_regex = dlq_regex.replace(r'\?', '(.)')
        # Ensure we match the entire string
        dlq_regex = f'^{dlq_regex}$'
        
        match = re.match(dlq_regex, dlq_name)
        if not match:
            # Shouldn't happen as dlq_name was matched by fnmatch, but fallback
            logger.warning(f"Unable to derive target queue for {dlq_name}, using pattern as-is")
            return target_pattern
        
        # Get captured groups (the parts that matched wildcards)
        captured_parts = match.groups()
        
        # Now replace wildcards in target_pattern with captured parts in order
        result = target_pattern
        part_index = 0
        
        # Process each character in the target pattern
        output = []
        i = 0
        while i < len(result):
            if result[i] == '*':
                # Replace with next captured part
                if part_index < len(captured_parts):
                    output.append(captured_parts[part_index])
                    part_index += 1
                i += 1
            elif result[i] == '?':
                # Replace with next captured part
                if part_index < len(captured_parts):
                    output.append(captured_parts[part_index])
                    part_index += 1
                i += 1
            else:
                output.append(result[i])
                i += 1
        
        return ''.join(output)
        
    def connect(self) -> bool:
        """
        Establish connection to RabbitMQ
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            credentials = pika.PlainCredentials(
                self.config.RMQ_USERNAME,
                self.config.RMQ_PASSWORD
            )
            
            parameters = pika.ConnectionParameters(
                host=self.config.RMQ_HOST,
                port=self.config.RMQ_PORT,
                virtual_host=self.config.RMQ_VHOST,
                credentials=credentials,
                heartbeat=600,
                blocked_connection_timeout=300
            )
            
            if self.config.RMQ_USE_SSL:
                # WARNING: Using context=None disables certificate verification
                # This is insecure and susceptible to man-in-the-middle attacks
                # For production use, configure proper SSL context with certificate verification
                ssl_options = pika.SSLOptions(context=None)
                parameters.ssl_options = ssl_options
            
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            
            logger.info("Connected to RabbitMQ")
            logger.debug(f"RabbitMQ connection details: host={self.config.RMQ_HOST}, port={self.config.RMQ_PORT}, vhost={self.config.RMQ_VHOST}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            return False
    
    def ensure_target_queue_exists(self, target_queue: str):
        """
        Ensure the target queue (permanent failure queue) exists
        
        Args:
            target_queue: Name of the target queue to ensure exists
        """
        try:
            # Try to declare as quorum queue first (RabbitMQ 3.8+)
            self.channel.queue_declare(
                queue=target_queue,
                durable=True,
                arguments={'x-queue-type': 'quorum'}
            )
            logger.info(f"Target queue '{target_queue}' is ready (quorum queue)")
        except Exception as e:
            # Fall back to classic durable queue if quorum queues not supported
            logger.debug(f"Quorum queue not supported, trying classic queue: {e}")
            logger.warning("Quorum queues not supported, using classic durable queue instead")
            try:
                self.channel.queue_declare(
                    queue=target_queue,
                    durable=True
                )
                logger.info(f"Target queue '{target_queue}' is ready (classic queue)")
            except Exception as e2:
                logger.error(f"Failed to declare target queue: {e2}")
                raise
    
    def get_death_count(self, headers: Dict[str, Any]) -> int:
        """
        Extract the death count from message headers
        
        Args:
            headers: Message headers dictionary
            
        Returns:
            int: The x-death count, or 0 if not found
        """
        if not headers:
            return 0
            
        x_death = headers.get('x-death')
        if not x_death or not isinstance(x_death, list) or len(x_death) == 0:
            return 0
        
        # x-death is a list of death records, we check the count in the first one
        first_death = x_death[0]
        if isinstance(first_death, dict):
            count = first_death.get('count', 0)
            return int(count) if count else 0
        
        return 0
    
    def process_message(self, ch, method, properties, body, target_queue: str, dlq_name: str):
        """
        Process a single message from the DLQ
        
        Args:
            ch: Channel
            method: Delivery method
            properties: Message properties
            body: Message body
            target_queue: Target queue for failed messages
            dlq_name: Name of the DLQ being processed
        """
        self.messages_processed += 1
        
        # Initialize stats for this DLQ if not exists
        if dlq_name not in self.queue_stats:
            self.queue_stats[dlq_name] = {
                'processed': 0,
                'moved': 0,
                'requeued': 0
            }
        
        self.queue_stats[dlq_name]['processed'] += 1
        
        # Extract death count from headers
        death_count = self.get_death_count(properties.headers if properties.headers else {})
        
        logger.info(f"Processing message {self.messages_processed}: "
                   f"delivery_tag={method.delivery_tag}, "
                   f"x-death count={death_count}")
        
        # Check if death count exceeds threshold
        if death_count > self.config.MAX_RETRY_COUNT:
            logger.warning(
                f"Message exceeded retry limit ({death_count} > {self.config.MAX_RETRY_COUNT}). "
                f"Moving to permanent failure queue: {target_queue}"
            )
            
            try:
                # Publish message to permanent failure queue
                self.channel.basic_publish(
                    exchange='',
                    routing_key=target_queue,
                    body=body,
                    properties=properties
                )
                
                # Acknowledge the message to remove it from DLQ
                ch.basic_ack(delivery_tag=method.delivery_tag)
                self.messages_moved += 1
                self.queue_stats[dlq_name]['moved'] += 1
                
                logger.info(f"Successfully moved message to {target_queue}")
                
            except Exception as e:
                logger.error(f"Failed to move message: {e}")
                # Don't acknowledge if we couldn't move it
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        else:
            # Message hasn't exceeded threshold, requeue it
            logger.info(f"Message retry count ({death_count}) is within limit. Requeuing.")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            self.messages_requeued += 1
            self.queue_stats[dlq_name]['requeued'] += 1
    
    def check_dlq(self):
        """
        Check all messages in the DLQ(s) and process them.
        Supports wildcard patterns to process multiple DLQs.
        """
        try:
            # Get list of queue pairs to process (supports wildcards)
            queue_pairs = self.get_matching_queue_pairs()
            
            if not queue_pairs:
                logger.warning("No queues to process")
                return
            
            logger.info(f"Processing {len(queue_pairs)} queue pair(s)")
            
            # Process each DLQ -> target queue pair
            for dlq_name, target_queue in queue_pairs:
                logger.info(f"Processing DLQ: '{dlq_name}' -> Target: '{target_queue}'")
                self._process_single_dlq(dlq_name, target_queue)
            
            logger.info(f"All queues processed. Total - Processed: {self.messages_processed}, "
                       f"Moved: {self.messages_moved}, Requeued: {self.messages_requeued}")
            
        except Exception as e:
            logger.error(f"Error checking DLQ: {e}")
            raise
    
    def _process_single_dlq(self, dlq_name: str, target_queue: str):
        """
        Process a single DLQ
        
        Args:
            dlq_name: Name of the DLQ to process
            target_queue: Name of the target queue for failed messages
        """
        try:
            # Ensure target queue exists
            self.ensure_target_queue_exists(target_queue)
            
            # Get queue information for logging
            try:
                queue_info = self.channel.queue_declare(
                    queue=dlq_name,
                    passive=True
                )
            except pika.exceptions.ChannelClosedByBroker as exc:
                logger.error(
                    "Dead Letter Queue '%s' does not exist or cannot be accessed: %s",
                    dlq_name,
                    exc,
                )
                raise
            
            message_count = queue_info.method.message_count
            
            logger.info(f"DLQ '{dlq_name}' has {message_count} messages")
            
            if message_count == 0:
                logger.info(f"No messages to process in '{dlq_name}'")
                return
            
            # Process messages until queue is empty
            # Don't rely on message_count as it can change during processing
            while True:
                method_frame, properties, body = self.channel.basic_get(
                    queue=dlq_name,
                    auto_ack=False
                )
                
                if method_frame is None:
                    # No more messages
                    break
                    
                self.process_message(self.channel, method_frame, properties, body, target_queue, dlq_name)
            
            # Log stats for this specific DLQ
            if dlq_name in self.queue_stats:
                stats = self.queue_stats[dlq_name]
                logger.info(f"Completed '{dlq_name}': Processed={stats['processed']}, "
                           f"Moved={stats['moved']}, Requeued={stats['requeued']}")
            
        except Exception as e:
            logger.error(f"Error processing DLQ '{dlq_name}': {e}")
            raise
    
    def close(self):
        """Close the RabbitMQ connection"""
        try:
            if self.connection and not self.connection.is_closed:
                self.connection.close()
                logger.info("Connection closed")
        except Exception as e:
            logger.error(f"Error closing connection: {e}")
    
    def get_result_dict(self) -> Dict[str, Any]:
        """
        Get execution results as a dictionary
        
        Returns:
            Dictionary with execution results
        """
        duration = None
        if self.start_time and self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
        
        result = {
            'status': 'success' if self.success else 'error',
            'timestamp': datetime.now().isoformat(),
            'config': {
                'rmq_host': self.config.RMQ_HOST,
                'rmq_port': self.config.RMQ_PORT,
                'dlq_name': self.config.DLQ_NAME,
                'target_queue': self.config.TARGET_QUEUE,
                'max_retry_count': self.config.MAX_RETRY_COUNT
            },
            'results': {
                'messages_processed': self.messages_processed,
                'messages_moved': self.messages_moved,
                'messages_requeued': self.messages_requeued,
                'duration_seconds': duration
            }
        }
        
        # Add per-queue statistics if we processed multiple queues
        if len(self.queue_stats) > 1:
            result['queue_statistics'] = self.queue_stats
        
        return result
    
    def output_results(self):
        """Output results in the configured format"""
        if self.output_format == 'json':
            print(json.dumps(self.get_result_dict(), indent=2))
        else:
            # Human-readable text output
            print("\n" + "=" * 60)
            print("RabbitMQ Retry Checker - Execution Summary")
            print("=" * 60)
            print(f"Status: {'SUCCESS' if self.success else 'ERROR'}")
            print(f"DLQ: {self.config.DLQ_NAME}")
            print(f"Target Queue: {self.config.TARGET_QUEUE}")
            print(f"Max Retry Count: {self.config.MAX_RETRY_COUNT}")
            print("-" * 60)
            print(f"Messages Processed: {self.messages_processed}")
            print(f"Messages Moved to Failure Queue: {self.messages_moved}")
            print(f"Messages Requeued: {self.messages_requeued}")
            if self.start_time and self.end_time:
                duration = (self.end_time - self.start_time).total_seconds()
                print(f"Duration: {duration:.2f} seconds")
            print("=" * 60 + "\n")
    
    def run(self) -> bool:
        """
        Main execution method
        
        Returns:
            bool: True if successful, False otherwise
        """
        self.start_time = datetime.now()
        
        try:
            if not self.connect():
                logger.error("Failed to connect to RabbitMQ. Exiting.")
                self.success = False
                return False
            
            self.check_dlq()
            self.success = True
            return True
            
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
            self.success = False
            return False
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            self.success = False
            return False
        finally:
            self.end_time = datetime.now()
            self.close()
            self.output_results()


def parse_arguments() -> argparse.Namespace:
    """
    Parse command-line arguments
    
    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description='RabbitMQ DLQ Retry Checker - Detect and handle infinite retry loops',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Using command-line arguments
  %(prog)s --host localhost --dlq my_dlq --target-queue failed_queue --max-retries 3
  
  # Using config file
  %(prog)s --config /path/to/config.yaml
  
  # Using .env file (default behavior)
  %(prog)s
  
  # JSON output for automation
  %(prog)s --output-format json
  
Configuration precedence (highest to lowest):
  1. Command-line arguments
  2. Config file (--config)
  3. Environment variables (.env file)
  4. Default values
        """
    )
    
    # Connection options
    conn_group = parser.add_argument_group('RabbitMQ Connection')
    conn_group.add_argument('--host', help='RabbitMQ host (default: localhost)')
    conn_group.add_argument('--port', type=int, help='RabbitMQ port (default: 5672)')
    conn_group.add_argument('--mgmt-port', type=int, dest='mgmt_port', help='RabbitMQ Management API port (default: 15672, required for wildcard support)')
    conn_group.add_argument('--username', help='RabbitMQ username (default: guest)')
    conn_group.add_argument('--password', help=(
        'RabbitMQ password (default: guest). '
        'WARNING: Avoid using --password in production as command-line arguments '
        'are visible in process lists. Use environment variables or config files instead.'
    ))
    conn_group.add_argument('--vhost', help='RabbitMQ virtual host (default: /)')
    conn_group.add_argument('--ssl', action='store_true', help='Use SSL/TLS connection')
    
    # Queue options
    queue_group = parser.add_argument_group('Queue Configuration')
    queue_group.add_argument('--dlq', help='Dead Letter Queue name (default: my_dlq)')
    queue_group.add_argument('--target-queue', help='Target queue for failed messages (default: permanent_failure_queue)')
    queue_group.add_argument('--max-retries', type=int, help='Maximum retry count before moving to target queue (default: 3)')
    
    # Config file
    parser.add_argument('--config', help='Path to YAML configuration file')
    
    # Output options
    parser.add_argument('--output-format', choices=['text', 'json'], default='text',
                       help='Output format: text (human-readable) or json (machine-readable) (default: text)')
    
    # Logging options
    parser.add_argument('--quiet', action='store_true', help='Suppress log output (only show summary)')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    
    return parser.parse_args()


def main():
    """Main entry point"""
    args = parse_arguments()
    
    # Configure logging level
    if args.quiet:
        logging.getLogger().setLevel(logging.ERROR)
    elif args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Load configuration
    config = Config()
    
    # Load from config file if provided
    if args.config:
        if not os.path.exists(args.config):
            logger.error(f"Config file not found: {args.config}")
            sys.exit(1)
        logger.info(f"Loading configuration from {args.config}")
        config.load_from_file(args.config)
    
    # Override with command-line arguments
    config.update_from_args(args)
    
    # Validate configuration
    try:
        config.validate()
    except ValueError as e:
        logger.error(f"Configuration validation failed: {e}")
        sys.exit(1)
    
    logger.info("Starting RabbitMQ Retry Checker")
    logger.debug(f"Configuration: DLQ='{config.DLQ_NAME}', "
                f"Target Queue='{config.TARGET_QUEUE}', "
                f"Max Retry Count={config.MAX_RETRY_COUNT}")
    
    # Run checker
    checker = RMQRetryChecker(config, output_format=args.output_format)
    success = checker.run()
    
    if success:
        logger.info("RabbitMQ Retry Checker completed successfully")
        sys.exit(0)
    else:
        logger.error("RabbitMQ Retry Checker encountered errors")
        sys.exit(1)


if __name__ == "__main__":
    main()
