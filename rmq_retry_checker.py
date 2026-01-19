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
from typing import Optional, Dict, Any
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
        self.RMQ_USERNAME = os.getenv('RMQ_USERNAME', 'guest')
        self.RMQ_PASSWORD = os.getenv('RMQ_PASSWORD', 'guest')
        self.RMQ_VHOST = os.getenv('RMQ_VHOST', '/')
        self.RMQ_USE_SSL = os.getenv('RMQ_USE_SSL', 'false').lower() == 'true'
        
        # Queue Settings
        self.DLQ_NAME = os.getenv('DLQ_NAME', 'my_dlq')
        self.TARGET_QUEUE = os.getenv('TARGET_QUEUE', 'permanent_failure_queue')
        self.MAX_RETRY_COUNT = int(os.getenv('MAX_RETRY_COUNT', 3))
    
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
            raise yaml.YAMLError(f"Invalid YAML in configuration file: {e}")
        
        # Empty YAML files return None - use defaults in this case
        if config_data is None:
            return
        
        # Load RabbitMQ settings
        if 'rabbitmq' in config_data:
            rmq = config_data['rabbitmq']
            self.RMQ_HOST = rmq.get('host', self.RMQ_HOST)
            self.RMQ_PORT = int(rmq.get('port', self.RMQ_PORT))
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
                ssl_options = pika.SSLOptions(context=None)
                parameters.ssl_options = ssl_options
            
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            
            logger.info(f"Connected to RabbitMQ at {self.config.RMQ_HOST}:{self.config.RMQ_PORT}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            return False
    
    def ensure_target_queue_exists(self):
        """
        Ensure the target queue (permanent failure queue) exists
        """
        try:
            self.channel.queue_declare(
                queue=self.config.TARGET_QUEUE,
                durable=True,
                arguments={'x-queue-type': 'quorum'}
            )
            logger.info(f"Target queue '{self.config.TARGET_QUEUE}' is ready")
        except Exception as e:
            logger.error(f"Failed to declare target queue: {e}")
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
    
    def process_message(self, ch, method, properties, body):
        """
        Process a single message from the DLQ
        
        Args:
            ch: Channel
            method: Delivery method
            properties: Message properties
            body: Message body
        """
        self.messages_processed += 1
        
        # Extract death count from headers
        death_count = self.get_death_count(properties.headers if properties.headers else {})
        
        logger.info(f"Processing message {self.messages_processed}: "
                   f"delivery_tag={method.delivery_tag}, "
                   f"x-death count={death_count}")
        
        # Check if death count exceeds threshold
        if death_count > self.config.MAX_RETRY_COUNT:
            logger.warning(
                f"Message exceeded retry limit ({death_count} > {self.config.MAX_RETRY_COUNT}). "
                f"Moving to permanent failure queue: {self.config.TARGET_QUEUE}"
            )
            
            try:
                # Publish message to permanent failure queue
                self.channel.basic_publish(
                    exchange='',
                    routing_key=self.config.TARGET_QUEUE,
                    body=body,
                    properties=properties
                )
                
                # Acknowledge the message to remove it from DLQ
                ch.basic_ack(delivery_tag=method.delivery_tag)
                self.messages_moved += 1
                
                logger.info(f"Successfully moved message to {self.config.TARGET_QUEUE}")
                
            except Exception as e:
                logger.error(f"Failed to move message: {e}")
                # Don't acknowledge if we couldn't move it
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        else:
            # Message hasn't exceeded threshold, requeue it
            logger.info(f"Message retry count ({death_count}) is within limit. Requeuing.")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            self.messages_requeued += 1
    
    def check_dlq(self):
        """
        Check all messages in the DLQ and process them
        """
        try:
            # Ensure target queue exists
            self.ensure_target_queue_exists()
            
            # Get queue information for logging
            queue_info = self.channel.queue_declare(
                queue=self.config.DLQ_NAME,
                passive=True
            )
            message_count = queue_info.method.message_count
            
            logger.info(f"DLQ '{self.config.DLQ_NAME}' has {message_count} messages")
            
            if message_count == 0:
                logger.info("No messages to process")
                return
            
            # Process messages until queue is empty
            # Don't rely on message_count as it can change during processing
            while True:
                method_frame, properties, body = self.channel.basic_get(
                    queue=self.config.DLQ_NAME,
                    auto_ack=False
                )
                
                if method_frame is None:
                    # No more messages
                    break
                    
                self.process_message(self.channel, method_frame, properties, body)
            
            logger.info(f"Processing complete. Processed: {self.messages_processed}, "
                       f"Moved to permanent failure queue: {self.messages_moved}, "
                       f"Requeued: {self.messages_requeued}")
            
        except Exception as e:
            logger.error(f"Error checking DLQ: {e}")
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
        
        return {
            'status': 'success' if self.messages_processed >= 0 else 'error',
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
    
    def output_results(self):
        """Output results in the configured format"""
        if self.output_format == 'json':
            print(json.dumps(self.get_result_dict(), indent=2))
        else:
            # Human-readable text output
            print("\n" + "=" * 60)
            print("RabbitMQ Retry Checker - Execution Summary")
            print("=" * 60)
            print(f"Status: {'SUCCESS' if self.messages_processed >= 0 else 'ERROR'}")
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
                return False
            
            self.check_dlq()
            return True
            
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
            return False
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
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
    conn_group.add_argument('--username', help='RabbitMQ username (default: guest)')
    conn_group.add_argument('--password', help='RabbitMQ password (default: guest)')
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
    
    logger.info("Starting RabbitMQ Retry Checker")
    logger.info(f"Configuration: DLQ='{config.DLQ_NAME}', "
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
