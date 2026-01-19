#!/usr/bin/env python3
"""
RabbitMQ Retry Checker

This script connects to RabbitMQ and checks messages in a Dead Letter Queue (DLQ)
for infinite retry loops. If a message's x-death count exceeds a threshold,
it is moved to a permanent failure queue.

Usage:
    python rmq_retry_checker.py
"""
import sys
import logging
import pika
from typing import Optional, Dict, Any
from config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RMQRetryChecker:
    """
    RabbitMQ Retry Checker class
    
    Connects to RabbitMQ, inspects DLQ messages for infinite retry loops,
    and moves messages exceeding the retry threshold to a permanent failure queue.
    """
    
    def __init__(self, config: Config):
        """
        Initialize the RMQ Retry Checker
        
        Args:
            config: Configuration object with RabbitMQ connection and queue settings
        """
        self.config = config
        self.connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[pika.channel.Channel] = None
        self.messages_processed = 0
        self.messages_moved = 0
        
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
                       f"Moved to permanent failure queue: {self.messages_moved}")
            
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
    
    def run(self):
        """
        Main execution method
        """
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
            self.close()


def main():
    """Main entry point"""
    logger.info("Starting RabbitMQ Retry Checker")
    logger.info(f"Configuration: DLQ='{Config.DLQ_NAME}', "
               f"Target Queue='{Config.TARGET_QUEUE}', "
               f"Max Retry Count={Config.MAX_RETRY_COUNT}")
    
    checker = RMQRetryChecker(Config)
    success = checker.run()
    
    if success:
        logger.info("RabbitMQ Retry Checker completed successfully")
        sys.exit(0)
    else:
        logger.error("RabbitMQ Retry Checker encountered errors")
        sys.exit(1)


if __name__ == "__main__":
    main()
