#!/usr/bin/env python3
"""
Example demonstrating the usage of RMQ Retry Checker

This example shows how to use the RMQRetryChecker class programmatically
rather than using the command-line interface.
"""
import logging
from config import Config
from rmq_retry_checker import RMQRetryChecker

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def main():
    """Example usage of RMQRetryChecker"""
    
    # You can override configuration programmatically
    # Config.DLQ_NAME = "my_custom_dlq"
    # Config.TARGET_QUEUE = "my_custom_failure_queue"
    # Config.MAX_RETRY_COUNT = 5
    
    print("=" * 60)
    print("RabbitMQ Retry Checker - Example Usage")
    print("=" * 60)
    print()
    print("Configuration:")
    print(f"  RMQ Host: {Config.RMQ_HOST}:{Config.RMQ_PORT}")
    print(f"  DLQ Name: {Config.DLQ_NAME}")
    print(f"  Target Queue: {Config.TARGET_QUEUE}")
    print(f"  Max Retry Count: {Config.MAX_RETRY_COUNT}")
    print()
    print("=" * 60)
    print()
    
    # Create checker instance
    checker = RMQRetryChecker(Config)
    
    # Run the checker
    success = checker.run()
    
    if success:
        print()
        print("=" * 60)
        print("Summary:")
        print(f"  Messages Processed: {checker.messages_processed}")
        print(f"  Messages Moved to Permanent Failure Queue: {checker.messages_moved}")
        print("=" * 60)
        return 0
    else:
        print()
        print("=" * 60)
        print("Checker encountered errors. Check logs above.")
        print("=" * 60)
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
