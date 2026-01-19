"""
Configuration module for RMQ Retry Checker
"""
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Config:
    """Configuration class for RabbitMQ connection and queue settings"""
    
    # RabbitMQ Connection Settings
    RMQ_HOST = os.getenv('RMQ_HOST', 'localhost')
    RMQ_PORT = int(os.getenv('RMQ_PORT', 5672))
    RMQ_USERNAME = os.getenv('RMQ_USERNAME', 'guest')
    RMQ_PASSWORD = os.getenv('RMQ_PASSWORD', 'guest')
    RMQ_VHOST = os.getenv('RMQ_VHOST', '/')
    RMQ_USE_SSL = os.getenv('RMQ_USE_SSL', 'false').lower() == 'true'
    
    # Queue Settings
    DLQ_NAME = os.getenv('DLQ_NAME', 'my_dlq')
    TARGET_QUEUE = os.getenv('TARGET_QUEUE', 'permanent_failure_queue')
    MAX_RETRY_COUNT = int(os.getenv('MAX_RETRY_COUNT', 3))
