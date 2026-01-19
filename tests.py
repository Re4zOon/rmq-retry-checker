#!/usr/bin/env python3
"""
Unit tests for RMQ Retry Checker

Run with: python -m pytest tests.py -v
or: python tests.py
"""
import unittest
from unittest.mock import Mock, MagicMock, patch
from rmq_retry_checker import RMQRetryChecker
from config import Config


class TestRMQRetryChecker(unittest.TestCase):
    """Test cases for RMQRetryChecker"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.config = Config
        self.checker = RMQRetryChecker(self.config)
    
    def test_initialization(self):
        """Test RMQRetryChecker initialization"""
        self.assertEqual(self.checker.messages_processed, 0)
        self.assertEqual(self.checker.messages_moved, 0)
        self.assertIsNone(self.checker.connection)
        self.assertIsNone(self.checker.channel)
    
    def test_get_death_count_no_headers(self):
        """Test death count extraction when headers are None"""
        count = self.checker.get_death_count(None)
        self.assertEqual(count, 0)
    
    def test_get_death_count_empty_headers(self):
        """Test death count extraction when headers are empty"""
        count = self.checker.get_death_count({})
        self.assertEqual(count, 0)
    
    def test_get_death_count_no_x_death(self):
        """Test death count extraction when x-death is missing"""
        headers = {'some-other-header': 'value'}
        count = self.checker.get_death_count(headers)
        self.assertEqual(count, 0)
    
    def test_get_death_count_with_valid_x_death(self):
        """Test death count extraction with valid x-death header"""
        headers = {
            'x-death': [
                {
                    'count': 5,
                    'reason': 'rejected',
                    'queue': 'test_queue',
                    'exchange': '',
                }
            ]
        }
        count = self.checker.get_death_count(headers)
        self.assertEqual(count, 5)
    
    def test_get_death_count_with_multiple_x_death_entries(self):
        """Test death count extraction with multiple x-death entries (takes first)"""
        headers = {
            'x-death': [
                {'count': 3, 'reason': 'rejected'},
                {'count': 5, 'reason': 'expired'},
            ]
        }
        count = self.checker.get_death_count(headers)
        self.assertEqual(count, 3)
    
    def test_get_death_count_with_zero_count(self):
        """Test death count extraction when count is 0"""
        headers = {
            'x-death': [
                {'count': 0, 'reason': 'rejected'}
            ]
        }
        count = self.checker.get_death_count(headers)
        self.assertEqual(count, 0)
    
    @patch('rmq_retry_checker.pika.BlockingConnection')
    def test_connect_success(self, mock_connection):
        """Test successful connection to RabbitMQ"""
        mock_conn_instance = MagicMock()
        mock_channel = MagicMock()
        mock_conn_instance.channel.return_value = mock_channel
        mock_connection.return_value = mock_conn_instance
        
        result = self.checker.connect()
        
        self.assertTrue(result)
        self.assertIsNotNone(self.checker.connection)
        self.assertIsNotNone(self.checker.channel)
        mock_connection.assert_called_once()
    
    @patch('rmq_retry_checker.pika.BlockingConnection')
    def test_connect_failure(self, mock_connection):
        """Test connection failure"""
        mock_connection.side_effect = Exception("Connection failed")
        
        result = self.checker.connect()
        
        self.assertFalse(result)
        self.assertIsNone(self.checker.connection)
    
    def test_process_message_below_threshold(self):
        """Test processing message with death count below threshold"""
        # Mock channel and method
        mock_channel = MagicMock()
        mock_method = MagicMock()
        mock_method.delivery_tag = 123
        
        # Mock properties with low death count
        mock_properties = MagicMock()
        mock_properties.headers = {
            'x-death': [{'count': 2}]
        }
        
        body = b'test message'
        
        self.checker.process_message(mock_channel, mock_method, mock_properties, body)
        
        # Should nack and requeue
        mock_channel.basic_nack.assert_called_once_with(
            delivery_tag=123, 
            requeue=True
        )
        mock_channel.basic_ack.assert_not_called()
        self.assertEqual(self.checker.messages_moved, 0)
    
    def test_process_message_above_threshold(self):
        """Test processing message with death count above threshold"""
        # Mock channel and method
        mock_channel = MagicMock()
        mock_method = MagicMock()
        mock_method.delivery_tag = 456
        
        # Mock properties with high death count
        mock_properties = MagicMock()
        mock_properties.headers = {
            'x-death': [{'count': 5}]
        }
        
        body = b'test message'
        
        # Set up checker with channel
        self.checker.channel = mock_channel
        
        self.checker.process_message(mock_channel, mock_method, mock_properties, body)
        
        # Should publish to target queue and ack
        mock_channel.basic_publish.assert_called_once()
        mock_channel.basic_ack.assert_called_once_with(delivery_tag=456)
        mock_channel.basic_nack.assert_not_called()
        self.assertEqual(self.checker.messages_moved, 1)


class TestConfig(unittest.TestCase):
    """Test cases for Config"""
    
    def test_config_defaults(self):
        """Test default configuration values"""
        self.assertEqual(Config.RMQ_HOST, 'localhost')
        self.assertEqual(Config.RMQ_PORT, 5672)
        self.assertEqual(Config.RMQ_USERNAME, 'guest')
        self.assertEqual(Config.RMQ_PASSWORD, 'guest')
        self.assertEqual(Config.RMQ_VHOST, '/')
        self.assertFalse(Config.RMQ_USE_SSL)
        self.assertEqual(Config.DLQ_NAME, 'my_dlq')
        self.assertEqual(Config.TARGET_QUEUE, 'permanent_failure_queue')
        self.assertEqual(Config.MAX_RETRY_COUNT, 3)


if __name__ == '__main__':
    unittest.main()
