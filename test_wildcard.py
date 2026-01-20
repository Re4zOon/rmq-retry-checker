#!/usr/bin/env python3
"""Test wildcard functionality in RabbitMQ Retry Checker."""
import sys
import unittest
from unittest.mock import patch

try:
    from rmq_retry_checker import Config, RMQRetryChecker
except ImportError:
    print("ERROR: Could not import rmq_retry_checker module")
    sys.exit(1)


class TestWildcardFunctionality(unittest.TestCase):
    """Test cases for wildcard pattern matching and queue name derivation."""
    
    def setUp(self):
        self.config = Config()
        self.config.RMQ_HOST = 'localhost'
        self.config.RMQ_PORT = 5672
        self.config.RMQ_USERNAME = 'guest'
        self.config.RMQ_PASSWORD = 'guest'
        self.config.RMQ_VHOST = '/'
        self.config.MAX_RETRY_COUNT = 3
    
    def test_has_wildcard_detection(self):
        self.assertTrue(Config.has_wildcard('dlq.*'))
        self.assertTrue(Config.has_wildcard('*_dlq'))
        self.assertTrue(Config.has_wildcard('service_?_dlq'))
        self.assertFalse(Config.has_wildcard('dlq.queue1'))
        self.assertFalse(Config.has_wildcard('my_dlq'))
    
    def test_derive_target_queue_simple_wildcard(self):
        checker = RMQRetryChecker(self.config)
        
        result = checker._derive_target_queue('service_auth_dlq', 'service_*_dlq', 'service_*_failed')
        self.assertEqual(result, 'service_auth_failed')
        
        result = checker._derive_target_queue('orders_retry', '*_retry', '*_dead')
        self.assertEqual(result, 'orders_dead')
    
    def test_derive_target_queue_dot_separator(self):
        checker = RMQRetryChecker(self.config)
        
        result = checker._derive_target_queue('dlq.queue1', 'dlq.*', 'dead.*')
        self.assertEqual(result, 'dead.queue1')
        
        result = checker._derive_target_queue('dlq.payments', 'dlq.*', 'dead.*')
        self.assertEqual(result, 'dead.payments')
    
    def test_derive_target_queue_multiple_wildcards(self):
        checker = RMQRetryChecker(self.config)
        result = checker._derive_target_queue('v1_dlq_orders', '*_dlq_*', '*_failed_*')
        self.assertEqual(result, 'v1_failed_orders')
    
    def test_derive_target_queue_question_mark_wildcard(self):
        checker = RMQRetryChecker(self.config)
        result = checker._derive_target_queue('dlq_a', 'dlq_?', 'dead_?')
        self.assertEqual(result, 'dead_a')
    
    def test_derive_target_queue_complex_patterns(self):
        checker = RMQRetryChecker(self.config)
        result = checker._derive_target_queue('app_payment_v2_dlq', 'app_*_v?_dlq', 'app_*_v?_dead')
        self.assertEqual(result, 'app_payment_v2_dead')
    
    def test_derive_target_queue_prefix_pattern(self):
        checker = RMQRetryChecker(self.config)
        result = checker._derive_target_queue('prefix.something', 'prefix.*', 'newprefix.*')
        self.assertEqual(result, 'newprefix.something')
    
    def test_derive_target_queue_suffix_pattern(self):
        checker = RMQRetryChecker(self.config)
        result = checker._derive_target_queue('something.suffix', '*.suffix', '*.newsuffix')
        self.assertEqual(result, 'something.newsuffix')
    
    @patch.object(RMQRetryChecker, 'list_queues_from_api')
    def test_get_matching_queue_pairs_no_wildcard(self, mock_list_queues):
        self.config.DLQ_NAME = 'my_dlq'
        self.config.TARGET_QUEUE = 'permanent_failure_queue'
        
        checker = RMQRetryChecker(self.config)
        pairs = checker.get_matching_queue_pairs()
        
        self.assertEqual(len(pairs), 1)
        self.assertEqual(pairs[0], ('my_dlq', 'permanent_failure_queue'))
        mock_list_queues.assert_not_called()
    
    @patch.object(RMQRetryChecker, 'list_queues_from_api')
    def test_get_matching_queue_pairs_with_wildcard(self, mock_list_queues):
        mock_list_queues.return_value = ['dlq.queue1', 'dlq.queue2', 'dlq.payments', 'other_queue', 'dead.queue1']
        
        self.config.DLQ_NAME = 'dlq.*'
        self.config.TARGET_QUEUE = 'dead.*'
        
        checker = RMQRetryChecker(self.config)
        pairs = checker.get_matching_queue_pairs()
        
        self.assertEqual(len(pairs), 3)
        expected_pairs = [('dlq.queue1', 'dead.queue1'), ('dlq.queue2', 'dead.queue2'), ('dlq.payments', 'dead.payments')]
        self.assertEqual(sorted(pairs), sorted(expected_pairs))
    
    @patch.object(RMQRetryChecker, 'list_queues_from_api')
    def test_get_matching_queue_pairs_fixed_target(self, mock_list_queues):
        mock_list_queues.return_value = ['service_a_dlq', 'service_b_dlq', 'service_auth_dlq', 'other_queue']
        
        self.config.DLQ_NAME = 'service_*_dlq'
        self.config.TARGET_QUEUE = 'all_failures'
        
        checker = RMQRetryChecker(self.config)
        pairs = checker.get_matching_queue_pairs()
        
        self.assertEqual(len(pairs), 3)
        for dlq, target in pairs:
            self.assertEqual(target, 'all_failures')
    
    @patch.object(RMQRetryChecker, 'list_queues_from_api')
    def test_get_matching_queue_pairs_no_matches(self, mock_list_queues):
        mock_list_queues.return_value = ['other_queue1', 'other_queue2']
        
        self.config.DLQ_NAME = 'dlq.*'
        self.config.TARGET_QUEUE = 'dead.*'
        
        checker = RMQRetryChecker(self.config)
        pairs = checker.get_matching_queue_pairs()
        
        self.assertEqual(len(pairs), 0)


class TestWildcardEdgeCases(unittest.TestCase):
    """Test edge cases for wildcard functionality."""
    
    def setUp(self):
        self.config = Config()
        self.config.RMQ_HOST = 'localhost'
        self.config.RMQ_PORT = 5672
        self.config.RMQ_USERNAME = 'guest'
        self.config.RMQ_PASSWORD = 'guest'
        self.config.RMQ_VHOST = '/'
        self.config.MAX_RETRY_COUNT = 3
    
    def test_derive_target_queue_special_characters(self):
        checker = RMQRetryChecker(self.config)
        
        result = checker._derive_target_queue('dlq.queue-1', 'dlq.*', 'dead.*')
        self.assertEqual(result, 'dead.queue-1')
        
        result = checker._derive_target_queue('dlq.my_queue_name', 'dlq.*', 'dead.*')
        self.assertEqual(result, 'dead.my_queue_name')
    
    def test_derive_target_queue_empty_capture(self):
        checker = RMQRetryChecker(self.config)
        result = checker._derive_target_queue('dlq.', 'dlq.*', 'dead.*')
        self.assertEqual(result, 'dead.')


if __name__ == '__main__':
    unittest.main(verbosity=2)
