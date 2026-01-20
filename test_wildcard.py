#!/usr/bin/env python3
"""
Test script to validate wildcard functionality in RabbitMQ Retry Checker.
This script tests the _derive_target_queue method to ensure it correctly
handles various wildcard patterns, including the specific case mentioned
in the issue: dlq.queue1 -> dead.queue1
"""
import sys
import unittest
from unittest.mock import Mock, patch, MagicMock
import re

# Import the main module
try:
    from rmq_retry_checker import Config, RMQRetryChecker
except ImportError:
    print("ERROR: Could not import rmq_retry_checker module")
    sys.exit(1)


class TestWildcardFunctionality(unittest.TestCase):
    """Test cases for wildcard pattern matching and queue name derivation"""
    
    def setUp(self):
        """Set up test configuration"""
        self.config = Config()
        self.config.RMQ_HOST = 'localhost'
        self.config.RMQ_PORT = 5672
        self.config.RMQ_USERNAME = 'guest'
        self.config.RMQ_PASSWORD = 'guest'
        self.config.RMQ_VHOST = '/'
        self.config.MAX_RETRY_COUNT = 3
    
    def test_has_wildcard_detection(self):
        """Test that wildcard detection works correctly"""
        # Test with wildcards
        self.assertTrue(Config.has_wildcard('dlq.*'))
        self.assertTrue(Config.has_wildcard('*_dlq'))
        self.assertTrue(Config.has_wildcard('service_?_dlq'))
        self.assertTrue(Config.has_wildcard('*'))
        self.assertTrue(Config.has_wildcard('?'))
        
        # Test without wildcards
        self.assertFalse(Config.has_wildcard('dlq.queue1'))
        self.assertFalse(Config.has_wildcard('my_dlq'))
        self.assertFalse(Config.has_wildcard('permanent_failure_queue'))
    
    def test_derive_target_queue_simple_wildcard(self):
        """Test deriving target queue with simple * wildcard"""
        checker = RMQRetryChecker(self.config)
        
        # Test case: service_*_dlq -> service_*_failed
        result = checker._derive_target_queue(
            dlq_name='service_auth_dlq',
            dlq_pattern='service_*_dlq',
            target_pattern='service_*_failed'
        )
        self.assertEqual(result, 'service_auth_failed')
        
        # Test case: *_retry -> *_dead
        result = checker._derive_target_queue(
            dlq_name='orders_retry',
            dlq_pattern='*_retry',
            target_pattern='*_dead'
        )
        self.assertEqual(result, 'orders_dead')
    
    def test_derive_target_queue_dot_separator(self):
        """Test deriving target queue with dot separator (the main issue scenario)"""
        checker = RMQRetryChecker(self.config)
        
        # Test case from problem statement: dlq.queue1 -> dead.queue1
        result = checker._derive_target_queue(
            dlq_name='dlq.queue1',
            dlq_pattern='dlq.*',
            target_pattern='dead.*'
        )
        self.assertEqual(result, 'dead.queue1')
        
        # Test with different queue names
        result = checker._derive_target_queue(
            dlq_name='dlq.queue2',
            dlq_pattern='dlq.*',
            target_pattern='dead.*'
        )
        self.assertEqual(result, 'dead.queue2')
        
        result = checker._derive_target_queue(
            dlq_name='dlq.payments',
            dlq_pattern='dlq.*',
            target_pattern='dead.*'
        )
        self.assertEqual(result, 'dead.payments')
        
        result = checker._derive_target_queue(
            dlq_name='dlq.orders_service',
            dlq_pattern='dlq.*',
            target_pattern='dead.*'
        )
        self.assertEqual(result, 'dead.orders_service')
    
    def test_derive_target_queue_multiple_wildcards(self):
        """Test deriving target queue with multiple wildcards"""
        checker = RMQRetryChecker(self.config)
        
        # Test case: *_dlq_* -> *_failed_*
        result = checker._derive_target_queue(
            dlq_name='v1_dlq_orders',
            dlq_pattern='*_dlq_*',
            target_pattern='*_failed_*'
        )
        self.assertEqual(result, 'v1_failed_orders')
    
    def test_derive_target_queue_question_mark_wildcard(self):
        """Test deriving target queue with ? wildcard"""
        checker = RMQRetryChecker(self.config)
        
        # Test case: dlq_? -> dead_?
        result = checker._derive_target_queue(
            dlq_name='dlq_a',
            dlq_pattern='dlq_?',
            target_pattern='dead_?'
        )
        self.assertEqual(result, 'dead_a')
    
    def test_derive_target_queue_complex_patterns(self):
        """Test deriving target queue with complex patterns"""
        checker = RMQRetryChecker(self.config)
        
        # Test case: app_*_v?_dlq -> app_*_v?_dead
        result = checker._derive_target_queue(
            dlq_name='app_payment_v2_dlq',
            dlq_pattern='app_*_v?_dlq',
            target_pattern='app_*_v?_dead'
        )
        self.assertEqual(result, 'app_payment_v2_dead')
    
    def test_derive_target_queue_prefix_pattern(self):
        """Test deriving target queue with prefix pattern"""
        checker = RMQRetryChecker(self.config)
        
        # Test case: prefix.* -> newprefix.*
        result = checker._derive_target_queue(
            dlq_name='prefix.something',
            dlq_pattern='prefix.*',
            target_pattern='newprefix.*'
        )
        self.assertEqual(result, 'newprefix.something')
    
    def test_derive_target_queue_suffix_pattern(self):
        """Test deriving target queue with suffix pattern"""
        checker = RMQRetryChecker(self.config)
        
        # Test case: *.suffix -> *.newsuffix
        result = checker._derive_target_queue(
            dlq_name='something.suffix',
            dlq_pattern='*.suffix',
            target_pattern='*.newsuffix'
        )
        self.assertEqual(result, 'something.newsuffix')
    
    @patch.object(RMQRetryChecker, 'list_queues_from_api')
    def test_get_matching_queue_pairs_no_wildcard(self, mock_list_queues):
        """Test getting queue pairs when no wildcards are present"""
        self.config.DLQ_NAME = 'my_dlq'
        self.config.TARGET_QUEUE = 'permanent_failure_queue'
        
        checker = RMQRetryChecker(self.config)
        pairs = checker.get_matching_queue_pairs()
        
        # Should return single pair without calling API
        self.assertEqual(len(pairs), 1)
        self.assertEqual(pairs[0], ('my_dlq', 'permanent_failure_queue'))
        
        # API should not be called
        mock_list_queues.assert_not_called()
    
    @patch.object(RMQRetryChecker, 'list_queues_from_api')
    def test_get_matching_queue_pairs_with_wildcard(self, mock_list_queues):
        """Test getting queue pairs with wildcard in DLQ name"""
        # Expected number of matching DLQ queues
        EXPECTED_MATCHES = 3
        
        # Mock the API to return some queues
        mock_list_queues.return_value = [
            'dlq.queue1',
            'dlq.queue2',
            'dlq.payments',
            'other_queue',
            'dead.queue1'  # This should not match dlq.* pattern
        ]
        
        self.config.DLQ_NAME = 'dlq.*'
        self.config.TARGET_QUEUE = 'dead.*'
        
        checker = RMQRetryChecker(self.config)
        pairs = checker.get_matching_queue_pairs()
        
        # Should return EXPECTED_MATCHES pairs (only queues matching dlq.*)
        self.assertEqual(len(pairs), EXPECTED_MATCHES)
        
        # Verify the pairs are correctly derived
        expected_pairs = [
            ('dlq.queue1', 'dead.queue1'),
            ('dlq.queue2', 'dead.queue2'),
            ('dlq.payments', 'dead.payments')
        ]
        
        # Sort both lists for comparison
        pairs_sorted = sorted(pairs)
        expected_sorted = sorted(expected_pairs)
        
        self.assertEqual(pairs_sorted, expected_sorted)
        
        # API should be called
        mock_list_queues.assert_called_once()
    
    @patch.object(RMQRetryChecker, 'list_queues_from_api')
    def test_get_matching_queue_pairs_fixed_target(self, mock_list_queues):
        """Test getting queue pairs with wildcard DLQ but fixed target"""
        # Mock the API to return some queues
        mock_list_queues.return_value = [
            'service_a_dlq',
            'service_b_dlq',
            'service_auth_dlq',
            'other_queue'
        ]
        
        self.config.DLQ_NAME = 'service_*_dlq'
        self.config.TARGET_QUEUE = 'all_failures'  # Fixed target, no wildcard
        
        checker = RMQRetryChecker(self.config)
        pairs = checker.get_matching_queue_pairs()
        
        # Should return three pairs, all with same target
        self.assertEqual(len(pairs), 3)
        
        # All should point to the same fixed target
        for dlq, target in pairs:
            self.assertEqual(target, 'all_failures')
    
    @patch.object(RMQRetryChecker, 'list_queues_from_api')
    def test_get_matching_queue_pairs_no_matches(self, mock_list_queues):
        """Test getting queue pairs when pattern doesn't match any queues"""
        # Mock the API to return queues that don't match
        mock_list_queues.return_value = [
            'other_queue1',
            'other_queue2',
            'different_format'
        ]
        
        self.config.DLQ_NAME = 'dlq.*'
        self.config.TARGET_QUEUE = 'dead.*'
        
        checker = RMQRetryChecker(self.config)
        pairs = checker.get_matching_queue_pairs()
        
        # Should return empty list
        self.assertEqual(len(pairs), 0)


class TestWildcardEdgeCases(unittest.TestCase):
    """Test edge cases for wildcard functionality"""
    
    def setUp(self):
        """Set up test configuration"""
        self.config = Config()
        self.config.RMQ_HOST = 'localhost'
        self.config.RMQ_PORT = 5672
        self.config.RMQ_USERNAME = 'guest'
        self.config.RMQ_PASSWORD = 'guest'
        self.config.RMQ_VHOST = '/'
        self.config.MAX_RETRY_COUNT = 3
    
    def test_derive_target_queue_special_characters(self):
        """Test that special regex characters in queue names are handled correctly"""
        checker = RMQRetryChecker(self.config)
        
        # Queue name with special characters that should be escaped
        result = checker._derive_target_queue(
            dlq_name='dlq.queue-1',
            dlq_pattern='dlq.*',
            target_pattern='dead.*'
        )
        self.assertEqual(result, 'dead.queue-1')
        
        # Queue name with underscores
        result = checker._derive_target_queue(
            dlq_name='dlq.my_queue_name',
            dlq_pattern='dlq.*',
            target_pattern='dead.*'
        )
        self.assertEqual(result, 'dead.my_queue_name')
    
    def test_derive_target_queue_empty_capture(self):
        """Test wildcard with potentially empty capture"""
        checker = RMQRetryChecker(self.config)
        
        # Edge case: what if the wildcard matches an empty string?
        # This depends on fnmatch behavior
        result = checker._derive_target_queue(
            dlq_name='dlq.',
            dlq_pattern='dlq.*',
            target_pattern='dead.*'
        )
        self.assertEqual(result, 'dead.')


def run_tests():
    """Run all tests and return results"""
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add test classes
    suite.addTests(loader.loadTestsFromTestCase(TestWildcardFunctionality))
    suite.addTests(loader.loadTestsFromTestCase(TestWildcardEdgeCases))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return result.wasSuccessful()


if __name__ == '__main__':
    success = run_tests()
    sys.exit(0 if success else 1)
