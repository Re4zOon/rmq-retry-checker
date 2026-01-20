#!/usr/bin/env python3
"""
Integration test script for wildcard setup validation.
"""
import sys
from unittest.mock import patch

try:
    from rmq_retry_checker import Config, RMQRetryChecker
except ImportError:
    print("ERROR: Could not import rmq_retry_checker module")
    sys.exit(1)


def test_integration_dlq_dead_pattern():
    """Integration test for dlq.* -> dead.* pattern."""
    print("=" * 60)
    print("INTEGRATION TEST: dlq.* -> dead.* Wildcard Pattern")
    print("=" * 60)
    print()
    
    config = Config()
    config.DLQ_NAME = 'dlq.*'
    config.TARGET_QUEUE = 'dead.*'
    config.MAX_RETRY_COUNT = 3
    config.RMQ_HOST = 'localhost'
    config.RMQ_PORT = 5672
    config.RMQ_MGMT_PORT = 15672
    config.RMQ_USERNAME = 'guest'
    config.RMQ_PASSWORD = 'guest'
    config.RMQ_VHOST = '/'
    
    print(f"DLQ Pattern: {config.DLQ_NAME}")
    print(f"Target Pattern: {config.TARGET_QUEUE}")
    print()
    
    checker = RMQRetryChecker(config)
    
    simulated_queues = [
        'dlq.queue1',
        'dlq.queue2', 
        'dlq.queue3',
        'dlq.payments',
        'dlq.orders',
        'other.queue',
        'dead.queue1',
        'my_dlq',
    ]
    
    with patch.object(checker, 'list_queues_from_api', return_value=simulated_queues):
        queue_pairs = checker.get_matching_queue_pairs()
    
    expected_pairs = [
        ('dlq.queue1', 'dead.queue1'),
        ('dlq.queue2', 'dead.queue2'),
        ('dlq.queue3', 'dead.queue3'),
        ('dlq.payments', 'dead.payments'),
        ('dlq.orders', 'dead.orders'),
    ]
    
    actual_sorted = sorted(queue_pairs)
    expected_sorted = sorted(expected_pairs)
    
    if actual_sorted == expected_sorted:
        print("✓ SUCCESS: All queue pairs derived correctly")
        for dlq, target in sorted(queue_pairs):
            print(f"  {dlq} -> {target}")
        return True
    else:
        print("✗ FAILURE: Queue pairs mismatch")
        return False


def test_config_file_loading():
    """Test loading configuration from example config file."""
    print()
    print("=" * 60)
    print("INTEGRATION TEST: Configuration File Loading")
    print("=" * 60)
    print()
    
    try:
        config = Config()
        config.load_from_file('config.yaml.example')
        print(f"✓ File loaded successfully")
        print(f"  Host: {config.RMQ_HOST}")
        print(f"  DLQ: {config.DLQ_NAME}")
        print(f"  Target: {config.TARGET_QUEUE}")
        return True
    except Exception as e:
        print(f"✗ Failed to load config file: {e}")
        return False


def main():
    results = []
    results.append(("Wildcard Pattern Integration", test_integration_dlq_dead_pattern()))
    results.append(("Configuration File Loading", test_config_file_loading()))
    
    print()
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    all_passed = all(r[1] for r in results)
    for name, passed in results:
        status = "✓ PASSED" if passed else "✗ FAILED"
        print(f"  {name}: {status}")
    
    return 0 if all_passed else 1


if __name__ == '__main__':
    sys.exit(main())
