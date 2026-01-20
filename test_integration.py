#!/usr/bin/env python3
"""
Integration test script for wildcard setup validation.

This script performs a comprehensive integration test to verify that the
wildcard setup works correctly for the pattern:
  - DLQs named: dlq.queue1, dlq.queue2, dlq.payments, etc.
  - Target queues: dead.queue1, dead.queue2, dead.payments, etc.
  - The queue name always remains the same

This test simulates the complete workflow without requiring an actual
RabbitMQ server.
"""
import sys
from unittest.mock import patch, Mock, MagicMock

try:
    from rmq_retry_checker import Config, RMQRetryChecker
except ImportError:
    print("ERROR: Could not import rmq_retry_checker module")
    sys.exit(1)


def test_integration_dlq_dead_pattern():
    """
    Integration test for dlq.* -> dead.* pattern.
    
    This test validates the complete workflow:
    1. Configuration loading
    2. Queue listing from Management API
    3. Pattern matching
    4. Target queue derivation
    5. Queue pair generation
    """
    print("=" * 80)
    print("INTEGRATION TEST: dlq.* -> dead.* Wildcard Pattern")
    print("=" * 80)
    print()
    
    # Step 1: Create configuration
    print("Step 1: Creating configuration...")
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
    print(f"  ✓ DLQ Pattern: {config.DLQ_NAME}")
    print(f"  ✓ Target Pattern: {config.TARGET_QUEUE}")
    print()
    
    # Step 2: Validate configuration
    print("Step 2: Validating configuration...")
    try:
        config.validate()
        print("  ✓ Configuration is valid")
    except Exception as e:
        print(f"  ✗ Configuration validation failed: {e}")
        return False
    print()
    
    # Step 3: Create checker instance
    print("Step 3: Creating RMQ Retry Checker instance...")
    checker = RMQRetryChecker(config)
    print("  ✓ Checker instance created")
    print()
    
    # Step 4: Simulate RabbitMQ Management API response
    print("Step 4: Simulating RabbitMQ Management API...")
    simulated_queues = [
        'dlq.queue1',
        'dlq.queue2', 
        'dlq.queue3',
        'dlq.payments',
        'dlq.orders',
        'dlq.inventory',
        'dlq.notifications',
        'other.queue',       # Doesn't match: different prefix
        'dead.queue1',       # Doesn't match: has 'dead.' not 'dlq.' prefix
        'my_dlq',            # Doesn't match: no dot separator
    ]
    print(f"  Simulated {len(simulated_queues)} queues in RabbitMQ")
    for q in simulated_queues:
        matches = 'dlq.' in q and q.startswith('dlq.')
        marker = '✓' if matches else ' '
        print(f"    {marker} {q}")
    print()
    
    # Step 5: Test queue matching
    print("Step 5: Testing queue pattern matching...")
    with patch.object(checker, 'list_queues_from_api', return_value=simulated_queues):
        queue_pairs = checker.get_matching_queue_pairs()
    
    print(f"  Found {len(queue_pairs)} matching queue pairs")
    print()
    
    # Step 6: Verify results
    print("Step 6: Verifying results...")
    expected_pairs = [
        ('dlq.queue1', 'dead.queue1'),
        ('dlq.queue2', 'dead.queue2'),
        ('dlq.queue3', 'dead.queue3'),
        ('dlq.payments', 'dead.payments'),
        ('dlq.orders', 'dead.orders'),
        ('dlq.inventory', 'dead.inventory'),
        ('dlq.notifications', 'dead.notifications'),
    ]
    
    actual_sorted = sorted(queue_pairs)
    expected_sorted = sorted(expected_pairs)
    
    if actual_sorted == expected_sorted:
        print("  ✓ All queue pairs are correct!")
        print()
        print("  Queue Mapping:")
        for dlq, target in sorted(queue_pairs):
            queue_name = dlq.replace('dlq.', '')
            print(f"    {dlq:25} -> {target:25} (queue: {queue_name})")
        success = True
    else:
        print("  ✗ Queue pairs do not match expected results!")
        print()
        print("  Expected:")
        for dlq, target in expected_sorted:
            print(f"    {dlq} -> {target}")
        print()
        print("  Actual:")
        for dlq, target in actual_sorted:
            print(f"    {dlq} -> {target}")
        success = False
    
    print()
    
    # Step 7: Verify individual derivations
    print("Step 7: Testing individual queue name derivations...")
    test_cases = [
        ('dlq.queue1', 'dead.queue1'),
        ('dlq.payments', 'dead.payments'),
        ('dlq.my_service_name', 'dead.my_service_name'),
    ]
    
    all_derivations_correct = True
    for dlq_name, expected_target in test_cases:
        derived = checker._derive_target_queue(dlq_name, 'dlq.*', 'dead.*')
        if derived == expected_target:
            print(f"  ✓ {dlq_name} -> {derived}")
        else:
            print(f"  ✗ {dlq_name} -> {derived} (expected: {expected_target})")
            all_derivations_correct = False
    
    success = success and all_derivations_correct
    print()
    
    # Step 8: Test edge cases
    print("Step 8: Testing edge cases...")
    edge_cases = [
        ('dlq.a', 'dead.a', 'Single character queue name'),
        ('dlq.queue_with_underscores', 'dead.queue_with_underscores', 'Queue name with underscores'),
        ('dlq.queue-with-dashes', 'dead.queue-with-dashes', 'Queue name with dashes'),
        ('dlq.UPPERCASE', 'dead.UPPERCASE', 'Uppercase queue name'),
        ('dlq.123', 'dead.123', 'Numeric queue name'),
    ]
    
    all_edge_cases_pass = True
    for dlq_name, expected_target, description in edge_cases:
        derived = checker._derive_target_queue(dlq_name, 'dlq.*', 'dead.*')
        if derived == expected_target:
            print(f"  ✓ {description}: {dlq_name} -> {derived}")
        else:
            print(f"  ✗ {description}: {dlq_name} -> {derived} (expected: {expected_target})")
            all_edge_cases_pass = False
    
    success = success and all_edge_cases_pass
    print()
    
    return success


def test_config_file_loading():
    """Test loading configuration from the example config file"""
    print("=" * 80)
    print("INTEGRATION TEST: Configuration File Loading")
    print("=" * 80)
    print()
    
    print("Testing config_dlq_dead_example.yaml...")
    
    try:
        config = Config()
        config.load_from_file('config_dlq_dead_example.yaml')
        
        print(f"  ✓ File loaded successfully")
        print(f"  ✓ DLQ Pattern: {config.DLQ_NAME}")
        print(f"  ✓ Target Pattern: {config.TARGET_QUEUE}")
        print(f"  ✓ Max Retries: {config.MAX_RETRY_COUNT}")
        
        # Verify the values
        if (config.DLQ_NAME == 'dlq.*' and 
            config.TARGET_QUEUE == 'dead.*' and 
            config.MAX_RETRY_COUNT == 3):
            print("  ✓ All configuration values are correct")
            print()
            return True
        else:
            print("  ✗ Configuration values do not match expected")
            print()
            return False
            
    except Exception as e:
        print(f"  ✗ Failed to load config file: {e}")
        print()
        return False


def main():
    """Run all integration tests"""
    print("\n" + "=" * 80)
    print(" WILDCARD SETUP INTEGRATION TESTS")
    print("=" * 80)
    print()
    print("Testing wildcard setup for the pattern:")
    print("  DLQs:    dlq.queue1, dlq.queue2, dlq.payments, ...")
    print("  Targets: dead.queue1, dead.queue2, dead.payments, ...")
    print("  Pattern: dlq.* -> dead.*")
    print()
    
    results = []
    
    # Test 1: Main integration test
    results.append(("Wildcard Pattern Integration", test_integration_dlq_dead_pattern()))
    
    # Test 2: Config file loading
    results.append(("Configuration File Loading", test_config_file_loading()))
    
    # Print summary
    print("=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)
    print()
    
    all_passed = True
    for test_name, passed in results:
        status = "PASSED" if passed else "FAILED"
        symbol = "✓" if passed else "✗"
        print(f"  {symbol} {test_name}: {status}")
        if not passed:
            all_passed = False
    
    print()
    print("=" * 80)
    
    if all_passed:
        print("✓ ALL INTEGRATION TESTS PASSED")
        print()
        print("CONCLUSION:")
        print("The wildcard setup works correctly for the dlq.* -> dead.* pattern.")
        print("DLQs named dlq.queue1 correctly map to dead.queue1 with the queue")
        print("name remaining the same.")
        print("=" * 80)
        return 0
    else:
        print("✗ SOME INTEGRATION TESTS FAILED")
        print("=" * 80)
        return 1


if __name__ == '__main__':
    sys.exit(main())
