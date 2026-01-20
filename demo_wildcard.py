#!/usr/bin/env python3
"""
Demonstration script to show wildcard functionality for dlq.* -> dead.* pattern.

This script demonstrates that the wildcard setup correctly handles:
- DLQs named: dlq.queue1, dlq.queue2, dlq.payments, etc.
- Target queues: dead.queue1, dead.queue2, dead.payments, etc.
- The queue name always remains the same between DLQ and target

This addresses the requirement: "Check if wildcard setup works, if the dlqs 
are named dlq.queue1 and their corresponding target is dead.queue1. The 
queuename always remains the same."
"""
import sys
from unittest.mock import patch, MagicMock

# Import the RMQ Retry Checker
try:
    from rmq_retry_checker import Config, RMQRetryChecker
except ImportError:
    print("ERROR: Could not import rmq_retry_checker module")
    sys.exit(1)


def demonstrate_wildcard_setup():
    """Demonstrate the wildcard setup for dlq.* -> dead.* pattern"""
    
    print("=" * 70)
    print("RabbitMQ Retry Checker - Wildcard Setup Demonstration")
    print("=" * 70)
    print()
    
    # Create configuration with the specific wildcard pattern
    config = Config()
    config.DLQ_NAME = 'dlq.*'
    config.TARGET_QUEUE = 'dead.*'
    config.MAX_RETRY_COUNT = 3
    
    print("Configuration:")
    print(f"  DLQ Pattern:    {config.DLQ_NAME}")
    print(f"  Target Pattern: {config.TARGET_QUEUE}")
    print(f"  Max Retries:    {config.MAX_RETRY_COUNT}")
    print()
    
    # Create checker instance
    checker = RMQRetryChecker(config)
    
    # Simulate RabbitMQ Management API returning a list of queues
    simulated_queues = [
        'dlq.queue1',
        'dlq.queue2',
        'dlq.payments',
        'dlq.orders',
        'dlq.inventory',
        'other_queue',  # This won't match the pattern
        'dead.queue1',  # This won't match the dlq.* pattern
    ]
    
    print("Simulated RabbitMQ Queues:")
    for q in simulated_queues:
        print(f"  - {q}")
    print()
    
    # Mock the API call to return our simulated queues
    with patch.object(checker, 'list_queues_from_api', return_value=simulated_queues):
        # Get matching queue pairs
        queue_pairs = checker.get_matching_queue_pairs()
    
    print("Matched Queue Pairs (DLQ -> Target):")
    print("-" * 70)
    
    if not queue_pairs:
        print("  No matching queue pairs found!")
    else:
        for dlq_name, target_queue in sorted(queue_pairs):
            # Extract the queue name portion
            queue_name = dlq_name.replace('dlq.', '')
            print(f"  {dlq_name:20} -> {target_queue:20} (queue: {queue_name})")
    
    print()
    print("=" * 70)
    print("Verification:")
    print("=" * 70)
    
    # Verify the results
    expected_pairs = [
        ('dlq.queue1', 'dead.queue1'),
        ('dlq.queue2', 'dead.queue2'),
        ('dlq.payments', 'dead.payments'),
        ('dlq.orders', 'dead.orders'),
        ('dlq.inventory', 'dead.inventory'),
    ]
    
    success = sorted(queue_pairs) == sorted(expected_pairs)
    
    if success:
        print("✓ SUCCESS: Wildcard setup works correctly!")
        print()
        print("Key Observations:")
        print("  1. Pattern 'dlq.*' correctly matches all queues starting with 'dlq.'")
        print("  2. The wildcard portion (*) captures the queue name (e.g., 'queue1')")
        print("  3. The captured portion is substituted into 'dead.*' pattern")
        print("  4. Result: dlq.queue1 -> dead.queue1 (queue name remains the same)")
        print("  5. Non-matching queues are correctly ignored")
    else:
        print("✗ FAILURE: Wildcard setup did not work as expected!")
        print()
        print("Expected pairs:")
        for dlq, target in sorted(expected_pairs):
            print(f"  {dlq} -> {target}")
        print()
        print("Actual pairs:")
        for dlq, target in sorted(queue_pairs):
            print(f"  {dlq} -> {target}")
    
    print()
    print("=" * 70)
    
    return success


def demonstrate_derive_function():
    """Demonstrate the _derive_target_queue function directly"""
    
    print("\n" + "=" * 70)
    print("Direct Function Test: _derive_target_queue()")
    print("=" * 70)
    print()
    
    config = Config()
    checker = RMQRetryChecker(config)
    
    test_cases = [
        ('dlq.queue1', 'dlq.*', 'dead.*', 'dead.queue1'),
        ('dlq.queue2', 'dlq.*', 'dead.*', 'dead.queue2'),
        ('dlq.payments', 'dlq.*', 'dead.*', 'dead.payments'),
        ('dlq.orders', 'dlq.*', 'dead.*', 'dead.orders'),
        ('dlq.inventory', 'dlq.*', 'dead.*', 'dead.inventory'),
    ]
    
    print("Test Cases:")
    print(f"{'DLQ Name':<20} {'DLQ Pattern':<15} {'Target Pattern':<15} {'Expected':<20} {'Result':<20} {'Status'}")
    print("-" * 110)
    
    all_passed = True
    for dlq_name, dlq_pattern, target_pattern, expected in test_cases:
        result = checker._derive_target_queue(dlq_name, dlq_pattern, target_pattern)
        status = '✓ PASS' if result == expected else '✗ FAIL'
        if result != expected:
            all_passed = False
        print(f"{dlq_name:<20} {dlq_pattern:<15} {target_pattern:<15} {expected:<20} {result:<20} {status}")
    
    print()
    if all_passed:
        print("✓ All test cases passed!")
    else:
        print("✗ Some test cases failed!")
    
    print("=" * 70)
    
    return all_passed


if __name__ == '__main__':
    print("\n")
    
    # Run demonstrations
    result1 = demonstrate_wildcard_setup()
    result2 = demonstrate_derive_function()
    
    print("\n" + "=" * 70)
    print("FINAL RESULT")
    print("=" * 70)
    
    if result1 and result2:
        print("✓ Wildcard setup verification: SUCCESS")
        print()
        print("The wildcard functionality correctly handles the pattern where:")
        print("  - DLQs are named: dlq.queue1, dlq.queue2, etc.")
        print("  - Targets are named: dead.queue1, dead.queue2, etc.")
        print("  - The queue name always remains the same")
        sys.exit(0)
    else:
        print("✗ Wildcard setup verification: FAILED")
        sys.exit(1)
