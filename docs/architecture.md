# Architecture

This document explains how the RabbitMQ Retry Checker works.

## Overview

The tool monitors Dead Letter Queues (DLQs) for messages that have exceeded a retry threshold and moves them to a permanent failure queue.

## Message Flow

```mermaid
flowchart TD
    A[Message Published] --> B[Main Queue]
    B -->|Processing Fails| C[Dead Letter Queue]
    C --> D{Retry Checker}
    D -->|x-death count ≤ max| E[Leave in DLQ]
    D -->|x-death count > max| F[permanent_failure_queue]
    F --> G[Manual Review]
```

> **Note:** Messages within the retry limit are left in the DLQ (via nack with requeue). The retry mechanism depends on your RabbitMQ setup—typically the DLQ is configured to dead-letter back to the main queue after a TTL, allowing the consumer to retry processing.

## Processing Logic

```mermaid
flowchart LR
    subgraph Initialization
        A1[Load Config] --> A2[Connect to RabbitMQ]
        A2 --> A3[Resolve Queue Patterns]
    end
    
    subgraph Processing
        B1[Get Message from DLQ] --> B2{Message Exists?}
        B2 -->|No| B4[Done]
        B2 -->|Yes| B3[Check x-death Count]
        B3 --> B5{Exceeds Limit?}
        B5 -->|Yes| B6[Move to Failure Queue]
        B5 -->|No| B7[Nack & Requeue]
        B6 --> B1
        B7 --> B1
    end
    
    A3 --> B1
```

## Wildcard Resolution

When using wildcard patterns like `dlq.*`:

```mermaid
flowchart TD
    A[Pattern: dlq.*] --> B[Query Management API]
    B --> C[Get All Queues]
    C --> D{Match Pattern}
    D --> E[dlq.orders]
    D --> F[dlq.users]
    D --> G[dlq.payments]
    E --> H[Target: dead.orders]
    F --> I[Target: dead.users]
    G --> J[Target: dead.payments]
```

## Components

| Component | Description |
|-----------|-------------|
| `Config` | Handles configuration from files, environment variables, and CLI |
| `RMQRetryChecker` | Main class that connects to RabbitMQ and processes messages |
| Management API | Used only when wildcard patterns are specified |

## Message Persistence

The tool uses RabbitMQ's publisher confirms to ensure no messages are lost during processing:

1. **Publisher Confirms**: The channel is set to confirm delivery mode. Each `basic_publish` blocks until the broker acknowledges receipt.
2. **Persistent Messages**: Messages moved to the failure queue are published with `delivery_mode=2` (persistent), ensuring they survive broker restarts.
3. **Mandatory Flag**: The `mandatory=True` flag ensures messages are routed to a queue; if not, an error is raised.
4. **Ack After Confirm**: The original message is only acknowledged from the DLQ after the broker confirms the publish succeeded.

```mermaid
flowchart TD
    A[Get Message from DLQ] --> B[Publish to Failure Queue]
    B --> C{Broker Confirms?}
    C -->|Yes| D[Ack Original Message]
    C -->|No/Error| E[Nack & Requeue]
    D --> F[Message Safely Moved]
    E --> G[Message Stays in DLQ]
```

**Failure Scenarios:**

| Scenario | Outcome |
|----------|---------|
| Script dies before publish | Message remains in DLQ (no loss) |
| Script dies after publish, before ack | Message may be duplicated (acceptable) |
| Broker rejects publish | Message remains in DLQ, error logged |
| Network failure during publish | Message remains in DLQ (no loss) |
