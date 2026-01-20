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
    D -->|x-death count ≤ max| E[Requeue to DLQ]
    D -->|x-death count > max| F[Permanent Failure Queue]
    E -->|Retry| C
    F --> G[Manual Review]
```

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
