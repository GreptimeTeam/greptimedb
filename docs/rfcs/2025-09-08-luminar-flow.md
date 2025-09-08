---
Feature Name: "luminar-flow"
Tracking Issue: https://github.com/GreptimeTeam/greptimedb/issues/TBD
Date: 2025-09-08
Author: "discord9 <discord9@163.com>"
---

# Luminar Flow

## Summary

This RFC proposes a redesign of the flow architecture where flownode becomes a lightweight in-memory state management and forwarding node, while delegating actual computation to frontend nodes. This approach optimizes resource utilization and improves scalability by separating state management from computation execution.

## Motivation

The current flow architecture has several limitations:

1. **Resource Inefficiency**: Flownodes perform both state management and computation, leading to resource duplication and inefficient utilization.
2. **Scalability Constraints**: Computation resources are tied to flownode instances, limiting horizontal scaling capabilities.
3. **State Management Complexity**: Mixing computation with state management makes the system harder to maintain and debug.
4. **Limited Computation Distribution**: Computation cannot be easily distributed across available frontend nodes.

The Luminar Flow architecture addresses these issues by:
- Separating concerns between state management (flownode) and computation (frontend)
- Enabling better resource utilization through computation distribution
- Simplifying state management by focusing flownode on its core responsibility
- Improving system scalability and maintainability

## Details

### Architecture Overview

The Luminar Flow architecture transforms flownode into a lightweight coordinator that maintains flow state while delegating computation to frontend nodes. The key components involved are:

1. **Flownode**: Maintains in-memory state and coordinates computation
2. **Frontend**: Executes **incremental** computations on demand
3. **Datanode**: Stores final results and source data

```mermaid
graph TB
    subgraph "Luminar Flow Architecture"
        subgraph Flownode["Flownode (State Manager)"]
            StateMap["Flow State Map<br/>Map<Timestamp, (Map<Key, Value>, Sequence)>"]
            Coordinator["Computation Coordinator"]
        end
        
        subgraph Frontend1["Frontend Node 1"]
            QueryEngine1["Query Engine"]
            AggrState1["__aggr_state Executor"]
        end
        
        subgraph Frontend2["Frontend Node 2"]
            QueryEngine2["Query Engine"]
            AggrState2["__aggr_state Executor"]
        end
        
        subgraph Datanode["Datanode"]
            Storage["Data Storage"]
            Results["Result Tables"]
        end
    end
    
    Flownode -->|Query Request| Frontend1
    Flownode -->|Query Request| Frontend2
    Frontend1 -->|State Results| Flownode
    Frontend2 -->|State Results| Flownode
    Flownode -->|Final Results| Datanode
    Frontend1 -.->|Read Data| Datanode
    Frontend2 -.->|Read Data| Datanode
```

### Core Components

#### 1. Flow State Management

Flownode maintains a state map for each flow:

```rust
type FlowState = Map<Timestamp, (Map<Key, Value>, Sequence)>;
```

Where:
- **Timestamp**: Time window identifier for aggregation groups
- **Key**: Aggregation group expressions (`group_exprs`)
- **Value**: Aggregation expressions results (`aggr_exprs`)
- **Sequence**: Computation progress marker for incremental updates

#### 2. Incremental Computation Process

The computation process follows these steps:

1. **Trigger Evaluation**: Flownode determines when to trigger computation based on:
   - Time intervals (periodic updates)
   - Data volume thresholds
   - Sequence progress requirements

2. **Query Dispatch**: Flownode sends `__aggr_state` queries to randomly selected frontend nodes with:
   - Time window filters
   - Sequence range constraints

3. **State Update**: Flownode receives partial state results and updates its internal state:
   - Merges new values with existing aggregation state
   - Updates sequence markers to track progress
   - Identifies changed time windows for result computation

4. **Result Materialization**: Flownode computes final results using `__aggr_merge` operations:
   - Processes only updated time windows(and time series) for efficiency
   - Writes results back to datanode through frontend or embedded frontend

### Detailed Workflow

#### Incremental State Query

```sql
-- Example incremental state query sent to frontend
SELECT 
    __aggr_state(avg(value)) as state,
    time_window,
    group_key
FROM source_table 
WHERE 
    timestamp >= :window_start 
    AND timestamp < :window_end
    AND __sequence >= :last_sequence 
    AND __sequence < :current_sequence 
    -- sequence range is actually written in grpc header, but shown here for clarity
GROUP BY time_window, group_key;
```

#### State Merge Process

```mermaid
sequenceDiagram
    participant F as Flownode
    participant FE as Frontend
    participant DN as Datanode
    
    F->>F: Evaluate trigger conditions
    F->>FE: Send __aggr_state query with sequence range
    FE->>DN: Read incremental data
    FE->>FE: Compute partial aggregation state
    FE->>F: Return state results
    F->>F: Merge with existing state
    F->>F: Update sequence markers
    F->>F: Compute final results with __aggr_merge
    F->>DN: Write final results
```

### Refill Implementation and State Management

#### Refill Process

Refill is implemented as a straightforward `__aggr_state` query with time and sequence constraints:

```sql
-- Refill query for flow state recovery
SELECT 
    __aggr_state(aggregation_functions) as state,
    time_window,
    group_keys
FROM source_table 
WHERE 
    timestamp >= :refill_start_time 
    AND timestamp < :refill_end_time
    AND __sequence >= :start_sequence 
    AND __sequence < :end_sequence
    -- sequence range is actually written in grpc header, but shown here for clarity
GROUP BY time_window, group_keys;
```

#### State Recovery Strategy

1. **Recent Data (Stream Mode)**: For recent time windows, flownode refills state using incremental queries
2. **Historical Data (Batch Mode)**: For older time windows, flownode triggers batch computation directly and no need to refill state
3. **Hybrid Approach**: Combines stream and batch processing based on data age and availability

#### Mirror Write Optimization

Mirror writes are simplified to only transmit timestamps to flownode:

```rust
struct MirrorWrite {
    timestamp: Timestamp,
    // Removed: actual data payload
}
```

This optimization:
- Reduces network overhead between frontend and flownode
- Enables flownode to track pending time windows efficiently
- Allows flownode to decide processing mode (stream vs batch) based on timestamp age

### Query Optimization Strategies

#### Sequence-Based Incremental Processing

The core optimization relies on sequence-constrained queries:

```sql
-- Optimized incremental query
SELECT __aggr_state(expr) 
FROM table 
WHERE time_range AND sequence_range
```

Benefits:
- **Reduced Scan Volume**: Only processes data since last computation
- **Efficient Resource Usage**: Minimizes CPU and I/O overhead
- **Predictable Performance**: Query cost scales with incremental data size

#### Time Window Partitioning

```mermaid
graph LR
    subgraph "Time Windows"
        W1["Window 1<br/>09:00-09:05"]
        W2["Window 2<br/>09:05-09:10"]
        W3["Window 3<br/>09:10-09:15"]
    end
    
    subgraph "Processing Strategy"
        W1 --> Batch["Batch Mode<br/>(Old Data)"]
        W2 --> Stream["Stream Mode<br/>(Recent Data)"]
        W3 --> Stream2["Stream Mode<br/>(Current Data)"]
    end
```

### Performance Characteristics

#### Memory Usage

- **Flownode**: O(active_time_windows × group_cardinality) for state storage
- **Frontend**: O(query_batch_size) for temporary computation
- **Overall**: Significantly reduced compared to current architecture

#### Computation Distribution

- **Load Balancing**: Queries distributed across available frontend nodes
- **Fault Tolerance**: Failed queries can be retried on different frontend nodes
- **Scalability**: Computation capacity scales with frontend node count

#### Network Optimization

- **Reduced Payload**: Mirror writes only contain timestamps
- **Efficient Queries**: Sequence constraints minimize data transfer
- **Result Caching**: State results cached in flownode memory

## Implementation Plan

### Phase 1: Core Infrastructure

1. **State Management**: Implement in-memory state map in flownode
2. **Query Interface**: Develop `__aggr_state` query interface in frontend(Already done in previous query pushdown optimizer work)
3. **Basic Coordination**: Implement query dispatch and result collection
4. **Sequence Tracking**: Implement sequence-based incremental processing(Can use similar interface which leader range read use)

After phase 1, the system should support basic flow operations with incremental updates.

### Phase 2: Optimization Features

1. **Refill Logic**: Develop state recovery mechanisms
2. **Mirror Write Optimization**: Simplify mirror write protocol

### Phase 3: Advanced Features

1. **Load Balancing**: Implement intelligent frontend selection
2. **Fault Tolerance**: Add retry mechanisms and error handling
3. **Performance Tuning**: Optimize query batching and state management

## Drawbacks

### Increased Network Communication

- **Additional Hops**: Computation requires flownode ↔ frontend communication
- **Latency Impact**: Query dispatch and result collection add latency
- **Network Dependency**: System becomes more sensitive to network issues

### Complexity in Error Handling

- **Distributed Failures**: Need to handle failures across multiple components
- **State Consistency**: Ensuring state consistency during partial failures
- **Recovery Complexity**: More complex recovery procedures

### Frontend Resource Requirements

- **Computation Load**: Frontend nodes need sufficient resources for flow queries
- **Query Interference**: Flow queries may impact regular query performance
- **Resource Contention**: Need careful resource management and isolation

## Alternatives

### Alternative 1: Enhanced Current Architecture

Keep computation in flownode but optimize through:
- Better resource management
- Improved query optimization
- Enhanced state persistence

**Pros:**
- Simpler architecture
- Fewer network hops
- Easier debugging

**Cons:**
- Limited scalability
- Resource inefficiency
- Harder to optimize computation distribution

### Alternative 2: Embedded Computation

Embed lightweight computation engines within flownode:

**Pros:**
- Reduced network communication
- Better performance for simple queries
- Simpler deployment

**Cons:**
- Limited scalability
- Resource constraints
- Harder to leverage existing frontend optimizations

## Future Work

### Advanced Query Optimization

- **Parallel Processing**: Enable parallel execution of flow queries
- **Query Caching**: Cache frequently executed query patterns

### Enhanced State Management

- **State Compression**: Implement efficient state serialization
- **Distributed State**: Support state distribution across multiple flownodes
- **State Persistence**: Add optional state persistence for durability

### Monitoring and Observability

- **Performance Metrics**: Track query execution times and resource usage
- **State Visualization**: Provide tools for state inspection and debugging
- **Health Monitoring**: Monitor system health and performance characteristics

### Integration Improvements

- **Frontend Integration**: Deeper integration with frontend query planning
- **Datanode Optimization**: Optimize result writing from flownode
- **Metasrv Coordination**: Enhanced metadata management and coordination

## Conclusion

The Luminar Flow architecture represents a significant improvement over the current flow system by separating state management from computation execution. This design enables better resource utilization, improved scalability, and simplified maintenance while maintaining the core functionality of continuous aggregation.

The key benefits include:

1. **Improved Scalability**: Computation can scale independently of state management
2. **Better Resource Utilization**: Leverages existing frontend infrastructure
3. **Simplified Architecture**: Clear separation of concerns between components
4. **Enhanced Performance**: Sequence-based incremental processing reduces computational overhead

While the architecture introduces some complexity in terms of distributed coordination and error handling, the benefits significantly outweigh the drawbacks, making it a compelling evolution of the flow system.