---
Feature Name: "laminar-flow"
Tracking Issue: https://github.com/GreptimeTeam/greptimedb/issues/TBD
Date: 2025-09-08
Author: "discord9 <discord9@163.com>"
---

# laminar Flow

## Summary

This RFC proposes a redesign of the flow architecture where flownode becomes a lightweight in-memory state management node with an embedded frontend for direct computation. This approach optimizes resource utilization and improves scalability by eliminating network hops while maintaining clear separation between coordination and computation tasks.

## Motivation

The current flow architecture has several limitations:

1. **Resource Inefficiency**: Flownodes perform both state management and computation, leading to resource duplication and inefficient utilization.
2. **Scalability Constraints**: Computation resources are tied to flownode instances, limiting horizontal scaling capabilities.
3. **State Management Complexity**: Mixing computation with state management makes the system harder to maintain and debug.
4. **Network Overhead**: Additional network hops between flownode and separate frontend nodes add latency.

The laminar Flow architecture addresses these issues by:
- Consolidating computation within flownode through embedded frontend
- Eliminating network overhead by removing separate frontend node communication
- Simplifying state management by focusing flownode on its core responsibility
- Improving system scalability and maintainability

## Details

### Architecture Overview

The laminar Flow architecture transforms flownode into a lightweight coordinator that maintains flow state with an embedded frontend for computation. The key components involved are:

1. **Flownode**: Maintains in-memory state, coordinates computation, and includes an embedded frontend for query execution
2. **Embedded Frontend**: Executes **incremental** computations within the flownode
3. **Datanode**: Stores final results and source data

```mermaid
graph TB
    subgraph "laminar Flow Architecture"
        subgraph Flownode["Flownode (State Manager + Embedded Frontend)"]
            StateMap["Flow State Map<br/>Map<Timestamp, (Map<Key, Value>, Sequence)>"]
            Coordinator["Computation Coordinator"]
            subgraph EmbeddedFrontend["Embedded Frontend"]
                QueryEngine["Query Engine"]
                AggrState["__aggr_state Executor"]
            end
        end
        
        subgraph Datanode["Datanode"]
            Storage["Data Storage"]
            Results["Result Tables"]
        end
    end
    
    Coordinator -->|Internal Query| EmbeddedFrontend
    EmbeddedFrontend -->|Incremental States| Coordinator
    Flownode -->|Incremental Results| Datanode
    EmbeddedFrontend -.->|Read Data| Datanode
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

2. **Query Execution**: Flownode executes `__aggr_state` queries using its embedded frontend with:
   - Time window filters
   - Sequence range constraints

3. **State Update**: Flownode receives partial state results and updates its internal state:
   - Merges new values with existing aggregation state
   - Updates sequence markers to track progress
   - Identifies changed time windows for result computation

4. **Result Materialization**: Flownode computes final results using `__aggr_merge` operations:
   - Processes only updated time windows(and time series) for efficiency
   - Writes results back to datanode directly through its embedded frontend

### Detailed Workflow

#### Incremental State Query

```sql
-- Example incremental state query executed by embedded frontend
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
    participant F as Flownode (Coordinator)
    participant EF as Embedded Frontend (Lightweight)
    participant DN as Datanode (Heavy Computation)
    
    F->>F: Evaluate trigger conditions
    F->>EF: Execute __aggr_state query with sequence range
    EF->>DN: Send query to datanode (Heavy scan & aggregation)
    DN->>DN: Scan data and compute partial aggregation state (Heavy CPU/I/O)
    DN->>EF: Return aggregated state results
    EF->>F: Forward state results (Lightweight merge)
    F->>F: Merge with existing state
    F->>F: Update sequence markers (Lightweight)
    F->>EF: Compute incremental results with __aggr_merge
    EF->>DN: Write incremental results to datanode
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
    timestamps: Vec<Timestamp>,
    // Removed: actual data payload
}
```

This optimization:
- Eliminates network overhead by using embedded frontend
- Enables flownode to track pending time windows efficiently
- Allows flownode to decide processing mode (stream vs batch) based on timestamp age

Another optimization could be just send dirty time windows range for each flow to flownode directly, no need to send timestamps one by one.

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
- **Embedded Frontend**: O(query_batch_size) for temporary computation
- **Overall**: Significantly reduced compared to current architecture

#### Computation Distribution

- **Direct Processing**: Queries processed directly within flownode's embedded frontend
- **Fault Tolerance**: Simplified error handling with fewer distributed components
- **Scalability**: Computation capacity scales with flownode instances

#### Network Optimization

- **Reduced Payload**: Mirror writes only contain timestamps
- **Efficient Queries**: Sequence constraints minimize data transfer
- **Result Caching**: State results cached in flownode memory

### Sequential Read Implementation for Incremental Queries

#### Sequence Management

Flow maintains two critical sequences to track incremental query progress for each region:

- **`memtable_last_seq`**: Tracks the latest sequence number read from the memtable
- **`sst_last_seq`**: Tracks the latest sequence number read from SST files

These sequences enable precise incremental data processing by defining the exact range of data to query in subsequent iterations.

#### Query Protocol

When executing incremental queries, flownode provides both sequence parameters to datanode:

```rust
struct GrpcHeader {
    ...
    // Sequence tracking for incremental reads
    memtable_last_seq: HashMap<RegionId, SequenceNumber>,
    sst_last_seqs: HashMap<RegionId, SequenceNumber>,
}
```

The datanode processes these parameters to return only the data within the specified sequence ranges, ensuring efficient incremental processing.

#### Sequence Invalidation and Refill Mechanism

A critical challenge occurs when data referenced by `memtable_last_seq` gets flushed from memory to disk. Since SST files only maintain a single maximum sequence number for the entire file (rather than per-record sequence tracking), precise incremental queries become impossible for the affected time ranges.

**Detection of Invalidation:**
```rust
// When memtable_last_seq data has been flushed to SST
if memtable_last_seq_flushed_to_disk {
    // Incremental query is no longer feasible
    // Need to trigger refill for affected time ranges
}
```

**Refill Process:**
1. **Identify Affected Time Range**: Query the time range corresponding to the flushed `memtable_last_seq` data
2. **Full Recomputation**: Execute a complete aggregation query for the affected time windows
3. **State Replacement**: Replace the existing flow state for these time ranges with newly computed values
4. **Sequence Update**: Update `memtable_last_seq` to the current latest sequence, while `sst_last_seq` continues normal incremental updates

```sql
-- Refill query when memtable data has been flushed
SELECT
    __aggr_state(aggregation_functions) as state,
    time_window,
    group_keys
FROM source_table
WHERE
    timestamp >= :affected_time_start
    AND timestamp < :affected_time_end
    -- Full scan required since sequence precision is lost in SST
GROUP BY time_window, group_keys;
```

#### Datanode Implementation Requirements

Datanode must implement enhanced query processing capabilities to support sequence-based incremental reads:

**Input Processing:**
- Accept `memtable_last_seq` and `sst_last_seq` parameters in query requests
- Filter data based on sequence ranges across both memtable and SST storage layers

**Output Enhancement:**
```rust
struct OutputMeta {
    pub plan: Option<Arc<dyn ExecutionPlan>>,
    pub cost: OutputCost,
    pub sequence_info: HashMap<RegionId, SequenceInfo>, // New field for sequence tracking per regions involved in the query
}

struct SequenceInfo {
    // Sequence tracking for next iteration
    max_memtable_seq: SequenceNumber,  // Highest sequence from memtable in this result
    max_sst_seq: SequenceNumber,       // Highest sequence from SST in this result
}
```

**Sequence Tracking Logic:**
datanode already impl `max_sst_seq` in leader range read, can reuse similar logic for `max_memtable_seq`.

#### Sequence Update Strategy

**Normal Incremental Updates:**
- Update both `memtable_last_seq` and `sst_last_seq` after successful query execution
- Use returned `max_memtable_seq` and `max_sst_seq` values for next iteration

**Refill Scenario:**
- Reset `memtable_last_seq` to current maximum after refill completion
- Continue normal `sst_last_seq` updates based on successful query responses
- Maintain separate tracking to detect future flush events

#### Performance Considerations

**Sequence Range Optimization:**
- Minimize sequence range spans to reduce scan overhead
- Batch multiple small incremental updates when beneficial
- Balance between query frequency and processing efficiency

**Memory Management:**
- Monitor memtable flush frequency to predict refill requirements
- Implement adaptive query scheduling based on flush patterns
- Optimize state storage to handle frequent updates efficiently

This sequential read implementation ensures reliable incremental processing while gracefully handling the complexities of storage architecture, maintaining both correctness and performance in the face of background compaction and flush operations.

## Implementation Plan

### Phase 1: Core Infrastructure

1. **State Management**: Implement in-memory state map in flownode
2. **Query Interface**: Integrate `__aggr_state` query interface in embedded frontend(Already done in previous query pushdown optimizer work)
3. **Basic Coordination**: Implement query dispatch and result collection
4. **Sequence Tracking**: Implement sequence-based incremental processing(Can use similar interface which leader range read use)

After phase 1, the system should support basic flow operations with incremental updates.

### Phase 2: Optimization Features

1. **Refill Logic**: Develop state recovery mechanisms
2. **Mirror Write Optimization**: Simplify mirror write protocol

### Phase 3: Advanced Features

1. **Load Balancing**: Implement intelligent resource allocation for partitioned flow(Flow distributed executed on multiple flownodes)
2. **Fault Tolerance**: Add retry mechanisms and error handling
3. **Performance Tuning**: Optimize query batching and state management

## Drawbacks

### Reduced Network Communication

- **Eliminated Hops**: Direct communication between flownode and datanode through embedded frontend
- **Reduced Latency**: No separate frontend node communication overhead
- **Simplified Network Topology**: Fewer network dependencies and failure points

### Complexity in Error Handling

- **Distributed Failures**: Need to handle failures across multiple components
- **State Consistency**: Ensuring state consistency during partial failures
- **Recovery Complexity**: More complex recovery procedures

### Datanode Resource Requirements

- **Computation Load**: Datanode handles the heavy computational workload for flow queries
- **Query Interference**: Flow queries may impact regular query performance on datanode
- **Resource Contention**: Need careful resource management and isolation on datanode

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

- **Embedded Frontend Optimization**: Optimize embedded frontend query planning and execution
- **Datanode Optimization**: Optimize result writing from flownode
- **Metasrv Coordination**: Enhanced metadata management and coordination

## Conclusion

The laminar Flow architecture represents a significant improvement over the current flow system by separating state management from computation execution. This design enables better resource utilization, improved scalability, and simplified maintenance while maintaining the core functionality of continuous aggregation.

The key benefits include:

1. **Improved Scalability**: Computation can scale independently of state management
2. **Better Resource Utilization**: Eliminates network overhead and leverages embedded frontend infrastructure
3. **Simplified Architecture**: Clear separation of concerns between components
4. **Enhanced Performance**: Sequence-based incremental processing reduces computational overhead

While the architecture introduces some complexity in terms of distributed coordination and error handling, the benefits significantly outweigh the drawbacks, making it a compelling evolution of the flow system.