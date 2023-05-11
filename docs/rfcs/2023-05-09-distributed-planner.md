---
Feature Name: distributed-planner
Tracking Issue: TBD
Date: 2023-05-09
Author: "Ruihang Xia <waynestxia@gmail.com>"
---

Distributed Planner
-------------------
# Summary
Enhance the logical planner with aware of distributed, multi-region table topology. To achieve "push computation down" execution rather than the current "pull data up" manner.

# Motivation
Query distributively can leverage the advantage of GreptimeDB's architecture to process large dataset that exceeds the capacity of a single node, or accelerate the query execution by executing it in parallel. This task includes two sub-tasks
  - Be able to transform the plan that can push as much as possible computation down to data source.
  - Be able to handle pipeline breaker (like `Join` or `Sort`) on multiple computation nodes.
This is a relatively complex topic. To keep this RFC concentrated I'll focus on the first one.

# Details
## Background: Partition and Region
GreptimeDB supports table partitioning, where the partition rule is set during table creation. Each partition can be further divided into one or more physical storage units known as "regions". Both partitions and regions are divided based on rows:
``` text
┌────────────────────────────────────┐
│                                    │
│               Table                │
│                                    │
└─────┬────────────┬────────────┬────┘
      │            │            │
      │            │            │
┌─────▼────┐ ┌─────▼────┐ ┌─────▼────┐
│ Region 1 │ │ Region 2 │ │ Region 3 │
└──────────┘ └──────────┘ └──────────┘
  Row 1~10     Row 11~20    Row 21~30
```
General speaking, region is the minimum element of data distribution, and we can also use it as the unit to distribute computation. This can greatly simplify the routing logic of this distributed planner, by always schedule the computation to the node that currently opening the corresponding region. And is also easy to scale more node for computing since GreptimeDB's data is persisted on shared storage backend like S3. But this is a bit beyond the scope of this specific topic.
## Background: Commutativity
Commutativity is an attribute that describes whether two operation can exchange their apply order: $P1(P2(R)) \Leftrightarrow P2(P1(R))$. If the equation keeps, we can transform one expression into another form without changing its result. This is useful on rewriting SQL expression, and is the theoretical basis of this RFC.

Take this SQL as an example

``` sql
SELECT a FROM t WHERE a > 10;
```

As we know projection and filter are commutative (todo: latex), it can be translated to the following two identical plan trees:

```text
┌─────────────┐       ┌─────────────┐
│Projection(a)│       │Filter(a>10) │
└──────▲──────┘       └──────▲──────┘
       │                     │
┌──────┴──────┐       ┌──────┴──────┐
│Filter(a>10) │       │Projection(a)│
└──────▲──────┘       └──────▲──────┘
       │                     │
┌──────┴──────┐       ┌──────┴──────┐
│  TableScan  │       │  TableScan  │
└─────────────┘       └─────────────┘
```

## Merge Operation

This RFC proposes to add a new expression node `MergeScan` to merge result from several regions in the frontend. It wrap the abstraction of remote data and execution, and expose a `TableScan` interface to upper level.

``` text
        ▲
        │
┌───────┼───────┐
│       │       │
│    ┌──┴──┐    │
│    └──▲──┘    │
│       │       │
│    ┌──┴──┐    │
│    └──▲──┘    │    ┌─────────────────────────────┐
│       │       │    │                             │
│  ┌────┴────┐  │    │ ┌──────────┐ ┌───┐    ┌───┐ │
│  │MergeScan◄──┼────┤ │ Region 1 │ │   │ .. │   │ │
│  └─────────┘  │    │ └──────────┘ └───┘    └───┘ │
│               │    │                             │
└─Frontend──────┘    └─Remote-Sources──────────────┘
```
This merge operation simply chains all the the underlying remote data sources and return `RecordBatch`, just like a coalesce op. And each remote sources is a gRPC query to datanode via the substrait logical plan interface. The plan is transformed and divided from the original query that comes to frontend.

## Commutativity of MergeScan

Obviously, The position of `MergeScan` is the key of the distributed plan. The more closer to the underlying `TableScan`, the less computation is taken by datanodes. Thus the goal is to pull the `MergeScan` up as more as possible. The word "pull up" means exchange `MergeScan` with its parent node in the plan tree, which means we should check the commutativity between the existing expression nodes and the `MergeScan`. Here I classify all the possibility into five categories:

- Commutative: $P1(P2(R)) \Leftrightarrow P2(P1(R))$
  - filter
  - projection
  - operations that match the partition key
- Partial Commutative: $P1(P2(R)) \Leftrightarrow P1(P2(P1(R)))$
  - $min(R) \rightarrow min(MERGE(min(R)))$
  - $max(R) \rightarrow max(MERGE(max(R)))$
- Conditional Commutative: $P1(P2(R)) \Leftrightarrow P3(P2(P1(R)))$
  - $count(R) \rightarrow sum(count(R))$
- Transformed Commutative: $P1(P2(R)) \Leftrightarrow P1(P3(R)) \Leftrightarrow P3(P1(R))$
  - $avg(R) \rightarrow sum(R)/count(R)$
- Non-commutative
  - sort
  - join
  - percentile
## Steps to plan
After establishing the set of commutative relations for all expressions, we can begin transforming the logical plan. There are four steps:

  - Add a merge node before table scan
  - Evaluate commutativity in a bottom-up way, stop at the first non-commutative node
  - Divide the TableScan to scan over partitions
  - Execute

First insert the `MergeScan` on top of the bottom `TableScan` node. Then examine the commutativity start from the `MergeScan` node transform the plan tree based on the result. Stop this process on the first non-commutative node.
``` text
                  ┌─────────────┐   ┌─────────────┐
                  │    Sort     │   │    Sort     │
                  └──────▲──────┘   └──────▲──────┘
                         │                 │
┌─────────────┐   ┌──────┴──────┐   ┌──────┴──────┐
│    Sort     │   │Projection(a)│   │  MergeScan  │
└──────▲──────┘   └──────▲──────┘   └──────▲──────┘
       │                 │                 │
┌──────┴──────┐   ┌──────┴──────┐   ┌──────┴──────┐
│Projection(a)│   │  MergeScan  │   │Projection(a)│
└──────▲──────┘   └──────▲──────┘   └──────▲──────┘
       │                 │                 │
┌──────┴──────┐   ┌──────┴──────┐   ┌──────┴──────┐
│  TableScan  │   │  TableScan  │   │  TableScan  │
└─────────────┘   └─────────────┘   └─────────────┘
      (a)               (b)               (c)
```
Then in the physical planning phase, convert the sub-tree below `MergeScan` into a remote query request and dispatch to all the regions. And let the `MergeScan` to receive the results and feed to it parent node.

To simplify the overall complexity, any error in the procedure will lead to a failure to the entire query, and cancel all other parts.
# Alternatives
## Spill
If only consider the ability of processing large dataset, we can enable DataFusion's spill ability to temporary persist intermediate data into disk, like the "swap" memory. But this will lead to a super slow performance and very large write amplification.
# Future Work
As described in the `Motivation` section we can further explore the distributed planner on the physical execution level, by introducing mechanism like Spark's shuffle to improve parallelism and reduce intermediate pipeline breaker's stage.