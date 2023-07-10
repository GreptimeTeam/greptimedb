---
Feature Name: metric-engine
Tracking Issue: TBD
Date: 2023-07-10
Author: "Ruihang Xia <waynestxia@gmail.com>"
---

# Summary

A new metric engine that can significantly enhance our ability to handle the tremendous number of small tables in scenarios like Prometheus metrics, by leveraging a synthetic wide table that offers storage and metadata multiplexing capabilities over the existing engine.

# Motivation

The concept "Table" in GreptimeDB is a bit "heavy" compared to other time-series storage like Prometheus or VictoriaMetrics. This has lots of disadvantages in aspects from performance, footprint, and storage to cost.

# Details

## Top level description

- User Interface

    This feature will add a new type of storage engine. It might be available to be an option like `with ENGINE=mito` or an internal interface like auto create table on Prometheus remote write. From the user side, there is no difference from tables in mito engine. All the DDL like `CREATE`, `ALTER` and DML like `SELECT` should be supported.

- Implementation Overlook

    This new engine doesn't re-implement low level components like file R/W etc. It's a wrapper layer over the existing mito engine, with extra storage and metadata multiplexing capabilities. I.e., it expose multiple table based on one mito engine table like this:
	``` plaintext
	   ┌───────────────┐ ┌───────────────┐ ┌───────────────┐
	   │ Metric Engine │ │ Metric Engine │ │ Metric Engine │
	   │   Table 1     │ │   Table 2     │ │   Table 3     │
	   └───────────────┘ └───────────────┘ └───────────────┘
	           ▲               ▲                   ▲
	           │               │                   │
	           └───────────────┼───────────────────┘
	                           │
	                 ┌─────────┴────────┐
	                 │ Metric Region    │
	                 │   Engine         │
	                 │    ┌─────────────┤
	                 │    │ Mito Region │
	                 │    │   Engine    │
	                 └────▲─────────────┘
	                      │
	                      │
	                ┌─────┴───────────────┐
	                │                     │
	                │  Mito Engine Table  │
	                │                     │
	                └─────────────────────┘
	```

The following parts will describe these implementation details:
    - How to route these metric region tables and how those table are distributed
    - How to maintain the schema and other metadata of the underlying mito engine table
    - How to maintain the schema of metric engine table
    - How the query goes

## Routing

Before this change, the region route rule was based on a group of partition keys. Relation of physical table to region is one-to-many.

``` rust
  pub struct PartitionDef {
      partition_columns: Vec<String>,
      partition_bounds: Vec<PartitionBound>,
  }
```

And for metric engine tables, the key difference is we split the concept of "physical table" and "logical table". Like the previous ASCII chart, multiple logical tables are based on one physical table. The relationship of logical table to region becomes many-to-many. Thus, we must include the table name (of logical table) into partition rules.

Consider the partition/route interface is a generic map of string array to region id, all we need to do is to insert logical table name into the request:

``` rust
  fn route(request: Vec<String>) -> RegionId;
```

The next question is, where to do this conversion? The basic idea is to dispatch different routing behavior based on the engine type. Since we have all the necessary information in frontend, it's a good place to do that. And can leave meta server untouched. The essential change is to associate engine type with route rule.

## Physical Region Schema

The idea "physical wide table" is to perform column-level multiplexing. I.e., map all logical columns to physical columns by their names.

```
   ┌────────────┐      ┌────────────┐         ┌────────────┐
   │   Table 1  │      │   Table 2  │         │   Table 3  │
   ├───┬────┬───┤      ├───┬────┬───┤         ├───┬────┬───┤
   │C1 │ C2 │ C3│      │C1 │ C3 │ C5├──────┐  │C2 │ C4 │ C6│
   └─┬─┴──┬─┴─┬─┘ ┌────┴───┴──┬─┴───┘      │  └─┬─┴──┬─┴─┬─┘
     │    │   │   │           │            │    │    │   │
     │    │   │   │           └──────────┐ │    │    │   │
     │    │   │   │                      │ │    │    │   │
     │    │   │   │  ┌─────────────────┐ │ │    │    │   │
     │    │   │   │  │ Physical Table  │ │ │    │    │   │
     │    │   │   │  ├──┬──┬──┬──┬──┬──┘ │ │    │    │   │
     └────x───x───┴─►│C1│C2│C3│C4│C5│C6◄─┼─x────x────x───┘
          │   │      └──┘▲─┘▲─┴─▲└─▲└──┘ │ │    │    │
          │   │          │  │   │  │     │ │    │    │
          ├───x──────────┘  ├───x──x─────┘ │    │    │
          │   │             │   │  │       │    │    │
          │   └─────────────┘   │  └───────┘    │    │
          │                     │               │    │
          └─────────────────────x───────────────┘    │
                                │                    │
                                └────────────────────┘
```

This approach is very straightforward but has one problem. It only works when two columns have different semantic type (time index, tag or field) or data types but with the same name. E.g., `CREATE TABLE t1 (c1 timestamp(3) TIME INDEX)` and `CREATE TABLE t2 (c1 STRING PRIMARY KEY)`.

One possible workaround is to prefix each column with its data type and semantic type, like `_STRING_PK_c1`. However, considering the primary goal at present is to support data from monitoring metrics like Prometheus remote write, it's acceptable not to support this at first because data types are often simple and limited here.


The next point is changing the physical table's schema. This is only needed when creating a new logical table or altering the existing table. Typically speaking, table creating and altering are explicit. We only need to emit an add column request to underlying physical table on processing logical table's DDL. GreptimeDB can create or alter table automatically on some protocols, but the internal logic is the same.

Also for simplicity, we don't support shrinking the underlying table at first. This can be achieved by introducing mechanism on the physical column.

Frontend needs not to keep physical table's schema.

## Metadata of physical regions

Those metric engine regions need to store extra metadata like the schema of logical table or all logical table's name. That information is relatively simple and can be stored in a format like key-value pair. For now, we have to use another physical mito region for metadata. This involves an issue with region scheduling. Since we don't have the ability to perform affinity scheduling, the initial version will just assume the data region and metadata region are in the same instance. See alternatives - other storage for physical region's metadata for possible future improvement.

Here is the schema of metadata region and how we would use it. The `CREATE TABLE` clause of metadata region looks like the following. Notice that it wouldn't be actually created by SQL.

``` sql
  CREATE TABLE metadata(
  	ts timestamp time index,
    	key string primary key,
    	value string
  );
```

The `ts` field is just a placeholder -- for the constraints that a mito region must contain a time index field. It will always be `0`. The other two fields `key` and `value` will be used as a k-v storage. It contains two group of key
    - `__table_<TABLE_NAME>` is used for marking table existence. It doesn't have value.
    - `__column_<TABLE_NAME>_<COLUMN_NAME>` is used for marking table existence, the value is column's semantic type.

## Query

Like other existing components, a user query always starts in the frontend. In the planning phase, frontend needs to fetch related schemas of the queried table. This part is the same. I.e., changes in this RFC don't affect components above the `Table` abstraction.

# Alternatives

## Other routing method

We can also do this "special" route rule in the meta server. But there is no difference with the proposed method.

## Other storage for physical region's metadata

Once we have implemented the "region family" that allows multiple physical schemas exist in one region, we can store the metadata and table data into one region.

Before that, we can also let the `MetricRegion` holds a `KvBackend` to access the storage layer directly. But this breaks the abstraction in some way.