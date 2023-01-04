---
Feature Name: "procedure-framework"
Tracking Issue:
Date: 2023-01-03
Author: "Yingwen <realevenyag@gmail.com>"
---

Procedure Framework
----------------------

# Summary
A framework for executing operations in a fault-tolerant manner.

# Motivation
Some operations in GreptimeDB require multiple steps to implement. For example, creating a table needs:
1. Check whether the table exists
2. Create the table in the table engine
  1. Create a region for the table in the storage engine
  2. Persist the metadata of the table to the table manifest
3. Add the table to the catalog manager

If the node dies or restarts in the middle of creating a table, it could leave the system in an inconsistent state. The procedure framework, inspired by [Apache HBase's ProcedureV2 framework](https://github.com/apache/hbase/blob/bfc9fc9605de638785435e404430a9408b99a8d0/src/main/asciidoc/_chapters/pv2.adoc) and [Apache Accumulo’s FATE framework](https://accumulo.apache.org/docs/2.x/administration/fate), aims to provide a unified way to implement multi-step operations that is tolerant to failure.

# Details
## Overview
The procedure framework consists of the following primary components:
- A `Procedure` represents an operation or a set of operations to be performed step-by-step
- `ProcedureManager`, the system to run `Procedures`
- `ProcedureStore` is a storage layer for persisting the procedure state


## Procedures
The `ProcedureManager` keeps calling `Procedure::execute()` until the Procedure is done, so the operation of the Procedure should be idempotent: it needs to be able to undo or replay a partial execution of itself.

```rust
trait Procedure {
    fn execute(&mut self, ctx: &Context) -> Result<Status>;

    fn restore(&mut self, data: &str) -> Result<()>;
}
```

The `Status` is an enum that has the following variants:
```rust
enum Status {
    Executing,
    Suspend,
    Done,
}
```

A call to `execute()` can result in the following possibilities:
- `Ok(Status::Done)`: we are done
- `Ok(Status::Executing)`: there are remaining steps to do
- `Ok(Status::Suspend)`: execution is suspended and can be resumed later
- `Err(e)`: error occurs during execution and the procedure is unable to proceed anymore


The Procedure can persist its state into the ProcedureStore via Context::store, so can control when and whether to store its state. The framework assigns a unique id for each procedure and the procedure can get this id via the `Context`.

```rust
struct Context {
    id: ProcedureId,
    procedure_store: Arc<dyn ProcedureStore>,
    // other fields ...
}

impl Context {
    fn store(&self, data: &str) -> Result<()> {
        // Stores the data into `procedure_store`
    }
}
```

After restart, the framework can restore the procedure's state by calling `Procedure::restore()`.

## ProcedureStore
We may need to provide two different ProcedureStore implementations:
- `LocalProcedureStore`, for standalone mode, stores data on the local disk.
- `RemoteProcedureStore`, for distributed mode, stores data on the meta server or the object store service.

These implementations should share the same storage structure. They store each procedure's state in a unique path based on the procedure id:

```
Sample paths:

/procedures/{PROCEDURE_ID}-000001.json
/procedures/{PROCEDURE_ID}-000002.json
/procedures/{PROCEDURE_ID}-000003.commit
```

`ProcedureStore` behaves like a WAL. Before performing each step, the procedure can store its current state in the ProcedureStore. The `000001` in the path is the number of the step. After the procedure is done, the framework should put a `.commit` file to indicate the procedure is finished (committed).

The framework can remove the procedure's files once the procedure is done, but it needs to leave the `.commit` as the last file to remove in case of failure during removal.

## ProcedureManager
`ProcedureManager` executes procedures submitted to it.

```rust
trait ProcedureManager {
    fn register_builder(&self, name: &str, procedure_builder: Box<dyn ProcedureBuilder>) -> Result<()>;

    fn create(&self, name: &str) -> Result<Box<dyn Procedure>>;

    fn submit(&self, procedure: Box<dyn Procedure>) -> Result<ProcedureId>;
}
```

It supports the following operations:
- Register a `ProcedureBuilder` by name. We can build a new `Procedure` instance by calling `ProcedureBuilder::build()`.
- Create a new `Procedure` instance by the registered builder name.
- Submit a `Procedure` to the manager and execute it.

When `ProcedureManager` starts, it loads procedures from the `ProcedureStore`, and restores their states by calling `Procedure::restore()`.

# Drawbacks
The `Procedure` framework introduces additional complexity and overhead to our database.
- To execute a `Procedure`, we need to write to the `ProcedureStore` multiple times, which may slow down the server
- We need to rewrite the logic of creating/dropping/altering a table using the procedure framework

# Alternatives
Another approach is to tolerate failure during execution and allow users to retry the operation until it succeeds. But we still need to:
- Make each step idempotent
- Record the status in some place to check whether we are done
