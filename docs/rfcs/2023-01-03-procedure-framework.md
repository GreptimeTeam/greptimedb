---
Feature Name: "procedure-framework"
Tracking Issue: https://github.com/GreptimeTeam/greptimedb/issues/286
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
- `ProcedureManager`, the runtime to run `Procedures`. It executes the submitted procedures, stores procedures' states to the `ProcedureStore` and restores procedures from `ProcedureStore` while the database restarts.
- `ProcedureStore` is a storage layer for persisting the procedure state


## Procedures
The `ProcedureManager` keeps calling `Procedure::execute()` until the Procedure is done, so the operation of the Procedure should be [idempotent](https://developer.mozilla.org/en-US/docs/Glossary/Idempotent): it needs to be able to undo or replay a partial execution of itself.

```rust
trait Procedure {
    fn execute(&mut self, ctx: &Context) -> Result<Status>;

    fn dump(&self) -> Result<String>;

    fn rollback(&self) -> Result<()>;

    // other methods...
}
```

The `Status` is an enum that has the following variants:
```rust
enum Status {
    Executing {
        persist: bool,
    },
    Suspended {
        subprocedures: Vec<ProcedureWithId>,
        persist: bool,
    },
    Done,
}
```

A call to `execute()` can result in the following possibilities:
- `Ok(Status::Done)`: we are done
- `Ok(Status::Executing { .. })`: there are remaining steps to do
- `Ok(Status::Suspend { sub_procedure, .. })`: execution is suspended and can be resumed later after the sub-procedure is done.
- `Err(e)`: error occurs during execution and the procedure is unable to proceed anymore.

Users need to assign a unique `ProcedureId` to the procedure and the procedure can get this id via the `Context`. The `ProcedureId` is typically a UUID.

```rust
struct Context {
    id: ProcedureId,
    // other fields ...
}
```

The `ProcedureManager` calls `Procedure::dump()` to serialize the internal state of the procedure and writes to the `ProcedureStore`. The `Status` has a field `persist` to tell the `ProcedureManager` whether it needs persistence.

## Sub-procedures
A procedure may need to create some sub-procedures to process its subtasks. For example, creating a distributed table with multiple regions (partitions) needs to set up the regions in each node, thus the parent procedure should instantiate a sub-procedure for each region. The `ProcedureManager` makes sure that the parent procedure does not proceed till all sub-procedures are successfully finished.

The procedure can submit sub-procedures to the `ProcedureManager` by returning `Status::Suspended`. It needs to assign a procedure id to each procedure manually so it can track the status of the sub-procedures.
```rust
struct ProcedureWithId {
    id: ProcedureId,
    procedure: BoxedProcedure,
}
```

## ProcedureStore
We might need to provide two different ProcedureStore implementations:
- In standalone mode, it stores data on the local disk.
- In distributed mode, it stores data on the meta server or the object store service.

These implementations should share the same storage structure. They store each procedure's state in a unique path based on the procedure id:

```
Sample paths:

/procedures/{PROCEDURE_ID}/000001.step
/procedures/{PROCEDURE_ID}/000002.step
/procedures/{PROCEDURE_ID}/000003.commit
```

`ProcedureStore` behaves like a WAL. Before performing each step, the `ProcedureManager` can write the procedure's current state to the ProcedureStore, which stores the state in the `.step` file. The `000001` in the path is a monotonic increasing sequence of the step. After the procedure is done, the `ProcedureManager` puts a `.commit` file to indicate the procedure is finished (committed).

The `ProcedureManager` can remove the procedure's files once the procedure is done, but it needs to leave the `.commit` as the last file to remove in case of failure during removal.

## ProcedureManager
`ProcedureManager` executes procedures submitted to it.

```rust
trait ProcedureManager {
    fn register_loader(&self, name: &str, loader: BoxedProcedureLoader) -> Result<()>;

    async fn submit(&self, procedure: ProcedureWithId) -> Result<()>;
}
```

It supports the following operations:
- Register a `ProcedureLoader` by the type name of the `Procedure`.
- Submit a `Procedure` to the manager and execute it.

When `ProcedureManager` starts, it loads procedures from the `ProcedureStore` and restores the procedures by the `ProcedureLoader`. The manager stores the type name from `Procedure::type_name()` with the data from `Procedure::dump()` in the `.step` file and uses the type name to find a `ProcedureLoader` to recover the procedure from its data.

```rust
type BoxedProcedureLoader = Box<dyn Fn(&str) -> Result<BoxedProcedure> + Send>;
```

## Rollback
The rollback step is supposed to clean up the resources created during the execute() step. When a procedure has failed, the `ProcedureManager` puts a `rollback` file and calls the `Procedure::rollback()` method.


```text
/procedures/{PROCEDURE_ID}/000001.step
/procedures/{PROCEDURE_ID}/000002.rollback
```

Rollback is complicated to implement so some procedures might not support rollback or only provide a best-efforts approach.

## Locking
The `ProcedureManager` can provide a locking mechanism that gives a procedure read/write access to a database object such as a table so other procedures are unable to modify the same table while the current one is executing.

# Drawbacks
The `Procedure` framework introduces additional complexity and overhead to our database.
- To execute a `Procedure`, we need to write to the `ProcedureStore` multiple times, which may slow down the server
- We need to rewrite the logic of creating/dropping/altering a table using the procedure framework

# Alternatives
Another approach is to tolerate failure during execution and allow users to retry the operation until it succeeds. But we still need to:
- Make each step idempotent
- Record the status in some place to check whether we are done
