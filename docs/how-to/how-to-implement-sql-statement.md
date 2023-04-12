This document introduces how to implement SQL statements in GreptimeDB.

The execution entry point for SQL statements locates at Frontend Instance. You can see it has
implemented `SqlQueryHandler`:

```rust
impl SqlQueryHandler for Instance {
    type Error = Error;

    async fn do_query(&self, query: &str, query_ctx: QueryContextRef) -> Vec<Result<Output>> {
        // ...
    }
}
```

Normally, when a SQL query arrives at GreptimeDB, the `do_query` method will be called. After some parsing work, the SQL
will be feed into `StatementExecutor`:

```rust
// in Frontend Instance:
self.statement_executor.execute_sql(stmt, query_ctx).await
```

That's where we handle our SQL statements. You can just create a new match arm for your statement there, then the
statement is implemented for both GreptimeDB Standalone and Cluster. You can see how `DESCRIBE TABLE` is implemented as
an example.

Now, what if the statements should be handled differently for GreptimeDB Standalone and Cluster? You can see there's
a `SqlStatementExecutor` field in `StatementExecutor`. Each GreptimeDB Standalone and Cluster has its own implementation
of `SqlStatementExecutor`. If you are going to implement the statements differently in the two mode (
like `CREATE TABLE`), you have to implement them in their own `SqlStatementExecutor`s.

Summarize as the diagram below:

```text
                             SQL query                            
                                |                                
                                v                                
                  +---------------------------+                  
                  | SqlQueryHandler::do_query |                  
                  +---------------------------+                  
                                |                                
                                | SQL parsing                    
                                v                                
               +--------------------------------+                
               | StatementExecutor::execute_sql |                
               +--------------------------------+                
                                |                                
                                | SQL execution                    
                                v                                
               +----------------------------------+                
               | commonly handled statements like |
               | "plan_exec" for selection or     |
               +----------------------------------+                
                       |                |                        
        For Standalone |                | For Cluster          
                       v                v                        
+---------------------------+      +---------------------------+ 
| SqlStatementExecutor impl |      | SqlStatementExecutor impl | 
| in Datanode Instance      |      | in Frontend DistInstance  | 
+---------------------------+      +---------------------------+ 
```

Note that some SQL statements can be executed in our QueryEngine, in the form of `LogicalPlan`. You can follow the
invocation path down to the `QueryEngine` implementation from `StatementExecutor::plan_exec`. For now, there's only
one `DatafusionQueryEngine` for both GreptimeDB Standalone and Cluster. That lone query engine works for both modes is
because GreptimeDB read/write data through `Table` trait, and each mode has its own `Table` implementation.

We don't have any bias towards whether statements should be handled in query engine or `StatementExecutor`. You can
implement one kind of statement in both places. For example, `Insert` with selection is handled in query engine, because
we can easily do the query part there. However, `Insert` without selection is not, for the cost of parsing statement
to `LogicalPlan` is not neglectable. So generally if the SQL query is simple enough, you can handle it
in `StatementExecutor`; otherwise if it is complex or has some part of selection, it should be parsed to `LogicalPlan`
and handled in query engine.  
