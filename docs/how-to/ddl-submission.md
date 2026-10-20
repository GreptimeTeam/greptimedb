# Controlling TRUNCATE submission and waiting

`TRUNCATE TABLE` accepts optional DDL submission options:

```sql
TRUNCATE TABLE metrics WITH (wait = false, timeout = '30s');
```

The options also work after `FILE RANGE`:

```sql
TRUNCATE TABLE metrics FILE RANGE (0, 1000)
WITH (wait = true, timeout = '30s');
```

Only `wait` and `timeout` are accepted in this `WITH` clause. They apply to the
current request and are not saved in the table's metadata.

- `wait` defaults to `true`. After submitting the procedure, the request waits
  for completion and returns the existing affected-rows result on success.
- With `wait = false`, successful submission returns a one-row result with a
  `Procedure ID` column. The procedure continues in the background. Returning
  an ID does not mean truncation has completed.
- `timeout` defaults to `60s` and accepts the existing DDL duration syntax, such
  as `'30s'` or `'5m'`. A single timeout covers metadata checks in the DDL task
  handler, procedure submission and, when requested, waiting for completion.
  With `wait = false`, it only bounds the metadata checks and submission.

A waiting request that times out returns an error without a procedure ID.
**A request timeout does not mean that the background operation failed.** A
submitted procedure continues independently; timeout does not cancel it or
initiate rollback. The table can therefore be truncated after the caller has
received a timeout error.

The shared wait timeout also applies in standalone mode. Distributed mode
additionally sets the DDL RPC deadline from the request timeout. It retains the
existing wire format: durations are rounded up to whole seconds, while local
submissions retain their original duration precision.

An ID returned by an asynchronous request can be inspected with:

```sql
SELECT procedure_state('<procedure-id>');
```

After completion, truncation removes the requested data and preserves the table
definition. Existing `TRUNCATE TABLE metrics` syntax remains synchronous.

These options currently apply to TRUNCATE and the existing Repartition path.
Other DDL statements have not been connected to the shared submission behavior.
Repartition retains its own procedure execution timeout and recovery behavior;
TRUNCATE's timeout is only a request/wait timeout.
