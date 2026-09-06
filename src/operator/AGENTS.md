# operator — Agent & Contributor Guide

Navigation map for `src/operator`. Repo-wide invariants:
[`.agents/architecture-invariants.md`](../../.agents/architecture-invariants.md).

`operator` connects frontend statements and writes to query, catalog, metasrv,
and datanodes. It owns statement dispatch, DDL task conversion, and region-level
insert/delete routing.

## Module map

| Area | Path | Entry point |
| --- | --- | --- |
| Statements | `src/operator/src/statement.rs`, `src/operator/src/statement/` | `StatementExecutor` and per-statement handlers |
| Inserts | `src/operator/src/insert.rs`, `src/operator/src/bulk_insert.rs` | Insert, auto-create/alter, routing, dispatch |
| Deletes | `src/operator/src/delete.rs` | Delete conversion and dispatch |
| Request conversion | `src/operator/src/req_convert.rs`, `src/operator/src/req_convert/` | Table/row/column requests to region requests |
| Region requests | `src/operator/src/region_req_factory.rs` | Region-level DDL/DML request construction |
| Procedures | `src/operator/src/procedure.rs` | Metasrv procedure administration |
| Flow | `src/operator/src/flow.rs`, `src/operator/src/statement/ddl.rs` | Flow requests and internal mode selection |

## Change coupling

- New statement kinds usually touch parser/AST handling, frontend permission
  and interception, executor dispatch, and sqlness cases.
- DDL task changes require matching `common-meta`, metasrv, recovery, and
  compatibility updates.
- Insert/delete conversion changes must cover both request formats, partition
  routing, and schema-on-write behavior.
- Flow creation changes must stay aligned with `src/flow/AGENTS.md` and
  `common-meta` flow metadata.

## Testing

```bash
cargo nextest run -p operator
cargo sqlness bare -t <case>
```

Check standalone and distributed routing when changing region dispatch.
