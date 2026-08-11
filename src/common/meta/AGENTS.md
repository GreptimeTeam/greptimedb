# common-meta — Agent & Contributor Guide

Navigation map for `src/common/meta`. Repo-wide invariants:
[`.agents/architecture-invariants.md`](../../../.agents/architecture-invariants.md).

`common-meta` contains shared metadata keys and values, KV backends, caches,
and durable DDL procedures. Metasrv process and service wiring live in
`src/meta-srv`.

## Module map

| Area | Path | Entry point |
| --- | --- | --- |
| Metadata model | `src/common/meta/src/key.rs`, `src/common/meta/src/key/` | Typed keys, values, and `TableMetadataManager` |
| KV storage | `src/common/meta/src/kv_backend.rs`, `src/common/meta/src/kv_backend/` | `KvBackend`, transactions, memory/etcd/RDS backends |
| DDL procedures | `src/common/meta/src/ddl.rs`, `src/common/meta/src/ddl/`, `src/common/meta/src/ddl_manager.rs` | Durable DDL state machines and task dispatch |
| Caches | `src/common/meta/src/cache.rs`, `src/common/meta/src/cache/`, `src/common/meta/src/cache_invalidator.rs` | Metadata caches and invalidation |
| RPC types | `src/common/meta/src/rpc.rs`, `src/common/meta/src/rpc/` | Shared metasrv request/response types |
| Recovery | `src/common/meta/src/reconciliation.rs`, `src/common/meta/src/reconciliation/` | Catalog/table/region reconciliation |

## Change coupling

- Key/value encoding changes require backward-compatible decoding and a case in
  `tests/compatibility/`.
- Multi-key metadata changes must preserve transaction boundaries and cache
  invalidation.
- Procedure changes must preserve persisted state and `TYPE_NAME`; new
  procedures need loader registration in `src/common/meta/src/ddl_manager.rs`.
- `KvBackend` behavior changes should extend the shared tests in
  `src/common/meta/src/kv_backend/test.rs`.

## Testing

```bash
cargo nextest run -p common-meta
```

Backend-specific coverage may also need `pg_kvbackend` or `mysql_kvbackend`.
