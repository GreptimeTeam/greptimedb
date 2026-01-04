---
Feature Name: Export/Import V2
Tracking Issue: TBD
Date: 2025-12-30
Author: @jeremy
---

# Summary

This RFC proposes a redesigned export/import system (V2) for GreptimeDB that addresses fundamental issues in the current implementation. The new design leverages time-series characteristics for efficient chunking, provides clear storage semantics, and ensures data reliability through comprehensive validation mechanisms.

# Motivation

## Problems with V1

The current export/import implementation has several critical issues:

1. **Ambiguous path semantics**: `output_dir` serves dual purposes (local vs remote), causing confusion
2. **No chunking strategy**: Large databases cannot be exported/imported efficiently
3. **Poor reliability**: No resume capability, no progress tracking
4. **Limited format support**: Relies on SQL dumps, not optimized for time-series data

## Goals

1. **Clear semantics**: Explicit distinction between remote storage and server-local paths
2. **Scalability**: Support TB-scale databases through time-based chunking
3. **Reliability**: Resume capability, progress tracking, data integrity verification
4. **Performance**: Streaming export/import using native COPY DATABASE

## Non-Goals

- Schema evolution/migration (out of scope)
- Cross-version compatibility (V2 does not support V1 format)
- Real-time replication (use dedicated replication mechanism)

# Guide-level Explanation

## Core Concepts

### Snapshot

A **snapshot** represents the complete state of a GreptimeDB catalog at a point in time, including schemas and data.

**Snapshot structure**:

```
snapshot-20250101/
├── manifest.json              # Snapshot metadata and chunk index
├── schema/
│   ├── schemas.json           # Schema definitions (JSON)
│   ├── tables.json            # Table definitions (JSON)
│   └── views.json             # View definitions (JSON)
└── data/
    ├── chunk_20240101_20240102/
    │   ├── public.metrics.parquet
    │   ├── public.logs.parquet
    │   └── _chunk.meta
    └── chunk_20240102_20240103/
        └── ...
```

**Key properties**:

- Self-contained (all information needed for restore)
- Immutable (content never changes after creation)
- Verifiable (checksums at file, chunk, and snapshot levels)

### Chunk

A **chunk** is a time-range partition of data. Each chunk is independently exportable/importable and retryable.

**Chunk properties**:

- Time-aligned (e.g., [2024-01-01, 2024-01-02))
- Independent (can be exported/imported in any order)
- Atomic (either fully succeeds or fully fails)

### Storage Types

V2 supports two storage types:

| Type               | Example                 | Use Case                 |
| ------------------ | ----------------------- | ------------------------ |
| **Remote Storage** | `s3://bucket/snapshots` | Production (recommended) |
| **Server Path**    | `file:///data/backup`   | Local dev/testing        |

**Important**: Local paths (e.g., `/tmp/export`, `./backup`) are **not supported** because schema export (CLI) and data export (server) run in different processes, which would split the snapshot across two machines.

## Basic Usage

### Export

```bash
# Full snapshot to S3
greptime export create \
  --to s3://my-bucket/snapshots/prod-20250101

# Incremental snapshot (time range)
greptime export create \
  --start-time 2024-12-01T00:00:00Z \
  --end-time 2024-12-31T23:59:59Z \
  --to s3://my-bucket/snapshots/prod-december

# Schema-only export
greptime export create \
  --schema-only \
  --to s3://my-bucket/snapshots/prod-schema-only

# Export to CSV format (for debugging/inspection)
greptime export create \
  --format csv \
  --to s3://my-bucket/snapshots/prod-csv

# Resume interrupted export (automatic if snapshot exists)
greptime export create \
  --to s3://my-bucket/snapshots/prod-20250101

# Force recreate (delete existing and start over)
greptime export create \
  --to s3://my-bucket/snapshots/prod-20250101 \
  --force
```

### Import

```bash
# Full restore
greptime import restore \
  --from s3://my-bucket/snapshots/prod-20250101

# Partial restore (selected schemas)
greptime import restore \
  --from s3://my-bucket/snapshots/prod-20250101 \
  --schemas public,private

# Dry-run (verify without importing)
greptime import restore \
  --from s3://my-bucket/snapshots/prod-20250101 \
  --dry-run
```

# Reference-level Explanation

## Architecture

The export/import system consists of four main components:

1. **CLI**: Command parsing, progress display, state management
2. **Coordinator**: Snapshot planning, chunk scheduling, retry logic
3. **Schema Engine**: DDL extraction, JSON serialization, schema validation
4. **Data Engine**: Time-based chunking, streaming export/import via COPY DATABASE

All components use OpenDAL for storage abstraction, supporting S3, OSS, GCS, Azure Blob, and local filesystem.

## Data Format

### Manifest File

The manifest is a JSON file containing snapshot metadata and chunk index:

**Key fields**:

- `snapshot_id`: Unique identifier (UUID)
- `catalog`, `schemas`: Catalog and schema list
- `time_range`: Overall time range covered
- `chunks[]`: Array of chunk metadata
- `checksum`: Snapshot-level SHA256 checksum

### Schema Files

Schema definitions are stored as JSON (not SQL) for better version compatibility and programmatic processing.

**Why JSON instead of SQL?**

- Version-agnostic (can handle schema evolution)
- Programmatically processable (direct deserialization)
- Extensible (easy to add new fields)

### Data Files

Data is exported via COPY DATABASE, supporting two formats:

#### Parquet Format (Default, Recommended)

Apache Parquet is the default format. Configuration (compression, row group size, etc.) is determined by the COPY DATABASE implementation.

**Parquet benefits**:

- Columnar storage (query-friendly)
- High compression ratio
- Self-describing (includes schema metadata)
- Efficient for large-scale data

**Use cases**: Production backups, long-term archival, large-scale data migration

#### CSV Format (Optional)

CSV format is available for human-readable exports and third-party integration.

**CSV benefits**:

- Human-readable (can be opened in text editors/Excel)
- Universal compatibility (supported by all tools)
- Easy to debug and inspect
- Simple hand-editing for small datasets

**Use cases**: Data inspection, debugging, small-scale migration, third-party system integration

**Limitations**:

- Larger file size (no compression by default)
- Type information loss (everything is text)
- Less efficient for large datasets

## Core Design Decisions

### 1. Storage Path Validation

Export/import operations validate storage paths to prevent misconfigurations:

**Path types**:

- `s3://`, `oss://`, `gs://`, `azblob://` → Remote storage (recommended)
- `file:///` → Server-local path (only allowed when CLI and server are co-located)
- `/path`, `./path` → Rejected (would split snapshot across machines)

**Validation**:

- Detect path type by URI scheme
- For `file:///`, verify server endpoint resolves to localhost or local IP
- Reject bare paths without URI scheme

### 2. Time-based Chunking

Data is partitioned into fixed time windows (default: 1 day).

**Algorithm**:

- Generate non-overlapping half-open intervals: [start, start+duration), [start+duration, start+2\*duration), ...
- Last chunk may be smaller to align with time_range.end
- Empty chunks (no data in time window) are skipped and not recorded in manifest
- Guarantees: no gaps, no overlaps, complete coverage of time range

**Chunk size selection**:

The optimal chunk time window depends on data density (volume per unit time):

- **Target**: 100MB - 1GB per chunk (balances parallelism and retry cost)
- **Default**: 1 day (suitable for most workloads)
- **Recommendations**:
    - High density (>1GB/day): Use 1h, 6h, or 12h
    - Low density (<100MB/day): Use 7d or 30d

**Example**: 500GB database spanning 30 days → ~16.7GB/day → use 1h chunks → ~695MB/chunk

### 3. Data Export via COPY DATABASE

V2 uses the existing `COPY DATABASE TO` SQL for data export:

```sql
COPY DATABASE <schema> TO '<path>' WITH (
    START_TIME = '<start>',
    END_TIME = '<end>',
    FORMAT = 'parquet'  -- or 'csv'
)
```

**Benefits**:

- Reuses proven implementation
- Streaming processing (constant memory usage)
- Supports multiple formats (Parquet, CSV)
- Built-in time range filtering

**Format selection**:

- Parquet (default): Efficient for production use, better compression
- CSV: Human-readable, useful for debugging and third-party integration
- Format is recorded in manifest, automatically detected during import

### 4. Data Integrity

Three-layer checksum validation ensures data integrity:

1. **File-level**: SHA256 of each Parquet file
2. **Chunk-level**: Aggregate checksum of all files in a chunk
3. **Snapshot-level**: Aggregate checksum of all chunks

Checksums are verified during import before data is written to the database.

### 5. Retry and Resume

**Chunk-level retry**:

- Each chunk is an independent unit of work
- Failed chunks are retried with exponential backoff (capped at 5 minutes)
- Successful chunks are never re-exported

**Resume capability**:

- Manifest tracks chunk status (Pending, Completed, Failed)
- Export/import automatically resumes when executed on existing snapshot
- Skips completed chunks, retries failed chunks, processes pending chunks
- Works across process restarts
- Use `--force` (export only) to delete existing snapshot and start over

### 6. Concurrent Export Safety

**Scenario 1**: Export to different paths ✅

- Multiple exports can run simultaneously to different storage locations
- No conflicts (only reads database, writes to different paths)

**Scenario 2**: Export to same path ⚠️

- Concurrent exports to the same path can corrupt the snapshot
- With default resume behavior: both processes will resume the same export (usually safe)
- Race condition exists during initial manifest creation, but unlikely in practice
- Recommendation: Include timestamp in snapshot path to avoid conflicts

**Why no distributed lock?**

- Users unlikely to intentionally export to the same path
- Path check prevents most accidents
- Distributed lock adds complexity (Meta service dependency, timeouts, deadlocks)

## CLI Interface

### Export Command

```
greptime export create [OPTIONS] --to <LOCATION>

Required:
  --to <LOCATION>                 Target storage location

Optional:
  --catalog <CATALOG>             Catalog name (default: greptime)
  --schemas <SCHEMAS>             Comma-separated schema list (default: all)
  --start-time <TIMESTAMP>        Time range start (default: earliest)
  --end-time <TIMESTAMP>          Time range end (default: now)
  --chunk-time-window <DURATION>  Chunk time window (default: 1d)
  --parallelism <N>               Concurrency level (default: 1)
  --format <FORMAT>               Export format: parquet (default) or csv
  --schema-only                   Export schema only, no data
  --force                         Delete existing snapshot and recreate

Behavior:
  - If snapshot doesn't exist: create new snapshot
  - If snapshot exists: automatically resume export (skip completed chunks,
    retry failed chunks, process pending chunks)
  - If --force is specified: delete existing snapshot first, then create new one
```

### Import Command

```
greptime import restore [OPTIONS] --from <SNAPSHOT>

Required:
  --from <SNAPSHOT>         Source snapshot location

Optional:
  --catalog <CATALOG>       Catalog name (default: greptime)
  --schemas <SCHEMAS>       Comma-separated schema list (default: all)
  --parallelism <N>         Concurrency level (default: 1)
  --dry-run                 Verify without importing
  --time-range <RANGE>      Import partial time range only

Behavior:
  - Automatically resumes if import was previously interrupted
  - Skips completed chunks, retries failed chunks, processes pending chunks
```

### Management Commands

```bash
# List snapshots
greptime export list --location s3://bucket/snapshots

# Verify snapshot integrity
greptime export verify --snapshot s3://bucket/snapshots/prod-20250101

# Delete snapshot
greptime export delete --snapshot s3://bucket/snapshots/old-snapshot
```

## Error Handling

Key error types:

- `UnsupportedLocalPath`: Bare path without URI scheme
- `RemoteServerWithLocalPath`: file:/// used with remote server
- `InvalidTimeRange`: start >= end
- `ChecksumMismatch`: File/chunk/snapshot checksum verification failed
- `SnapshotNotFound`: Resume requested but snapshot doesn't exist (import only)

Errors include detailed hints for resolution.

# Drawbacks

1. **Breaking change**: V2 does not support V1 format (migration required)
2. **Storage dependency**: Relies on object storage for production use
3. **Time-series assumption**: Assumes data has time index (not suitable for non-time-series tables)
4. **Chunk granularity**: Very sparse data may result in many small chunks

# Rationale and Alternatives

## Why time-based chunking?

**Alternatives considered**:

- **Size-based chunking**: Requires scanning data to estimate size (double I/O overhead)
- **Adaptive chunking**: Complex, requires metadata scanning, marginal benefit

**Decision**: Fixed time-window chunking

- Simple and predictable
- Aligns with time-series data characteristics
- User can adjust based on data density
- No pre-scanning required

## Why JSON for schema, not SQL?

**Alternatives**: SQL dumps (V1 approach)

**Decision**: JSON schema format

- Version-agnostic (can add compatibility layer)
- Programmatically processable
- Easier to validate and transform
- Avoids SQL dialect issues

## Why no distributed lock for concurrent exports?

**Alternative**: Implement distributed lock via Meta service

**Decision**: Path existence check only

- Users unlikely to intentionally conflict
- Path check prevents most accidents
- Distributed lock adds significant complexity
- YAGNI (You Aren't Gonna Need It)

# Unresolved Questions

1. **Cross-version restore**: Should V2 support restoring to older GreptimeDB versions?
2. **Schema evolution**: How to handle schema changes between export and import?
3. **Partial schema export**: Should we support table-level filtering (not just schema-level)?

# Future Possibilities

1. **Incremental backup**: Export only data changes since last backup (requires WAL integration)
2. **Parallel chunk processing**: Export/import multiple chunks simultaneously (requires careful resource management)
3. **Cloud-native snapshots**: Direct integration with S3 Glacier, Azure Archive for cold storage
4. **Point-in-time recovery**: Combine full + incremental snapshots for PITR capability
5. **Snapshot metadata service**: Centralized snapshot registry and lifecycle management
