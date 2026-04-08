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
    ├── 1/
    │   ├── public.metrics.parquet
    │   └── public.logs.parquet
    ├── 2/
    │   ├── public.metrics.parquet
    │   └── public.logs.parquet
    └── 3/
        ├── public.metrics.parquet
        └── public.logs.parquet
```

**Key properties**:

- Self-contained (all information needed for restore)
- Immutable (content never changes after creation)
- Verifiable (checksums at file, chunk, and snapshot levels)
- Schema-only snapshots contain only `manifest.json` and `schema/`; `data/` is absent, `chunks` is empty, and later data append is rejected (use `--force` to recreate)

### Chunk

A **chunk** is a time-range partition of data. Each chunk is independently exportable/importable and retryable.

**Chunk properties**:

- Has explicit `start_time` and `end_time` (recorded in manifest)
- Non-overlapping with other chunks
- Covers a contiguous time range
- Independent (can be exported/imported in any order)
- Atomic (either fully succeeds or fully fails)

**Chunk directory naming**:

- Sequential numbers: `1/`, `2/`, `3/`, ...
- Time ranges are recorded in manifest.json

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

Schema-only snapshots cannot be resumed with data; use `--force` to recreate.

# Export with specific format (default: parquet)
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
# Full import
greptime import \
  --from s3://my-bucket/snapshots/prod-20250101

# Partial import (selected schemas)
greptime import \
  --from s3://my-bucket/snapshots/prod-20250101 \
  --schemas public,private

# Dry-run (verify without importing)
greptime import \
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
- `schema_only`: Whether the snapshot contains schema only
- `chunks[]`: Array of chunk metadata
- `format`: Data format for exported files
- `checksum`: Snapshot-level SHA256 checksum

**Chunk metadata structure**:

Each chunk entry in the manifest contains:

- `id`: Chunk identifier (sequential number)
- `time_range`: Start and end timestamps
- `status`: Export status (Pending, InProgress, Completed, Failed)
- `files`: List of data files in the chunk directory
- `checksum`: Chunk-level checksum for integrity verification

### Schema Files

Schema definitions are stored as JSON (not SQL) for better version compatibility and programmatic processing.

**Why JSON instead of SQL?**

- Version-agnostic (can handle schema evolution)
- Programmatically processable (direct deserialization)
- Extensible (easy to add new fields)

### Data Files

Data is exported via COPY DATABASE, supporting multiple formats:

- **Parquet** (default): Columnar format, efficient compression, recommended for production use
- **CSV**: Human-readable, universally compatible, useful for debugging and third-party integration
- **JSON**: Structured text format, flexible schema representation
- Other formats supported by COPY DATABASE

Format is specified via `--format` flag and recorded in manifest.json. Import automatically detects the format from manifest.

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

Data is partitioned into time-range chunks for efficient parallel processing and retry.

**Algorithm**:

- Generate non-overlapping half-open intervals with configurable time window (default: 1 day)
- Chunks are numbered sequentially (1, 2, 3, ...)
- Each chunk's time range is recorded in manifest.json
- Empty chunks (no data in time window) are skipped and not recorded in manifest
- Guarantees: no gaps, no overlaps, complete coverage of time range

**Chunk time window selection**:

The optimal chunk time window depends on data density (volume per unit time):

- **Target**: 100MB - 1GB per chunk (balances parallelism and retry cost)
- **Default**: 1 day (suitable for most workloads)
- **Recommendations**:
    - High density (>1GB/day): Use smaller windows like 1h, 6h, or 12h
    - Low density (<100MB/day): Use larger windows like 7d or 30d
    - Time windows can be adjusted flexibly (not required to align to day boundaries)

**Example**: 500GB database spanning 30 days → ~16.7GB/day → use 1h chunks → ~695MB/chunk

### 3. Data Export via COPY DATABASE

V2 leverages the existing `COPY DATABASE TO` for data export, with additional tooling layer for chunking, resume, and metadata management.

**How it works**:

1. **Export tool** generates chunks based on time range and chunk window
2. For each chunk, **calls COPY DATABASE** with specific time range:
    ```sql
    COPY DATABASE <schema> TO '<chunk_path>' WITH (
        START_TIME = '<chunk_start>',
        END_TIME = '<chunk_end>',
        FORMAT = 'parquet'
    )
    ```
3. **Export tool** records chunk metadata, calculates checksums, and updates manifest

**Separation of concerns**:

- **COPY DATABASE** (data layer): Streaming export, format support, time filtering
- **Export tool** (tooling layer): Chunking, resume, manifest management, checksum calculation, schema export

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

- Manifest tracks chunk status (Pending, InProgress, Completed, Failed)
- Export/import automatically resumes when executed on existing snapshot
- Skips completed chunks, retries failed/in-progress chunks, processes pending chunks
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
  --format <FORMAT>               Export format for data file: parquet (default), csv, json, or other formats supported by COPY DATABASE
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
greptime import [OPTIONS] --from <SNAPSHOT>

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

# Unresolved Questions

1. **Cross-version restore**: Should V2 support restoring to older GreptimeDB versions?
2. **Partial schema export**: Should we support table-level filtering (not just schema-level)?

# Future Possibilities

1. **Incremental backup**: Export only data changes since last backup (requires WAL integration)
2. **Parallel chunk processing**: Export/import multiple chunks simultaneously (requires careful resource management)
3. **Snapshot metadata service**: Centralized snapshot registry and lifecycle management
