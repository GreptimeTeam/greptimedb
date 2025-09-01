# DuckDB Test Migration Guide

## Overview
This guide documents the process of migrating DuckDB tests to GreptimeDB's sqlness test framework.

## Migration Status
- **Total DuckDB tests**: 3030+ test files across 71 directories
- **Priority categories for migration**:
  1. SELECT (partially done)
  2. SUBQUERY (needs expansion)
  3. INSERT...SELECT (already covered)
  4. TYPES (well covered)
  5. COPY (basic coverage exists)
  6. PREPARED statements (basic coverage exists)

## Format Conversion

### DuckDB Test Format
```
# name: test/sql/select/test_name.test
# description: Test description
# group: [select]

statement ok
CREATE TABLE ...

query I
SELECT ...
----
expected_result
```

### GreptimeDB Sqlness Format
```sql
-- Migrated from DuckDB test: test/sql/category/test_name.test
-- Description: Test description
-- Note: Any adaptations made for GreptimeDB

CREATE TABLE ... (
    -- Must include TIME INDEX column for GreptimeDB
    ts TIMESTAMP TIME INDEX
);

SELECT ...;

-- Clean up
DROP TABLE ...;
```

## Key Adaptation Rules

### 1. TIME INDEX Requirement
GreptimeDB requires every table to have a TIME INDEX column:
```sql
-- DuckDB
CREATE TABLE t(i INTEGER);

-- GreptimeDB
CREATE TABLE t(i INTEGER, ts TIMESTAMP TIME INDEX);
```

### 2. Unsupported Features
- **Positional references** (`#1`, `#2`) - Not supported
- **PRAGMA commands** - Use information_schema instead
- **ROW/STRUCT types** - Limited support
- **SELECT INTO** - Not supported
- **Table locking** - Not applicable for time-series DB

### 3. Alternative Approaches
| DuckDB Feature | GreptimeDB Alternative |
|----------------|----------------------|
| `pragma_table_info('table')` | `SELECT * FROM information_schema.columns WHERE table_name = 'table'` |
| `VALUES(1),(2),(3)` | Create temp table with INSERT |
| Complex STRUCT operations | Skip or simplify |

## Migration Process

1. **Identify suitable tests**
   - Check if test uses GreptimeDB-compatible features
   - Skip tests for unsupported features
   - Check for existing similar tests to avoid duplication

2. **Adapt SQL statements**
   - Add TIME INDEX columns to all CREATE TABLE statements
   - Replace PRAGMA with information_schema queries
   - Adjust for GreptimeDB-specific syntax

3. **Create test file**
   - Place in appropriate directory under `tests/cases/standalone/common/`
   - Use descriptive filename that reflects the test purpose
   - Include migration comments noting the DuckDB source

4. **Generate result file**
   - Run: `make sqlness-test SQLNESS_TEST_CASE=test_name`
   - Review generated `.result` file for correctness
   - Do NOT manually edit `.result` files

## Examples of Migrated Tests

### Simple SELECT Test
- **Original**: `/duckdb/test/sql/select/test_select_empty_table.test`
- **Migrated**: `tests/cases/standalone/common/select/select_empty_table.sql`

### Projection Names Test
- **Original**: `/duckdb/test/sql/select/test_projection_names.test`
- **Migrated**: `tests/cases/standalone/common/select/projection_names.sql`
- **Adaptation**: Used information_schema instead of pragma_table_info

### Multi-Column Reference Test
- **Original**: `/duckdb/test/sql/select/test_multi_column_reference.test`
- **Migrated**: `tests/cases/standalone/common/select/multi_column_ref.sql`
- **Adaptation**: Removed STRUCT/ROW type tests, focused on schema.table.column references

### Subquery Offset Test
- **Original**: `/duckdb/test/sql/subquery/test_offset.test`
- **Migrated**: `tests/cases/standalone/common/subquery/offset.sql`
- **Adaptation**: Replaced VALUES clause with temp table

## Next Steps

1. Continue migrating SELECT tests that don't rely on unsupported features
2. Expand SUBQUERY test coverage
3. Consider adding PARSER-specific tests
4. Evaluate TPC-H benchmark tests for performance testing

## Notes
- Always check existing GreptimeDB tests before migration to avoid duplication
- Focus on tests that add value for time-series database use cases
- Document any significant adaptations in test comments