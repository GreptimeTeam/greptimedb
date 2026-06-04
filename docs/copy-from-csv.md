# COPY FROM CSV

`COPY FROM` supports CSV files with or without a header row.

By default, CSV imports expect a header row and map file columns to table columns
by name. This keeps the existing behavior for files whose first row contains
column names:

```sql
COPY metrics FROM '/data/metrics.csv' WITH (FORMAT = 'csv');
```

For headerless CSV files, set `HEADERS = 'false'`:

```sql
COPY metrics FROM '/data/metrics.csv' WITH (FORMAT = 'csv', HEADERS = 'false');
```

When `HEADERS = 'false'`, columns are mapped by position:

- The first CSV column maps to the first table column, the second CSV column maps
  to the second table column, and so on.
- Extra CSV columns are ignored.
- If the CSV file has fewer columns than the table, only the matching table
  prefix is imported.
- The imported values are cast to the target table column types.

`HEADERS` is the public CSV option. `HAS_HEADER` is accepted as an internal
alias. If both options are specified, they must have the same boolean value.

`SKIP_BAD_RECORDS = 'true'` can be used together with `HEADERS = 'false'`; bad
CSV rows are skipped after applying the same positional mapping.
