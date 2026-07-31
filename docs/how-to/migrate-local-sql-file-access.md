# Migrate Local SQL File Access

SQL access to local files is sandboxed in standalone deployments and disabled in
distributed deployments.

## Standalone

The default sandbox is `<storage.data_home>/copy`. Relative paths in `COPY` and
external-table locations are resolved below this directory. Absolute paths work
only when they are inside the sandbox.

Before upgrading, identify existing `COPY` workflows and external tables that
use local paths outside the default sandbox. Choose one of these migrations:

- Move the files below `<storage.data_home>/copy` and update the SQL locations.
- Set `storage.copy_root` to a dedicated local directory containing the files.
- Move the files to S3, OSS, GCS, or AzBlob and update the SQL locations.

Do not set `storage.copy_root` to `storage.data_home` or to a directory that
contains GreptimeDB data, WAL, manifests, or configuration files. GreptimeDB
rejects copy roots that expose its internal data directory.

When `storage.data_home` is an object-storage URL, local SQL file access is
disabled unless `storage.copy_root` explicitly names a local directory.

## Distributed

Distributed frontend and datanode processes reject local paths for `COPY TABLE`,
`COPY QUERY`, `COPY DATABASE`, and external tables. Migrate these workflows and
tables to S3, OSS, GCS, or AzBlob before upgrading.
