// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! External storage of large string and binary field values.
//!
//! A field column with `STORAGE WITH (type = 'external', threshold = ...)` keeps short values in
//! the parquet column. Each value at least `threshold` long is zstd compressed and appended to a
//! sidecar blob object next to the SST, and the parquet column holds a reference to it instead:
//! [REF_PREFIX] followed by `offset:compressed_len:len`. Values that start with [REF_PREFIX] are
//! stored externally regardless of their size, so a value is a reference if and only if it
//! starts with the prefix.
//!
//! An SST with a sidecar lists its external columns in footer keys starting with
//! [BLOB_COLUMN_KEY_PREFIX] and records the sidecar size under [BLOB_FILE_SIZE_KEY]. The sidecar
//! shares the SST's file id and is deleted with it.
//!
//! Readers resolve references right after decoding a row group, so filters, merges and
//! compaction see the original values.

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use common_runtime::spawn_blocking_global;
use datatypes::arrow::array::{ArrayRef, AsArray, BinaryBuilder, StringBuilder};
use datatypes::arrow::datatypes::DataType;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::schema::StorageType;
use object_store::ObjectStore;
use parquet::file::metadata::{KeyValue, ParquetMetaData};
use snafu::ResultExt;
use store_api::metadata::RegionMetadata;

use crate::error::{Error, JoinSnafu, NewRecordBatchSnafu, OpenDalSnafu, Result, UnexpectedSnafu};

/// Prefix of footer keys that mark a column as stored externally; the column name follows it.
/// One key per column keeps any column name intact.
pub(crate) const BLOB_COLUMN_KEY_PREFIX: &str = "greptime:blob_column:";
/// Footer key holding the size of the sidecar object in bytes.
pub(crate) const BLOB_FILE_SIZE_KEY: &str = "greptime:blob_file_size";
/// Marks a value as a reference into the sidecar. Valid UTF-8, so string columns can hold it.
const REF_PREFIX: &str = "\0greptime-blob:";
/// Upper bound of resolved value bytes in one output batch; keeps i32 offsets valid.
const MAX_RESOLVED_BATCH_BYTES: usize = 128 * 1024 * 1024;
/// Neighboring values closer than this are fetched in one request.
const COALESCE_GAP: u64 = 1024 * 1024;
/// Upper bound of one coalesced request.
const MAX_REQUEST_BYTES: u64 = 16 * 1024 * 1024;
const ZSTD_LEVEL: i32 = 1;

/// Returns the sidecar path of the SST at `sst_path`.
pub(crate) fn blob_path(sst_path: &str) -> String {
    let stem = sst_path.strip_suffix(".parquet").unwrap_or(sst_path);
    format!("{stem}.blob")
}

/// Returns the external columns recorded in the footer.
pub(crate) fn blob_columns(meta: &ParquetMetaData) -> Vec<String> {
    meta.file_metadata()
        .key_value_metadata()
        .into_iter()
        .flatten()
        .filter_map(|kv| kv.key.strip_prefix(BLOB_COLUMN_KEY_PREFIX))
        .map(str::to_string)
        .collect()
}

/// Returns the sidecar size recorded in the footer, or `None` when the SST has no sidecar.
pub(crate) fn blob_file_size(meta: &ParquetMetaData) -> Option<u64> {
    meta.file_metadata()
        .key_value_metadata()?
        .iter()
        .find(|kv| kv.key == BLOB_FILE_SIZE_KEY)?
        .value
        .as_deref()?
        .parse()
        .ok()
}

/// A field column that stores long values externally.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExternalColumn {
    pub(crate) name: String,
    pub(crate) threshold: usize,
}

/// Returns the field columns of `metadata` with external storage.
pub(crate) fn external_columns(metadata: &RegionMetadata) -> Result<Vec<ExternalColumn>> {
    let mut columns = Vec::new();
    for column in metadata.field_columns() {
        let schema = &column.column_schema;
        let options = schema.storage_options().map_err(|e| {
            UnexpectedSnafu {
                reason: format!(
                    "invalid storage options of column {} in region {}: {e}",
                    schema.name, metadata.region_id
                ),
            }
            .build()
        })?;
        if let Some(options) = options
            && options.storage_type == StorageType::External
        {
            columns.push(ExternalColumn {
                name: schema.name.clone(),
                threshold: options.threshold().as_bytes() as usize,
            });
        }
    }
    Ok(columns)
}

/// Location of one value in a blob object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BlobRef {
    offset: u64,
    compressed_len: u64,
    len: u64,
}

impl BlobRef {
    fn encode(&self) -> String {
        format!(
            "{REF_PREFIX}{}:{}:{}",
            self.offset, self.compressed_len, self.len
        )
    }

    /// Decodes `value`, or returns `None` when it is an inline value.
    fn decode(value: &[u8]) -> Result<Option<Self>> {
        let Some(body) = value.strip_prefix(REF_PREFIX.as_bytes()) else {
            return Ok(None);
        };
        let invalid = || {
            UnexpectedSnafu {
                reason: format!(
                    "invalid blob reference {:?}",
                    String::from_utf8_lossy(value)
                ),
            }
            .build()
        };
        let text = std::str::from_utf8(body).map_err(|_| invalid())?;
        let mut parts = text.split(':').map(|p| p.parse::<u64>());
        let mut next = || parts.next().and_then(|p| p.ok()).ok_or_else(invalid);
        Ok(Some(Self {
            offset: next()?,
            compressed_len: next()?,
            len: next()?,
        }))
    }

    fn range(&self) -> Range<u64> {
        self.offset..self.offset + self.compressed_len
    }
}

/// A value to store externally, compressed.
pub(crate) struct CompressedValue {
    data: Vec<u8>,
    len: u64,
}

/// Compresses the values of `array` that must be stored externally: values at least
/// `threshold` long and values that look like references. Other positions are `None`.
pub(crate) async fn compress_external_values(
    array: ArrayRef,
    threshold: usize,
) -> Result<Vec<Option<CompressedValue>>> {
    // Compressing multi-MiB values is CPU-bound; keep it off the async workers.
    spawn_blocking_global(move || {
        byte_values(&array)?
            .map(|value| match value {
                Some(v) if v.len() >= threshold || v.starts_with(REF_PREFIX.as_bytes()) => {
                    let data = zstd::bulk::compress(v, ZSTD_LEVEL).map_err(|e| {
                        UnexpectedSnafu {
                            reason: format!("failed to compress blob value: {e}"),
                        }
                        .build()
                    })?;
                    Ok(Some(CompressedValue {
                        data,
                        len: v.len() as u64,
                    }))
                }
                _ => Ok(None),
            })
            .collect()
    })
    .await
    .context(JoinSnafu)?
}

/// Appends values of one SST to its blob object.
pub(crate) struct BlobFileWriter {
    writer: object_store::Writer,
    offset: u64,
    /// Bytes the uploader can buffer.
    buffer_bytes: usize,
}

impl BlobFileWriter {
    pub(crate) async fn new(
        store: &ObjectStore,
        path: &str,
        chunk: usize,
        concurrency: usize,
    ) -> Result<Self> {
        let writer = store
            .writer_with(path)
            .chunk(chunk)
            .concurrent(concurrency)
            .await
            .context(OpenDalSnafu)?;
        Ok(Self {
            writer,
            offset: 0,
            buffer_bytes: chunk.saturating_mul(concurrency),
        })
    }

    /// Bytes written to the blob object so far.
    pub(crate) fn written_bytes(&self) -> u64 {
        self.offset
    }

    /// Upper bound of bytes held by the uploader.
    pub(crate) fn buffer_bytes(&self) -> usize {
        self.buffer_bytes
    }

    /// Appends the compressed `values` and returns `array` with each appended value replaced
    /// by its reference.
    pub(crate) async fn externalize(
        &mut self,
        array: &ArrayRef,
        values: Vec<Option<CompressedValue>>,
    ) -> Result<ArrayRef> {
        let mut refs = Vec::with_capacity(values.len());
        for value in values {
            refs.push(match value {
                Some(value) => {
                    let blob_ref = BlobRef {
                        offset: self.offset,
                        compressed_len: value.data.len() as u64,
                        len: value.len,
                    };
                    self.offset += blob_ref.compressed_len;
                    self.writer.write(value.data).await.context(OpenDalSnafu)?;
                    Some(blob_ref.encode())
                }
                None => None,
            });
        }
        let mut builder = ByteBuilder::new(array.data_type(), refs.len());
        for (value, blob_ref) in byte_values(array)?.zip(&refs) {
            match blob_ref {
                Some(blob_ref) => builder.append(Some(blob_ref.as_bytes()))?,
                None => builder.append(value)?,
            }
        }
        Ok(builder.finish())
    }

    /// Completes the object and returns its footer entries.
    pub(crate) async fn finish(mut self, columns: &[ExternalColumn]) -> Result<Vec<KeyValue>> {
        self.writer.close().await.context(OpenDalSnafu)?;
        let mut entries: Vec<_> = columns
            .iter()
            .map(|column| {
                KeyValue::new(
                    format!("{BLOB_COLUMN_KEY_PREFIX}{}", column.name),
                    String::new(),
                )
            })
            .collect();
        entries.push(KeyValue::new(
            BLOB_FILE_SIZE_KEY.to_string(),
            self.offset.to_string(),
        ));
        Ok(entries)
    }

    pub(crate) async fn abort(mut self) -> Result<()> {
        self.writer.abort().await.context(OpenDalSnafu)
    }
}

/// A decoded batch whose references are resolved chunk by chunk.
pub(crate) struct PendingBatch {
    batch: RecordBatch,
    targets: Vec<(usize, Vec<Option<BlobRef>>)>,
    next_row: usize,
}

/// Resolves references of an SST's external columns.
pub(crate) struct BlobResolver {
    store: ObjectStore,
    path: String,
    columns: Vec<String>,
}

impl BlobResolver {
    pub(crate) fn new(store: ObjectStore, sst_path: &str, columns: Vec<String>) -> Self {
        Self {
            store,
            path: blob_path(sst_path),
            columns,
        }
    }

    /// Parses references in `batch`. Returns `None` when the batch holds no reference.
    pub(crate) fn prepare(&self, batch: RecordBatch) -> Result<Option<PendingBatch>> {
        let mut targets = Vec::new();
        for name in &self.columns {
            if let Some((index, _)) = batch.schema().column_with_name(name) {
                let refs = byte_values(batch.column(index))?
                    .map(|v| Ok(v.map(BlobRef::decode).transpose()?.flatten()))
                    .collect::<Result<Vec<_>>>()?;
                if refs.iter().any(Option::is_some) {
                    targets.push((index, refs));
                }
            }
        }
        Ok((!targets.is_empty()).then_some(PendingBatch {
            batch,
            targets,
            next_row: 0,
        }))
    }

    /// Resolves the next rows of `pending`, holding at most [MAX_RESOLVED_BATCH_BYTES] of
    /// external values (or one row). Values of later rows are not fetched until asked for.
    pub(crate) async fn next_chunk(
        &self,
        pending: &mut PendingBatch,
    ) -> Result<Option<RecordBatch>> {
        let start = pending.next_row;
        if start >= pending.batch.num_rows() {
            return Ok(None);
        }
        let mut end = start;
        let mut bytes = 0usize;
        while end < pending.batch.num_rows() {
            let row_bytes: usize = pending
                .targets
                .iter()
                .map(|(_, refs)| refs[end].map_or(0, |r| r.len as usize))
                .sum();
            if end > start && bytes + row_bytes > MAX_RESOLVED_BATCH_BYTES {
                break;
            }
            bytes += row_bytes;
            end += 1;
        }
        pending.next_row = end;
        self.resolve_rows(&pending.batch, &pending.targets, start..end)
            .await
            .map(Some)
    }

    async fn resolve_rows(
        &self,
        batch: &RecordBatch,
        targets: &[(usize, Vec<Option<BlobRef>>)],
        rows: Range<usize>,
    ) -> Result<RecordBatch> {
        let sliced = batch.slice(rows.start, rows.len());
        let targets: Vec<_> = targets
            .iter()
            .map(|(index, refs)| (*index, refs[rows.clone()].to_vec()))
            .collect();
        let mut wanted: Vec<BlobRef> = targets
            .iter()
            .flat_map(|(_, refs)| refs.iter().flatten().copied())
            .collect();
        wanted.sort_by_key(|r| r.offset);
        wanted.dedup();
        let fetched = self.fetch(&wanted).await?;
        // Decompressing up to MAX_RESOLVED_BATCH_BYTES is CPU-bound; keep it off the async workers.
        spawn_blocking_global(move || decode_rows(sliced, targets, &fetched))
            .await
            .context(JoinSnafu)?
    }

    /// Fetches `refs` (sorted by offset) with coalesced range requests.
    async fn fetch(&self, refs: &[BlobRef]) -> Result<Vec<(Range<u64>, Bytes)>> {
        let mut ranges: Vec<Range<u64>> = Vec::new();
        for r in refs {
            let range = r.range();
            match ranges.last_mut() {
                Some(last)
                    if range.start <= last.end + COALESCE_GAP
                        && range.end - last.start <= MAX_REQUEST_BYTES =>
                {
                    last.end = last.end.max(range.end);
                }
                _ => ranges.push(range),
            }
        }
        let reads = ranges.into_iter().map(|range| async move {
            let data = self
                .store
                .read_with(&self.path)
                .range(range.clone())
                .await
                .context(OpenDalSnafu)?;
            Ok::<_, Error>((range, data.to_bytes()))
        });
        futures::future::try_join_all(reads).await
    }
}

/// Returns the values of a string or binary column as bytes.
fn byte_values(array: &ArrayRef) -> Result<Box<dyn Iterator<Item = Option<&[u8]>> + '_>> {
    match array.data_type() {
        DataType::Binary => Ok(Box::new(array.as_binary::<i32>().iter())),
        DataType::Utf8 => Ok(Box::new(
            array
                .as_string::<i32>()
                .iter()
                .map(|v| v.map(str::as_bytes)),
        )),
        other => UnexpectedSnafu {
            reason: format!("external storage does not support type {other}"),
        }
        .fail(),
    }
}

/// Builds a string or binary array from bytes.
enum ByteBuilder {
    Binary(BinaryBuilder),
    String(StringBuilder),
}

impl ByteBuilder {
    fn new(data_type: &DataType, capacity: usize) -> Self {
        match data_type {
            DataType::Utf8 => ByteBuilder::String(StringBuilder::with_capacity(capacity, 0)),
            _ => ByteBuilder::Binary(BinaryBuilder::with_capacity(capacity, 0)),
        }
    }

    fn append(&mut self, value: Option<&[u8]>) -> Result<()> {
        match (self, value) {
            (ByteBuilder::Binary(builder), Some(value)) => builder.append_value(value),
            (ByteBuilder::Binary(builder), None) => builder.append_null(),
            (ByteBuilder::String(builder), Some(value)) => {
                let value = std::str::from_utf8(value).map_err(|e| {
                    UnexpectedSnafu {
                        reason: format!("blob value is not UTF-8: {e}"),
                    }
                    .build()
                })?;
                builder.append_value(value)
            }
            (ByteBuilder::String(builder), None) => builder.append_null(),
        }
        Ok(())
    }

    fn finish(self) -> ArrayRef {
        match self {
            ByteBuilder::Binary(mut builder) => Arc::new(builder.finish()),
            ByteBuilder::String(mut builder) => Arc::new(builder.finish()),
        }
    }
}

/// Replaces references of `targets` in `batch` with values from `fetched`, which holds
/// coalesced ranges sorted by offset that cover every reference.
fn decode_rows(
    batch: RecordBatch,
    targets: Vec<(usize, Vec<Option<BlobRef>>)>,
    fetched: &[(Range<u64>, Bytes)],
) -> Result<RecordBatch> {
    let value_of = |r: &BlobRef| -> Result<Vec<u8>> {
        let corrupted = || {
            UnexpectedSnafu {
                reason: format!(
                    "blob value at {} of {} bytes is missing from the blob object",
                    r.offset, r.compressed_len
                ),
            }
            .build()
        };
        let pos = fetched
            .partition_point(|(range, _)| range.start <= r.offset)
            .checked_sub(1)
            .ok_or_else(corrupted)?;
        let (range, data) = &fetched[pos];
        let begin = (r.offset - range.start) as usize;
        let compressed = data
            .get(begin..begin + r.compressed_len as usize)
            .ok_or_else(corrupted)?;
        zstd::bulk::decompress(compressed, r.len as usize).map_err(|e| {
            UnexpectedSnafu {
                reason: format!("failed to decompress blob value at {}: {e}", r.offset),
            }
            .build()
        })
    };

    let mut arrays = batch.columns().to_vec();
    for (index, refs) in targets {
        let column = batch.column(index);
        let mut builder = ByteBuilder::new(column.data_type(), refs.len());
        for (value, blob_ref) in byte_values(column)?.zip(&refs) {
            match blob_ref {
                Some(blob_ref) => builder.append(Some(&value_of(blob_ref)?))?,
                None => builder.append(value)?,
            }
        }
        arrays[index] = builder.finish();
    }
    RecordBatch::try_new(batch.schema(), arrays).context(NewRecordBatchSnafu)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_only_prefixed_values_are_references() {
        let blob_ref = BlobRef {
            offset: 7,
            compressed_len: 3,
            len: 5,
        };
        assert_eq!(
            Some(blob_ref),
            BlobRef::decode(blob_ref.encode().as_bytes()).unwrap()
        );
        assert_eq!(None, BlobRef::decode(b"7:3:5").unwrap());
        assert!(BlobRef::decode(b"\0greptime-blob:7:x").is_err());
    }
}
