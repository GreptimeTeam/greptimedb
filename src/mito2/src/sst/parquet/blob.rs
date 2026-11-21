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
use datatypes::arrow::array::{Array, ArrayRef, AsArray, BinaryBuilder, StringBuilder};
use datatypes::arrow::datatypes::DataType;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::schema::StorageType;
use futures::{StreamExt, TryStreamExt};
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
pub(crate) const MAX_RESOLVED_BATCH_BYTES: usize = 128 * 1024 * 1024;
/// Neighboring values closer than this are fetched in one request.
const COALESCE_GAP: u64 = 1024 * 1024;
/// Upper bound of one coalesced request.
const MAX_REQUEST_BYTES: u64 = 16 * 1024 * 1024;
/// Requests in flight for one chunk of values.
const FETCH_CONCURRENCY: usize = 8;
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
        let blob_ref = Self {
            offset: next()?,
            compressed_len: next()?,
            len: next()?,
        };
        if parts.next().is_some()
            || blob_ref
                .offset
                .checked_add(blob_ref.compressed_len)
                .is_none()
        {
            return Err(invalid());
        }
        Ok(Some(blob_ref))
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
        let mut builder = ByteBuilder::new(array.data_type(), refs.len(), 0);
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
        if let Err(error) = self.writer.close().await {
            // Nothing else holds the writer, so a failed close must not leave the upload behind.
            if let Err(abort_error) = self.writer.abort().await {
                common_telemetry::warn!(abort_error; "Failed to abort blob writer after close failed");
            }
            return Err(error).context(OpenDalSnafu);
        }
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
    /// Upper bound of resolved value bytes in one output batch.
    chunk_bytes: usize,
}

impl BlobResolver {
    pub(crate) fn new(store: ObjectStore, sst_path: &str, columns: Vec<String>) -> Self {
        Self {
            store,
            path: blob_path(sst_path),
            columns,
            chunk_bytes: MAX_RESOLVED_BATCH_BYTES,
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

    /// Resolves the next rows of `pending`, holding at most `chunk_bytes` of
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
            if end > start && bytes + row_bytes > self.chunk_bytes {
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
        let reads = coalesce_ranges(refs).into_iter().map(|range| async move {
            let data = self
                .store
                .read_with(&self.path)
                .range(range.clone())
                .await
                .context(OpenDalSnafu)?;
            Ok::<_, Error>((range, data.to_bytes()))
        });
        futures::stream::iter(reads)
            .buffered(FETCH_CONCURRENCY)
            .try_collect()
            .await
    }
}

/// Merges the ranges of `refs` (sorted by offset) into requests. A request never spans more
/// than twice the wanted bytes in it, so sparse selections read at most twice what they need.
fn coalesce_ranges(refs: &[BlobRef]) -> Vec<Range<u64>> {
    // (range, wanted bytes in the range)
    let mut ranges: Vec<(Range<u64>, u64)> = Vec::new();
    for r in refs {
        let range = r.range();
        match ranges.last_mut() {
            Some((last, wanted))
                if range.start <= last.end + COALESCE_GAP
                    && range.end.saturating_sub(last.start)
                        <= MAX_REQUEST_BYTES.min(2 * (*wanted + r.compressed_len)) =>
            {
                last.end = last.end.max(range.end);
                *wanted += r.compressed_len;
            }
            _ => ranges.push((range, r.compressed_len)),
        }
    }
    ranges.into_iter().map(|(range, _)| range).collect()
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
    fn new(data_type: &DataType, items: usize, data_bytes: usize) -> Self {
        match data_type {
            DataType::Utf8 => ByteBuilder::String(StringBuilder::with_capacity(items, data_bytes)),
            _ => ByteBuilder::Binary(BinaryBuilder::with_capacity(items, data_bytes)),
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
    // Locates the compressed frame of `r` and checks that it decodes to `r.len` bytes.
    let frame_of = |r: &BlobRef| -> Result<&[u8]> {
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
        let content_size = zstd::zstd_safe::get_frame_content_size(compressed)
            .ok()
            .flatten();
        if content_size != Some(r.len) {
            return Err(corrupted());
        }
        Ok(compressed)
    };
    let value_of = |r: &BlobRef| -> Result<Vec<u8>> {
        let value = zstd::bulk::decompress(frame_of(r)?, r.len as usize).map_err(|e| {
            UnexpectedSnafu {
                reason: format!("failed to decompress blob value at {}: {e}", r.offset),
            }
            .build()
        })?;
        if value.len() as u64 != r.len {
            return UnexpectedSnafu {
                reason: format!(
                    "blob value at {} decompressed to {} bytes, expected {}",
                    r.offset,
                    value.len(),
                    r.len
                ),
            }
            .fail();
        }
        Ok(value)
    };

    let mut arrays = batch.columns().to_vec();
    for (index, refs) in targets {
        let column = batch.column(index);
        // Reference lengths size the builder, so check them against their frames first.
        let mut data_bytes = 0;
        for (value, blob_ref) in byte_values(column)?.zip(&refs) {
            data_bytes += match blob_ref {
                Some(blob_ref) => {
                    frame_of(blob_ref)?;
                    blob_ref.len as usize
                }
                None => value.map_or(0, <[u8]>::len),
            };
        }
        let mut builder = ByteBuilder::new(column.data_type(), refs.len(), data_bytes);
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
    use datatypes::arrow::array::{BinaryArray, StringArray};

    use super::*;

    #[tokio::test]
    async fn test_values_at_threshold_or_prefixed_are_external() {
        let at_threshold = vec![1u8; 8];
        let below = vec![1u8; 7];
        let prefixed = format!("{REF_PREFIX}x");
        let binary: ArrayRef = Arc::new(BinaryArray::from(vec![
            Some(at_threshold.as_slice()),
            Some(below.as_slice()),
            None,
            Some(prefixed.as_bytes()),
        ]));
        let string: ArrayRef = Arc::new(StringArray::from(vec![
            Some("12345678"),
            Some("1234567"),
            None,
            Some(prefixed.as_str()),
        ]));
        for array in [binary, string] {
            let external: Vec<_> = compress_external_values(array, 8)
                .await
                .unwrap()
                .iter()
                .map(Option::is_some)
                .collect();
            assert_eq!(vec![true, false, false, true], external);
        }
    }

    #[tokio::test]
    async fn test_resolver_restores_values_across_chunks() {
        let store = ObjectStore::new(object_store::services::Memory::default()).unwrap();
        let sst_path = "region/sst.parquet";
        let columns = vec![
            ExternalColumn {
                name: "bin".to_string(),
                threshold: 16,
            },
            ExternalColumn {
                name: "str".to_string(),
                threshold: 16,
            },
        ];
        let long = |i: usize| format!("{i:02}-").repeat(20);
        let binary: ArrayRef = Arc::new(BinaryArray::from_iter((0..12).map(|i| match i % 3 {
            0 => Some(long(i).into_bytes()),
            1 => None,
            _ => Some(b"short".to_vec()),
        })));
        let string: ArrayRef = Arc::new(StringArray::from_iter((0..12).map(|i| match i % 4 {
            0 => None,
            1 => Some("tiny".to_string()),
            _ => Some(long(i)),
        })));
        let original =
            RecordBatch::try_from_iter([("bin", binary.clone()), ("str", string.clone())]).unwrap();

        let mut writer = BlobFileWriter::new(&store, &blob_path(sst_path), 1024, 1)
            .await
            .unwrap();
        let mut stored = Vec::new();
        for (array, column) in [binary, string].iter().zip(&columns) {
            let values = compress_external_values(array.clone(), column.threshold)
                .await
                .unwrap();
            stored.push(writer.externalize(array, values).await.unwrap());
        }
        writer.finish(&columns).await.unwrap();
        let stored = RecordBatch::try_new(original.schema(), stored).unwrap();
        assert_ne!(original, stored);

        let mut resolver = BlobResolver::new(
            store,
            sst_path,
            columns.iter().map(|c| c.name.clone()).collect(),
        );
        // Each long value is 60 bytes, so a chunk holds at most two rows of them.
        resolver.chunk_bytes = 130;
        let mut pending = resolver.prepare(stored).unwrap().unwrap();
        let mut chunks = Vec::new();
        while let Some(chunk) = resolver.next_chunk(&mut pending).await.unwrap() {
            chunks.push(chunk);
        }
        assert!(chunks.len() > 2, "{}", chunks.len());
        let resolved =
            datatypes::arrow::compute::concat_batches(&original.schema(), &chunks).unwrap();
        assert_eq!(original, resolved);
    }

    #[test]
    fn test_reference_length_is_checked_before_allocation() {
        let frame = Bytes::from(zstd::bulk::compress(b"value", 1).unwrap());
        let blob_ref = BlobRef {
            offset: 0,
            compressed_len: frame.len() as u64,
            len: 1 << 60,
        };
        let column: ArrayRef = Arc::new(BinaryArray::from(vec![blob_ref.encode().as_bytes()]));
        let batch = RecordBatch::try_from_iter([("bin", column)]).unwrap();
        let fetched = [(blob_ref.range(), frame)];
        assert!(decode_rows(batch, vec![(0, vec![Some(blob_ref)])], &fetched).is_err());
    }

    #[test]
    fn test_sparse_values_are_not_coalesced_across_large_gaps() {
        let refs = |offsets: &[u64], len: u64| -> Vec<BlobRef> {
            offsets
                .iter()
                .map(|offset| BlobRef {
                    offset: *offset,
                    compressed_len: len,
                    len,
                })
                .collect()
        };
        // Adjacent values share one request.
        assert_eq!(vec![0..300], coalesce_ranges(&refs(&[0, 100, 200], 100)));
        // A gap wider than the wanted bytes starts a new request.
        assert_eq!(
            vec![0..100, 1000..1100],
            coalesce_ranges(&refs(&[0, 1000], 100))
        );
        // Gaps add up: each one is small next to the wanted bytes so far, but the request
        // still reads at most twice the wanted bytes.
        let mut offsets = vec![0];
        for gap in (1..16).map(|i| i * 100) {
            offsets.push(offsets.last().unwrap() + 100 + gap);
        }
        let read: u64 = coalesce_ranges(&refs(&offsets, 100))
            .iter()
            .map(|r| r.end - r.start)
            .sum();
        assert!(read <= 2 * 16 * 100, "read {read} bytes");
    }

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
        for corrupted in [
            "\0greptime-blob:7:x".to_string(),
            "\0greptime-blob:7:3:5:1".to_string(),
            format!("\0greptime-blob:{}:1:1", u64::MAX),
        ] {
            assert!(
                BlobRef::decode(corrupted.as_bytes()).is_err(),
                "{corrupted:?}"
            );
        }
    }
}
