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

//! Out-of-line storage for large string and binary field values.
//!
//! A column is stored out of line in one SST when the region sets
//! `experimental_sst_blob_threshold` and the SST's first written batch holds a value of at least
//! that many bytes. Every value of that column in the SST is then appended,
//! zstd compressed, to a sidecar blob object next to the SST, and the parquet column holds
//! `offset:compressed_len:len` references instead. Each such column has a footer key
//! [BLOB_COLUMN_KEY_PREFIX] followed by its name. The sidecar shares the SST's file id and is deleted with it.
//!
//! Readers resolve references right after decoding a row group, so filters, merges and
//! compaction see the original values.

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use common_runtime::spawn_blocking_global;
use datatypes::arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, BinaryBuilder, GenericByteArray, StringArray,
    StringBuilder,
};
use datatypes::arrow::datatypes::{ByteArrayType, DataType};
use datatypes::arrow::record_batch::RecordBatch;
use object_store::ObjectStore;
use parquet::file::metadata::ParquetMetaData;
use snafu::ResultExt;

use crate::error::{Error, JoinSnafu, NewRecordBatchSnafu, OpenDalSnafu, Result, UnexpectedSnafu};

/// Prefix of footer keys that mark a column as stored out of line; the column name follows it.
/// One key per column keeps any column name intact.
pub(crate) const BLOB_COLUMN_KEY_PREFIX: &str = "greptime:blob_column:";
/// Footer key holding the uncompressed bytes of all out-of-line values, written when the file
/// finishes. Size estimates of row groups must include these bytes: the parquet column only
/// holds references.
pub(crate) const BLOB_BYTES_KEY: &str = "greptime:blob_bytes";
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

/// Returns the out-of-line columns recorded in the footer.
pub(crate) fn blob_columns(meta: &ParquetMetaData) -> Vec<String> {
    meta.file_metadata()
        .key_value_metadata()
        .into_iter()
        .flatten()
        .filter_map(|kv| kv.key.strip_prefix(BLOB_COLUMN_KEY_PREFIX))
        .map(str::to_string)
        .collect()
}

/// Returns `(num_rows, uncompressed_bytes)` of each row group, with out-of-line values spread
/// over row groups by row count.
pub(crate) fn row_group_sizes(meta: &ParquetMetaData) -> impl Iterator<Item = (u64, u64)> + '_ {
    let blob_bytes = meta
        .file_metadata()
        .key_value_metadata()
        .and_then(|kvs| kvs.iter().find(|kv| kv.key == BLOB_BYTES_KEY))
        .and_then(|kv| kv.value.as_deref())
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);
    let total_rows = meta.file_metadata().num_rows().max(1) as u64;
    meta.row_groups().iter().map(move |rg| {
        let rows = rg.num_rows() as u64;
        let bytes: u64 = rg
            .columns()
            .iter()
            .map(|c| c.uncompressed_size() as u64)
            .sum();
        (rows, bytes + blob_bytes * rows / total_rows)
    })
}

/// Returns the names of string and binary columns among `candidates` whose longest value in
/// `batch` reaches `threshold` bytes.
pub(crate) fn pick_blob_columns<'a>(
    candidates: impl Iterator<Item = &'a str>,
    batch: &RecordBatch,
    threshold: usize,
) -> Vec<String> {
    candidates
        .filter(|name| {
            batch.column_by_name(name).is_some_and(|array| {
                let offsets: Vec<usize> = match array.data_type() {
                    DataType::Binary => offsets(array.as_binary::<i32>()),
                    DataType::Utf8 => offsets(array.as_string::<i32>()),
                    _ => return false,
                };
                offsets.windows(2).any(|w| w[1] - w[0] >= threshold)
            })
        })
        .map(str::to_string)
        .collect()
}

fn offsets<T: ByteArrayType<Offset = i32>>(array: &GenericByteArray<T>) -> Vec<usize> {
    array.value_offsets().iter().map(|o| *o as usize).collect()
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
        format!("{}:{}:{}", self.offset, self.compressed_len, self.len)
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        let invalid = || {
            UnexpectedSnafu {
                reason: format!(
                    "invalid blob reference {:?}",
                    String::from_utf8_lossy(bytes)
                ),
            }
            .build()
        };
        let text = std::str::from_utf8(bytes).map_err(|_| invalid())?;
        let mut parts = text.split(':').map(|p| p.parse::<u64>());
        let mut next = || parts.next().and_then(|p| p.ok()).ok_or_else(invalid);
        Ok(Self {
            offset: next()?,
            compressed_len: next()?,
            len: next()?,
        })
    }

    fn range(&self) -> Range<u64> {
        self.offset..self.offset + self.compressed_len
    }
}

/// Appends values of one SST to its blob object.
pub(crate) struct BlobFileWriter {
    /// Uncompressed bytes of values appended so far.
    value_bytes: u64,
    writer: object_store::Writer,
    offset: u64,
}

impl BlobFileWriter {
    pub(crate) async fn new(store: &ObjectStore, path: String) -> Result<Self> {
        let writer = store
            .writer_with(&path)
            .chunk(8 * 1024 * 1024)
            .concurrent(4)
            .await
            .context(OpenDalSnafu)?;
        Ok(Self {
            value_bytes: 0,
            writer,
            offset: 0,
        })
    }

    async fn append(&mut self, compressed: Vec<u8>, len: u64) -> Result<BlobRef> {
        let blob_ref = BlobRef {
            offset: self.offset,
            compressed_len: compressed.len() as u64,
            len,
        };
        self.offset += blob_ref.compressed_len;
        self.value_bytes += len;
        self.writer.write(compressed).await.context(OpenDalSnafu)?;
        Ok(blob_ref)
    }

    /// Bytes written to the blob object so far.
    pub(crate) fn written_bytes(&self) -> u64 {
        self.offset
    }

    /// Replaces values of `columns` in `batch` with references to values appended to the blob.
    pub(crate) async fn externalize(
        &mut self,
        batch: RecordBatch,
        columns: &[String],
    ) -> Result<RecordBatch> {
        let mut arrays = batch.columns().to_vec();
        for name in columns {
            let Some((index, _)) = batch.schema().column_with_name(name) else {
                continue;
            };
            let array = batch.column(index).clone();
            let is_string = *array.data_type() == DataType::Utf8;
            // Compressing multi-MiB values is CPU-bound; keep it off the async workers.
            let compressed = spawn_blocking_global(move || compress_values(&array))
                .await
                .context(JoinSnafu)??;
            let mut refs = Vec::with_capacity(compressed.len());
            for value in compressed {
                refs.push(match value {
                    Some(value) => Some(self.append(value.data, value.len).await?.encode()),
                    None => None,
                });
            }
            arrays[index] = if is_string {
                Arc::new(StringArray::from(refs)) as ArrayRef
            } else {
                Arc::new(BinaryArray::from_iter(refs)) as ArrayRef
            };
        }
        RecordBatch::try_new(batch.schema(), arrays).context(NewRecordBatchSnafu)
    }

    /// Completes the object and returns the uncompressed bytes of its values.
    pub(crate) async fn finish(mut self) -> Result<u64> {
        self.writer.close().await.context(OpenDalSnafu)?;
        Ok(self.value_bytes)
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

/// Resolves references of an SST's out-of-line columns.
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

    /// Parses references in `batch`. Returns `None` when the batch has no out-of-line column.
    pub(crate) fn prepare(&self, batch: RecordBatch) -> Result<Option<PendingBatch>> {
        let mut targets = Vec::new();
        for name in &self.columns {
            if let Some((index, _)) = batch.schema().column_with_name(name) {
                let refs = byte_values(batch.column(index))?
                    .map(|v| v.map(BlobRef::decode).transpose())
                    .collect::<Result<Vec<_>>>()?;
                targets.push((index, refs));
            }
        }
        Ok((!targets.is_empty()).then_some(PendingBatch {
            batch,
            targets,
            next_row: 0,
        }))
    }

    /// Resolves the next rows of `pending`, holding at most [MAX_RESOLVED_BATCH_BYTES] of
    /// values (or one row). Values of later rows are not fetched until asked for.
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

/// Returns the values of a string or binary blob column as bytes.
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
            reason: format!("blob column has unsupported type {other}"),
        }
        .fail(),
    }
}

struct CompressedValue {
    data: Vec<u8>,
    len: u64,
}

fn compress_values(array: &ArrayRef) -> Result<Vec<Option<CompressedValue>>> {
    byte_values(array)?
        .map(|value| {
            value
                .map(|v| {
                    let compressed = zstd::bulk::compress(v, ZSTD_LEVEL).map_err(|e| {
                        UnexpectedSnafu {
                            reason: format!("failed to compress blob value: {e}"),
                        }
                        .build()
                    })?;
                    Ok(CompressedValue {
                        data: compressed,
                        len: v.len() as u64,
                    })
                })
                .transpose()
        })
        .collect()
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
        let total: usize = refs.iter().flatten().map(|r| r.len as usize).sum();
        arrays[index] = match batch.column(index).data_type() {
            DataType::Binary => {
                let mut builder = BinaryBuilder::with_capacity(refs.len(), total);
                for r in &refs {
                    match r {
                        Some(r) => builder.append_value(value_of(r)?),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish()) as ArrayRef
            }
            _ => {
                let mut builder = StringBuilder::with_capacity(refs.len(), total);
                for r in &refs {
                    match r {
                        Some(r) => {
                            let value = String::from_utf8(value_of(r)?).map_err(|e| {
                                UnexpectedSnafu {
                                    reason: format!("blob value is not UTF-8: {e}"),
                                }
                                .build()
                            })?;
                            builder.append_value(value)
                        }
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish()) as ArrayRef
            }
        };
    }
    RecordBatch::try_new(batch.schema(), arrays).context(NewRecordBatchSnafu)
}
