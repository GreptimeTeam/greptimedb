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

//! Lazy range reads with per-stream traversal and dictionary predicate evaluation.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use common_recordbatch::filter::SimpleFilterEvaluator;
use datatypes::arrow::array::{ArrayRef, StringArray, UInt32Array, UInt64Array};
use datatypes::arrow::datatypes::SchemaRef;
use datatypes::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datatypes::value::Value;
use futures::stream::BoxStream;
use object_store::ObjectStore;
use snafu::ResultExt;

use crate::error::{NewRecordBatchSnafu, OpenDalSnafu, RecordBatchSnafu, Result, SerdeJsonSnafu};
use crate::series_index::artifact::ArtifactFormat;
use crate::series_index::columnar::codec::{self, Dictionary};
use crate::series_index::columnar::metadata::{self, Block, Column, Page, PageReference, Trailer};
use crate::series_index::columnar::{
    MANIFEST_LIMIT, Manifest, VERSION, invalid, require, unsupported,
};
use crate::series_index::format::{FormatReader, SeriesReadRequest};
use crate::series_index::{
    MetricSeriesId, ROW_COUNT_COLUMN, SeriesIndexFileHandle, TABLE_ID_COLUMN, TSID_COLUMN,
};
use crate::sst::parquet::helper::fetch_byte_ranges;

pub(crate) struct ColumnarReader {
    store: ObjectStore,
    manifest: Arc<Manifest>,
    handle: SeriesIndexFileHandle,
}
impl ColumnarReader {
    pub(crate) async fn open(store: ObjectStore, handle: SeriesIndexFileHandle) -> Result<Self> {
        let entry = handle.entry();
        let artifact = entry
            .artifact
            .as_ref()
            .ok_or_else(|| invalid("missing columnar artifact"))?;
        require(
            artifact.format == ArtifactFormat::ColumnarSeries
                && artifact.generation == entry.index_uuid,
            "series artifact identity mismatch",
        )?;
        let region = handle.file_id().region_id().as_u64();
        let path = format!("{region}/series/{}/manifest.json", entry.index_uuid);
        let mut objects = HashMap::with_capacity(artifact.objects.len());
        for object in &artifact.objects {
            require(
                objects
                    .insert(object.path.as_str(), object.file_size)
                    .is_none(),
                "duplicate series artifact object",
            )?;
        }
        let length = *objects
            .get(path.as_str())
            .ok_or_else(|| invalid("missing series manifest object"))?;
        require(
            length <= MANIFEST_LIMIT as u64,
            "series manifest exceeds size limit",
        )?;
        let bytes = read_range(&store, &path, 0, length).await?;
        let manifest: Manifest = serde_json::from_slice(&bytes).context(SerdeJsonSnafu)?;
        if manifest.version != VERSION {
            return Err(unsupported("series manifest version"));
        }
        require(
            manifest.format == ArtifactFormat::ColumnarSeries
                && manifest.region_id == region
                && manifest.generation == entry.index_uuid,
            "series manifest identity mismatch",
        )?;
        let mut coverage = entry.clone();
        coverage.artifact = None;
        coverage.file_size = 0;
        require(
            manifest.coverage == coverage,
            "series manifest coverage mismatch",
        )?;
        let mut names = HashSet::from([
            TABLE_ID_COLUMN.to_owned(),
            TSID_COLUMN.to_owned(),
            ROW_COUNT_COLUMN.to_owned(),
        ]);
        let mut ids = HashSet::new();
        for tag in &manifest.tags {
            require(
                names.insert(tag.name.clone()) && ids.insert(tag.column_id),
                "duplicate series tag name/id",
            )?;
        }
        let mut paths = HashSet::from([path]);
        let mut file_size = length;
        let mut previous = None;
        for (index, part) in manifest.files.iter().enumerate() {
            require(
                index as u64 == u64::from(part.part_id)
                    && part.first <= part.last
                    && part.num_series > 0
                    && part.file_size >= metadata::HEADER_SIZE + metadata::TRAILER_SIZE,
                "invalid series part entry",
            )?;
            if let Some(last) = previous {
                require(last < part.first, "unordered series parts")?;
            }
            previous = Some(part.last);
            let part_path = manifest.part_path(part.part_id);
            require(
                objects.get(part_path.as_str()) == Some(&part.file_size),
                "series part length mismatch",
            )?;
            paths.insert(part_path);
            file_size = file_size
                .checked_add(part.file_size)
                .ok_or_else(|| invalid("series artifact length overflow"))?;
        }
        require(
            paths.len() == artifact.objects.len()
                && artifact
                    .objects
                    .iter()
                    .all(|object| paths.contains(&object.path)),
            "series artifact object set mismatch",
        )?;
        require(
            file_size == artifact.file_size && file_size == entry.file_size,
            "series artifact size mismatch",
        )?;
        Ok(Self {
            store,
            manifest: Arc::new(manifest),
            handle,
        })
    }
}

impl FormatReader for ColumnarReader {
    fn schema(&self) -> SchemaRef {
        self.manifest.schema()
    }

    fn read(&self, request: SeriesReadRequest) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let schema = self.schema();
        let mut projection = Vec::new();
        for name in &request.projection {
            projection.push(
                schema
                    .index_of(name)
                    .map_err(|_| invalid(format!("unknown series projection {name}")))?,
            );
        }
        for filter in &request.filters {
            schema
                .index_of(filter.column_name())
                .map_err(|_| invalid(format!("unknown series filter {}", filter.column_name())))?;
        }
        let projected = Arc::new(
            schema
                .project(&projection)
                .map_err(|e| invalid(format!("series projection: {e}")))?,
        );
        let manifest = self.manifest.clone();
        let store = self.store.clone();
        let handle = self.handle.clone();
        let names: Vec<_> = schema
            .fields()
            .iter()
            .skip(1)
            .map(|f| f.name().clone())
            .collect();
        Ok(Box::pin(async_stream::try_stream! {
            // Retained across every yield, even after the reader object is dropped.
            let _handle = handle;
            for part in &manifest.files {
                if !matches_range(part.first, part.last, &request) { continue; }
                let path = manifest.part_path(part.part_id);
                let size = part.file_size;
                let framing = fetch_byte_ranges(&path, store.clone(), &[0..metadata::HEADER_SIZE, size-metadata::TRAILER_SIZE..size]).await.context(OpenDalSnafu)?;
                require(
                    framing.len() == 2
                        && framing[0].len() as u64 == metadata::HEADER_SIZE
                        && framing[1].len() as u64 == metadata::TRAILER_SIZE,
                    "series framing length mismatch",
                )?;
                metadata::validate_header(&framing[0])?;
                let trailer = Trailer::decode(&framing[1], size)?;
                require(trailer.root.first == part.first && trailer.root.last == part.last, "series part/root bounds mismatch")?;
                let root = load_metadata_page(&store, &path, &trailer.root, trailer.depth, trailer.payload_end, &names).await?;
                let (mut active, children) = match root {
                    Page::Leaf(blocks) => (Some(blocks), Vec::new()),
                    Page::Internal(children) => (None, children),
                };
                let mut child_iter = children.into_iter();
                let mut observed_rows = 0u64;
                let mut previous_payload_end = metadata::HEADER_SIZE;
                loop {
                    let blocks = if let Some(blocks) = active.take() { blocks } else {
                        let Some(child) = child_iter.next() else { break; };
                        if !matches_range(child.first, child.last, &request) { continue; }
                        match load_metadata_page(&store, &path, &child, 1, trailer.payload_end, &names).await? {
                            Page::Leaf(blocks) => blocks,
                            Page::Internal(_) => Err(unsupported("internal child in depth-two series index"))?,
                        }
                    };
                    for block in blocks {
                        observed_rows = observed_rows.checked_add(u64::from(block.rows)).ok_or_else(|| invalid("series part row count overflow"))?;
                        for column in &block.columns {
                            column.payload.validate(previous_payload_end, trailer.payload_end, false)?;
                            previous_payload_end = column.payload.end()?;
                        }
                        if !matches_range(block.first_id(), block.last_id(), &request) || !matches_statistics(&block, &request.filters) { continue; }
                        if let Some(mut output) = read_block(&store, &path, &block, &request, projected.clone()).await? {
                            while let Some(batch) = output.next_batch()? { yield batch; }
                        }
                    }
                }
                if request.ids.is_none() && request.table_id.is_none() {
                    require(observed_rows == part.num_series, "series part row count mismatch")?;
                }
            }
        }))
    }
}

fn matches_range(first: MetricSeriesId, last: MetricSeriesId, request: &SeriesReadRequest) -> bool {
    let (mut first, mut last) = (first, last);
    if let Some(table) = request.table_id {
        first = first.max(MetricSeriesId {
            table_id: table,
            tsid: 0,
        });
        last = last.min(MetricSeriesId {
            table_id: table,
            tsid: u64::MAX,
        });
        if first > last {
            return false;
        }
    }
    request
        .ids
        .as_ref()
        .is_none_or(|ids| ids.range(first..=last).next().is_some())
}

fn matches_statistics(block: &Block, filters: &[SimpleFilterEvaluator]) -> bool {
    for filter in filters {
        if !filter.is_eq() {
            continue;
        }
        if let Some(Value::String(value)) = filter.literal_value()
            && let Some((min, max)) = block
                .columns
                .iter()
                .find(|c| c.name == filter.column_name())
                .and_then(|c| c.bounds.as_ref())
            && (value.as_utf8() < min.as_str() || value.as_utf8() > max.as_str())
        {
            return false;
        }
    }
    true
}

async fn read_range(store: &ObjectStore, path: &str, start: u64, end: u64) -> Result<bytes::Bytes> {
    let mut buffers = fetch_byte_ranges(path, store.clone(), std::slice::from_ref(&(start..end)))
        .await
        .context(OpenDalSnafu)?;
    let bytes = buffers
        .pop()
        .ok_or_else(|| invalid("missing series range response"))?;
    require(
        bytes.len() as u64 == end - start,
        "series range length mismatch",
    )?;
    Ok(bytes)
}

/// TODO: integrate with Mito's cache using store/region, generation, part, reference,
/// checksum and codec IDs. The immutable path and reference remain available here.
async fn load_metadata_page(
    store: &ObjectStore,
    path: &str,
    reference: &PageReference,
    depth: u32,
    payload_end: u64,
    names: &[String],
) -> Result<Page> {
    if reference.payload.encoding != 1 {
        return Err(unsupported(format!(
            "series metadata encoding {}",
            reference.payload.encoding
        )));
    }
    let bytes = read_range(
        store,
        path,
        reference.payload.offset,
        reference.payload.end()?,
    )
    .await?;
    // TODO: benchmark inline versus offloaded codecs for latency, throughput,
    // and async-worker responsiveness before adding scheduling overhead.
    let decoded = reference.payload.unpack(&bytes)?;
    Page::decode(&decoded, reference, depth, payload_end, names)
}

#[derive(Debug)]
enum Decoded {
    Integers(Vec<u64>),
    Strings(Dictionary),
    Table(u32, usize),
    Null(usize),
}
impl Decoded {
    fn value_bytes(&self, index: usize) -> usize {
        match self {
            Self::Integers(_) => 8,
            Self::Table(_, _) => 4,
            Self::Null(_) => 5,
            Self::Strings(dict) => {
                dict.values[dict.codes[index] as usize]
                    .as_ref()
                    .map_or(0, String::len)
                    + 5
            }
        }
    }

    fn array(&self, selected: &[usize]) -> ArrayRef {
        match self {
            Self::Integers(values) => Arc::new(UInt64Array::from_iter_values(
                selected.iter().map(|&i| values[i]),
            )),
            Self::Strings(dict) => Arc::new(StringArray::from_iter(
                selected
                    .iter()
                    .map(|&i| dict.values[dict.codes[i] as usize].as_deref()),
            )),
            Self::Table(value, _) => Arc::new(UInt32Array::from(vec![*value; selected.len()])),
            Self::Null(_) => Arc::new(StringArray::from(vec![None::<&str>; selected.len()])),
        }
    }
    fn filter(&self, filter: &SimpleFilterEvaluator, selected: &mut Vec<usize>) -> Result<()> {
        if let Self::Strings(dict) = self {
            let values: ArrayRef = Arc::new(StringArray::from_iter(
                dict.values.iter().map(|v| v.as_deref()),
            ));
            let mask = filter.evaluate_array(&values).context(RecordBatchSnafu)?;
            selected.retain(|&i| mask.value(dict.codes[i] as usize));
        } else {
            let length = match self {
                Self::Integers(v) => v.len(),
                Self::Null(n) | Self::Table(_, n) => *n,
                Self::Strings(_) => 0,
            };
            let values = self.array(&(0..length).collect::<Vec<_>>());
            let mask = filter.evaluate_array(&values).context(RecordBatchSnafu)?;
            selected.retain(|&i| mask.value(i));
        }
        Ok(())
    }
}

/// TODO: share decoded chunks through the existing Mito cache, not a new cache framework.
async fn load_column_chunk(
    store: &ObjectStore,
    path: &str,
    column: &Column,
    block: &Block,
) -> Result<Decoded> {
    let integer = column.name == TSID_COLUMN || column.name == ROW_COUNT_COLUMN;
    let expected = if integer {
        codec::INTEGER
    } else {
        codec::DICTIONARY
    };
    if column.payload.encoding != expected {
        return Err(unsupported(format!(
            "series column encoding {} for {}",
            column.payload.encoding, column.name
        )));
    }
    let bytes = read_range(store, path, column.payload.offset, column.payload.end()?).await?;
    let rows = block.rows as usize;
    let first = block.first;
    let last = block.last;
    let bytes = column.payload.unpack(&bytes)?;
    if integer {
        let values = codec::decode_integers(&bytes, rows)?;
        if column.name == TSID_COLUMN {
            require(
                values.first() == Some(&first)
                    && values.last() == Some(&last)
                    && values.windows(2).all(|v| v[0] < v[1]),
                "invalid series TSID ordering/bounds",
            )?;
        } else {
            require(values.iter().all(|&v| v > 0), "zero series row count")?;
        }
        Ok(Decoded::Integers(values))
    } else {
        let dictionary = codec::decode_dictionary(&bytes, rows)?;
        require(
            dictionary.codes.iter().filter(|&&code| code == 0).count()
                == column.null_count as usize,
            "series dictionary null count mismatch",
        )?;
        let min = dictionary
            .codes
            .iter()
            .filter_map(|&code| dictionary.values[code as usize].as_ref())
            .min();
        let max = dictionary
            .codes
            .iter()
            .filter_map(|&code| dictionary.values[code as usize].as_ref())
            .max();
        require(
            column.bounds.as_ref().map(|(a, b)| (a, b)) == min.zip(max),
            "series dictionary bounds mismatch",
        )?;
        Ok(Decoded::Strings(dictionary))
    }
}

async fn get_column(store: &ObjectStore, path: &str, block: &Block, name: &str) -> Result<Decoded> {
    if name == TABLE_ID_COLUMN {
        return Ok(Decoded::Table(block.table_id, block.rows as usize));
    }
    match block.columns.iter().find(|c| c.name == name) {
        Some(column) => load_column_chunk(store, path, column, block).await,
        None => Ok(Decoded::Null(block.rows as usize)),
    }
}

async fn read_block(
    store: &ObjectStore,
    path: &str,
    block: &Block,
    request: &SeriesReadRequest,
    schema: SchemaRef,
) -> Result<Option<ProjectedBlock>> {
    let mut decoded = HashMap::new();
    let mut selected: Vec<_> = (0..block.rows as usize).collect();
    if let Some(ids) = &request.ids {
        let column = get_column(store, path, block, TSID_COLUMN).await?;
        if let Decoded::Integers(values) = &column {
            selected.retain(|&i| {
                ids.contains(&MetricSeriesId {
                    table_id: block.table_id,
                    tsid: values[i],
                })
            });
        }
        decoded.insert(TSID_COLUMN.to_owned(), Arc::new(column));
    }
    for filter in &request.filters {
        if selected.is_empty() {
            return Ok(None);
        }
        let name = filter.column_name();
        if !decoded.contains_key(name) {
            decoded.insert(
                name.to_owned(),
                Arc::new(get_column(store, path, block, name).await?),
            );
        }
        decoded
            .get(name)
            .ok_or_else(|| invalid("missing decoded filter column"))?
            .filter(filter, &mut selected)?;
    }
    if selected.is_empty() {
        return Ok(None);
    }
    let mut columns = Vec::with_capacity(request.projection.len());
    for name in &request.projection {
        if !decoded.contains_key(name) {
            decoded.insert(
                name.clone(),
                Arc::new(get_column(store, path, block, name).await?),
            );
        }
        columns.push(
            decoded
                .get(name)
                .ok_or_else(|| invalid("missing decoded output column"))?
                .clone(),
        );
    }
    Ok(Some(ProjectedBlock {
        columns,
        selected,
        schema,
        position: 0,
    }))
}

/// Dictionary expansion can be much larger than the stored chunk. Bound each
/// materialized batch, allowing one oversized row, without expanding the block.
struct ProjectedBlock {
    columns: Vec<Arc<Decoded>>,
    selected: Vec<usize>,
    schema: SchemaRef,
    position: usize,
}
impl ProjectedBlock {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        const TARGET_BYTES: usize = 8 * 1024 * 1024;
        if self.position == self.selected.len() {
            return Ok(None);
        }
        let start = self.position;
        let mut bytes = 0usize;
        while self.position < self.selected.len() {
            let index = self.selected[self.position];
            let row_bytes = self.columns.iter().fold(0usize, |sum, column| {
                sum.saturating_add(column.value_bytes(index))
            });
            if self.position > start && bytes.saturating_add(row_bytes) > TARGET_BYTES {
                break;
            }
            bytes = bytes.saturating_add(row_bytes);
            self.position += 1;
        }
        let selected = &self.selected[start..self.position];
        let arrays = self
            .columns
            .iter()
            .map(|column| column.array(selected))
            .collect();
        Ok(Some(
            RecordBatch::try_new_with_options(
                self.schema.clone(),
                arrays,
                &RecordBatchOptions::new().with_row_count(Some(selected.len())),
            )
            .context(NewRecordBatchSnafu)?,
        ))
    }
}
