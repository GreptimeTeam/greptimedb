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

//! Authoritative narthex inputs for scanner-wide series preparation.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use datatypes::arrow::array::{ArrayRef, StringArray, UInt32Array, UInt64Array};
use datatypes::arrow::compute::filter_record_batch;
use datatypes::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::value::Value;
use futures::{TryStreamExt, stream::BoxStream};
use mito_codec::row_converter::{CompositeValues, PrimaryKeyCodec, SparsePrimaryKeyCodec};
use snafu::ResultExt;
use store_api::metadata::RegionMetadataRef;
use store_api::region_engine::PartitionRange;
use store_api::storage::consts::{ReservedColumnId, SEQUENCE_COLUMN_NAME};

use crate::error::{ComputeArrowSnafu, DecodeSnafu, NewRecordBatchSnafu, RecordBatchSnafu, Result};
use crate::read::scan_region::StreamContext;
use crate::read::series_scan::prepared::invalid;
use crate::series_index::ROW_COUNT_COLUMN;
use crate::series_index::artifact::SamplesFileHandle;
use crate::series_index::format::{
    FormatReader, SamplesBatch, SamplesFormatReader, SamplesReadRequest, SeriesReadRequest,
};
use crate::series_index::identity::ExactSeriesKey;
use crate::series_index::samples::reader::SamplesReader;
use crate::sst::file::FileFormat;
use store_api::metric_engine_consts::{
    DATA_SCHEMA_TABLE_ID_COLUMN_NAME as TABLE_ID_COLUMN,
    DATA_SCHEMA_TSID_COLUMN_NAME as TSID_COLUMN,
};

/// Validates every assignment before opening authoritative objects. Missing support is an
/// error, never permission to read narthex files through the legacy adapter.
pub(crate) fn file_indices(
    context: &StreamContext,
    ranges: &[PartitionRange],
) -> Result<BTreeSet<usize>> {
    let mut files = BTreeSet::new();
    for range in ranges {
        let range = context
            .ranges
            .get(range.identifier)
            .ok_or_else(|| invalid("invalid narthex prepared scan range"))?;
        for index in &range.row_group_indices {
            if context.is_mem_range_index(*index) {
                continue;
            }
            if !context.is_file_range_index(*index) {
                return Err(invalid(
                    "narthex preparation requires a prepared memtable source",
                ));
            }
            let i = index.index - context.input.num_memtables();
            if index.row_group_index != -1 {
                return Err(invalid(
                    "narthex preparation requires a complete samples assignment",
                ));
            }
            files.insert(i);
        }
    }
    Ok(files)
}

pub(crate) async fn open_sources(
    context: &StreamContext,
    ranges: &[PartitionRange],
    pool: &Arc<dyn datafusion::execution::memory_pool::MemoryPool>,
) -> Result<(
    Vec<Arc<dyn SamplesFormatReader>>,
    Vec<Box<dyn FormatReader>>,
)> {
    let mut samples = Vec::<Arc<dyn SamplesFormatReader>>::new();
    let mut series = Vec::<Box<dyn FormatReader>>::new();
    let indices = file_indices(context, ranges)?;
    let mut covered = std::collections::HashSet::new();
    for file in &context.input.narthex_series_indexes {
        let FileFormat::NarthexSeriesIndex(index) = &file.meta_ref().format else {
            continue;
        };
        let region = context.input.region_metadata().region_id;
        let selected: Vec<_> = if let Some(proof) = &index.coverage {
            if proof.region_id != region
                || proof.truncated_entry_id != context.input.truncated_entry_id
            {
                continue;
            }
            indices
                .iter()
                .map(|i| &context.input.files[*i])
                .filter(|sample| proof.covers(sample.meta_ref()))
                .map(|sample| sample.file_id())
                .collect()
        } else {
            if !index
                .sources
                .iter()
                .all(|id| context.input.files.iter().any(|f| f.file_id() == *id))
            {
                continue;
            }
            indices
                .iter()
                .map(|i| context.input.files[*i].file_id())
                .filter(|id| index.sources.contains(id))
                .collect()
        };
        if selected.is_empty() {
            continue;
        }
        let reader = match crate::series_index::columnar::reader::ColumnarReader::open_cached(
            context.input.access_layer.object_store().clone(),
            crate::series_index::SeriesIndexFileHandle::from_narthex(
                file.clone(),
                &context
                    .input
                    .access_layer
                    .build_region_dir(file.meta_ref().region_id),
            )?,
            context
                .input
                .cache_strategy
                .columnar_metadata(file.meta_ref().file_id),
        )
        .await
        {
            Ok(reader) => reader,
            Err(error) if error.is_object_not_found() => continue,
            Err(error) => return Err(error),
        };
        // Indexes built before a schema extension need sample-key discovery until
        // they can supply the current tag schema, including newly nullable tags.
        if crate::series_index::format::validate_series_schema(
            &reader.schema(),
            context.input.region_metadata(),
        )
        .is_err()
        {
            continue;
        }
        covered.extend(selected);
        series.push(Box::new(reader));
    }
    for i in indices {
        let file = &context.input.files[i];
        let namespace = file.meta_ref().file_id;
        let reader = SamplesReader::open_cached(
            context.input.access_layer.object_store().clone(),
            SamplesFileHandle::from_narthex(
                file.clone(),
                &context
                    .input
                    .access_layer
                    .build_region_dir(file.meta_ref().region_id),
            )?,
            context.input.cache_strategy.columnar_metadata(namespace),
        )
        .await?;
        // Match Parquet: foreign SSTs use the destination admission sequence,
        // while local SSTs retain their encoded row sequences. A local file's
        // sequence metadata is only a bound, not a replacement for every row.
        let sequence = if file.region_id() != context.input.region_metadata().region_id {
            file.meta_ref().sequence.map(|sequence| sequence.get())
        } else {
            None
        };
        let reader: Arc<dyn SamplesFormatReader> =
            Arc::new(NarthexSamplesReader { reader, sequence });
        if !covered.contains(&file.file_id()) {
            series.push(Box::new(NarthexSeriesReader::new(
                context.input.region_metadata().clone(),
                reader.clone(),
                pool.clone(),
            )?));
        }
        samples.push(reader);
    }
    let mut seen = BTreeSet::new();
    for range in ranges {
        for index in &context.ranges[range.identifier].row_group_indices {
            if !context.is_mem_range_index(*index) || !seen.insert(index.index) {
                continue;
            }
            for range in context.input.build_mem_ranges(*index) {
                let reader: Arc<dyn SamplesFormatReader> = Arc::new(NarthexMemtableReader {
                    range,
                    schema: context.input.mapper.input_arrow_schema(false),
                });
                series.push(Box::new(NarthexSeriesReader::new(
                    context.input.region_metadata().clone(),
                    reader.clone(),
                    pool.clone(),
                )?));
                samples.push(reader);
            }
        }
    }
    Ok((samples, series))
}

/// Applies COPY admission sequences before snapshot visibility and deduplication.
struct NarthexSamplesReader {
    reader: SamplesReader,
    sequence: Option<u64>,
}

impl SamplesFormatReader for NarthexSamplesReader {
    fn schema(&self) -> SchemaRef {
        self.reader.schema()
    }

    fn read(
        &self,
        request: SamplesReadRequest,
    ) -> Result<BoxStream<'static, Result<SamplesBatch>>> {
        let mut stream = self.reader.read(request)?;
        let sequence = self.sequence;
        Ok(Box::pin(async_stream::try_stream! {
            while let Some(mut samples) = stream.try_next().await? {
                if let Some(sequence) = sequence
                    && let Ok(index) = samples.batch.schema().index_of(SEQUENCE_COLUMN_NAME)
                {
                    let mut columns = samples.batch.columns().to_vec();
                    columns[index] = Arc::new(UInt64Array::from(vec![sequence; samples.batch.num_rows()]));
                    samples.batch = RecordBatch::try_new(samples.batch.schema(), columns).context(NewRecordBatchSnafu)?;
                }
                yield samples;
            }
        }))
    }
}

/// A series index is optional: sample metadata already owns the full exact key. Empty
/// projection discovers keys without reading sample payloads. Actual sample reads enforce
/// time/sequence visibility, so discovery may conservatively retain empty series.
struct NarthexSeriesReader {
    pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool>,
    schema: SchemaRef,
    tags: Vec<(u32, String)>,
    samples: Arc<dyn SamplesFormatReader>,
}

impl NarthexSeriesReader {
    fn new(
        metadata: RegionMetadataRef,
        samples: Arc<dyn SamplesFormatReader>,
        pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool>,
    ) -> Result<Self> {
        let tags: Vec<_> = metadata
            .primary_key_columns()
            .filter(|column| {
                ![ReservedColumnId::table_id(), ReservedColumnId::tsid()]
                    .contains(&column.column_id)
            })
            .map(|column| (column.column_id, column.column_schema.name.clone()))
            .collect();
        let mut fields = vec![
            Field::new(TABLE_ID_COLUMN, DataType::UInt32, false),
            Field::new(TSID_COLUMN, DataType::UInt64, false),
            Field::new(ROW_COUNT_COLUMN, DataType::UInt64, false),
        ];
        fields.extend(tags.iter().map(|(id, name)| {
            crate::sst::with_field_id(Field::new(name, DataType::Utf8, true), *id)
        }));
        let schema = Arc::new(Schema::new(fields));
        Ok(Self {
            schema,
            tags,
            samples,
            pool,
        })
    }
}

impl FormatReader for NarthexSeriesReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn read(&self, request: SeriesReadRequest) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let schema = self.schema.clone();
        let tags = self.tags.clone();
        let mut stream = self.samples.read(SamplesReadRequest {
            table_id: request.table_id,
            memory_pool: Some(self.pool.clone()),
            ..Default::default()
        })?;
        Ok(Box::pin(async_stream::try_stream! {
            let mut previous: Option<ExactSeriesKey> = None;
            let mut keys = Vec::with_capacity(1024);
            while let Some(samples) = stream.try_next().await? {
                if previous.as_ref() == Some(&samples.key) { continue; }
                previous = Some(samples.key.clone());
                if request.ids.as_ref().is_some_and(|ids| !ids.contains(&samples.key.id())) { continue; }
                keys.push(samples.key);
                if keys.len() == 1024 {
                    yield series_batch(&schema, &tags, &keys, &request)?;
                    keys.clear();
                }
            }
            if !keys.is_empty() { yield series_batch(&schema, &tags, &keys, &request)?; }
        }))
    }
}

fn series_batch(
    schema: &SchemaRef,
    tags: &[(u32, String)],
    keys: &[ExactSeriesKey],
    request: &SeriesReadRequest,
) -> Result<RecordBatch> {
    let codec = SparsePrimaryKeyCodec::schemaless();
    let mut values = HashMap::<u32, Vec<Option<String>>>::new();
    for (id, _) in tags {
        values.insert(*id, Vec::with_capacity(keys.len()));
    }
    for key in keys {
        let CompositeValues::Sparse(decoded) = codec.decode(key.bytes()).context(DecodeSnafu)?
        else {
            return Err(invalid("narthex series requires sparse keys"));
        };
        for (id, column) in &mut values {
            column.push(match decoded.get_or_null(*id) {
                Value::String(value) => Some(value.as_utf8().to_owned()),
                Value::Null => None,
                _ => return Err(invalid("narthex series requires string tags")),
            });
        }
    }
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(UInt32Array::from(
            keys.iter().map(|key| key.id().table_id).collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            keys.iter().map(|key| key.id().tsid).collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(vec![1; keys.len()])),
    ];
    for (id, _) in tags {
        columns.push(Arc::new(StringArray::from(
            values
                .remove(id)
                .ok_or_else(|| invalid("missing narthex series tag"))?,
        )));
    }
    let mut batch = RecordBatch::try_new(schema.clone(), columns).context(NewRecordBatchSnafu)?;
    for filter in &request.filters {
        let column = batch
            .column_by_name(filter.column_name())
            .ok_or_else(|| invalid("unknown narthex series filter"))?;
        let mask = filter.evaluate_array(column).context(RecordBatchSnafu)?;
        batch = filter_record_batch(
            &batch,
            &datatypes::arrow::array::BooleanArray::new(mask, None),
        )
        .context(ComputeArrowSnafu)?;
    }
    let projection = request
        .projection
        .iter()
        .map(|name| {
            schema
                .index_of(name)
                .map_err(|error| invalid(error.to_string()))
        })
        .collect::<Result<Vec<_>>>()?;
    batch.project(&projection).context(ComputeArrowSnafu)
}

/// Reopens the pinned memtable range for independent selected-series reads. Discovery
/// projects only keys at this boundary; no rows are merged or deduplicated before the
/// prepared source combines memory and persisted samples.
struct NarthexMemtableReader {
    range: crate::memtable::MemtableRange,
    schema: SchemaRef,
}

impl SamplesFormatReader for NarthexMemtableReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn read(
        &self,
        request: SamplesReadRequest,
    ) -> Result<BoxStream<'static, Result<SamplesBatch>>> {
        if request.projection.is_empty()
            && let Some(keys) = self.range.primary_keys()
        {
            return Ok(Box::pin(async_stream::try_stream! {
                let mut offsets = mito_codec::row_converter::SparseOffsetsCache::new();
                let schema = Arc::new(Schema::empty());
                for bytes in keys {
                    let key = ExactSeriesKey::from_sparse_key(&bytes, &mut offsets)?;
                    if request.table_id.is_none_or(|table| key.id().table_id == table)
                        && request.keys.as_ref().is_none_or(|keys| keys.contains(&key)) {
                        let batch = RecordBatch::try_new_with_options(schema.clone(), vec![],
                            &datatypes::arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(1)))
                            .context(NewRecordBatchSnafu)?;
                        yield SamplesBatch { batch, key };
                    }
                }
            }));
        }
        let iter = self.range.build_record_batch_iter(None, None)?;
        Ok(Box::pin(async_stream::try_stream! {
            use datatypes::arrow::array::{Array, BinaryArray};
            use crate::sst::parquet::format::PrimaryKeyArray;
            let mut offsets = mito_codec::row_converter::SparseOffsetsCache::new();
            for batch in iter {
                let batch = batch?;
                let keys = batch.column_by_name(store_api::storage::consts::PRIMARY_KEY_COLUMN_NAME)
                    .and_then(|array| array.as_any().downcast_ref::<PrimaryKeyArray>())
                    .ok_or_else(|| invalid("narthex memtable requires dictionary primary keys"))?;
                let values = keys.values().as_any().downcast_ref::<BinaryArray>()
                    .ok_or_else(|| invalid("narthex memtable requires binary primary keys"))?;
                let projection = request.projection.iter().map(|name| batch.schema().index_of(name).map_err(|error| invalid(error.to_string()))).collect::<Result<Vec<_>>>()?;
                let projected = batch.project(&projection).context(ComputeArrowSnafu)?;
                let mut start = 0;
                while start < batch.num_rows() {
                    let bytes = values.value(keys.keys().value(start) as usize);
                    let mut end = start + 1;
                    while end < batch.num_rows() && values.value(keys.keys().value(end) as usize) == bytes { end += 1; }
                    let key = ExactSeriesKey::from_sparse_key(bytes, &mut offsets)?;
                    if request.table_id.is_none_or(|table| key.id().table_id == table)
                        && request.keys.as_ref().is_none_or(|keys| keys.contains(&key)) {
                        yield SamplesBatch { batch: projected.slice(start, end-start), key };
                    }
                    start = end;
                }
            }
        }))
    }
}
