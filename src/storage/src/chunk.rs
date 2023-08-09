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

use std::sync::Arc;

use async_trait::async_trait;
use common_query::logical_plan::Expr;
use common_recordbatch::OrderOption;
use common_telemetry::logging;
use common_time::range::TimestampRange;
use snafu::ResultExt;
use store_api::storage::{Chunk, ChunkReader, RegionId, SchemaRef, SequenceNumber};
use table::predicate::{Predicate, TimeRangePredicateBuilder};

use crate::error::{self, Error, Result};
use crate::memtable::{IterContext, MemtableRef};
use crate::read::{
    Batch, BoxedBatchReader, ChainReader, DedupReader, MergeReaderBuilder, WindowedReader,
};
use crate::schema::{ProjectedSchema, ProjectedSchemaRef, RegionSchemaRef};
use crate::sst::{AccessLayerRef, FileHandle, LevelMetas, ReadOptions};
use crate::window_infer::{PlainWindowInference, WindowInfer};

/// Chunk reader implementation.
// Now we use async-trait to implement the chunk reader, which is easier to implement than
// using `Stream`, maybe change to `Stream` if we find out it is more efficient and have
// necessary to do so.
pub struct ChunkReaderImpl {
    schema: ProjectedSchemaRef,
    batch_reader: BoxedBatchReader,
    output_ordering: Option<Vec<OrderOption>>,
}

#[async_trait]
impl ChunkReader for ChunkReaderImpl {
    type Error = Error;

    fn user_schema(&self) -> &SchemaRef {
        self.schema.projected_user_schema()
    }

    async fn next_chunk(&mut self) -> Result<Option<Chunk>> {
        let batch = match self.batch_reader.next_batch().await? {
            Some(b) => b,
            None => return Ok(None),
        };
        Ok(Some(Chunk::new(batch.columns)))
    }

    fn project_chunk(&self, chunk: Chunk) -> Chunk {
        let batch = Batch {
            columns: chunk.columns,
        };
        self.schema.batch_to_chunk(&batch)
    }

    fn output_ordering(&self) -> Option<Vec<OrderOption>> {
        self.output_ordering.clone()
    }
}

impl ChunkReaderImpl {
    pub fn new(
        schema: ProjectedSchemaRef,
        batch_reader: BoxedBatchReader,
        output_ordering: Option<Vec<OrderOption>>,
    ) -> ChunkReaderImpl {
        ChunkReaderImpl {
            schema,
            batch_reader,
            output_ordering,
        }
    }

    #[inline]
    pub fn projected_schema(&self) -> &ProjectedSchemaRef {
        &self.schema
    }
}

/// Builder to create a new [ChunkReaderImpl] from scan request.
pub struct ChunkReaderBuilder {
    region_id: RegionId,
    schema: RegionSchemaRef,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    sst_layer: AccessLayerRef,
    iter_ctx: IterContext,
    memtables: Vec<MemtableRef>,
    files_to_read: Vec<FileHandle>,
    output_ordering: Option<Vec<OrderOption>>,
    use_chain_reader: bool,
}

impl ChunkReaderBuilder {
    pub fn new(region_id: RegionId, schema: RegionSchemaRef, sst_layer: AccessLayerRef) -> Self {
        ChunkReaderBuilder {
            region_id,
            schema,
            projection: None,
            filters: vec![],
            sst_layer,
            iter_ctx: IterContext::default(),
            memtables: Vec::new(),
            files_to_read: Vec::new(),
            output_ordering: None,
            use_chain_reader: false,
        }
    }

    /// Reserve space for iterating `num` memtables.
    pub fn reserve_num_memtables(mut self, num: usize) -> Self {
        self.memtables.reserve(num);
        self
    }

    pub fn projection(mut self, projection: Option<Vec<usize>>) -> Self {
        self.projection = projection;
        self
    }

    pub fn filters(mut self, filters: Vec<Expr>) -> Self {
        self.filters = filters;
        self
    }

    pub fn output_ordering(mut self, ordering: Option<Vec<OrderOption>>) -> Self {
        self.output_ordering = ordering;
        self
    }

    pub fn batch_size(mut self, batch_size: usize) -> Self {
        self.iter_ctx.batch_size = batch_size;
        self
    }

    pub fn visible_sequence(mut self, sequence: SequenceNumber) -> Self {
        self.iter_ctx.visible_sequence = sequence;
        self
    }

    pub fn pick_memtables(mut self, memtables: MemtableRef) -> Self {
        self.memtables.push(memtables);
        self
    }

    /// Partition files and memtables according to their time windows and scan time windows
    /// one by one.
    ///
    /// Note that compaction should not enable this.
    pub fn use_chain_reader(mut self, use_chain_reader: bool) -> Self {
        self.use_chain_reader = use_chain_reader;
        self
    }

    /// Picks all SSTs in all levels
    pub fn pick_all_ssts(mut self, ssts: &LevelMetas) -> Result<Self> {
        let files = ssts.levels().iter().flat_map(|level| level.files());
        // Now we read all files, so just reserve enough space to hold all files.
        self.files_to_read.reserve(files.size_hint().0);
        for file in files {
            // We can't invoke async functions here, so we collects all files first, and
            // create the batch reader later in `ChunkReaderBuilder`.
            self.files_to_read.push(file.clone());
        }
        Ok(self)
    }

    /// Picks given SSTs to read.
    pub fn pick_ssts(mut self, ssts: &[FileHandle]) -> Self {
        for file in ssts {
            self.files_to_read.push(file.clone());
        }
        self
    }

    /// Try to infer time window from output ordering. If the result
    /// is `None` means the output ordering is not obeyed, otherwise
    /// means the output ordering is obeyed and is same with request.
    fn infer_time_windows(&self, output_ordering: &[OrderOption]) -> Option<Vec<TimestampRange>> {
        if output_ordering.is_empty() {
            return None;
        }
        let OrderOption { name, options } = &output_ordering[0];

        if name != self.schema.timestamp_column_name() {
            return None;
        }
        let memtable_stats = self
            .memtables
            .iter()
            .filter(|m| m.num_rows() > 0) // Skip empty memtables.
            .map(|m| m.stats())
            .collect::<Vec<_>>();
        let files = self
            .files_to_read
            .iter()
            .map(FileHandle::meta)
            .collect::<Vec<_>>();

        Some(PlainWindowInference {}.infer_window(&files, &memtable_stats, options.descending))
    }

    async fn build_windowed(
        self,
        schema: &ProjectedSchemaRef,
        time_range_predicate: &TimestampRange,
        windows: Vec<TimestampRange>,
        order_options: Vec<OrderOption>,
    ) -> Result<BoxedBatchReader> {
        let mut readers = Vec::with_capacity(windows.len());
        for window in windows {
            let time_range_predicate = time_range_predicate.and(&window);
            let reader = self.build_reader(schema, &time_range_predicate).await?;
            readers.push(reader);
        }
        let windowed_reader = WindowedReader::new(schema.clone(), readers, order_options);
        Ok(Box::new(windowed_reader) as Box<_>)
    }

    async fn build_reader(
        &self,
        schema: &ProjectedSchemaRef,
        time_range: &TimestampRange,
    ) -> Result<BoxedBatchReader> {
        let num_sources = self.memtables.len() + self.files_to_read.len();
        let mut reader_builder = MergeReaderBuilder::with_capacity(schema.clone(), num_sources)
            .batch_size(self.iter_ctx.batch_size);

        for mem in &self.memtables {
            let mut iter_ctx = self.iter_ctx.clone();
            iter_ctx.time_range = Some(*time_range);
            let iter = mem.iter(iter_ctx)?;
            reader_builder = reader_builder.push_batch_iter(iter);
        }

        let predicate = Predicate::try_new(
            self.filters.clone(),
            self.schema.store_schema().schema().clone(),
        )
        .context(error::BuildPredicateSnafu)?;

        let read_opts = ReadOptions {
            batch_size: self.iter_ctx.batch_size,
            projected_schema: schema.clone(),
            predicate,
            time_range: *time_range,
        };

        let mut num_read_files = 0;
        for file in &self.files_to_read {
            if !Self::file_in_range(file, time_range) {
                logging::debug!(
                    "Skip region {} file {:?}, predicate: {:?}",
                    self.region_id,
                    file,
                    time_range
                );
                continue;
            }

            let reader = self.sst_layer.read_sst(file.clone(), &read_opts).await?;
            reader_builder = reader_builder.push_batch_reader(reader);
            num_read_files += 1;
        }

        logging::debug!(
            "build reader done, region_id: {}, time_range: {:?}, total_files: {}, num_read_files: {}",
            self.region_id,
            time_range,
            self.files_to_read.len(),
            num_read_files,
        );

        let reader = reader_builder.build();
        let reader = DedupReader::new(schema.clone(), reader);
        Ok(Box::new(reader) as Box<_>)
    }

    pub async fn build(mut self) -> Result<ChunkReaderImpl> {
        let time_range_predicate = self.build_time_range_predicate();
        let schema = Arc::new(
            ProjectedSchema::new(self.schema.clone(), self.projection.clone())
                .context(error::InvalidProjectionSnafu)?,
        );
        self.iter_ctx.projected_schema = Some(schema.clone());

        let mut output_ordering = None;
        let reader = if let Some(ordering) = self.output_ordering.take() &&
            let Some(windows) = self.infer_time_windows(&ordering) {
                output_ordering = Some(ordering.clone());
                self.build_windowed(&schema, &time_range_predicate, windows, ordering)
                    .await?
        } else if self.use_chain_reader {
            self.build_chained(&schema, &time_range_predicate).await?
        } else {
            self.build_reader(&schema, &time_range_predicate).await?
        };

        Ok(ChunkReaderImpl::new(schema, reader, output_ordering))
    }

    async fn build_chained(
        &self,
        schema: &ProjectedSchemaRef,
        time_range: &TimestampRange,
    ) -> Result<BoxedBatchReader> {
        let windows = self.infer_window_for_chain_reader(time_range);

        logging::debug!(
            "Infer window for chain reader, region_id: {}, memtables: {}, files: {}, num_windows: {}",
            self.region_id,
            self.memtables.len(),
            self.files_to_read.len(),
            windows.len(),
        );

        let mut readers = Vec::with_capacity(windows.len());
        for window in &windows {
            let time_range = time_range.and(window);
            let reader = self.build_reader(schema, &time_range).await?;
            readers.push(reader);
        }

        logging::debug!(
            "Build chain reader, region_id: {}, time_range: {:?}, num_readers: {}",
            self.region_id,
            time_range,
            readers.len(),
        );

        let chain_reader = ChainReader::new(schema.clone(), readers);
        Ok(Box::new(chain_reader) as Box<_>)
    }

    /// Build time range predicate from schema and filters.
    fn build_time_range_predicate(&self) -> TimestampRange {
        let Some(ts_col) = self.schema.user_schema().timestamp_column() else {
            return TimestampRange::min_to_max();
        };
        let unit = ts_col
            .data_type
            .as_timestamp()
            .expect("Timestamp column must have timestamp-compatible type")
            .unit();
        TimeRangePredicateBuilder::new(&ts_col.name, unit, &self.filters).build()
    }

    /// Check if SST file's time range matches predicate.
    fn file_in_range(file: &FileHandle, predicate: &TimestampRange) -> bool {
        if predicate == &TimestampRange::min_to_max() {
            return true;
        }
        // end_timestamp of sst file is inclusive.
        let Some((start, end)) = *file.time_range() else {
            return true;
        };
        let file_ts_range = TimestampRange::new_inclusive(Some(start), Some(end));
        file_ts_range.intersects(predicate)
    }

    /// Returns the time range of memtables to read.
    fn compute_memtable_range(&self) -> Option<TimestampRange> {
        let (min_timestamp, max_timestamp) = self
            .memtables
            .iter()
            .filter(|m| m.num_rows() > 0) // Skip empty memtables.
            .map(|m| {
                let stats = m.stats();
                (stats.min_timestamp, stats.max_timestamp)
            })
            .reduce(|acc, e| (acc.0.min(e.0), acc.1.max(e.1)))?;

        logging::debug!(
            "Compute memtable range, region_id: {}, min: {:?}, max: {:?}",
            self.region_id,
            min_timestamp,
            max_timestamp,
        );

        Some(TimestampRange::new_inclusive(
            Some(min_timestamp),
            Some(max_timestamp),
        ))
    }

    /// Infer time window for chain reader according to the time range of memtables and files.
    fn infer_window_for_chain_reader(&self, time_range: &TimestampRange) -> Vec<TimestampRange> {
        let mut memtable_range = self.compute_memtable_range();
        // file ranges: (start, end)
        let mut file_ranges = Vec::with_capacity(self.files_to_read.len());
        for file in &self.files_to_read {
            if !Self::file_in_range(file, time_range) || file.time_range().is_none() {
                continue;
            }
            // Safety: we have skip files whose range is `None`.
            let range = file.time_range().unwrap();

            // Filter by memtable's time range.
            if let Some(mem_range) = &mut memtable_range {
                let file_range = TimestampRange::new_inclusive(Some(range.0), Some(range.1));
                if mem_range.intersects(&file_range) {
                    // If the range of the SST intersects with the range of the
                    // memtable, we merge it into the memtable's range.
                    *mem_range = mem_range.or(&file_range);
                    continue;
                }
            }

            file_ranges.push((range.0, range.1));
        }

        if file_ranges.is_empty() {
            return memtable_range.map(|range| vec![range]).unwrap_or_default();
        }

        // Sort by start times.
        file_ranges.sort_unstable_by(|left, right| left.0.cmp(&right.0));

        // Compute ranges for all SSTs.
        let mut time_ranges = Vec::with_capacity(file_ranges.len() + 1);
        // Safety: file_ranges is not empty.
        let mut prev =
            TimestampRange::new_inclusive(Some(file_ranges[0].0), Some(file_ranges[0].1));
        for file_range in &file_ranges[1..] {
            let current = TimestampRange::new_inclusive(Some(file_range.0), Some(file_range.1));
            if prev.intersects(&current) {
                prev = prev.or(&current);
            } else {
                time_ranges.push(prev);
                prev = current;
            }
        }
        time_ranges.push(prev);

        if let Some(mem_range) = memtable_range {
            time_ranges.push(mem_range);
            // We have pushed the memtable range, resort the array.
            time_ranges.sort_unstable_by(|left, right| left.start().cmp(right.start()));
        }

        time_ranges
    }
}
