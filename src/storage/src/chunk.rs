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
use common_telemetry::{debug, logging};
use common_time::range::TimestampRange;
use snafu::ResultExt;
use store_api::storage::{Chunk, ChunkReader, SchemaRef, SequenceNumber};
use table::predicate::{Predicate, TimeRangePredicateBuilder};

use crate::error::{self, Error, Result};
use crate::memtable::{IterContext, MemtableRef};
use crate::read::windowed::WindowedReader;
use crate::read::{Batch, BoxedBatchReader, DedupReader, MergeReaderBuilder};
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
    schema: RegionSchemaRef,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    sst_layer: AccessLayerRef,
    iter_ctx: IterContext,
    memtables: Vec<MemtableRef>,
    files_to_read: Vec<FileHandle>,
    output_ordering: Option<Vec<OrderOption>>,
}

impl ChunkReaderBuilder {
    pub fn new(schema: RegionSchemaRef, sst_layer: AccessLayerRef) -> Self {
        ChunkReaderBuilder {
            schema,
            projection: None,
            filters: vec![],
            sst_layer,
            iter_ctx: IterContext::default(),
            memtables: Vec::new(),
            files_to_read: Vec::new(),
            output_ordering: None,
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
        let memtable_stats = self.memtables.iter().map(|m| m.stats()).collect::<Vec<_>>();
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
        logging::info!(
            "build sst reader begin, time_range: {:?}, num_files: {}",
            time_range,
            self.files_to_read.len()
        );
        let mut read_files = Vec::new();
        for file in &self.files_to_read {
            if !Self::file_in_range(file, time_range) {
                debug!("Skip file {:?}, predicate: {:?}", file, time_range);
                logging::info!(
                    "skip file {}, time_range: {:?}",
                    file.file_id(),
                    file.time_range()
                );
                continue;
            }
            read_files.push(file.meta());
            let reader = self.sst_layer.read_sst(file.clone(), &read_opts).await?;
            reader_builder = reader_builder.push_batch_reader(reader);
        }

        let window = PlainWindowInference {}.infer_window(&read_files, &[], true);

        logging::info!(
            "build sst reader done, time_range: {:?}, read_files: {:?}, window: {:?}",
            time_range,
            read_files,
            window,
        );

        logging::info!(
            "build reader done, time_range: {:?}, total_files:{}, num_read_files: {}",
            time_range,
            self.files_to_read.len(),
            read_files.len()
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
        } else {
            self.build_reader(&schema, &time_range_predicate).await?
        };

        Ok(ChunkReaderImpl::new(schema, reader, output_ordering))
    }

    /// Build time range predicate from schema and filters.
    pub fn build_time_range_predicate(&self) -> TimestampRange {
        let Some(ts_col) = self.schema.user_schema().timestamp_column() else { return TimestampRange::min_to_max() };
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
        let Some((start, end)) = *file.time_range() else { return true; };
        let file_ts_range = TimestampRange::new_inclusive(Some(start), Some(end));
        file_ts_range.intersects(predicate)
    }
}
