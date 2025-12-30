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

//! Vector scan for KNN (K-Nearest Neighbors) search using vector indexes.

use std::collections::{BTreeSet, BinaryHeap, HashMap};
use std::fmt;
use std::sync::Arc;

use common_error::ext::BoxedError;
use common_recordbatch::filter::batch_filter;
use common_recordbatch::{RecordBatchStreamWrapper, SendableRecordBatchStream};
use common_telemetry::{debug, warn};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, PhysicalExpr};
use datatypes::prelude::{ScalarVector, ScalarVectorBuilder};
use datatypes::schema::SchemaRef;
use datatypes::value::OrderedFloat;
use datatypes::vectors::{BinaryVector, BooleanVectorBuilder};
use futures::StreamExt;
use index::vector::compute_distance;
use smallvec::SmallVec;
use store_api::metadata::RegionMetadataRef;
use store_api::region_engine::{
    PrepareRequest, QueryScanContext, RegionScanner, ScannerProperties,
};

use crate::error::{ApplyVectorIndexSnafu, Result};
use crate::memtable::MemtableRange;
use crate::metrics::READ_ROWS_TOTAL;
use crate::read::scan_region::ScanInput;
use crate::read::scan_util::PartitionMetricsList;
use crate::read::stream::ScanBatchStream;
use crate::read::{Batch, Source};
use crate::sst::index::vector_index::applier::VectorIndexApplyMetrics;
use crate::sst::index::vector_index::util::bytes_to_f32_slice;
use crate::sst::parquet::file_range::FileRange;
use crate::sst::parquet::reader::ReaderMetrics;
use crate::sst::parquet::row_selection::RowGroupSelection;

/// Source of a vector candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CandidateSource {
    /// From an SST file (index is file index in input.files).
    SstFile(usize),
    /// From a memtable (index is memtable index in input.memtables).
    Memtable(usize),
}

/// A candidate result from vector search.
#[derive(Debug, Clone)]
struct VectorCandidate {
    /// Source of this candidate.
    source: CandidateSource,
    /// Row offset within the source.
    row_offset: u64,
    /// Distance to the query vector.
    distance: f32,
}

impl PartialEq for VectorCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.distance == other.distance
    }
}

impl Eq for VectorCandidate {}

impl PartialOrd for VectorCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for VectorCandidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Compare by distance (smaller is better)
        OrderedFloat(self.distance).cmp(&OrderedFloat(other.distance))
    }
}

/// Scanner for KNN vector search.
///
/// This scanner uses vector indexes to efficiently find the K nearest neighbors
/// to a query vector. Unlike regular scanners that scan all rows, this scanner:
/// 1. Searches each SST file's vector index to get candidate rows
/// 2. Merges results from all SSTs by distance
/// 3. Returns only the top-K results
pub struct VectorScan {
    /// Properties of the scanner.
    properties: ScannerProperties,
    /// Input for the scan.
    input: Arc<ScanInput>,
    /// Metrics for each partition.
    #[allow(dead_code)]
    metrics_list: PartitionMetricsList,
}

impl VectorScan {
    /// Creates a new [VectorScan] with the given input.
    pub(crate) fn new(input: ScanInput) -> Self {
        let properties = ScannerProperties::default()
            .with_append_mode(input.append_mode)
            .with_total_rows(input.total_rows());

        Self {
            properties,
            input: Arc::new(input),
            metrics_list: PartitionMetricsList::default(),
        }
    }

    /// Performs vector search across all SST files and memtables, returns merged results.
    ///
    /// The `k` parameter specifies how many candidates to return after merging.
    /// This may be larger than the original k to support over-fetching for post-filtering.
    async fn search_all_files(&self, k: usize) -> Result<Vec<VectorCandidate>> {
        let Some(applier) = &self.input.vector_index_applier else {
            return Ok(Vec::new());
        };

        let mut all_candidates = Vec::new();
        let mut total_metrics = VectorIndexApplyMetrics::default();

        // 1. Search each SST file using vector index
        // Use k (which may be overfetched) instead of the original k stored in the applier
        for (file_index, file) in self.input.files.iter().enumerate() {
            let file_id = file.file_id();
            let file_size = Some(file.meta_ref().index_file_size);

            let mut metrics = VectorIndexApplyMetrics::default();
            match applier
                .apply_with_k(file_id, file_size, Some(&mut metrics), k)
                .await
            {
                Ok(result) => {
                    total_metrics.merge_from(&metrics);

                    // Convert results to candidates
                    for (row_offset, distance) in result
                        .row_offsets
                        .into_iter()
                        .zip(result.distances.into_iter())
                    {
                        all_candidates.push(VectorCandidate {
                            source: CandidateSource::SstFile(file_index),
                            row_offset,
                            distance,
                        });
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to search vector index for file {:?}: {}",
                        file_id, e
                    );
                    // Continue with other files
                }
            }
        }

        // 2. Brute-force search memtables
        let memtable_candidates = self.search_memtables()?;
        all_candidates.extend(memtable_candidates);

        debug!(
            "Vector search completed: {} candidates from {} SST files + {} memtables",
            all_candidates.len(),
            self.input.files.len(),
            self.input.memtables.len()
        );

        // Report metrics for vectors searched
        READ_ROWS_TOTAL
            .with_label_values(&["vector_index"])
            .inc_by(total_metrics.vectors_searched as u64);

        // Merge results and get top-k using a min-heap
        let top_k = self.merge_topk(all_candidates, k);

        Ok(top_k)
    }

    /// Brute-force searches through memtables for nearest neighbors.
    fn search_memtables(&self) -> Result<Vec<VectorCandidate>> {
        let Some(applier) = &self.input.vector_index_applier else {
            return Ok(Vec::new());
        };

        if self.input.memtables.is_empty() {
            return Ok(Vec::new());
        }

        let query_vector = applier.query_vector();
        let column_id = applier.column_id();
        let metric = applier.metric();

        let mut candidates = Vec::new();

        for (memtable_index, mem_range_builder) in self.input.memtables.iter().enumerate() {
            // Build memtable ranges
            let mut ranges: SmallVec<[MemtableRange; 2]> = SmallVec::new();
            mem_range_builder.build_ranges(0, &mut ranges);

            // Track row offset across all ranges within this memtable
            let mut row_offset_in_memtable = 0u64;

            for range in ranges {
                // Build iterator (synchronous)
                let iter = range.build_iter()?;

                // Iterate over all batches
                for batch_result in iter {
                    let batch = batch_result?;

                    // Find the vector column by column_id
                    let vector_col = batch.fields().iter().find(|col| col.column_id == column_id);

                    let Some(vector_col) = vector_col else {
                        row_offset_in_memtable += batch.num_rows() as u64;
                        continue;
                    };

                    // Try to downcast to BinaryVector
                    let Some(binary_vector) =
                        vector_col.data.as_any().downcast_ref::<BinaryVector>()
                    else {
                        row_offset_in_memtable += batch.num_rows() as u64;
                        continue;
                    };

                    // Iterate over each row
                    for row_idx in 0..batch.num_rows() {
                        if let Some(bytes) = binary_vector.get_data(row_idx) {
                            // Validate bytes length is a multiple of 4 (f32 size) to avoid panic
                            if bytes.len() % 4 != 0 {
                                return ApplyVectorIndexSnafu {
                                    reason: format!(
                                        "Invalid vector data: bytes length {} is not a multiple of 4 at row {}",
                                        bytes.len(),
                                        row_offset_in_memtable
                                    ),
                                }
                                .fail();
                            }

                            let floats = bytes_to_f32_slice(bytes);

                            // Validate dimension matches query vector
                            if floats.len() != query_vector.len() {
                                return ApplyVectorIndexSnafu {
                                    reason: format!(
                                        "Vector dimension mismatch in memtable: expected {}, got {} at row {}",
                                        query_vector.len(),
                                        floats.len(),
                                        row_offset_in_memtable
                                    ),
                                }
                                .fail();
                            }

                            let distance = compute_distance(&floats, query_vector, metric);

                            candidates.push(VectorCandidate {
                                source: CandidateSource::Memtable(memtable_index),
                                row_offset: row_offset_in_memtable,
                                distance,
                            });
                        }
                        row_offset_in_memtable += 1;
                    }
                }
            }
        }

        debug!(
            "Memtable search completed: {} candidates from {} memtables",
            candidates.len(),
            self.input.memtables.len()
        );

        Ok(candidates)
    }

    /// Merges candidates from all SSTs and returns the top-k by distance.
    fn merge_topk(&self, candidates: Vec<VectorCandidate>, k: usize) -> Vec<VectorCandidate> {
        if candidates.len() <= k {
            // Already have fewer than k candidates
            let mut result = candidates;
            result.sort();
            return result;
        }

        // Use a max-heap to keep track of top-k (smallest distances)
        // The heap keeps the k smallest distances, with the largest of these at the top.
        // When a new candidate has a smaller distance than the top (worst of our k best),
        // we replace it.
        let mut heap: BinaryHeap<VectorCandidate> = BinaryHeap::with_capacity(k + 1);

        for candidate in candidates {
            if heap.len() < k {
                heap.push(candidate);
            } else if let Some(worst) = heap.peek()
                && candidate.distance < worst.distance
            {
                heap.pop();
                heap.push(candidate);
            }
        }

        // Extract results in sorted order (smallest distance first)
        let mut result: Vec<_> = heap.into_iter().collect();
        result.sort();
        result
    }

    /// Over-fetch multiplier for post-filtering.
    /// We fetch more candidates than requested to account for rows that may be
    /// filtered out by predicates. This follows the design in vector-index-usearch.md.
    ///
    /// The value 2 is chosen as a balance between:
    /// - Too small (e.g., 1.5): May require more iterations when selectivity is low
    /// - Too large (e.g., 4): Wastes resources reading unnecessary data
    ///
    /// With multiplier=2, if 50% of candidates pass the filter, we get exactly k results.
    /// For lower selectivity, the loop will automatically increase the factor exponentially.
    const OVERFETCH_MULTIPLIER: usize = 2;

    /// Builds a stream for the query.
    pub(crate) async fn build_stream(&self) -> Result<SendableRecordBatchStream, BoxedError> {
        let Some(applier) = &self.input.vector_index_applier else {
            let schema = self.schema();
            let stream = futures::stream::empty();
            return Ok(Box::pin(RecordBatchStreamWrapper::new(
                schema,
                Box::pin(stream),
            )));
        };

        let original_k = applier.k();

        // Check if we have predicates that need post-filtering
        let physical_exprs = self.build_physical_exprs();
        let has_predicates = !physical_exprs.is_empty();

        let mut reach_end: bool = false;
        let mut factor = Self::OVERFETCH_MULTIPLIER;

        loop {
            // Over-fetch if we have predicates to filter
            let fetch_k = if has_predicates {
                original_k * factor
            } else {
                original_k
            };

            // Search all files and memtables using vector index
            let candidates = self
                .search_all_files(fetch_k)
                .await
                .map_err(BoxedError::new)?;

            debug!(
                "VectorScan found {} candidates for region {} (k={}, fetch_k={}, has_predicates={})",
                candidates.len(),
                self.input.mapper.metadata().region_id,
                original_k,
                fetch_k,
                has_predicates
            );

            if candidates.len() < fetch_k {
                // Reach the end.
                reach_end = true;
            }

            if candidates.is_empty() {
                let schema = self.schema();
                let stream = futures::stream::empty();
                return Ok(Box::pin(RecordBatchStreamWrapper::new(
                    schema,
                    Box::pin(stream),
                )));
            }

            // Partition candidates by source type
            let (sst_candidates, memtable_candidates): (Vec<_>, Vec<_>) = candidates
                .into_iter()
                .partition(|c| matches!(c.source, CandidateSource::SstFile(_)));

            // Group SST candidates by file index
            let grouped = Self::group_candidates_by_file(sst_candidates);

            // Fetch data from SST files
            let mut all_batches = self
                .fetch_rows_from_files(grouped)
                .await
                .map_err(BoxedError::new)?;

            // Fetch data from memtables
            if !memtable_candidates.is_empty() {
                let memtable_batches = self
                    .fetch_rows_from_memtables(memtable_candidates)
                    .map_err(BoxedError::new)?;
                all_batches.extend(memtable_batches);
            }

            // Convert Batch to RecordBatch using the projection mapper
            let mapper = self.input.mapper.as_primary_key().ok_or_else(|| {
                BoxedError::new(common_error::ext::PlainError::new(
                    "Vector scan requires primary key format".to_string(),
                    common_error::status_code::StatusCode::InvalidArguments,
                ))
            })?;
            let cache_strategy = &self.input.cache_strategy;

            // Convert Batch to DfRecordBatch for predicate evaluation
            let mut df_record_batches: Vec<datatypes::arrow::array::RecordBatch> = all_batches
                .iter()
                .filter_map(|batch| {
                    if batch.is_empty() {
                        None
                    } else {
                        mapper
                            .convert(batch, cache_strategy)
                            .ok()
                            .map(|rb| rb.into_df_record_batch())
                    }
                })
                .collect();

            // Apply post-filter predicates if any
            if !physical_exprs.is_empty() {
                df_record_batches = self.apply_predicates(df_record_batches, &physical_exprs)?;
            }

            // Limit to original_k rows
            df_record_batches = Self::limit_rows(df_record_batches, original_k);

            if df_record_batches
                .iter()
                .map(|batch| batch.num_rows())
                .sum::<usize>()
                < original_k
                && !reach_end
            {
                factor *= Self::OVERFETCH_MULTIPLIER;
                continue;
            }

            // Convert back to common_recordbatch RecordBatch for the stream
            let record_batches: Vec<_> = df_record_batches
                .into_iter()
                .map(|batch| {
                    common_recordbatch::RecordBatch::from_df_record_batch(self.schema(), batch)
                })
                .collect();

            let schema = self.schema();
            let stream = futures::stream::iter(record_batches.into_iter().map(Ok));
            return Ok(Box::pin(RecordBatchStreamWrapper::new(
                schema,
                Box::pin(stream),
            )));
        }
    }

    /// Builds physical expressions from predicates for post-filtering.
    fn build_physical_exprs(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        let predicate = self.input.predicate_group().predicate();
        let Some(predicate) = predicate else {
            return Vec::new();
        };

        let schema = self.input.mapper.output_schema();
        let arrow_schema = schema.arrow_schema();

        let physical_exprs = predicate
            .to_physical_exprs(arrow_schema)
            .unwrap_or_default();

        debug!(
            "VectorScan: built {} physical exprs from {} predicates",
            physical_exprs.len(),
            predicate.exprs().len()
        );

        physical_exprs
    }

    /// Applies predicate filters to record batches.
    fn apply_predicates(
        &self,
        batches: Vec<datatypes::arrow::array::RecordBatch>,
        physical_exprs: &[Arc<dyn PhysicalExpr>],
    ) -> Result<Vec<datatypes::arrow::array::RecordBatch>, BoxedError> {
        let mut result = Vec::with_capacity(batches.len());
        let total_rows_before: usize = batches.iter().map(|b| b.num_rows()).sum();

        for mut batch in batches {
            // Apply each predicate expression
            for expr in physical_exprs {
                batch = batch_filter(&batch, expr).map_err(|e| {
                    BoxedError::new(common_error::ext::PlainError::new(
                        e.to_string(),
                        common_error::status_code::StatusCode::Internal,
                    ))
                })?;
            }
            if batch.num_rows() > 0 {
                result.push(batch);
            }
        }

        let total_rows_after: usize = result.iter().map(|b| b.num_rows()).sum();
        debug!(
            "VectorScan: applied {} predicates, {} rows -> {} rows",
            physical_exprs.len(),
            total_rows_before,
            total_rows_after
        );

        Ok(result)
    }

    /// Limits the total number of rows across all batches to `limit`.
    fn limit_rows(
        batches: Vec<datatypes::arrow::array::RecordBatch>,
        limit: usize,
    ) -> Vec<datatypes::arrow::array::RecordBatch> {
        let mut result = Vec::new();
        let mut remaining = limit;

        for batch in batches {
            if remaining == 0 {
                break;
            }

            let batch_rows = batch.num_rows();
            if batch_rows <= remaining {
                result.push(batch);
                remaining -= batch_rows;
            } else {
                // Slice the batch to only include the remaining rows
                result.push(batch.slice(0, remaining));
                break;
            }
        }

        result
    }

    /// Groups SST candidates by file index, returning a map from file index to candidates.
    /// Only processes candidates with CandidateSource::SstFile.
    fn group_candidates_by_file(
        candidates: Vec<VectorCandidate>,
    ) -> HashMap<usize, Vec<VectorCandidate>> {
        let mut grouped: HashMap<usize, Vec<VectorCandidate>> = HashMap::new();
        for candidate in candidates {
            if let CandidateSource::SstFile(file_index) = candidate.source {
                grouped.entry(file_index).or_default().push(candidate);
            }
        }
        grouped
    }

    /// Fetches row data from SST files based on the grouped candidates.
    ///
    /// This method:
    /// 1. Reads selected rows from SST files based on vector index results
    /// 2. Filters out deleted rows (rows with OpType::Delete)
    async fn fetch_rows_from_files(
        &self,
        grouped: HashMap<usize, Vec<VectorCandidate>>,
    ) -> Result<Vec<Batch>> {
        let mut all_batches = Vec::new();
        let mut reader_metrics = ReaderMetrics::default();
        let filter_deleted = self.input.filter_deleted;

        for (file_index, candidates) in grouped {
            let file = &self.input.files[file_index];

            // Build file range context without predicate-based row group pruning.
            // Vector scan relies on vector index to determine which rows to read,
            // and applies predicates as post-filter in memory.
            let file_range_builder = self
                .input
                .prune_file_for_vector_scan(file, &mut reader_metrics)
                .await?;

            let Some(context) = file_range_builder.context() else {
                continue;
            };

            // Get parquet metadata to determine row group sizes
            let parquet_meta = context.reader_builder().parquet_metadata();
            let num_row_groups = parquet_meta.num_row_groups();
            let row_group_size = if num_row_groups > 0 {
                parquet_meta.row_group(0).num_rows() as usize
            } else {
                continue;
            };

            // Convert row offsets to BTreeSet for RowGroupSelection
            let row_ids: BTreeSet<u32> = candidates.iter().map(|c| c.row_offset as u32).collect();

            debug!(
                "VectorScan: fetching {} rows from file {} (row_group_size={}, num_row_groups={})",
                row_ids.len(),
                file_index,
                row_group_size,
                num_row_groups
            );

            // Create row selection from row IDs
            let selection =
                RowGroupSelection::from_row_ids(row_ids, row_group_size, num_row_groups);

            // Read selected rows from each row group
            for (row_group_idx, row_selection) in selection.iter() {
                if row_selection.row_count() == 0 {
                    continue;
                }

                let file_range =
                    FileRange::new(context.clone(), *row_group_idx, Some(row_selection.clone()));

                // Read batches from this file range
                let Some(reader) = file_range.reader(None, None).await? else {
                    continue;
                };
                let mut source = Source::PruneReader(reader);
                while let Some(mut batch) = source.next_batch().await? {
                    // Filter deleted rows if required
                    if filter_deleted {
                        batch.filter_deleted()?;
                    }
                    // Only add non-empty batches
                    if !batch.is_empty() {
                        all_batches.push(batch);
                    }
                }
            }
        }

        Ok(all_batches)
    }

    /// Fetches row data from memtables based on the candidates.
    ///
    /// Unlike SST files, we need to re-iterate through memtables to fetch
    /// the actual row data since memtables don't support random row access.
    fn fetch_rows_from_memtables(&self, candidates: Vec<VectorCandidate>) -> Result<Vec<Batch>> {
        if candidates.is_empty() {
            return Ok(Vec::new());
        }

        // Group candidates by memtable index and row offset for efficient lookup
        let mut memtable_rows: HashMap<usize, BTreeSet<u64>> = HashMap::new();
        for candidate in &candidates {
            if let CandidateSource::Memtable(memtable_index) = candidate.source {
                memtable_rows
                    .entry(memtable_index)
                    .or_default()
                    .insert(candidate.row_offset);
            }
        }

        let mut all_batches = Vec::new();
        let filter_deleted = self.input.filter_deleted;

        for (memtable_index, target_rows) in memtable_rows {
            let mem_range_builder = &self.input.memtables[memtable_index];

            // Build memtable ranges
            let mut ranges: SmallVec<[MemtableRange; 2]> = SmallVec::new();
            mem_range_builder.build_ranges(0, &mut ranges);

            // Track row offset across all ranges within this memtable
            let mut current_row_offset = 0u64;

            for range in ranges {
                let iter = range.build_iter()?;

                for batch_result in iter {
                    let mut batch = batch_result?;
                    let batch_rows = batch.num_rows();

                    // Check if any target rows fall within this batch
                    let batch_end_offset = current_row_offset + batch_rows as u64;

                    // Find rows in this batch that we need
                    let rows_in_batch: Vec<usize> = target_rows
                        .range(current_row_offset..batch_end_offset)
                        .map(|&offset| (offset - current_row_offset) as usize)
                        .collect();

                    if !rows_in_batch.is_empty() {
                        // Create a boolean mask for the target rows
                        let mut mask_builder = BooleanVectorBuilder::with_capacity(batch_rows);
                        let rows_set: std::collections::HashSet<usize> =
                            rows_in_batch.into_iter().collect();
                        for i in 0..batch_rows {
                            mask_builder.push(Some(rows_set.contains(&i)));
                        }
                        let mask = mask_builder.finish();

                        // Filter the batch to only include the target rows
                        batch.filter(&mask)?;

                        // Filter deleted rows if required
                        if filter_deleted {
                            batch.filter_deleted()?;
                        }

                        if !batch.is_empty() {
                            all_batches.push(batch);
                        }
                    }

                    current_row_offset = batch_end_offset;
                }
            }
        }

        Ok(all_batches)
    }

    /// Scan [`Batch`] in all partitions one by one.
    ///
    /// For VectorScan, this returns an empty stream since vector search
    /// doesn't use the batch-based approach. The actual results are fetched
    /// in `build_stream`.
    pub(crate) fn scan_all_partitions(&self) -> Result<ScanBatchStream> {
        // Vector scan doesn't use the partition-based batch approach
        // Return an empty stream
        Ok(Box::pin(futures::stream::empty()))
    }
}

impl RegionScanner for VectorScan {
    fn name(&self) -> &str {
        "VectorScan"
    }

    fn properties(&self) -> &ScannerProperties {
        &self.properties
    }

    fn schema(&self) -> SchemaRef {
        self.input.mapper.output_schema()
    }

    fn metadata(&self) -> RegionMetadataRef {
        self.input.mapper.metadata().clone()
    }

    fn prepare(&mut self, request: PrepareRequest) -> Result<(), BoxedError> {
        self.properties.prepare(request);
        Ok(())
    }

    fn scan_partition(
        &self,
        _ctx: &QueryScanContext,
        _metrics_set: &ExecutionPlanMetricsSet,
        partition: usize,
    ) -> Result<SendableRecordBatchStream, BoxedError> {
        if partition >= self.properties.num_partitions() {
            return Err(BoxedError::new(
                crate::error::PartitionOutOfRangeSnafu {
                    given: partition,
                    all: self.properties.num_partitions(),
                }
                .build(),
            ));
        }

        // Vector scan uses single partition (partition 0) for actual work
        // Other partitions return empty streams
        if partition > 0 {
            let schema = self.schema();
            let stream = futures::stream::empty();
            return Ok(Box::pin(RecordBatchStreamWrapper::new(
                schema,
                Box::pin(stream),
            )));
        }

        // Create a stream that runs the vector search
        let input = self.input.clone();
        let schema = self.schema();

        let stream = futures::stream::once(async move {
            let scan = VectorScan {
                properties: ScannerProperties::default()
                    .with_append_mode(input.append_mode)
                    .with_total_rows(input.total_rows()),
                input,
                metrics_list: PartitionMetricsList::default(),
            };
            scan.build_stream().await
        })
        .flat_map(|result| {
            match result {
                Ok(stream) => stream.boxed(),
                Err(e) => {
                    // Convert BoxedError to DataFusionError for the record batch stream
                    let df_error = datafusion::common::DataFusionError::External(Box::new(e));
                    futures::stream::once(async move {
                        Err(common_recordbatch::error::Error::from(df_error))
                    })
                    .boxed()
                }
            }
        });

        Ok(Box::pin(RecordBatchStreamWrapper::new(
            schema,
            Box::pin(stream),
        )))
    }

    fn has_predicate_without_region(&self) -> bool {
        let predicate = self.input.predicate_group().predicate_without_region();
        predicate.map(|p| !p.exprs().is_empty()).unwrap_or(false)
    }

    fn set_logical_region(&mut self, logical_region: bool) {
        self.properties.set_logical_region(logical_region);
    }
}

impl DisplayAs for VectorScan {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "VectorScan: region={}, ",
            self.input.mapper.metadata().region_id
        )?;
        match t {
            DisplayFormatType::Default | DisplayFormatType::TreeRender => {
                write!(f, "files={}", self.input.files.len())
            }
            DisplayFormatType::Verbose => {
                write!(f, "files={}", self.input.files.len())?;
                self.metrics_list.format_verbose_metrics(f)
            }
        }
    }
}

impl fmt::Debug for VectorScan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VectorScan")
            .field("num_files", &self.input.files.len())
            .finish()
    }
}

#[cfg(test)]
impl VectorScan {
    /// Returns the input.
    pub(crate) fn input(&self) -> &ScanInput {
        &self.input
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::arrow::array::{Int32Array, RecordBatch};
    use datatypes::arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    /// Helper function to create a simple RecordBatch for testing.
    fn create_test_batch(num_rows: usize) -> datatypes::arrow::array::RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let array = Int32Array::from_iter_values(0..num_rows as i32);
        RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
    }

    #[test]
    fn test_limit_rows_empty() {
        let batches: Vec<datatypes::arrow::array::RecordBatch> = vec![];
        let result = VectorScan::limit_rows(batches, 10);
        assert!(result.is_empty());
    }

    #[test]
    fn test_limit_rows_single_batch_under_limit() {
        let batch = create_test_batch(5);
        let batches = vec![batch];
        let result = VectorScan::limit_rows(batches, 10);

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 5);
    }

    #[test]
    fn test_limit_rows_single_batch_over_limit() {
        let batch = create_test_batch(10);
        let batches = vec![batch];
        let result = VectorScan::limit_rows(batches, 5);

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 5);
    }

    #[test]
    fn test_limit_rows_multiple_batches_under_limit() {
        let batches = vec![
            create_test_batch(3),
            create_test_batch(4),
            create_test_batch(2),
        ];
        let result = VectorScan::limit_rows(batches, 15);

        assert_eq!(result.len(), 3);
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 9);
    }

    #[test]
    fn test_limit_rows_multiple_batches_exact_limit() {
        let batches = vec![
            create_test_batch(3),
            create_test_batch(4),
            create_test_batch(3),
        ];
        let result = VectorScan::limit_rows(batches, 10);

        assert_eq!(result.len(), 3);
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 10);
    }

    #[test]
    fn test_limit_rows_multiple_batches_over_limit() {
        let batches = vec![
            create_test_batch(5),
            create_test_batch(5),
            create_test_batch(5),
        ];
        let result = VectorScan::limit_rows(batches, 8);

        // Should include first batch (5 rows) + partial second batch (3 rows)
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].num_rows(), 5);
        assert_eq!(result[1].num_rows(), 3);
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 8);
    }

    #[test]
    fn test_limit_rows_zero_limit() {
        let batches = vec![create_test_batch(5)];
        let result = VectorScan::limit_rows(batches, 0);
        assert!(result.is_empty());
    }

    /// Helper function to merge topk candidates (extracted for testing).
    /// This duplicates the logic from VectorScan::merge_topk to enable unit testing
    /// without requiring a full VectorScan instance.
    fn merge_topk_helper(candidates: Vec<VectorCandidate>, k: usize) -> Vec<VectorCandidate> {
        if candidates.len() <= k {
            let mut result = candidates;
            result.sort();
            return result;
        }

        let mut heap: BinaryHeap<VectorCandidate> = BinaryHeap::with_capacity(k + 1);

        for candidate in candidates {
            if heap.len() < k {
                heap.push(candidate);
            } else if let Some(worst) = heap.peek()
                && candidate.distance < worst.distance
            {
                heap.pop();
                heap.push(candidate);
            }
        }

        let mut result: Vec<_> = heap.into_iter().collect();
        result.sort();
        result
    }

    #[test]
    fn test_merge_topk_fewer_than_k() {
        let candidates = vec![
            VectorCandidate {
                source: CandidateSource::SstFile(0),
                row_offset: 0,
                distance: 1.0,
            },
            VectorCandidate {
                source: CandidateSource::SstFile(0),
                row_offset: 1,
                distance: 2.0,
            },
        ];

        let result = merge_topk_helper(candidates, 5);

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].distance, 1.0);
        assert_eq!(result[1].distance, 2.0);
    }

    #[test]
    fn test_merge_topk_exact_k() {
        let candidates = vec![
            VectorCandidate {
                source: CandidateSource::SstFile(0),
                row_offset: 0,
                distance: 3.0,
            },
            VectorCandidate {
                source: CandidateSource::SstFile(0),
                row_offset: 1,
                distance: 1.0,
            },
            VectorCandidate {
                source: CandidateSource::SstFile(0),
                row_offset: 2,
                distance: 2.0,
            },
        ];

        let result = merge_topk_helper(candidates, 3);

        assert_eq!(result.len(), 3);
        // Should be sorted by distance
        assert_eq!(result[0].distance, 1.0);
        assert_eq!(result[1].distance, 2.0);
        assert_eq!(result[2].distance, 3.0);
    }

    #[test]
    fn test_merge_topk_more_than_k() {
        let candidates = vec![
            VectorCandidate {
                source: CandidateSource::SstFile(0),
                row_offset: 0,
                distance: 5.0,
            },
            VectorCandidate {
                source: CandidateSource::SstFile(0),
                row_offset: 1,
                distance: 1.0,
            },
            VectorCandidate {
                source: CandidateSource::SstFile(0),
                row_offset: 2,
                distance: 3.0,
            },
            VectorCandidate {
                source: CandidateSource::SstFile(0),
                row_offset: 3,
                distance: 2.0,
            },
            VectorCandidate {
                source: CandidateSource::SstFile(0),
                row_offset: 4,
                distance: 4.0,
            },
        ];

        let result = merge_topk_helper(candidates, 3);

        assert_eq!(result.len(), 3);
        // Should have top 3 smallest distances: 1.0, 2.0, 3.0
        assert_eq!(result[0].distance, 1.0);
        assert_eq!(result[1].distance, 2.0);
        assert_eq!(result[2].distance, 3.0);
    }

    #[test]
    fn test_group_candidates_by_file() {
        let candidates = vec![
            VectorCandidate {
                source: CandidateSource::SstFile(0),
                row_offset: 10,
                distance: 1.0,
            },
            VectorCandidate {
                source: CandidateSource::SstFile(1),
                row_offset: 20,
                distance: 2.0,
            },
            VectorCandidate {
                source: CandidateSource::SstFile(0),
                row_offset: 30,
                distance: 3.0,
            },
            VectorCandidate {
                source: CandidateSource::Memtable(0),
                row_offset: 40,
                distance: 4.0,
            },
        ];

        let grouped = VectorScan::group_candidates_by_file(candidates);

        assert_eq!(grouped.len(), 2); // Only SST files
        assert_eq!(grouped.get(&0).unwrap().len(), 2);
        assert_eq!(grouped.get(&1).unwrap().len(), 1);
    }

    #[test]
    fn test_vector_candidate_ordering() {
        let c1 = VectorCandidate {
            source: CandidateSource::SstFile(0),
            row_offset: 0,
            distance: 1.0,
        };
        let c2 = VectorCandidate {
            source: CandidateSource::SstFile(0),
            row_offset: 1,
            distance: 2.0,
        };
        let c3 = VectorCandidate {
            source: CandidateSource::SstFile(0),
            row_offset: 2,
            distance: 1.0,
        };

        // c1 < c2 because distance 1.0 < 2.0
        assert!(c1 < c2);
        // c1 == c3 because distances are equal
        assert!(c1 == c3);
    }
}
