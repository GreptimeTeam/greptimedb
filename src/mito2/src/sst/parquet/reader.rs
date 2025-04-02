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

//! Parquet reader.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, Instant};

use api::v1::SemanticType;
use async_trait::async_trait;
use common_recordbatch::filter::SimpleFilterEvaluator;
use common_telemetry::{debug, warn};
use datafusion_expr::Expr;
use datatypes::arrow::error::ArrowError;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::data_type::ConcreteDataType;
use itertools::Itertools;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, RowSelection};
use parquet::arrow::{parquet_to_arrow_field_levels, FieldLevels, ProjectionMask};
use parquet::file::metadata::ParquetMetaData;
use parquet::format::KeyValue;
use snafu::{OptionExt, ResultExt};
use store_api::metadata::{RegionMetadata, RegionMetadataRef};
use store_api::storage::ColumnId;
use table::predicate::Predicate;

use crate::cache::CacheStrategy;
use crate::error::{
    ArrowReaderSnafu, InvalidMetadataSnafu, InvalidParquetSnafu, ReadDataPartSnafu,
    ReadParquetSnafu, Result,
};
use crate::metrics::{
    PRECISE_FILTER_ROWS_TOTAL, READ_ROWS_IN_ROW_GROUP_TOTAL, READ_ROWS_TOTAL,
    READ_ROW_GROUPS_TOTAL, READ_STAGE_ELAPSED,
};
use crate::read::prune::{PruneReader, Source};
use crate::read::{Batch, BatchReader};
use crate::row_converter::build_primary_key_codec;
use crate::sst::file::FileHandle;
use crate::sst::index::bloom_filter::applier::BloomFilterIndexApplierRef;
use crate::sst::index::fulltext_index::applier::FulltextIndexApplierRef;
use crate::sst::index::inverted_index::applier::InvertedIndexApplierRef;
use crate::sst::parquet::file_range::{FileRangeContext, FileRangeContextRef};
use crate::sst::parquet::format::ReadFormat;
use crate::sst::parquet::metadata::MetadataLoader;
use crate::sst::parquet::row_group::InMemoryRowGroup;
use crate::sst::parquet::row_selection::{
    intersect_row_selections, row_selection_from_row_ranges, row_selection_from_sorted_row_ids,
};
use crate::sst::parquet::stats::RowGroupPruningStats;
use crate::sst::parquet::{DEFAULT_READ_BATCH_SIZE, PARQUET_METADATA_KEY};

/// Parquet SST reader builder.
pub struct ParquetReaderBuilder {
    /// SST directory.
    file_dir: String,
    file_handle: FileHandle,
    object_store: ObjectStore,
    /// Predicate to push down.
    predicate: Option<Predicate>,
    /// Metadata of columns to read.
    ///
    /// `None` reads all columns. Due to schema change, the projection
    /// can contain columns not in the parquet file.
    projection: Option<Vec<ColumnId>>,
    /// Strategy to cache SST data.
    cache_strategy: CacheStrategy,
    /// Index appliers.
    inverted_index_applier: Option<InvertedIndexApplierRef>,
    bloom_filter_index_applier: Option<BloomFilterIndexApplierRef>,
    fulltext_index_applier: Option<FulltextIndexApplierRef>,
    /// Expected metadata of the region while reading the SST.
    /// This is usually the latest metadata of the region. The reader use
    /// it get the correct column id of a column by name.
    expected_metadata: Option<RegionMetadataRef>,
}

impl ParquetReaderBuilder {
    /// Returns a new [ParquetReaderBuilder] to read specific SST.
    pub fn new(
        file_dir: String,
        file_handle: FileHandle,
        object_store: ObjectStore,
    ) -> ParquetReaderBuilder {
        ParquetReaderBuilder {
            file_dir,
            file_handle,
            object_store,
            predicate: None,
            projection: None,
            cache_strategy: CacheStrategy::Disabled,
            inverted_index_applier: None,
            bloom_filter_index_applier: None,
            fulltext_index_applier: None,
            expected_metadata: None,
        }
    }

    /// Attaches the predicate to the builder.
    #[must_use]
    pub fn predicate(mut self, predicate: Option<Predicate>) -> ParquetReaderBuilder {
        self.predicate = predicate;
        self
    }

    /// Attaches the projection to the builder.
    ///
    /// The reader only applies the projection to fields.
    #[must_use]
    pub fn projection(mut self, projection: Option<Vec<ColumnId>>) -> ParquetReaderBuilder {
        self.projection = projection;
        self
    }

    /// Attaches the cache to the builder.
    #[must_use]
    pub fn cache(mut self, cache: CacheStrategy) -> ParquetReaderBuilder {
        self.cache_strategy = cache;
        self
    }

    /// Attaches the inverted index applier to the builder.
    #[must_use]
    pub(crate) fn inverted_index_applier(
        mut self,
        index_applier: Option<InvertedIndexApplierRef>,
    ) -> Self {
        self.inverted_index_applier = index_applier;
        self
    }

    /// Attaches the bloom filter index applier to the builder.
    #[must_use]
    pub(crate) fn bloom_filter_index_applier(
        mut self,
        index_applier: Option<BloomFilterIndexApplierRef>,
    ) -> Self {
        self.bloom_filter_index_applier = index_applier;
        self
    }

    /// Attaches the fulltext index applier to the builder.
    #[must_use]
    pub(crate) fn fulltext_index_applier(
        mut self,
        index_applier: Option<FulltextIndexApplierRef>,
    ) -> Self {
        self.fulltext_index_applier = index_applier;
        self
    }

    /// Attaches the expected metadata to the builder.
    #[must_use]
    pub fn expected_metadata(mut self, expected_metadata: Option<RegionMetadataRef>) -> Self {
        self.expected_metadata = expected_metadata;
        self
    }

    /// Builds a [ParquetReader].
    ///
    /// This needs to perform IO operation.
    pub async fn build(&self) -> Result<ParquetReader> {
        let mut metrics = ReaderMetrics::default();

        let (context, row_groups) = self.build_reader_input(&mut metrics).await?;
        ParquetReader::new(Arc::new(context), row_groups).await
    }

    /// Builds a [FileRangeContext] and collects row groups to read.
    ///
    /// This needs to perform IO operation.
    pub(crate) async fn build_reader_input(
        &self,
        metrics: &mut ReaderMetrics,
    ) -> Result<(FileRangeContext, RowGroupMap)> {
        let start = Instant::now();

        let file_path = self.file_handle.file_path(&self.file_dir);
        let file_size = self.file_handle.meta_ref().file_size;
        // Loads parquet metadata of the file.
        let parquet_meta = self.read_parquet_metadata(&file_path, file_size).await?;
        // Decodes region metadata.
        let key_value_meta = parquet_meta.file_metadata().key_value_metadata();
        // Gets the metadata stored in the SST.
        let region_meta = Arc::new(Self::get_region_metadata(&file_path, key_value_meta)?);
        let read_format = if let Some(column_ids) = &self.projection {
            ReadFormat::new(region_meta.clone(), column_ids.iter().copied())
        } else {
            // Lists all column ids to read, we always use the expected metadata if possible.
            let expected_meta = self.expected_metadata.as_ref().unwrap_or(&region_meta);
            ReadFormat::new(
                region_meta.clone(),
                expected_meta
                    .column_metadatas
                    .iter()
                    .map(|col| col.column_id),
            )
        };

        // Computes the projection mask.
        let parquet_schema_desc = parquet_meta.file_metadata().schema_descr();
        let indices = read_format.projection_indices();
        // Now we assumes we don't have nested schemas.
        // TODO(yingwen): Revisit this if we introduce nested types such as JSON type.
        let projection_mask = ProjectionMask::roots(parquet_schema_desc, indices.iter().copied());

        // Computes the field levels.
        let hint = Some(read_format.arrow_schema().fields());
        let field_levels =
            parquet_to_arrow_field_levels(parquet_schema_desc, projection_mask.clone(), hint)
                .context(ReadDataPartSnafu)?;
        let row_groups = self
            .row_groups_to_read(&read_format, &parquet_meta, &mut metrics.filter_metrics)
            .await;

        let reader_builder = RowGroupReaderBuilder {
            file_handle: self.file_handle.clone(),
            file_path,
            parquet_meta,
            object_store: self.object_store.clone(),
            projection: projection_mask,
            field_levels,
            cache_strategy: self.cache_strategy.clone(),
        };

        let filters = if let Some(predicate) = &self.predicate {
            predicate
                .exprs()
                .iter()
                .filter_map(|expr| {
                    SimpleFilterContext::new_opt(
                        &region_meta,
                        self.expected_metadata.as_deref(),
                        expr,
                    )
                })
                .collect::<Vec<_>>()
        } else {
            vec![]
        };

        let codec = build_primary_key_codec(read_format.metadata());

        let context = FileRangeContext::new(reader_builder, filters, read_format, codec);

        metrics.build_cost += start.elapsed();

        Ok((context, row_groups))
    }

    /// Decodes region metadata from key value.
    fn get_region_metadata(
        file_path: &str,
        key_value_meta: Option<&Vec<KeyValue>>,
    ) -> Result<RegionMetadata> {
        let key_values = key_value_meta.context(InvalidParquetSnafu {
            file: file_path,
            reason: "missing key value meta",
        })?;
        let meta_value = key_values
            .iter()
            .find(|kv| kv.key == PARQUET_METADATA_KEY)
            .with_context(|| InvalidParquetSnafu {
                file: file_path,
                reason: format!("key {} not found", PARQUET_METADATA_KEY),
            })?;
        let json = meta_value
            .value
            .as_ref()
            .with_context(|| InvalidParquetSnafu {
                file: file_path,
                reason: format!("No value for key {}", PARQUET_METADATA_KEY),
            })?;

        RegionMetadata::from_json(json).context(InvalidMetadataSnafu)
    }

    /// Reads parquet metadata of specific file.
    async fn read_parquet_metadata(
        &self,
        file_path: &str,
        file_size: u64,
    ) -> Result<Arc<ParquetMetaData>> {
        let _t = READ_STAGE_ELAPSED
            .with_label_values(&["read_parquet_metadata"])
            .start_timer();

        let region_id = self.file_handle.region_id();
        let file_id = self.file_handle.file_id();
        // Tries to get from global cache.
        if let Some(metadata) = self
            .cache_strategy
            .get_parquet_meta_data(region_id, file_id)
            .await
        {
            return Ok(metadata);
        }

        // Cache miss, load metadata directly.
        let metadata_loader = MetadataLoader::new(self.object_store.clone(), file_path, file_size);
        let metadata = metadata_loader.load().await?;
        let metadata = Arc::new(metadata);
        // Cache the metadata.
        self.cache_strategy.put_parquet_meta_data(
            self.file_handle.region_id(),
            self.file_handle.file_id(),
            metadata.clone(),
        );

        Ok(metadata)
    }

    /// Computes row groups to read, along with their respective row selections.
    async fn row_groups_to_read(
        &self,
        read_format: &ReadFormat,
        parquet_meta: &ParquetMetaData,
        metrics: &mut ReaderFilterMetrics,
    ) -> BTreeMap<usize, Option<RowSelection>> {
        let num_row_groups = parquet_meta.num_row_groups();
        let num_rows = parquet_meta.file_metadata().num_rows();
        if num_row_groups == 0 || num_rows == 0 {
            return BTreeMap::default();
        }

        // Let's assume that the number of rows in the first row group
        // can represent the `row_group_size` of the Parquet file.
        let row_group_size = parquet_meta.row_group(0).num_rows() as usize;
        if row_group_size == 0 {
            return BTreeMap::default();
        }

        metrics.rg_total += num_row_groups;
        metrics.rows_total += num_rows as usize;

        let mut output = (0..num_row_groups).map(|i| (i, None)).collect();

        self.prune_row_groups_by_fulltext_index(row_group_size, parquet_meta, &mut output, metrics)
            .await;
        if output.is_empty() {
            return output;
        }

        let inverted_filtered = self
            .prune_row_groups_by_inverted_index(row_group_size, parquet_meta, &mut output, metrics)
            .await;
        if output.is_empty() {
            return output;
        }

        if !inverted_filtered {
            self.prune_row_groups_by_minmax(read_format, parquet_meta, &mut output, metrics);
        }

        self.prune_row_groups_by_bloom_filter(parquet_meta, &mut output, metrics)
            .await;

        output
    }

    /// Prunes row groups by fulltext index. Returns `true` if the row groups are pruned.
    async fn prune_row_groups_by_fulltext_index(
        &self,
        row_group_size: usize,
        parquet_meta: &ParquetMetaData,
        output: &mut BTreeMap<usize, Option<RowSelection>>,
        metrics: &mut ReaderFilterMetrics,
    ) -> bool {
        let Some(index_applier) = &self.fulltext_index_applier else {
            return false;
        };
        if !self.file_handle.meta_ref().fulltext_index_available() {
            return false;
        }

        let file_size_hint = self.file_handle.meta_ref().index_file_size();
        let apply_res = match index_applier
            .apply(self.file_handle.file_id(), Some(file_size_hint))
            .await
        {
            Ok(res) => res,
            Err(err) => {
                if cfg!(any(test, feature = "test")) {
                    panic!(
                        "Failed to apply full-text index, region_id: {}, file_id: {}, err: {:?}",
                        self.file_handle.region_id(),
                        self.file_handle.file_id(),
                        err
                    );
                } else {
                    warn!(
                        err; "Failed to apply full-text index, region_id: {}, file_id: {}",
                        self.file_handle.region_id(), self.file_handle.file_id()
                    );
                }

                return false;
            }
        };

        let row_group_to_row_ids =
            Self::group_row_ids(apply_res, row_group_size, parquet_meta.num_row_groups());
        Self::prune_row_groups_by_rows(
            parquet_meta,
            row_group_to_row_ids,
            output,
            &mut metrics.rg_fulltext_filtered,
            &mut metrics.rows_fulltext_filtered,
        );

        true
    }

    /// Groups row IDs into row groups, with each group's row IDs starting from 0.
    fn group_row_ids(
        row_ids: BTreeSet<u32>,
        row_group_size: usize,
        num_row_groups: usize,
    ) -> Vec<(usize, Vec<usize>)> {
        let est_rows_per_group = row_ids.len() / num_row_groups;

        let mut row_group_to_row_ids: Vec<(usize, Vec<usize>)> = Vec::with_capacity(num_row_groups);
        for row_id in row_ids {
            let row_group_id = row_id as usize / row_group_size;
            let row_id_in_group = row_id as usize % row_group_size;

            if let Some((rg_id, row_ids)) = row_group_to_row_ids.last_mut()
                && *rg_id == row_group_id
            {
                row_ids.push(row_id_in_group);
            } else {
                let mut row_ids = Vec::with_capacity(est_rows_per_group);
                row_ids.push(row_id_in_group);
                row_group_to_row_ids.push((row_group_id, row_ids));
            }
        }

        row_group_to_row_ids
    }

    /// Applies index to prune row groups.
    ///
    /// TODO(zhongzc): Devise a mechanism to enforce the non-use of indices
    /// as an escape route in case of index issues, and it can be used to test
    /// the correctness of the index.
    async fn prune_row_groups_by_inverted_index(
        &self,
        row_group_size: usize,
        parquet_meta: &ParquetMetaData,
        output: &mut BTreeMap<usize, Option<RowSelection>>,
        metrics: &mut ReaderFilterMetrics,
    ) -> bool {
        let Some(index_applier) = &self.inverted_index_applier else {
            return false;
        };

        if !self.file_handle.meta_ref().inverted_index_available() {
            return false;
        }
        let file_size_hint = self.file_handle.meta_ref().index_file_size();
        let apply_output = match index_applier
            .apply(self.file_handle.file_id(), Some(file_size_hint))
            .await
        {
            Ok(output) => output,
            Err(err) => {
                if cfg!(any(test, feature = "test")) {
                    panic!(
                        "Failed to apply inverted index, region_id: {}, file_id: {}, err: {:?}",
                        self.file_handle.region_id(),
                        self.file_handle.file_id(),
                        err
                    );
                } else {
                    warn!(
                        err; "Failed to apply inverted index, region_id: {}, file_id: {}",
                        self.file_handle.region_id(), self.file_handle.file_id()
                    );
                }

                return false;
            }
        };

        let segment_row_count = apply_output.segment_row_count;
        let grouped_in_row_groups = apply_output
            .matched_segment_ids
            .iter_ones()
            .map(|seg_id| {
                let begin_row_id = seg_id * segment_row_count;
                let row_group_id = begin_row_id / row_group_size;

                let rg_begin_row_id = begin_row_id % row_group_size;
                let rg_end_row_id = rg_begin_row_id + segment_row_count;

                (row_group_id, rg_begin_row_id..rg_end_row_id)
            })
            .chunk_by(|(row_group_id, _)| *row_group_id);

        let ranges_in_row_groups = grouped_in_row_groups
            .into_iter()
            .map(|(row_group_id, group)| (row_group_id, group.map(|(_, ranges)| ranges)));

        Self::prune_row_groups_by_ranges(
            parquet_meta,
            ranges_in_row_groups,
            output,
            &mut metrics.rg_inverted_filtered,
            &mut metrics.rows_inverted_filtered,
        );

        true
    }

    /// Prunes row groups by min-max index.
    fn prune_row_groups_by_minmax(
        &self,
        read_format: &ReadFormat,
        parquet_meta: &ParquetMetaData,
        output: &mut BTreeMap<usize, Option<RowSelection>>,
        metrics: &mut ReaderFilterMetrics,
    ) -> bool {
        let Some(predicate) = &self.predicate else {
            return false;
        };

        let row_groups_before = output.len();

        let region_meta = read_format.metadata();
        let row_groups = parquet_meta.row_groups();
        let stats =
            RowGroupPruningStats::new(row_groups, read_format, self.expected_metadata.clone());
        // Here we use the schema of the SST to build the physical expression. If the column
        // in the SST doesn't have the same column id as the column in the expected metadata,
        // we will get a None statistics for that column.
        let res = predicate
            .prune_with_stats(&stats, region_meta.schema.arrow_schema())
            .iter()
            .zip(0..parquet_meta.num_row_groups())
            .filter_map(|(mask, row_group)| {
                if !*mask {
                    return None;
                }

                let selection = output.remove(&row_group)?;
                Some((row_group, selection))
            })
            .collect::<BTreeMap<_, _>>();

        let row_groups_after = res.len();
        metrics.rg_minmax_filtered += row_groups_before - row_groups_after;

        *output = res;
        true
    }

    async fn prune_row_groups_by_bloom_filter(
        &self,
        parquet_meta: &ParquetMetaData,
        output: &mut BTreeMap<usize, Option<RowSelection>>,
        metrics: &mut ReaderFilterMetrics,
    ) -> bool {
        let Some(index_applier) = &self.bloom_filter_index_applier else {
            return false;
        };

        if !self.file_handle.meta_ref().bloom_filter_index_available() {
            return false;
        }

        let file_size_hint = self.file_handle.meta_ref().index_file_size();
        let apply_output = match index_applier
            .apply(
                self.file_handle.file_id(),
                Some(file_size_hint),
                parquet_meta
                    .row_groups()
                    .iter()
                    .enumerate()
                    .map(|(i, rg)| (rg.num_rows() as usize, output.contains_key(&i))),
            )
            .await
        {
            Ok(apply_output) => apply_output,
            Err(err) => {
                if cfg!(any(test, feature = "test")) {
                    panic!(
                        "Failed to apply bloom filter index, region_id: {}, file_id: {}, err: {:?}",
                        self.file_handle.region_id(),
                        self.file_handle.file_id(),
                        err
                    );
                } else {
                    warn!(
                        err; "Failed to apply bloom filter index, region_id: {}, file_id: {}",
                        self.file_handle.region_id(), self.file_handle.file_id()
                    );
                }

                return false;
            }
        };

        Self::prune_row_groups_by_ranges(
            parquet_meta,
            apply_output
                .into_iter()
                .map(|(rg, ranges)| (rg, ranges.into_iter())),
            output,
            &mut metrics.rg_bloom_filtered,
            &mut metrics.rows_bloom_filtered,
        );

        true
    }

    /// Prunes row groups by rows. The `rows_in_row_groups` is like a map from row group to
    /// a list of row ids to keep.
    fn prune_row_groups_by_rows(
        parquet_meta: &ParquetMetaData,
        rows_in_row_groups: Vec<(usize, Vec<usize>)>,
        output: &mut BTreeMap<usize, Option<RowSelection>>,
        filtered_row_groups: &mut usize,
        filtered_rows: &mut usize,
    ) {
        let row_groups_before = output.len();
        let mut rows_in_row_group_before = 0;
        let mut rows_in_row_group_after = 0;

        let mut res = BTreeMap::new();
        for (row_group, row_ids) in rows_in_row_groups {
            let Some(selection) = output.remove(&row_group) else {
                continue;
            };

            let total_row_count = parquet_meta.row_group(row_group).num_rows() as usize;
            rows_in_row_group_before += selection
                .as_ref()
                .map_or(total_row_count, |s| s.row_count());

            let new_selection =
                row_selection_from_sorted_row_ids(row_ids.into_iter(), total_row_count);
            let intersected_selection = intersect_row_selections(selection, Some(new_selection));

            let num_rows_after = intersected_selection
                .as_ref()
                .map_or(total_row_count, |s| s.row_count());
            rows_in_row_group_after += num_rows_after;

            if num_rows_after > 0 {
                res.insert(row_group, intersected_selection);
            }
        }

        // Pruned row groups.
        while let Some((row_group, selection)) = output.pop_first() {
            let total_row_count = parquet_meta.row_group(row_group).num_rows() as usize;
            rows_in_row_group_before += selection
                .as_ref()
                .map_or(total_row_count, |s| s.row_count());
        }

        let row_groups_after = res.len();
        *filtered_row_groups += row_groups_before - row_groups_after;
        *filtered_rows += rows_in_row_group_before - rows_in_row_group_after;

        *output = res;
    }

    /// Prunes row groups by ranges. The `ranges_in_row_groups` is like a map from row group to
    /// a list of row ranges to keep.
    fn prune_row_groups_by_ranges(
        parquet_meta: &ParquetMetaData,
        ranges_in_row_groups: impl Iterator<Item = (usize, impl Iterator<Item = Range<usize>>)>,
        output: &mut BTreeMap<usize, Option<RowSelection>>,
        filtered_row_groups: &mut usize,
        filtered_rows: &mut usize,
    ) {
        let row_groups_before = output.len();
        let mut rows_in_row_group_before = 0;
        let mut rows_in_row_group_after = 0;

        let mut res = BTreeMap::new();
        for (row_group, row_ranges) in ranges_in_row_groups {
            let Some(selection) = output.remove(&row_group) else {
                continue;
            };

            let total_row_count = parquet_meta.row_group(row_group).num_rows() as usize;
            rows_in_row_group_before += selection
                .as_ref()
                .map_or(total_row_count, |s| s.row_count());

            let new_selection = row_selection_from_row_ranges(row_ranges, total_row_count);
            let intersected_selection = intersect_row_selections(selection, Some(new_selection));

            let num_rows_after = intersected_selection
                .as_ref()
                .map_or(total_row_count, |s| s.row_count());
            rows_in_row_group_after += num_rows_after;

            if num_rows_after > 0 {
                res.insert(row_group, intersected_selection);
            }
        }

        // Pruned row groups.
        while let Some((row_group, selection)) = output.pop_first() {
            let total_row_count = parquet_meta.row_group(row_group).num_rows() as usize;
            rows_in_row_group_before += selection
                .as_ref()
                .map_or(total_row_count, |s| s.row_count());
        }

        let row_groups_after = res.len();
        *filtered_row_groups += row_groups_before - row_groups_after;
        *filtered_rows += rows_in_row_group_before - rows_in_row_group_after;

        *output = res;
    }
}

/// Metrics of filtering rows groups and rows.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct ReaderFilterMetrics {
    /// Number of row groups before filtering.
    pub(crate) rg_total: usize,
    /// Number of row groups filtered by fulltext index.
    pub(crate) rg_fulltext_filtered: usize,
    /// Number of row groups filtered by inverted index.
    pub(crate) rg_inverted_filtered: usize,
    /// Number of row groups filtered by min-max index.
    pub(crate) rg_minmax_filtered: usize,
    /// Number of row groups filtered by bloom filter index.
    pub(crate) rg_bloom_filtered: usize,

    /// Number of rows in row group before filtering.
    pub(crate) rows_total: usize,
    /// Number of rows in row group filtered by fulltext index.
    pub(crate) rows_fulltext_filtered: usize,
    /// Number of rows in row group filtered by inverted index.
    pub(crate) rows_inverted_filtered: usize,
    /// Number of rows in row group filtered by bloom filter index.
    pub(crate) rows_bloom_filtered: usize,
    /// Number of rows filtered by precise filter.
    pub(crate) rows_precise_filtered: usize,
}

impl ReaderFilterMetrics {
    /// Adds `other` metrics to this metrics.
    pub(crate) fn merge_from(&mut self, other: &ReaderFilterMetrics) {
        self.rg_total += other.rg_total;
        self.rg_fulltext_filtered += other.rg_fulltext_filtered;
        self.rg_inverted_filtered += other.rg_inverted_filtered;
        self.rg_minmax_filtered += other.rg_minmax_filtered;
        self.rg_bloom_filtered += other.rg_bloom_filtered;

        self.rows_total += other.rows_total;
        self.rows_fulltext_filtered += other.rows_fulltext_filtered;
        self.rows_inverted_filtered += other.rows_inverted_filtered;
        self.rows_bloom_filtered += other.rows_bloom_filtered;
        self.rows_precise_filtered += other.rows_precise_filtered;
    }

    /// Reports metrics.
    pub(crate) fn observe(&self) {
        READ_ROW_GROUPS_TOTAL
            .with_label_values(&["before_filtering"])
            .inc_by(self.rg_total as u64);
        READ_ROW_GROUPS_TOTAL
            .with_label_values(&["fulltext_index_filtered"])
            .inc_by(self.rg_fulltext_filtered as u64);
        READ_ROW_GROUPS_TOTAL
            .with_label_values(&["inverted_index_filtered"])
            .inc_by(self.rg_inverted_filtered as u64);
        READ_ROW_GROUPS_TOTAL
            .with_label_values(&["minmax_index_filtered"])
            .inc_by(self.rg_minmax_filtered as u64);
        READ_ROW_GROUPS_TOTAL
            .with_label_values(&["bloom_filter_index_filtered"])
            .inc_by(self.rg_bloom_filtered as u64);

        PRECISE_FILTER_ROWS_TOTAL
            .with_label_values(&["parquet"])
            .inc_by(self.rows_precise_filtered as u64);
        READ_ROWS_IN_ROW_GROUP_TOTAL
            .with_label_values(&["before_filtering"])
            .inc_by(self.rows_total as u64);
        READ_ROWS_IN_ROW_GROUP_TOTAL
            .with_label_values(&["fulltext_index_filtered"])
            .inc_by(self.rows_fulltext_filtered as u64);
        READ_ROWS_IN_ROW_GROUP_TOTAL
            .with_label_values(&["inverted_index_filtered"])
            .inc_by(self.rows_inverted_filtered as u64);
        READ_ROWS_IN_ROW_GROUP_TOTAL
            .with_label_values(&["bloom_filter_index_filtered"])
            .inc_by(self.rows_bloom_filtered as u64);
    }
}

/// Parquet reader metrics.
#[derive(Debug, Default, Clone)]
pub(crate) struct ReaderMetrics {
    /// Filtered row groups and rows metrics.
    pub(crate) filter_metrics: ReaderFilterMetrics,
    /// Duration to build the parquet reader.
    pub(crate) build_cost: Duration,
    /// Duration to scan the reader.
    pub(crate) scan_cost: Duration,
    /// Number of record batches read.
    pub(crate) num_record_batches: usize,
    /// Number of batches decoded.
    pub(crate) num_batches: usize,
    /// Number of rows read.
    pub(crate) num_rows: usize,
}

impl ReaderMetrics {
    /// Adds `other` metrics to this metrics.
    pub(crate) fn merge_from(&mut self, other: &ReaderMetrics) {
        self.filter_metrics.merge_from(&other.filter_metrics);
        self.build_cost += other.build_cost;
        self.scan_cost += other.scan_cost;
        self.num_record_batches += other.num_record_batches;
        self.num_batches += other.num_batches;
        self.num_rows += other.num_rows;
    }

    /// Reports total rows.
    pub(crate) fn observe_rows(&self, read_type: &str) {
        READ_ROWS_TOTAL
            .with_label_values(&[read_type])
            .inc_by(self.num_rows as u64);
    }
}

/// Builder to build a [ParquetRecordBatchReader] for a row group.
pub(crate) struct RowGroupReaderBuilder {
    /// SST file to read.
    ///
    /// Holds the file handle to avoid the file purge it.
    file_handle: FileHandle,
    /// Path of the file.
    file_path: String,
    /// Metadata of the parquet file.
    parquet_meta: Arc<ParquetMetaData>,
    /// Object store as an Operator.
    object_store: ObjectStore,
    /// Projection mask.
    projection: ProjectionMask,
    /// Field levels to read.
    field_levels: FieldLevels,
    /// Cache.
    cache_strategy: CacheStrategy,
}

impl RowGroupReaderBuilder {
    /// Path of the file to read.
    pub(crate) fn file_path(&self) -> &str {
        &self.file_path
    }

    /// Handle of the file to read.
    pub(crate) fn file_handle(&self) -> &FileHandle {
        &self.file_handle
    }

    pub(crate) fn parquet_metadata(&self) -> &Arc<ParquetMetaData> {
        &self.parquet_meta
    }

    pub(crate) fn cache_strategy(&self) -> &CacheStrategy {
        &self.cache_strategy
    }

    /// Builds a [ParquetRecordBatchReader] to read the row group at `row_group_idx`.
    pub(crate) async fn build(
        &self,
        row_group_idx: usize,
        row_selection: Option<RowSelection>,
    ) -> Result<ParquetRecordBatchReader> {
        let mut row_group = InMemoryRowGroup::create(
            self.file_handle.region_id(),
            self.file_handle.file_id(),
            &self.parquet_meta,
            row_group_idx,
            self.cache_strategy.clone(),
            &self.file_path,
            self.object_store.clone(),
        );
        // Fetches data into memory.
        row_group
            .fetch(&self.projection, row_selection.as_ref())
            .await
            .context(ReadParquetSnafu {
                path: &self.file_path,
            })?;

        // Builds the parquet reader.
        // Now the row selection is None.
        ParquetRecordBatchReader::try_new_with_row_groups(
            &self.field_levels,
            &row_group,
            DEFAULT_READ_BATCH_SIZE,
            row_selection,
        )
        .context(ReadParquetSnafu {
            path: &self.file_path,
        })
    }
}

/// The state of a [ParquetReader].
enum ReaderState {
    /// The reader is reading a row group.
    Readable(PruneReader),
    /// The reader is exhausted.
    Exhausted(ReaderMetrics),
}

impl ReaderState {
    /// Returns the metrics of the reader.
    fn metrics(&self) -> ReaderMetrics {
        match self {
            ReaderState::Readable(reader) => reader.metrics(),
            ReaderState::Exhausted(m) => m.clone(),
        }
    }
}

/// Context to evaluate the column filter.
pub(crate) struct SimpleFilterContext {
    /// Filter to evaluate.
    filter: SimpleFilterEvaluator,
    /// Id of the column to evaluate.
    column_id: ColumnId,
    /// Semantic type of the column.
    semantic_type: SemanticType,
    /// The data type of the column.
    data_type: ConcreteDataType,
}

impl SimpleFilterContext {
    /// Creates a context for the `expr`.
    ///
    /// Returns None if the column to filter doesn't exist in the SST metadata or the
    /// expected metadata.
    pub(crate) fn new_opt(
        sst_meta: &RegionMetadataRef,
        expected_meta: Option<&RegionMetadata>,
        expr: &Expr,
    ) -> Option<Self> {
        let filter = SimpleFilterEvaluator::try_new(expr)?;
        let column_metadata = match expected_meta {
            Some(meta) => {
                // Gets the column metadata from the expected metadata.
                let column = meta.column_by_name(filter.column_name())?;
                // Checks if the column is present in the SST metadata. We still uses the
                // column from the expected metadata.
                let sst_column = sst_meta.column_by_id(column.column_id)?;
                debug_assert_eq!(column.semantic_type, sst_column.semantic_type);

                column
            }
            None => sst_meta.column_by_name(filter.column_name())?,
        };

        Some(Self {
            filter,
            column_id: column_metadata.column_id,
            semantic_type: column_metadata.semantic_type,
            data_type: column_metadata.column_schema.data_type.clone(),
        })
    }

    /// Returns the filter to evaluate.
    pub(crate) fn filter(&self) -> &SimpleFilterEvaluator {
        &self.filter
    }

    /// Returns the column id.
    pub(crate) fn column_id(&self) -> ColumnId {
        self.column_id
    }

    /// Returns the semantic type of the column.
    pub(crate) fn semantic_type(&self) -> SemanticType {
        self.semantic_type
    }

    /// Returns the data type of the column.
    pub(crate) fn data_type(&self) -> &ConcreteDataType {
        &self.data_type
    }
}

type RowGroupMap = BTreeMap<usize, Option<RowSelection>>;

/// Parquet batch reader to read our SST format.
pub struct ParquetReader {
    /// File range context.
    context: FileRangeContextRef,
    /// Indices of row groups to read, along with their respective row selections.
    row_groups: RowGroupMap,
    /// Reader of current row group.
    reader_state: ReaderState,
}

#[async_trait]
impl BatchReader for ParquetReader {
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        let ReaderState::Readable(reader) = &mut self.reader_state else {
            return Ok(None);
        };

        // We don't collect the elapsed time if the reader returns an error.
        if let Some(batch) = reader.next_batch().await? {
            return Ok(Some(batch));
        }

        // No more items in current row group, reads next row group.
        while let Some((row_group_idx, row_selection)) = self.row_groups.pop_first() {
            let parquet_reader = self
                .context
                .reader_builder()
                .build(row_group_idx, row_selection)
                .await?;

            // Resets the parquet reader.
            reader.reset_source(Source::RowGroup(RowGroupReader::new(
                self.context.clone(),
                parquet_reader,
            )));
            if let Some(batch) = reader.next_batch().await? {
                return Ok(Some(batch));
            }
        }

        // The reader is exhausted.
        self.reader_state = ReaderState::Exhausted(reader.metrics().clone());
        Ok(None)
    }
}

impl Drop for ParquetReader {
    fn drop(&mut self) {
        let metrics = self.reader_state.metrics();
        debug!(
            "Read parquet {} {}, range: {:?}, {}/{} row groups, metrics: {:?}",
            self.context.reader_builder().file_handle.region_id(),
            self.context.reader_builder().file_handle.file_id(),
            self.context.reader_builder().file_handle.time_range(),
            metrics.filter_metrics.rg_total
                - metrics.filter_metrics.rg_inverted_filtered
                - metrics.filter_metrics.rg_minmax_filtered
                - metrics.filter_metrics.rg_fulltext_filtered
                - metrics.filter_metrics.rg_bloom_filtered,
            metrics.filter_metrics.rg_total,
            metrics
        );

        // Report metrics.
        READ_STAGE_ELAPSED
            .with_label_values(&["build_parquet_reader"])
            .observe(metrics.build_cost.as_secs_f64());
        READ_STAGE_ELAPSED
            .with_label_values(&["scan_row_groups"])
            .observe(metrics.scan_cost.as_secs_f64());
        metrics.observe_rows("parquet_reader");
        metrics.filter_metrics.observe();
    }
}

impl ParquetReader {
    /// Creates a new reader.
    async fn new(
        context: FileRangeContextRef,
        mut row_groups: BTreeMap<usize, Option<RowSelection>>,
    ) -> Result<Self> {
        // No more items in current row group, reads next row group.
        let reader_state = if let Some((row_group_idx, row_selection)) = row_groups.pop_first() {
            let parquet_reader = context
                .reader_builder()
                .build(row_group_idx, row_selection)
                .await?;
            ReaderState::Readable(PruneReader::new_with_row_group_reader(
                context.clone(),
                RowGroupReader::new(context.clone(), parquet_reader),
            ))
        } else {
            ReaderState::Exhausted(ReaderMetrics::default())
        };

        Ok(ParquetReader {
            context,
            row_groups,
            reader_state,
        })
    }

    /// Returns the metadata of the SST.
    pub fn metadata(&self) -> &RegionMetadataRef {
        self.context.read_format().metadata()
    }

    #[cfg(test)]
    pub fn parquet_metadata(&self) -> Arc<ParquetMetaData> {
        self.context.reader_builder().parquet_meta.clone()
    }
}

/// RowGroupReaderContext represents the fields that cannot be shared
/// between different `RowGroupReader`s.
pub(crate) trait RowGroupReaderContext: Send {
    fn map_result(
        &self,
        result: std::result::Result<Option<RecordBatch>, ArrowError>,
    ) -> Result<Option<RecordBatch>>;

    fn read_format(&self) -> &ReadFormat;
}

impl RowGroupReaderContext for FileRangeContextRef {
    fn map_result(
        &self,
        result: std::result::Result<Option<RecordBatch>, ArrowError>,
    ) -> Result<Option<RecordBatch>> {
        result.context(ArrowReaderSnafu {
            path: self.file_path(),
        })
    }

    fn read_format(&self) -> &ReadFormat {
        self.as_ref().read_format()
    }
}

/// [RowGroupReader] that reads from [FileRange].
pub(crate) type RowGroupReader = RowGroupReaderBase<FileRangeContextRef>;

impl RowGroupReader {
    /// Creates a new reader from file range.
    pub(crate) fn new(context: FileRangeContextRef, reader: ParquetRecordBatchReader) -> Self {
        Self {
            context,
            reader,
            batches: VecDeque::new(),
            metrics: ReaderMetrics::default(),
        }
    }
}

/// Reader to read a row group of a parquet file.
pub(crate) struct RowGroupReaderBase<T> {
    /// Context of [RowGroupReader] so adapts to different underlying implementation.
    context: T,
    /// Inner parquet reader.
    reader: ParquetRecordBatchReader,
    /// Buffered batches to return.
    batches: VecDeque<Batch>,
    /// Local scan metrics.
    metrics: ReaderMetrics,
}

impl<T> RowGroupReaderBase<T>
where
    T: RowGroupReaderContext,
{
    /// Creates a new reader.
    pub(crate) fn create(context: T, reader: ParquetRecordBatchReader) -> Self {
        Self {
            context,
            reader,
            batches: VecDeque::new(),
            metrics: ReaderMetrics::default(),
        }
    }

    /// Gets the metrics.
    pub(crate) fn metrics(&self) -> &ReaderMetrics {
        &self.metrics
    }

    /// Gets [ReadFormat] of underlying reader.
    pub(crate) fn read_format(&self) -> &ReadFormat {
        self.context.read_format()
    }

    /// Tries to fetch next [RecordBatch] from the reader.
    fn fetch_next_record_batch(&mut self) -> Result<Option<RecordBatch>> {
        self.context.map_result(self.reader.next().transpose())
    }

    /// Returns the next [Batch].
    pub(crate) fn next_inner(&mut self) -> Result<Option<Batch>> {
        let scan_start = Instant::now();
        if let Some(batch) = self.batches.pop_front() {
            self.metrics.num_rows += batch.num_rows();
            self.metrics.scan_cost += scan_start.elapsed();
            return Ok(Some(batch));
        }

        // We need to fetch next record batch and convert it to batches.
        while self.batches.is_empty() {
            let Some(record_batch) = self.fetch_next_record_batch()? else {
                self.metrics.scan_cost += scan_start.elapsed();
                return Ok(None);
            };
            self.metrics.num_record_batches += 1;

            self.context
                .read_format()
                .convert_record_batch(&record_batch, &mut self.batches)?;
            self.metrics.num_batches += self.batches.len();
        }
        let batch = self.batches.pop_front();
        self.metrics.num_rows += batch.as_ref().map(|b| b.num_rows()).unwrap_or(0);
        self.metrics.scan_cost += scan_start.elapsed();
        Ok(batch)
    }
}

#[async_trait::async_trait]
impl<T> BatchReader for RowGroupReaderBase<T>
where
    T: RowGroupReaderContext,
{
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        self.next_inner()
    }
}

#[cfg(test)]
mod tests {
    use parquet::arrow::arrow_reader::RowSelector;
    use parquet::file::metadata::{FileMetaData, RowGroupMetaData};
    use parquet::schema::types::{SchemaDescriptor, Type};

    use super::*;

    fn mock_parquet_metadata_from_row_groups(num_rows_in_row_groups: Vec<i64>) -> ParquetMetaData {
        let tp = Arc::new(Type::group_type_builder("test").build().unwrap());
        let schema_descr = Arc::new(SchemaDescriptor::new(tp));

        let mut row_groups = Vec::new();
        for num_rows in &num_rows_in_row_groups {
            let row_group = RowGroupMetaData::builder(schema_descr.clone())
                .set_num_rows(*num_rows)
                .build()
                .unwrap();
            row_groups.push(row_group);
        }

        let file_meta = FileMetaData::new(
            0,
            num_rows_in_row_groups.iter().sum(),
            None,
            None,
            schema_descr,
            None,
        );
        ParquetMetaData::new(file_meta, row_groups)
    }

    #[test]
    fn test_group_row_ids() {
        let row_ids = [0, 1, 2, 5, 6, 7, 8, 12].into_iter().collect();
        let row_group_size = 5;
        let num_row_groups = 3;

        let row_group_to_row_ids =
            ParquetReaderBuilder::group_row_ids(row_ids, row_group_size, num_row_groups);

        assert_eq!(
            row_group_to_row_ids,
            vec![(0, vec![0, 1, 2]), (1, vec![0, 1, 2, 3]), (2, vec![2])]
        );
    }

    #[test]
    fn prune_row_groups_by_rows_from_empty() {
        let parquet_meta = mock_parquet_metadata_from_row_groups(vec![10, 10, 5]);

        let rows_in_row_groups = vec![(0, vec![5, 6, 7, 8, 9]), (2, vec![0, 1, 2, 3, 4])];

        // The original output is empty. No row groups are pruned.
        let mut output = BTreeMap::new();
        let mut filtered_row_groups = 0;
        let mut filtered_rows = 0;

        ParquetReaderBuilder::prune_row_groups_by_rows(
            &parquet_meta,
            rows_in_row_groups,
            &mut output,
            &mut filtered_row_groups,
            &mut filtered_rows,
        );

        assert!(output.is_empty());
        assert_eq!(filtered_row_groups, 0);
        assert_eq!(filtered_rows, 0);
    }

    #[test]
    fn prune_row_groups_by_rows_from_full() {
        let parquet_meta = mock_parquet_metadata_from_row_groups(vec![10, 10, 5]);

        let rows_in_row_groups = vec![(0, vec![5, 6, 7, 8, 9]), (2, vec![0, 1, 2, 3, 4])];

        // The original output is full.
        let mut output = BTreeMap::from([(0, None), (1, None), (2, None)]);
        let mut filtered_row_groups = 0;
        let mut filtered_rows = 0;

        ParquetReaderBuilder::prune_row_groups_by_rows(
            &parquet_meta,
            rows_in_row_groups,
            &mut output,
            &mut filtered_row_groups,
            &mut filtered_rows,
        );

        assert_eq!(
            output,
            BTreeMap::from([
                (
                    0,
                    Some(RowSelection::from(vec![
                        RowSelector::skip(5),
                        RowSelector::select(5),
                    ]))
                ),
                (2, Some(RowSelection::from(vec![RowSelector::select(5)]))),
            ])
        );
        assert_eq!(filtered_row_groups, 1);
        assert_eq!(filtered_rows, 15);
    }

    #[test]
    fn prune_row_groups_by_rows_from_not_full() {
        let parquet_meta = mock_parquet_metadata_from_row_groups(vec![10, 10, 5]);

        let rows_in_row_groups = vec![(0, vec![5, 6, 7, 8, 9]), (2, vec![0, 1, 2, 3, 4])];

        // The original output is not full.
        let mut output = BTreeMap::from([
            (
                0,
                Some(RowSelection::from(vec![
                    RowSelector::select(5),
                    RowSelector::skip(5),
                ])),
            ),
            (
                1,
                Some(RowSelection::from(vec![
                    RowSelector::select(5),
                    RowSelector::skip(5),
                ])),
            ),
            (2, Some(RowSelection::from(vec![RowSelector::select(5)]))),
        ]);
        let mut filtered_row_groups = 0;
        let mut filtered_rows = 0;

        ParquetReaderBuilder::prune_row_groups_by_rows(
            &parquet_meta,
            rows_in_row_groups,
            &mut output,
            &mut filtered_row_groups,
            &mut filtered_rows,
        );

        assert_eq!(
            output,
            BTreeMap::from([(2, Some(RowSelection::from(vec![RowSelector::select(5)])))])
        );
        assert_eq!(filtered_row_groups, 2);
        assert_eq!(filtered_rows, 10);
    }

    #[test]
    fn prune_row_groups_by_ranges_from_empty() {
        let parquet_meta = mock_parquet_metadata_from_row_groups(vec![10, 10, 5]);

        let ranges_in_row_groups = [(0, Some(5..10).into_iter()), (2, Some(0..5).into_iter())];

        // The original output is empty. No row groups are pruned.
        let mut output = BTreeMap::new();
        let mut filtered_row_groups = 0;
        let mut filtered_rows = 0;

        ParquetReaderBuilder::prune_row_groups_by_ranges(
            &parquet_meta,
            ranges_in_row_groups.into_iter(),
            &mut output,
            &mut filtered_row_groups,
            &mut filtered_rows,
        );

        assert!(output.is_empty());
        assert_eq!(filtered_row_groups, 0);
        assert_eq!(filtered_rows, 0);
    }

    #[test]
    fn prune_row_groups_by_ranges_from_full() {
        let parquet_meta = mock_parquet_metadata_from_row_groups(vec![10, 10, 5]);

        let ranges_in_row_groups = [(0, Some(5..10).into_iter()), (2, Some(0..5).into_iter())];

        // The original output is full.
        let mut output = BTreeMap::from([(0, None), (1, None), (2, None)]);
        let mut filtered_row_groups = 0;
        let mut filtered_rows = 0;

        ParquetReaderBuilder::prune_row_groups_by_ranges(
            &parquet_meta,
            ranges_in_row_groups.into_iter(),
            &mut output,
            &mut filtered_row_groups,
            &mut filtered_rows,
        );

        assert_eq!(
            output,
            BTreeMap::from([
                (
                    0,
                    Some(RowSelection::from(vec![
                        RowSelector::skip(5),
                        RowSelector::select(5),
                    ]))
                ),
                (2, Some(RowSelection::from(vec![RowSelector::select(5)]))),
            ])
        );
        assert_eq!(filtered_row_groups, 1);
        assert_eq!(filtered_rows, 15);
    }

    #[test]
    fn prune_row_groups_by_ranges_from_not_full() {
        let parquet_meta = mock_parquet_metadata_from_row_groups(vec![10, 10, 5]);

        let ranges_in_row_groups = [(0, Some(5..10).into_iter()), (2, Some(0..5).into_iter())];

        // The original output is not full.
        let mut output = BTreeMap::from([
            (
                0,
                Some(RowSelection::from(vec![
                    RowSelector::select(5),
                    RowSelector::skip(5),
                ])),
            ),
            (
                1,
                Some(RowSelection::from(vec![
                    RowSelector::select(5),
                    RowSelector::skip(5),
                ])),
            ),
            (2, Some(RowSelection::from(vec![RowSelector::select(5)]))),
        ]);
        let mut filtered_row_groups = 0;
        let mut filtered_rows = 0;

        ParquetReaderBuilder::prune_row_groups_by_ranges(
            &parquet_meta,
            ranges_in_row_groups.into_iter(),
            &mut output,
            &mut filtered_row_groups,
            &mut filtered_rows,
        );

        assert_eq!(
            output,
            BTreeMap::from([(2, Some(RowSelection::from(vec![RowSelector::select(5)])))])
        );
        assert_eq!(filtered_row_groups, 2);
        assert_eq!(filtered_rows, 10);
    }

    #[test]
    fn prune_row_groups_by_ranges_exceed_end() {
        let parquet_meta = mock_parquet_metadata_from_row_groups(vec![10, 10, 5]);

        // The range exceeds the end of the row group.
        let ranges_in_row_groups = [(0, Some(5..10).into_iter()), (2, Some(0..10).into_iter())];

        let mut output = BTreeMap::from([
            (
                0,
                Some(RowSelection::from(vec![
                    RowSelector::select(5),
                    RowSelector::skip(5),
                ])),
            ),
            (
                1,
                Some(RowSelection::from(vec![
                    RowSelector::select(5),
                    RowSelector::skip(5),
                ])),
            ),
            (2, Some(RowSelection::from(vec![RowSelector::select(5)]))),
        ]);
        let mut filtered_row_groups = 0;
        let mut filtered_rows = 0;

        ParquetReaderBuilder::prune_row_groups_by_ranges(
            &parquet_meta,
            ranges_in_row_groups.into_iter(),
            &mut output,
            &mut filtered_row_groups,
            &mut filtered_rows,
        );

        assert_eq!(
            output,
            BTreeMap::from([(2, Some(RowSelection::from(vec![RowSelector::select(5)])))])
        );
        assert_eq!(filtered_row_groups, 2);
        assert_eq!(filtered_rows, 10);
    }
}
