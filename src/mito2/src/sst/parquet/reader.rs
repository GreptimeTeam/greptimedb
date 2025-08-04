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

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use api::v1::SemanticType;
use async_trait::async_trait;
use common_recordbatch::filter::SimpleFilterEvaluator;
use common_telemetry::{debug, warn};
use datafusion_expr::Expr;
use datatypes::arrow::array::ArrayRef;
use datatypes::arrow::error::ArrowError;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::data_type::ConcreteDataType;
use mito_codec::row_converter::build_primary_key_codec;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, RowSelection};
use parquet::arrow::{parquet_to_arrow_field_levels, FieldLevels, ProjectionMask};
use parquet::file::metadata::ParquetMetaData;
use parquet::format::KeyValue;
use snafu::{OptionExt, ResultExt};
use store_api::metadata::{ColumnMetadata, RegionMetadata, RegionMetadataRef};
use store_api::region_request::PathType;
use store_api::storage::ColumnId;
use table::predicate::Predicate;

use crate::cache::index::result_cache::PredicateKey;
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
use crate::sst::file::{FileHandle, FileId};
use crate::sst::index::bloom_filter::applier::BloomFilterIndexApplierRef;
use crate::sst::index::fulltext_index::applier::FulltextIndexApplierRef;
use crate::sst::index::inverted_index::applier::InvertedIndexApplierRef;
use crate::sst::parquet::file_range::{FileRangeContext, FileRangeContextRef};
use crate::sst::parquet::format::{need_override_sequence, PrimaryKeyReadFormat, ReadFormat};
use crate::sst::parquet::metadata::MetadataLoader;
use crate::sst::parquet::row_group::InMemoryRowGroup;
use crate::sst::parquet::row_selection::RowGroupSelection;
use crate::sst::parquet::stats::RowGroupPruningStats;
use crate::sst::parquet::{DEFAULT_READ_BATCH_SIZE, PARQUET_METADATA_KEY};

const INDEX_TYPE_FULLTEXT: &str = "fulltext";
const INDEX_TYPE_INVERTED: &str = "inverted";
const INDEX_TYPE_BLOOM: &str = "bloom filter";

macro_rules! handle_index_error {
    ($err:expr, $file_handle:expr, $index_type:expr) => {
        if cfg!(any(test, feature = "test")) {
            panic!(
                "Failed to apply {} index, region_id: {}, file_id: {}, err: {:?}",
                $index_type,
                $file_handle.region_id(),
                $file_handle.file_id(),
                $err
            );
        } else {
            warn!(
                $err; "Failed to apply {} index, region_id: {}, file_id: {}",
                $index_type,
                $file_handle.region_id(),
                $file_handle.file_id()
            );
        }
    };
}

/// Parquet SST reader builder.
pub struct ParquetReaderBuilder {
    /// SST directory.
    file_dir: String,
    /// Path type for generating file paths.
    path_type: PathType,
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
        path_type: PathType,
        file_handle: FileHandle,
        object_store: ObjectStore,
    ) -> ParquetReaderBuilder {
        ParquetReaderBuilder {
            file_dir,
            path_type,
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

        let (context, selection) = self.build_reader_input(&mut metrics).await?;
        ParquetReader::new(Arc::new(context), selection).await
    }

    /// Builds a [FileRangeContext] and collects row groups to read.
    ///
    /// This needs to perform IO operation.
    pub(crate) async fn build_reader_input(
        &self,
        metrics: &mut ReaderMetrics,
    ) -> Result<(FileRangeContext, RowGroupSelection)> {
        let start = Instant::now();

        let file_path = self.file_handle.file_path(&self.file_dir, self.path_type);
        let file_size = self.file_handle.meta_ref().file_size;

        // Loads parquet metadata of the file.
        let parquet_meta = self.read_parquet_metadata(&file_path, file_size).await?;
        // Decodes region metadata.
        let key_value_meta = parquet_meta.file_metadata().key_value_metadata();
        // Gets the metadata stored in the SST.
        let region_meta = Arc::new(Self::get_region_metadata(&file_path, key_value_meta)?);
        let mut read_format = if let Some(column_ids) = &self.projection {
            PrimaryKeyReadFormat::new(region_meta.clone(), column_ids.iter().copied())
        } else {
            // Lists all column ids to read, we always use the expected metadata if possible.
            let expected_meta = self.expected_metadata.as_ref().unwrap_or(&region_meta);
            PrimaryKeyReadFormat::new(
                region_meta.clone(),
                expected_meta
                    .column_metadatas
                    .iter()
                    .map(|col| col.column_id),
            )
        };
        if need_override_sequence(&parquet_meta) {
            read_format
                .set_override_sequence(self.file_handle.meta_ref().sequence.map(|x| x.get()));
        }
        let read_format = ReadFormat::PrimaryKey(read_format);

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
        let selection = self
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

        Ok((context, selection))
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

        let file_id = self.file_handle.file_id();
        // Tries to get from global cache.
        if let Some(metadata) = self.cache_strategy.get_parquet_meta_data(file_id).await {
            return Ok(metadata);
        }

        // Cache miss, load metadata directly.
        let metadata_loader = MetadataLoader::new(self.object_store.clone(), file_path, file_size);
        let metadata = metadata_loader.load().await?;
        let metadata = Arc::new(metadata);
        // Cache the metadata.
        self.cache_strategy
            .put_parquet_meta_data(file_id, metadata.clone());

        Ok(metadata)
    }

    /// Computes row groups to read, along with their respective row selections.
    async fn row_groups_to_read(
        &self,
        read_format: &ReadFormat,
        parquet_meta: &ParquetMetaData,
        metrics: &mut ReaderFilterMetrics,
    ) -> RowGroupSelection {
        let num_row_groups = parquet_meta.num_row_groups();
        let num_rows = parquet_meta.file_metadata().num_rows();
        if num_row_groups == 0 || num_rows == 0 {
            return RowGroupSelection::default();
        }

        // Let's assume that the number of rows in the first row group
        // can represent the `row_group_size` of the Parquet file.
        let row_group_size = parquet_meta.row_group(0).num_rows() as usize;
        if row_group_size == 0 {
            return RowGroupSelection::default();
        }

        metrics.rg_total += num_row_groups;
        metrics.rows_total += num_rows as usize;

        let mut output = RowGroupSelection::new(row_group_size, num_rows as _);

        self.prune_row_groups_by_minmax(read_format, parquet_meta, &mut output, metrics);
        if output.is_empty() {
            return output;
        }

        let fulltext_filtered = self
            .prune_row_groups_by_fulltext_index(
                row_group_size,
                num_row_groups,
                &mut output,
                metrics,
            )
            .await;
        if output.is_empty() {
            return output;
        }

        self.prune_row_groups_by_inverted_index(
            row_group_size,
            num_row_groups,
            &mut output,
            metrics,
        )
        .await;
        if output.is_empty() {
            return output;
        }

        self.prune_row_groups_by_bloom_filter(row_group_size, parquet_meta, &mut output, metrics)
            .await;
        if output.is_empty() {
            return output;
        }

        if !fulltext_filtered {
            self.prune_row_groups_by_fulltext_bloom(
                row_group_size,
                parquet_meta,
                &mut output,
                metrics,
            )
            .await;
        }
        output
    }

    /// Prunes row groups by fulltext index. Returns `true` if the row groups are pruned.
    async fn prune_row_groups_by_fulltext_index(
        &self,
        row_group_size: usize,
        num_row_groups: usize,
        output: &mut RowGroupSelection,
        metrics: &mut ReaderFilterMetrics,
    ) -> bool {
        let Some(index_applier) = &self.fulltext_index_applier else {
            return false;
        };
        if !self.file_handle.meta_ref().fulltext_index_available() {
            return false;
        }

        let predicate_key = index_applier.predicate_key();
        // Fast path: return early if the result is in the cache.
        let cached = self
            .cache_strategy
            .index_result_cache()
            .and_then(|cache| cache.get(predicate_key, self.file_handle.file_id().file_id()));
        if let Some(result) = cached.as_ref() {
            if all_required_row_groups_searched(output, result) {
                apply_selection_and_update_metrics(output, result, metrics, INDEX_TYPE_FULLTEXT);
                return true;
            }
        }

        // Slow path: apply the index from the file.
        let file_size_hint = self.file_handle.meta_ref().index_file_size();
        let apply_res = index_applier
            .apply_fine(self.file_handle.file_id(), Some(file_size_hint))
            .await;
        let selection = match apply_res {
            Ok(Some(res)) => RowGroupSelection::from_row_ids(res, row_group_size, num_row_groups),
            Ok(None) => return false,
            Err(err) => {
                handle_index_error!(err, self.file_handle, INDEX_TYPE_FULLTEXT);
                return false;
            }
        };

        self.apply_index_result_and_update_cache(
            predicate_key,
            self.file_handle.file_id().file_id(),
            selection,
            output,
            metrics,
            INDEX_TYPE_FULLTEXT,
        );
        true
    }

    /// Applies index to prune row groups.
    ///
    /// TODO(zhongzc): Devise a mechanism to enforce the non-use of indices
    /// as an escape route in case of index issues, and it can be used to test
    /// the correctness of the index.
    async fn prune_row_groups_by_inverted_index(
        &self,
        row_group_size: usize,
        num_row_groups: usize,
        output: &mut RowGroupSelection,
        metrics: &mut ReaderFilterMetrics,
    ) -> bool {
        let Some(index_applier) = &self.inverted_index_applier else {
            return false;
        };
        if !self.file_handle.meta_ref().inverted_index_available() {
            return false;
        }

        let predicate_key = index_applier.predicate_key();
        // Fast path: return early if the result is in the cache.
        let cached = self
            .cache_strategy
            .index_result_cache()
            .and_then(|cache| cache.get(predicate_key, self.file_handle.file_id().file_id()));
        if let Some(result) = cached.as_ref() {
            if all_required_row_groups_searched(output, result) {
                apply_selection_and_update_metrics(output, result, metrics, INDEX_TYPE_INVERTED);
                return true;
            }
        }

        // Slow path: apply the index from the file.
        let file_size_hint = self.file_handle.meta_ref().index_file_size();
        let apply_res = index_applier
            .apply(self.file_handle.file_id(), Some(file_size_hint))
            .await;
        let selection = match apply_res {
            Ok(output) => RowGroupSelection::from_inverted_index_apply_output(
                row_group_size,
                num_row_groups,
                output,
            ),
            Err(err) => {
                handle_index_error!(err, self.file_handle, INDEX_TYPE_INVERTED);
                return false;
            }
        };

        self.apply_index_result_and_update_cache(
            predicate_key,
            self.file_handle.file_id().file_id(),
            selection,
            output,
            metrics,
            INDEX_TYPE_INVERTED,
        );
        true
    }

    async fn prune_row_groups_by_bloom_filter(
        &self,
        row_group_size: usize,
        parquet_meta: &ParquetMetaData,
        output: &mut RowGroupSelection,
        metrics: &mut ReaderFilterMetrics,
    ) -> bool {
        let Some(index_applier) = &self.bloom_filter_index_applier else {
            return false;
        };
        if !self.file_handle.meta_ref().bloom_filter_index_available() {
            return false;
        }

        let predicate_key = index_applier.predicate_key();
        // Fast path: return early if the result is in the cache.
        let cached = self
            .cache_strategy
            .index_result_cache()
            .and_then(|cache| cache.get(predicate_key, self.file_handle.file_id().file_id()));
        if let Some(result) = cached.as_ref() {
            if all_required_row_groups_searched(output, result) {
                apply_selection_and_update_metrics(output, result, metrics, INDEX_TYPE_BLOOM);
                return true;
            }
        }

        // Slow path: apply the index from the file.
        let file_size_hint = self.file_handle.meta_ref().index_file_size();
        let rgs = parquet_meta.row_groups().iter().enumerate().map(|(i, rg)| {
            (
                rg.num_rows() as usize,
                // Optimize: only search the row group that required by `output` and not stored in `cached`.
                output.contains_non_empty_row_group(i)
                    && cached
                        .as_ref()
                        .map(|c| !c.contains_row_group(i))
                        .unwrap_or(true),
            )
        });
        let apply_res = index_applier
            .apply(self.file_handle.file_id(), Some(file_size_hint), rgs)
            .await;
        let mut selection = match apply_res {
            Ok(apply_output) => RowGroupSelection::from_row_ranges(apply_output, row_group_size),
            Err(err) => {
                handle_index_error!(err, self.file_handle, INDEX_TYPE_BLOOM);
                return false;
            }
        };

        // New searched row groups are added to `selection`, concat them with `cached`.
        if let Some(cached) = cached.as_ref() {
            selection.concat(cached);
        }

        self.apply_index_result_and_update_cache(
            predicate_key,
            self.file_handle.file_id().file_id(),
            selection,
            output,
            metrics,
            INDEX_TYPE_BLOOM,
        );
        true
    }

    async fn prune_row_groups_by_fulltext_bloom(
        &self,
        row_group_size: usize,
        parquet_meta: &ParquetMetaData,
        output: &mut RowGroupSelection,
        metrics: &mut ReaderFilterMetrics,
    ) -> bool {
        let Some(index_applier) = &self.fulltext_index_applier else {
            return false;
        };
        if !self.file_handle.meta_ref().fulltext_index_available() {
            return false;
        }

        let predicate_key = index_applier.predicate_key();
        // Fast path: return early if the result is in the cache.
        let cached = self
            .cache_strategy
            .index_result_cache()
            .and_then(|cache| cache.get(predicate_key, self.file_handle.file_id().file_id()));
        if let Some(result) = cached.as_ref() {
            if all_required_row_groups_searched(output, result) {
                apply_selection_and_update_metrics(output, result, metrics, INDEX_TYPE_FULLTEXT);
                return true;
            }
        }

        // Slow path: apply the index from the file.
        let file_size_hint = self.file_handle.meta_ref().index_file_size();
        let rgs = parquet_meta.row_groups().iter().enumerate().map(|(i, rg)| {
            (
                rg.num_rows() as usize,
                // Optimize: only search the row group that required by `output` and not stored in `cached`.
                output.contains_non_empty_row_group(i)
                    && cached
                        .as_ref()
                        .map(|c| !c.contains_row_group(i))
                        .unwrap_or(true),
            )
        });
        let apply_res = index_applier
            .apply_coarse(self.file_handle.file_id(), Some(file_size_hint), rgs)
            .await;
        let mut selection = match apply_res {
            Ok(Some(apply_output)) => {
                RowGroupSelection::from_row_ranges(apply_output, row_group_size)
            }
            Ok(None) => return false,
            Err(err) => {
                handle_index_error!(err, self.file_handle, INDEX_TYPE_FULLTEXT);
                return false;
            }
        };

        // New searched row groups are added to `selection`, concat them with `cached`.
        if let Some(cached) = cached.as_ref() {
            selection.concat(cached);
        }

        self.apply_index_result_and_update_cache(
            predicate_key,
            self.file_handle.file_id().file_id(),
            selection,
            output,
            metrics,
            INDEX_TYPE_FULLTEXT,
        );
        true
    }

    /// Prunes row groups by min-max index.
    fn prune_row_groups_by_minmax(
        &self,
        read_format: &ReadFormat,
        parquet_meta: &ParquetMetaData,
        output: &mut RowGroupSelection,
        metrics: &mut ReaderFilterMetrics,
    ) -> bool {
        let Some(predicate) = &self.predicate else {
            return false;
        };

        let row_groups_before = output.row_group_count();

        let region_meta = read_format.metadata();
        let row_groups = parquet_meta.row_groups();
        let stats =
            RowGroupPruningStats::new(row_groups, read_format, self.expected_metadata.clone());
        let prune_schema = self
            .expected_metadata
            .as_ref()
            .map(|meta| meta.schema.arrow_schema())
            .unwrap_or_else(|| region_meta.schema.arrow_schema());

        // Here we use the schema of the SST to build the physical expression. If the column
        // in the SST doesn't have the same column id as the column in the expected metadata,
        // we will get a None statistics for that column.
        predicate
            .prune_with_stats(&stats, prune_schema)
            .iter()
            .zip(0..parquet_meta.num_row_groups())
            .for_each(|(mask, row_group)| {
                if !*mask {
                    output.remove_row_group(row_group);
                }
            });

        let row_groups_after = output.row_group_count();
        metrics.rg_minmax_filtered += row_groups_before - row_groups_after;

        true
    }

    fn apply_index_result_and_update_cache(
        &self,
        predicate_key: &PredicateKey,
        file_id: FileId,
        result: RowGroupSelection,
        output: &mut RowGroupSelection,
        metrics: &mut ReaderFilterMetrics,
        index_type: &str,
    ) {
        apply_selection_and_update_metrics(output, &result, metrics, index_type);

        if let Some(index_result_cache) = &self.cache_strategy.index_result_cache() {
            index_result_cache.put(predicate_key.clone(), file_id, Arc::new(result));
        }
    }
}

fn apply_selection_and_update_metrics(
    output: &mut RowGroupSelection,
    result: &RowGroupSelection,
    metrics: &mut ReaderFilterMetrics,
    index_type: &str,
) {
    let intersection = output.intersect(result);

    let row_group_count = output.row_group_count() - intersection.row_group_count();
    let row_count = output.row_count() - intersection.row_count();

    metrics.update_index_metrics(index_type, row_group_count, row_count);

    *output = intersection;
}

fn all_required_row_groups_searched(
    required_row_groups: &RowGroupSelection,
    cached_row_groups: &RowGroupSelection,
) -> bool {
    required_row_groups.iter().all(|(rg_id, _)| {
        // Row group with no rows is not required to search.
        !required_row_groups.contains_non_empty_row_group(*rg_id)
            // The row group is already searched.
            || cached_row_groups.contains_row_group(*rg_id)
    })
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

    fn update_index_metrics(&mut self, index_type: &str, row_group_count: usize, row_count: usize) {
        match index_type {
            INDEX_TYPE_FULLTEXT => {
                self.rg_fulltext_filtered += row_group_count;
                self.rows_fulltext_filtered += row_count;
            }
            INDEX_TYPE_INVERTED => {
                self.rg_inverted_filtered += row_group_count;
                self.rows_inverted_filtered += row_count;
            }
            INDEX_TYPE_BLOOM => {
                self.rg_bloom_filtered += row_group_count;
                self.rows_bloom_filtered += row_count;
            }
            _ => {}
        }
    }
}

/// Parquet reader metrics.
#[derive(Debug, Default, Clone)]
pub struct ReaderMetrics {
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
            self.file_handle.file_id().file_id(),
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

/// The filter to evaluate or the prune result of the default value.
pub(crate) enum MaybeFilter {
    /// The filter to evaluate.
    Filter(SimpleFilterEvaluator),
    /// The filter matches the default value.
    Matched,
    /// The filter is pruned.
    Pruned,
}

/// Context to evaluate the column filter for a parquet file.
pub(crate) struct SimpleFilterContext {
    /// Filter to evaluate.
    filter: MaybeFilter,
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
        let (column_metadata, maybe_filter) = match expected_meta {
            Some(meta) => {
                // Gets the column metadata from the expected metadata.
                let column = meta.column_by_name(filter.column_name())?;
                // Checks if the column is present in the SST metadata. We still uses the
                // column from the expected metadata.
                match sst_meta.column_by_id(column.column_id) {
                    Some(sst_column) => {
                        debug_assert_eq!(column.semantic_type, sst_column.semantic_type);

                        (column, MaybeFilter::Filter(filter))
                    }
                    None => {
                        // If the column is not present in the SST metadata, we evaluate the filter
                        // against the default value of the column.
                        // If we can't evaluate the filter, we return None.
                        if pruned_by_default(&filter, column)? {
                            (column, MaybeFilter::Pruned)
                        } else {
                            (column, MaybeFilter::Matched)
                        }
                    }
                }
            }
            None => {
                let column = sst_meta.column_by_name(filter.column_name())?;
                (column, MaybeFilter::Filter(filter))
            }
        };

        Some(Self {
            filter: maybe_filter,
            column_id: column_metadata.column_id,
            semantic_type: column_metadata.semantic_type,
            data_type: column_metadata.column_schema.data_type.clone(),
        })
    }

    /// Returns the filter to evaluate.
    pub(crate) fn filter(&self) -> &MaybeFilter {
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

/// Prune a column by its default value.
/// Returns false if we can't create the default value or evaluate the filter.
fn pruned_by_default(filter: &SimpleFilterEvaluator, column: &ColumnMetadata) -> Option<bool> {
    let value = column.column_schema.create_default().ok().flatten()?;
    let scalar_value = value
        .try_to_scalar_value(&column.column_schema.data_type)
        .ok()?;
    let matches = filter.evaluate_scalar(&scalar_value).ok()?;
    Some(!matches)
}

/// Parquet batch reader to read our SST format.
pub struct ParquetReader {
    /// File range context.
    context: FileRangeContextRef,
    /// Row group selection to read.
    selection: RowGroupSelection,
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
        while let Some((row_group_idx, row_selection)) = self.selection.pop_first() {
            let parquet_reader = self
                .context
                .reader_builder()
                .build(row_group_idx, Some(row_selection))
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
    pub(crate) async fn new(
        context: FileRangeContextRef,
        mut selection: RowGroupSelection,
    ) -> Result<Self> {
        // No more items in current row group, reads next row group.
        let reader_state = if let Some((row_group_idx, row_selection)) = selection.pop_first() {
            let parquet_reader = context
                .reader_builder()
                .build(row_group_idx, Some(row_selection))
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
            selection,
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
        Self::create(context, reader)
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
    /// Cached sequence array to override sequences.
    override_sequence: Option<ArrayRef>,
}

impl<T> RowGroupReaderBase<T>
where
    T: RowGroupReaderContext,
{
    /// Creates a new reader to read the primary key format.
    pub(crate) fn create(context: T, reader: ParquetRecordBatchReader) -> Self {
        // The batch length from the reader should be less than or equal to DEFAULT_READ_BATCH_SIZE.
        let override_sequence = context
            .read_format()
            .new_override_sequence_array(DEFAULT_READ_BATCH_SIZE);
        assert!(context.read_format().as_primary_key().is_some());

        Self {
            context,
            reader,
            batches: VecDeque::new(),
            metrics: ReaderMetrics::default(),
            override_sequence,
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

            // Safety: We ensures the format is primary key in the RowGroupReaderBase::create().
            self.context
                .read_format()
                .as_primary_key()
                .unwrap()
                .convert_record_batch(
                    &record_batch,
                    self.override_sequence.as_ref(),
                    &mut self.batches,
                )?;
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
