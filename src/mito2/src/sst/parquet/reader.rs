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

#[cfg(feature = "vector_index")]
use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use api::v1::SemanticType;
use async_trait::async_trait;
use common_recordbatch::filter::SimpleFilterEvaluator;
use common_telemetry::{debug, tracing, warn};
use datafusion_expr::Expr;
use datatypes::arrow::array::ArrayRef;
use datatypes::arrow::datatypes::Field;
use datatypes::arrow::error::ArrowError;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::data_type::ConcreteDataType;
use datatypes::prelude::DataType;
use mito_codec::row_converter::build_primary_key_codec;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, RowSelection};
use parquet::arrow::{FieldLevels, ProjectionMask, parquet_to_arrow_field_levels};
use parquet::file::metadata::ParquetMetaData;
use parquet::format::KeyValue;
use snafu::{OptionExt, ResultExt};
use store_api::metadata::{ColumnMetadata, RegionMetadata, RegionMetadataRef};
use store_api::region_request::PathType;
use store_api::storage::{ColumnId, FileId};
use table::predicate::Predicate;

use crate::cache::CacheStrategy;
use crate::cache::index::result_cache::PredicateKey;
#[cfg(feature = "vector_index")]
use crate::error::ApplyVectorIndexSnafu;
use crate::error::{
    ArrowReaderSnafu, InvalidMetadataSnafu, InvalidParquetSnafu, ReadDataPartSnafu,
    ReadParquetSnafu, Result, SerializePartitionExprSnafu,
};
use crate::metrics::{
    PRECISE_FILTER_ROWS_TOTAL, READ_ROW_GROUPS_TOTAL, READ_ROWS_IN_ROW_GROUP_TOTAL,
    READ_ROWS_TOTAL, READ_STAGE_ELAPSED,
};
use crate::read::prune::{PruneReader, Source};
use crate::read::{Batch, BatchReader};
use crate::sst::file::FileHandle;
use crate::sst::index::bloom_filter::applier::{
    BloomFilterIndexApplierRef, BloomFilterIndexApplyMetrics,
};
use crate::sst::index::fulltext_index::applier::{
    FulltextIndexApplierRef, FulltextIndexApplyMetrics,
};
use crate::sst::index::inverted_index::applier::{
    InvertedIndexApplierRef, InvertedIndexApplyMetrics,
};
#[cfg(feature = "vector_index")]
use crate::sst::index::vector_index::applier::VectorIndexApplierRef;
use crate::sst::parquet::file_range::{
    FileRangeContext, FileRangeContextRef, PartitionFilterContext, PreFilterMode, RangeBase,
    row_group_contains_delete,
};
use crate::sst::parquet::format::{ReadFormat, need_override_sequence};
use crate::sst::parquet::metadata::MetadataLoader;
use crate::sst::parquet::row_group::{InMemoryRowGroup, ParquetFetchMetrics};
use crate::sst::parquet::row_selection::RowGroupSelection;
use crate::sst::parquet::stats::RowGroupPruningStats;
use crate::sst::parquet::{DEFAULT_READ_BATCH_SIZE, PARQUET_METADATA_KEY};
use crate::sst::tag_maybe_to_dictionary_field;

const INDEX_TYPE_FULLTEXT: &str = "fulltext";
const INDEX_TYPE_INVERTED: &str = "inverted";
const INDEX_TYPE_BLOOM: &str = "bloom filter";
const INDEX_TYPE_VECTOR: &str = "vector";

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
    table_dir: String,
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
    inverted_index_appliers: [Option<InvertedIndexApplierRef>; 2],
    bloom_filter_index_appliers: [Option<BloomFilterIndexApplierRef>; 2],
    fulltext_index_appliers: [Option<FulltextIndexApplierRef>; 2],
    /// Vector index applier for KNN search.
    #[cfg(feature = "vector_index")]
    vector_index_applier: Option<VectorIndexApplierRef>,
    /// Over-fetched k for vector index scan.
    #[cfg(feature = "vector_index")]
    vector_index_k: Option<usize>,
    /// Expected metadata of the region while reading the SST.
    /// This is usually the latest metadata of the region. The reader use
    /// it get the correct column id of a column by name.
    expected_metadata: Option<RegionMetadataRef>,
    /// Whether to use flat format for reading.
    flat_format: bool,
    /// Whether this reader is for compaction.
    compaction: bool,
    /// Mode to pre-filter columns.
    pre_filter_mode: PreFilterMode,
    /// Whether to decode primary key values eagerly when reading primary key format SSTs.
    decode_primary_key_values: bool,
}

impl ParquetReaderBuilder {
    /// Returns a new [ParquetReaderBuilder] to read specific SST.
    pub fn new(
        table_dir: String,
        path_type: PathType,
        file_handle: FileHandle,
        object_store: ObjectStore,
    ) -> ParquetReaderBuilder {
        ParquetReaderBuilder {
            table_dir,
            path_type,
            file_handle,
            object_store,
            predicate: None,
            projection: None,
            cache_strategy: CacheStrategy::Disabled,
            inverted_index_appliers: [None, None],
            bloom_filter_index_appliers: [None, None],
            fulltext_index_appliers: [None, None],
            #[cfg(feature = "vector_index")]
            vector_index_applier: None,
            #[cfg(feature = "vector_index")]
            vector_index_k: None,
            expected_metadata: None,
            flat_format: false,
            compaction: false,
            pre_filter_mode: PreFilterMode::All,
            decode_primary_key_values: false,
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

    /// Attaches the inverted index appliers to the builder.
    #[must_use]
    pub(crate) fn inverted_index_appliers(
        mut self,
        index_appliers: [Option<InvertedIndexApplierRef>; 2],
    ) -> Self {
        self.inverted_index_appliers = index_appliers;
        self
    }

    /// Attaches the bloom filter index appliers to the builder.
    #[must_use]
    pub(crate) fn bloom_filter_index_appliers(
        mut self,
        index_appliers: [Option<BloomFilterIndexApplierRef>; 2],
    ) -> Self {
        self.bloom_filter_index_appliers = index_appliers;
        self
    }

    /// Attaches the fulltext index appliers to the builder.
    #[must_use]
    pub(crate) fn fulltext_index_appliers(
        mut self,
        index_appliers: [Option<FulltextIndexApplierRef>; 2],
    ) -> Self {
        self.fulltext_index_appliers = index_appliers;
        self
    }

    /// Attaches the vector index applier to the builder.
    #[cfg(feature = "vector_index")]
    #[must_use]
    pub(crate) fn vector_index_applier(
        mut self,
        applier: Option<VectorIndexApplierRef>,
        k: Option<usize>,
    ) -> Self {
        self.vector_index_applier = applier;
        self.vector_index_k = k;
        self
    }

    /// Attaches the expected metadata to the builder.
    #[must_use]
    pub fn expected_metadata(mut self, expected_metadata: Option<RegionMetadataRef>) -> Self {
        self.expected_metadata = expected_metadata;
        self
    }

    /// Sets the flat format flag.
    #[must_use]
    pub fn flat_format(mut self, flat_format: bool) -> Self {
        self.flat_format = flat_format;
        self
    }

    /// Sets the compaction flag.
    #[must_use]
    pub fn compaction(mut self, compaction: bool) -> Self {
        self.compaction = compaction;
        self
    }

    /// Sets the pre-filter mode.
    #[must_use]
    pub(crate) fn pre_filter_mode(mut self, pre_filter_mode: PreFilterMode) -> Self {
        self.pre_filter_mode = pre_filter_mode;
        self
    }

    /// Decodes primary key values eagerly when reading primary key format SSTs.
    #[must_use]
    pub(crate) fn decode_primary_key_values(mut self, decode: bool) -> Self {
        self.decode_primary_key_values = decode;
        self
    }

    /// Builds a [ParquetReader].
    ///
    /// This needs to perform IO operation.
    #[tracing::instrument(
        skip_all,
        fields(
            region_id = %self.file_handle.region_id(),
            file_id = %self.file_handle.file_id()
        )
    )]
    pub async fn build(&self) -> Result<ParquetReader> {
        let mut metrics = ReaderMetrics::default();

        let (context, selection) = self.build_reader_input(&mut metrics).await?;
        ParquetReader::new(Arc::new(context), selection).await
    }

    /// Builds a [FileRangeContext] and collects row groups to read.
    ///
    /// This needs to perform IO operation.
    #[tracing::instrument(
        skip_all,
        fields(
            region_id = %self.file_handle.region_id(),
            file_id = %self.file_handle.file_id()
        )
    )]
    pub(crate) async fn build_reader_input(
        &self,
        metrics: &mut ReaderMetrics,
    ) -> Result<(FileRangeContext, RowGroupSelection)> {
        let start = Instant::now();

        let file_path = self.file_handle.file_path(&self.table_dir, self.path_type);
        let file_size = self.file_handle.meta_ref().file_size;

        // Loads parquet metadata of the file.
        let (parquet_meta, cache_miss) = self
            .read_parquet_metadata(&file_path, file_size, &mut metrics.metadata_cache_metrics)
            .await?;
        // Decodes region metadata.
        let key_value_meta = parquet_meta.file_metadata().key_value_metadata();
        // Gets the metadata stored in the SST.
        let region_meta = Arc::new(Self::get_region_metadata(&file_path, key_value_meta)?);
        let mut read_format = if let Some(column_ids) = &self.projection {
            ReadFormat::new(
                region_meta.clone(),
                Some(column_ids),
                self.flat_format,
                Some(parquet_meta.file_metadata().schema_descr().num_columns()),
                &file_path,
                self.compaction,
            )?
        } else {
            // Lists all column ids to read, we always use the expected metadata if possible.
            let expected_meta = self.expected_metadata.as_ref().unwrap_or(&region_meta);
            let column_ids: Vec<_> = expected_meta
                .column_metadatas
                .iter()
                .map(|col| col.column_id)
                .collect();
            ReadFormat::new(
                region_meta.clone(),
                Some(&column_ids),
                self.flat_format,
                Some(parquet_meta.file_metadata().schema_descr().num_columns()),
                &file_path,
                self.compaction,
            )?
        };
        if self.decode_primary_key_values {
            read_format.set_decode_primary_key_values(true);
        }
        if need_override_sequence(&parquet_meta) {
            read_format
                .set_override_sequence(self.file_handle.meta_ref().sequence.map(|x| x.get()));
        }

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

        // Trigger background download if metadata had a cache miss and selection is not empty
        if cache_miss && !selection.is_empty() {
            use crate::cache::file_cache::{FileType, IndexKey};
            let index_key = IndexKey::new(
                self.file_handle.region_id(),
                self.file_handle.file_id().file_id(),
                FileType::Parquet,
            );
            self.cache_strategy.maybe_download_background(
                index_key,
                file_path.clone(),
                self.object_store.clone(),
                file_size,
            );
        }

        let prune_schema = self
            .expected_metadata
            .as_ref()
            .map(|meta| meta.schema.clone())
            .unwrap_or_else(|| region_meta.schema.clone());

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

        let dyn_filters = if let Some(predicate) = &self.predicate {
            predicate.dyn_filters().clone()
        } else {
            Arc::new(vec![])
        };

        let codec = build_primary_key_codec(read_format.metadata());

        // Compare partition expression and build filter if they are different.
        let region_partition_expr_str = self
            .expected_metadata
            .as_ref()
            .and_then(|meta| meta.partition_expr.as_ref());
        let file_partition_expr_ref = self.file_handle.meta_ref().partition_expr.as_ref();

        let partition_filter = if let Some(region_str) = region_partition_expr_str {
            match crate::region::parse_partition_expr(Some(region_str))? {
                Some(region_partition_expr)
                    if Some(&region_partition_expr) != file_partition_expr_ref =>
                {
                    // Collect columns referenced by the partition expression.
                    let mut referenced_columns = std::collections::HashSet::new();
                    region_partition_expr.collect_column_names(&mut referenced_columns);

                    // Build a partition_schema containing only referenced columns.
                    let use_dictionary_tags = read_format.as_flat().is_some();
                    let partition_schema = Arc::new(datatypes::schema::Schema::new(
                        prune_schema
                            .column_schemas()
                            .iter()
                            .filter(|col| referenced_columns.contains(&col.name))
                            .map(|col| {
                                if use_dictionary_tags {
                                    if let Some(column_meta) =
                                        read_format.metadata().column_by_name(&col.name)
                                    {
                                        if column_meta.semantic_type == SemanticType::Tag
                                            && col.data_type.is_string()
                                        {
                                            let field = Arc::new(Field::new(
                                                &col.name,
                                                col.data_type.as_arrow_type(),
                                                col.is_nullable(),
                                            ));
                                            let dict_field = tag_maybe_to_dictionary_field(
                                                &col.data_type,
                                                &field,
                                            );
                                            let mut column = col.clone();
                                            column.data_type = ConcreteDataType::from_arrow_type(
                                                dict_field.data_type(),
                                            );
                                            return column;
                                        }
                                    }
                                }
                                col.clone()
                            })
                            .collect::<Vec<_>>(),
                    ));

                    let region_partition_physical_expr = region_partition_expr
                        .try_as_physical_expr(&partition_schema.arrow_schema())
                        .context(SerializePartitionExprSnafu)?;

                    Some(PartitionFilterContext {
                        file_partition_expr: file_partition_expr_ref.cloned(),
                        region_partition_physical_expr,
                        partition_schema,
                    })
                }
                _ => None,
            }
        } else {
            None
        };

        let context = FileRangeContext::new(
            reader_builder,
            RangeBase {
                filters,
                dyn_filters,
                read_format,
                expected_metadata: self.expected_metadata.clone(),
                prune_schema,
                codec,
                compat_batch: None,
                pre_filter_mode: self.pre_filter_mode,
                partition_filter,
            },
        );

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
    /// Returns (metadata, cache_miss_flag).
    async fn read_parquet_metadata(
        &self,
        file_path: &str,
        file_size: u64,
        cache_metrics: &mut MetadataCacheMetrics,
    ) -> Result<(Arc<ParquetMetaData>, bool)> {
        let start = Instant::now();
        let _t = READ_STAGE_ELAPSED
            .with_label_values(&["read_parquet_metadata"])
            .start_timer();

        let file_id = self.file_handle.file_id();
        // Tries to get from cache with metrics tracking.
        if let Some(metadata) = self
            .cache_strategy
            .get_parquet_meta_data(file_id, cache_metrics)
            .await
        {
            cache_metrics.metadata_load_cost += start.elapsed();
            return Ok((metadata, false));
        }

        // Cache miss, load metadata directly.
        let metadata_loader = MetadataLoader::new(self.object_store.clone(), file_path, file_size);
        let metadata = metadata_loader.load(cache_metrics).await?;

        let metadata = Arc::new(metadata);
        // Cache the metadata.
        self.cache_strategy
            .put_parquet_meta_data(file_id, metadata.clone());

        cache_metrics.metadata_load_cost += start.elapsed();
        Ok((metadata, true))
    }

    /// Computes row groups to read, along with their respective row selections.
    #[tracing::instrument(
        skip_all,
        fields(
            region_id = %self.file_handle.region_id(),
            file_id = %self.file_handle.file_id()
        )
    )]
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

        // Compute skip_fields once for all pruning operations
        let skip_fields = self.compute_skip_fields(parquet_meta);

        self.prune_row_groups_by_minmax(
            read_format,
            parquet_meta,
            &mut output,
            metrics,
            skip_fields,
        );
        if output.is_empty() {
            return output;
        }

        let fulltext_filtered = self
            .prune_row_groups_by_fulltext_index(
                row_group_size,
                num_row_groups,
                &mut output,
                metrics,
                skip_fields,
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
            skip_fields,
        )
        .await;
        if output.is_empty() {
            return output;
        }

        self.prune_row_groups_by_bloom_filter(
            row_group_size,
            parquet_meta,
            &mut output,
            metrics,
            skip_fields,
        )
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
                skip_fields,
            )
            .await;
        }
        #[cfg(feature = "vector_index")]
        {
            self.prune_row_groups_by_vector_index(
                row_group_size,
                num_row_groups,
                &mut output,
                metrics,
            )
            .await;
            if output.is_empty() {
                return output;
            }
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
        skip_fields: bool,
    ) -> bool {
        if !self.file_handle.meta_ref().fulltext_index_available() {
            return false;
        }

        let mut pruned = false;
        // If skip_fields is true, only apply the first applier (for tags).
        let appliers = if skip_fields {
            &self.fulltext_index_appliers[..1]
        } else {
            &self.fulltext_index_appliers[..]
        };
        for index_applier in appliers.iter().flatten() {
            let predicate_key = index_applier.predicate_key();
            // Fast path: return early if the result is in the cache.
            let cached = self
                .cache_strategy
                .index_result_cache()
                .and_then(|cache| cache.get(predicate_key, self.file_handle.file_id().file_id()));
            if let Some(result) = cached.as_ref()
                && all_required_row_groups_searched(output, result)
            {
                apply_selection_and_update_metrics(output, result, metrics, INDEX_TYPE_FULLTEXT);
                metrics.fulltext_index_cache_hit += 1;
                pruned = true;
                continue;
            }

            // Slow path: apply the index from the file.
            metrics.fulltext_index_cache_miss += 1;
            let file_size_hint = self.file_handle.meta_ref().index_file_size();
            let apply_res = index_applier
                .apply_fine(
                    self.file_handle.index_id(),
                    Some(file_size_hint),
                    metrics.fulltext_index_apply_metrics.as_mut(),
                )
                .await;
            let selection = match apply_res {
                Ok(Some(res)) => {
                    RowGroupSelection::from_row_ids(res, row_group_size, num_row_groups)
                }
                Ok(None) => continue,
                Err(err) => {
                    handle_index_error!(err, self.file_handle, INDEX_TYPE_FULLTEXT);
                    continue;
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
            pruned = true;
        }
        pruned
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
        skip_fields: bool,
    ) -> bool {
        if !self.file_handle.meta_ref().inverted_index_available() {
            return false;
        }

        let mut pruned = false;
        // If skip_fields is true, only apply the first applier (for tags).
        let appliers = if skip_fields {
            &self.inverted_index_appliers[..1]
        } else {
            &self.inverted_index_appliers[..]
        };
        for index_applier in appliers.iter().flatten() {
            let predicate_key = index_applier.predicate_key();
            // Fast path: return early if the result is in the cache.
            let cached = self
                .cache_strategy
                .index_result_cache()
                .and_then(|cache| cache.get(predicate_key, self.file_handle.file_id().file_id()));
            if let Some(result) = cached.as_ref()
                && all_required_row_groups_searched(output, result)
            {
                apply_selection_and_update_metrics(output, result, metrics, INDEX_TYPE_INVERTED);
                metrics.inverted_index_cache_hit += 1;
                pruned = true;
                continue;
            }

            // Slow path: apply the index from the file.
            metrics.inverted_index_cache_miss += 1;
            let file_size_hint = self.file_handle.meta_ref().index_file_size();
            let apply_res = index_applier
                .apply(
                    self.file_handle.index_id(),
                    Some(file_size_hint),
                    metrics.inverted_index_apply_metrics.as_mut(),
                )
                .await;
            let selection = match apply_res {
                Ok(apply_output) => RowGroupSelection::from_inverted_index_apply_output(
                    row_group_size,
                    num_row_groups,
                    apply_output,
                ),
                Err(err) => {
                    handle_index_error!(err, self.file_handle, INDEX_TYPE_INVERTED);
                    continue;
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
            pruned = true;
        }
        pruned
    }

    async fn prune_row_groups_by_bloom_filter(
        &self,
        row_group_size: usize,
        parquet_meta: &ParquetMetaData,
        output: &mut RowGroupSelection,
        metrics: &mut ReaderFilterMetrics,
        skip_fields: bool,
    ) -> bool {
        if !self.file_handle.meta_ref().bloom_filter_index_available() {
            return false;
        }

        let mut pruned = false;
        // If skip_fields is true, only apply the first applier (for tags).
        let appliers = if skip_fields {
            &self.bloom_filter_index_appliers[..1]
        } else {
            &self.bloom_filter_index_appliers[..]
        };
        for index_applier in appliers.iter().flatten() {
            let predicate_key = index_applier.predicate_key();
            // Fast path: return early if the result is in the cache.
            let cached = self
                .cache_strategy
                .index_result_cache()
                .and_then(|cache| cache.get(predicate_key, self.file_handle.file_id().file_id()));
            if let Some(result) = cached.as_ref()
                && all_required_row_groups_searched(output, result)
            {
                apply_selection_and_update_metrics(output, result, metrics, INDEX_TYPE_BLOOM);
                metrics.bloom_filter_cache_hit += 1;
                pruned = true;
                continue;
            }

            // Slow path: apply the index from the file.
            metrics.bloom_filter_cache_miss += 1;
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
                .apply(
                    self.file_handle.index_id(),
                    Some(file_size_hint),
                    rgs,
                    metrics.bloom_filter_apply_metrics.as_mut(),
                )
                .await;
            let mut selection = match apply_res {
                Ok(apply_output) => {
                    RowGroupSelection::from_row_ranges(apply_output, row_group_size)
                }
                Err(err) => {
                    handle_index_error!(err, self.file_handle, INDEX_TYPE_BLOOM);
                    continue;
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
            pruned = true;
        }
        pruned
    }

    /// Prunes row groups by vector index results.
    #[cfg(feature = "vector_index")]
    async fn prune_row_groups_by_vector_index(
        &self,
        row_group_size: usize,
        num_row_groups: usize,
        output: &mut RowGroupSelection,
        metrics: &mut ReaderFilterMetrics,
    ) {
        let Some(applier) = &self.vector_index_applier else {
            return;
        };
        let Some(k) = self.vector_index_k else {
            return;
        };
        if !self.file_handle.meta_ref().vector_index_available() {
            return;
        }

        let file_size_hint = self.file_handle.meta_ref().index_file_size();
        let apply_res = applier
            .apply_with_k(self.file_handle.index_id(), Some(file_size_hint), k)
            .await;
        let row_ids = match apply_res {
            Ok(res) => res.row_offsets,
            Err(err) => {
                handle_index_error!(err, self.file_handle, INDEX_TYPE_VECTOR);
                return;
            }
        };

        let selection = match vector_selection_from_offsets(row_ids, row_group_size, num_row_groups)
        {
            Ok(selection) => selection,
            Err(err) => {
                handle_index_error!(err, self.file_handle, INDEX_TYPE_VECTOR);
                return;
            }
        };
        apply_selection_and_update_metrics(output, &selection, metrics, INDEX_TYPE_VECTOR);
    }

    async fn prune_row_groups_by_fulltext_bloom(
        &self,
        row_group_size: usize,
        parquet_meta: &ParquetMetaData,
        output: &mut RowGroupSelection,
        metrics: &mut ReaderFilterMetrics,
        skip_fields: bool,
    ) -> bool {
        if !self.file_handle.meta_ref().fulltext_index_available() {
            return false;
        }

        let mut pruned = false;
        // If skip_fields is true, only apply the first applier (for tags).
        let appliers = if skip_fields {
            &self.fulltext_index_appliers[..1]
        } else {
            &self.fulltext_index_appliers[..]
        };
        for index_applier in appliers.iter().flatten() {
            let predicate_key = index_applier.predicate_key();
            // Fast path: return early if the result is in the cache.
            let cached = self
                .cache_strategy
                .index_result_cache()
                .and_then(|cache| cache.get(predicate_key, self.file_handle.file_id().file_id()));
            if let Some(result) = cached.as_ref()
                && all_required_row_groups_searched(output, result)
            {
                apply_selection_and_update_metrics(output, result, metrics, INDEX_TYPE_FULLTEXT);
                metrics.fulltext_index_cache_hit += 1;
                pruned = true;
                continue;
            }

            // Slow path: apply the index from the file.
            metrics.fulltext_index_cache_miss += 1;
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
                .apply_coarse(
                    self.file_handle.index_id(),
                    Some(file_size_hint),
                    rgs,
                    metrics.fulltext_index_apply_metrics.as_mut(),
                )
                .await;
            let mut selection = match apply_res {
                Ok(Some(apply_output)) => {
                    RowGroupSelection::from_row_ranges(apply_output, row_group_size)
                }
                Ok(None) => continue,
                Err(err) => {
                    handle_index_error!(err, self.file_handle, INDEX_TYPE_FULLTEXT);
                    continue;
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
            pruned = true;
        }
        pruned
    }

    /// Computes whether to skip field columns when building statistics based on PreFilterMode.
    fn compute_skip_fields(&self, parquet_meta: &ParquetMetaData) -> bool {
        match self.pre_filter_mode {
            PreFilterMode::All => false,
            PreFilterMode::SkipFields => true,
            PreFilterMode::SkipFieldsOnDelete => {
                // Check if any row group contains delete op
                let file_path = self.file_handle.file_path(&self.table_dir, self.path_type);
                (0..parquet_meta.num_row_groups()).any(|rg_idx| {
                    row_group_contains_delete(parquet_meta, rg_idx, &file_path)
                        .inspect_err(|e| {
                            warn!(e; "Failed to decode min value of op_type, fallback to not skipping fields");
                        })
                        .unwrap_or(false)
                })
            }
        }
    }

    /// Prunes row groups by min-max index.
    fn prune_row_groups_by_minmax(
        &self,
        read_format: &ReadFormat,
        parquet_meta: &ParquetMetaData,
        output: &mut RowGroupSelection,
        metrics: &mut ReaderFilterMetrics,
        skip_fields: bool,
    ) -> bool {
        let Some(predicate) = &self.predicate else {
            return false;
        };

        let row_groups_before = output.row_group_count();

        let region_meta = read_format.metadata();
        let row_groups = parquet_meta.row_groups();
        let stats = RowGroupPruningStats::new(
            row_groups,
            read_format,
            self.expected_metadata.clone(),
            skip_fields,
        );
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

#[cfg(feature = "vector_index")]
fn vector_selection_from_offsets(
    row_offsets: Vec<u64>,
    row_group_size: usize,
    num_row_groups: usize,
) -> Result<RowGroupSelection> {
    let mut row_ids = BTreeSet::new();
    for offset in row_offsets {
        let row_id = u32::try_from(offset).map_err(|_| {
            ApplyVectorIndexSnafu {
                reason: format!("Row offset {} exceeds u32::MAX", offset),
            }
            .build()
        })?;
        row_ids.insert(row_id);
    }
    Ok(RowGroupSelection::from_row_ids(
        row_ids,
        row_group_size,
        num_row_groups,
    ))
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
#[derive(Debug, Default, Clone)]
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
    /// Number of row groups filtered by vector index.
    pub(crate) rg_vector_filtered: usize,

    /// Number of rows in row group before filtering.
    pub(crate) rows_total: usize,
    /// Number of rows in row group filtered by fulltext index.
    pub(crate) rows_fulltext_filtered: usize,
    /// Number of rows in row group filtered by inverted index.
    pub(crate) rows_inverted_filtered: usize,
    /// Number of rows in row group filtered by bloom filter index.
    pub(crate) rows_bloom_filtered: usize,
    /// Number of rows filtered by vector index.
    pub(crate) rows_vector_filtered: usize,
    /// Number of rows filtered by precise filter.
    pub(crate) rows_precise_filtered: usize,

    /// Number of index result cache hits for fulltext index.
    pub(crate) fulltext_index_cache_hit: usize,
    /// Number of index result cache misses for fulltext index.
    pub(crate) fulltext_index_cache_miss: usize,
    /// Number of index result cache hits for inverted index.
    pub(crate) inverted_index_cache_hit: usize,
    /// Number of index result cache misses for inverted index.
    pub(crate) inverted_index_cache_miss: usize,
    /// Number of index result cache hits for bloom filter index.
    pub(crate) bloom_filter_cache_hit: usize,
    /// Number of index result cache misses for bloom filter index.
    pub(crate) bloom_filter_cache_miss: usize,

    /// Optional metrics for inverted index applier.
    pub(crate) inverted_index_apply_metrics: Option<InvertedIndexApplyMetrics>,
    /// Optional metrics for bloom filter index applier.
    pub(crate) bloom_filter_apply_metrics: Option<BloomFilterIndexApplyMetrics>,
    /// Optional metrics for fulltext index applier.
    pub(crate) fulltext_index_apply_metrics: Option<FulltextIndexApplyMetrics>,
}

impl ReaderFilterMetrics {
    /// Adds `other` metrics to this metrics.
    pub(crate) fn merge_from(&mut self, other: &ReaderFilterMetrics) {
        self.rg_total += other.rg_total;
        self.rg_fulltext_filtered += other.rg_fulltext_filtered;
        self.rg_inverted_filtered += other.rg_inverted_filtered;
        self.rg_minmax_filtered += other.rg_minmax_filtered;
        self.rg_bloom_filtered += other.rg_bloom_filtered;
        self.rg_vector_filtered += other.rg_vector_filtered;

        self.rows_total += other.rows_total;
        self.rows_fulltext_filtered += other.rows_fulltext_filtered;
        self.rows_inverted_filtered += other.rows_inverted_filtered;
        self.rows_bloom_filtered += other.rows_bloom_filtered;
        self.rows_vector_filtered += other.rows_vector_filtered;
        self.rows_precise_filtered += other.rows_precise_filtered;

        self.fulltext_index_cache_hit += other.fulltext_index_cache_hit;
        self.fulltext_index_cache_miss += other.fulltext_index_cache_miss;
        self.inverted_index_cache_hit += other.inverted_index_cache_hit;
        self.inverted_index_cache_miss += other.inverted_index_cache_miss;
        self.bloom_filter_cache_hit += other.bloom_filter_cache_hit;
        self.bloom_filter_cache_miss += other.bloom_filter_cache_miss;

        // Merge optional applier metrics
        if let Some(other_metrics) = &other.inverted_index_apply_metrics {
            self.inverted_index_apply_metrics
                .get_or_insert_with(Default::default)
                .merge_from(other_metrics);
        }
        if let Some(other_metrics) = &other.bloom_filter_apply_metrics {
            self.bloom_filter_apply_metrics
                .get_or_insert_with(Default::default)
                .merge_from(other_metrics);
        }
        if let Some(other_metrics) = &other.fulltext_index_apply_metrics {
            self.fulltext_index_apply_metrics
                .get_or_insert_with(Default::default)
                .merge_from(other_metrics);
        }
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
        READ_ROW_GROUPS_TOTAL
            .with_label_values(&["vector_index_filtered"])
            .inc_by(self.rg_vector_filtered as u64);

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
        READ_ROWS_IN_ROW_GROUP_TOTAL
            .with_label_values(&["vector_index_filtered"])
            .inc_by(self.rows_vector_filtered as u64);
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
            INDEX_TYPE_VECTOR => {
                self.rg_vector_filtered += row_group_count;
                self.rows_vector_filtered += row_count;
            }
            _ => {}
        }
    }
}

#[cfg(all(test, feature = "vector_index"))]
mod tests {
    use super::*;

    #[test]
    fn test_vector_selection_from_offsets() {
        let row_group_size = 4;
        let num_row_groups = 3;
        let selection =
            vector_selection_from_offsets(vec![0, 1, 5, 9], row_group_size, num_row_groups)
                .unwrap();

        assert_eq!(selection.row_group_count(), 3);
        assert_eq!(selection.row_count(), 4);
        assert!(selection.contains_non_empty_row_group(0));
        assert!(selection.contains_non_empty_row_group(1));
        assert!(selection.contains_non_empty_row_group(2));
    }

    #[test]
    fn test_vector_selection_from_offsets_out_of_range() {
        let row_group_size = 4;
        let num_row_groups = 2;
        let selection = vector_selection_from_offsets(
            vec![0, 7, u64::from(u32::MAX) + 1],
            row_group_size,
            num_row_groups,
        );
        assert!(selection.is_err());
    }

    #[test]
    fn test_vector_selection_updates_metrics() {
        let row_group_size = 4;
        let total_rows = 8;
        let mut output = RowGroupSelection::new(row_group_size, total_rows);
        let selection = vector_selection_from_offsets(vec![1], row_group_size, 2).unwrap();
        let mut metrics = ReaderFilterMetrics::default();

        apply_selection_and_update_metrics(
            &mut output,
            &selection,
            &mut metrics,
            INDEX_TYPE_VECTOR,
        );

        assert_eq!(metrics.rg_vector_filtered, 1);
        assert_eq!(metrics.rows_vector_filtered, 7);
        assert_eq!(output.row_count(), 1);
    }
}

/// Metrics for parquet metadata cache operations.
#[derive(Default, Clone, Copy)]
pub(crate) struct MetadataCacheMetrics {
    /// Number of memory cache hits for parquet metadata.
    pub(crate) mem_cache_hit: usize,
    /// Number of file cache hits for parquet metadata.
    pub(crate) file_cache_hit: usize,
    /// Number of cache misses for parquet metadata.
    pub(crate) cache_miss: usize,
    /// Duration to load parquet metadata.
    pub(crate) metadata_load_cost: Duration,
    /// Number of read operations performed.
    pub(crate) num_reads: usize,
    /// Total bytes read from storage.
    pub(crate) bytes_read: u64,
}

impl std::fmt::Debug for MetadataCacheMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            mem_cache_hit,
            file_cache_hit,
            cache_miss,
            metadata_load_cost,
            num_reads,
            bytes_read,
        } = self;

        if self.is_empty() {
            return write!(f, "{{}}");
        }
        write!(f, "{{")?;

        write!(f, "\"metadata_load_cost\":\"{:?}\"", metadata_load_cost)?;

        if *mem_cache_hit > 0 {
            write!(f, ", \"mem_cache_hit\":{}", mem_cache_hit)?;
        }
        if *file_cache_hit > 0 {
            write!(f, ", \"file_cache_hit\":{}", file_cache_hit)?;
        }
        if *cache_miss > 0 {
            write!(f, ", \"cache_miss\":{}", cache_miss)?;
        }
        if *num_reads > 0 {
            write!(f, ", \"num_reads\":{}", num_reads)?;
        }
        if *bytes_read > 0 {
            write!(f, ", \"bytes_read\":{}", bytes_read)?;
        }

        write!(f, "}}")
    }
}

impl MetadataCacheMetrics {
    /// Returns true if the metrics are empty (contain no meaningful data).
    pub(crate) fn is_empty(&self) -> bool {
        self.metadata_load_cost.is_zero()
    }

    /// Adds `other` metrics to this metrics.
    pub(crate) fn merge_from(&mut self, other: &MetadataCacheMetrics) {
        self.mem_cache_hit += other.mem_cache_hit;
        self.file_cache_hit += other.file_cache_hit;
        self.cache_miss += other.cache_miss;
        self.metadata_load_cost += other.metadata_load_cost;
        self.num_reads += other.num_reads;
        self.bytes_read += other.bytes_read;
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
    /// Metrics for parquet metadata cache.
    pub(crate) metadata_cache_metrics: MetadataCacheMetrics,
    /// Optional metrics for page/row group fetch operations.
    pub(crate) fetch_metrics: Option<Arc<ParquetFetchMetrics>>,
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
        self.metadata_cache_metrics
            .merge_from(&other.metadata_cache_metrics);
        if let Some(other_fetch) = &other.fetch_metrics {
            if let Some(self_fetch) = &self.fetch_metrics {
                self_fetch.merge_from(other_fetch);
            } else {
                self.fetch_metrics = Some(other_fetch.clone());
            }
        }
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
        fetch_metrics: Option<&ParquetFetchMetrics>,
    ) -> Result<ParquetRecordBatchReader> {
        let fetch_start = Instant::now();

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
            .fetch(&self.projection, row_selection.as_ref(), fetch_metrics)
            .await
            .context(ReadParquetSnafu {
                path: &self.file_path,
            })?;

        // Record total fetch elapsed time.
        if let Some(metrics) = fetch_metrics {
            metrics.data.lock().unwrap().total_fetch_elapsed += fetch_start.elapsed();
        }

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
    /// Metrics for tracking row group fetch operations.
    fetch_metrics: ParquetFetchMetrics,
}

#[async_trait]
impl BatchReader for ParquetReader {
    #[tracing::instrument(
        skip_all,
        fields(
            region_id = %self.context.reader_builder().file_handle.region_id(),
            file_id = %self.context.reader_builder().file_handle.file_id()
        )
    )]
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
                .build(
                    row_group_idx,
                    Some(row_selection),
                    Some(&self.fetch_metrics),
                )
                .await?;

            // Resets the parquet reader.
            // Compute skip_fields for this row group
            let skip_fields = self.context.should_skip_fields(row_group_idx);
            reader.reset_source(
                Source::RowGroup(RowGroupReader::new(self.context.clone(), parquet_reader)),
                skip_fields,
            );
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
    #[tracing::instrument(
        skip_all,
        fields(
            region_id = %context.reader_builder().file_handle.region_id(),
            file_id = %context.reader_builder().file_handle.file_id()
        )
    )]
    pub(crate) async fn new(
        context: FileRangeContextRef,
        mut selection: RowGroupSelection,
    ) -> Result<Self> {
        let fetch_metrics = ParquetFetchMetrics::default();
        // No more items in current row group, reads next row group.
        let reader_state = if let Some((row_group_idx, row_selection)) = selection.pop_first() {
            let parquet_reader = context
                .reader_builder()
                .build(row_group_idx, Some(row_selection), Some(&fetch_metrics))
                .await?;
            // Compute skip_fields once for this row group
            let skip_fields = context.should_skip_fields(row_group_idx);
            ReaderState::Readable(PruneReader::new_with_row_group_reader(
                context.clone(),
                RowGroupReader::new(context.clone(), parquet_reader),
                skip_fields,
            ))
        } else {
            ReaderState::Exhausted(ReaderMetrics::default())
        };

        Ok(ParquetReader {
            context,
            selection,
            reader_state,
            fetch_metrics,
        })
    }

    /// Returns the metadata of the SST.
    pub fn metadata(&self) -> &RegionMetadataRef {
        self.context.read_format().metadata()
    }

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

/// Reader to read a row group of a parquet file in flat format, returning RecordBatch.
pub(crate) struct FlatRowGroupReader {
    /// Context for file ranges.
    context: FileRangeContextRef,
    /// Inner parquet reader.
    reader: ParquetRecordBatchReader,
    /// Cached sequence array to override sequences.
    override_sequence: Option<ArrayRef>,
}

impl FlatRowGroupReader {
    /// Creates a new flat reader from file range.
    pub(crate) fn new(context: FileRangeContextRef, reader: ParquetRecordBatchReader) -> Self {
        // The batch length from the reader should be less than or equal to DEFAULT_READ_BATCH_SIZE.
        let override_sequence = context
            .read_format()
            .new_override_sequence_array(DEFAULT_READ_BATCH_SIZE);

        Self {
            context,
            reader,
            override_sequence,
        }
    }

    /// Returns the next RecordBatch.
    pub(crate) fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        match self.reader.next() {
            Some(batch_result) => {
                let record_batch = batch_result.context(ArrowReaderSnafu {
                    path: self.context.file_path(),
                })?;

                // Safety: Only flat format use FlatRowGroupReader.
                let flat_format = self.context.read_format().as_flat().unwrap();
                let record_batch =
                    flat_format.convert_batch(record_batch, self.override_sequence.as_ref())?;
                Ok(Some(record_batch))
            }
            None => Ok(None),
        }
    }
}
