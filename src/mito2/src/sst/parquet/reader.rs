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

mod stream;

#[cfg(feature = "vector_index")]
use std::collections::BTreeSet;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use api::v1::SemanticType;
use common_recordbatch::filter::SimpleFilterEvaluator;
use common_telemetry::{error, tracing, warn};
use datafusion::physical_plan::PhysicalExpr;
use datafusion_expr::Expr;
use datafusion_expr::utils::expr_to_columns;
use datatypes::arrow::array::ArrayRef;
use datatypes::arrow::datatypes::{Field, Schema as ArrowSchema, SchemaRef};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::data_type::ConcreteDataType;
use datatypes::prelude::DataType;
use futures::StreamExt;
use mito_codec::row_converter::build_primary_key_codec;
use object_store::ObjectStore;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, RowSelection};
use parquet::arrow::async_reader::{ParquetRecordBatchStream, ParquetRecordBatchStreamBuilder};
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData};
use partition::expr::PartitionExpr;
use snafu::ResultExt;
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::{ColumnMetadata, RegionMetadata, RegionMetadataRef};
use store_api::region_request::PathType;
use store_api::storage::{ColumnId, FileId};
use table::predicate::Predicate;

use self::stream::{ParquetErrorAdapter, ProjectedRecordBatchStream};
use crate::cache::index::result_cache::PredicateKey;
use crate::cache::{CacheStrategy, CachedSstMeta};
#[cfg(feature = "vector_index")]
use crate::error::ApplyVectorIndexSnafu;
use crate::error::{ReadDataPartSnafu, ReadParquetSnafu, Result, SerializePartitionExprSnafu};
use crate::metrics::{
    PRECISE_FILTER_ROWS_TOTAL, READ_ROW_GROUPS_TOTAL, READ_ROWS_IN_ROW_GROUP_TOTAL,
    READ_ROWS_TOTAL, READ_STAGE_ELAPSED,
};
use crate::read::flat_projection::CompactionProjectionMapper;
use crate::read::prune::FlatPruneReader;
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
use crate::sst::parquet::DEFAULT_READ_BATCH_SIZE;
use crate::sst::parquet::async_reader::SstAsyncFileReader;
use crate::sst::parquet::file_range::{
    FileRangeContext, FileRangeContextRef, PartitionFilterContext, PreFilterMode, RangeBase,
};
use crate::sst::parquet::flat_format::FlatReadFormat;
use crate::sst::parquet::format::need_override_sequence;
use crate::sst::parquet::metadata::MetadataLoader;
use crate::sst::parquet::prefilter::{
    PrefilterContextBuilder, build_reader_filter_plan, execute_prefilter,
};
use crate::sst::parquet::read_columns::{
    ParquetReadColumns, ProjectionMaskPlan, build_projection_plan,
};
use crate::sst::parquet::row_group::ParquetFetchMetrics;
use crate::sst::parquet::row_selection::RowGroupSelection;
use crate::sst::parquet::stats::RowGroupPruningStats;
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
    /// Whether this reader is for compaction.
    compaction: bool,
    /// Mode to pre-filter columns.
    pre_filter_mode: PreFilterMode,
    /// Whether to decode primary key values eagerly when reading primary key format SSTs.
    decode_primary_key_values: bool,
    page_index_policy: PageIndexPolicy,
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
            compaction: false,
            pre_filter_mode: PreFilterMode::All,
            decode_primary_key_values: false,
            page_index_policy: Default::default(),
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

    #[must_use]
    pub fn page_index_policy(mut self, page_index_policy: PageIndexPolicy) -> Self {
        self.page_index_policy = page_index_policy;
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
    pub async fn build(&self) -> Result<Option<ParquetReader>> {
        let mut metrics = ReaderMetrics::default();

        let Some((context, selection)) = self.build_reader_input_inner(&mut metrics).await? else {
            return Ok(None);
        };
        ParquetReader::new(Arc::new(context), selection)
            .await
            .map(Some)
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
    ) -> Result<Option<(FileRangeContext, RowGroupSelection)>> {
        self.build_reader_input_inner(metrics).await
    }

    async fn build_reader_input_inner(
        &self,
        metrics: &mut ReaderMetrics,
    ) -> Result<Option<(FileRangeContext, RowGroupSelection)>> {
        let start = Instant::now();

        let file_path = self.file_handle.file_path(&self.table_dir, self.path_type);
        let file_size = self.file_handle.meta_ref().file_size;

        // Loads parquet metadata of the file.
        let (sst_meta, cache_miss) = self
            .read_parquet_metadata(
                &file_path,
                file_size,
                &mut metrics.metadata_cache_metrics,
                self.page_index_policy,
            )
            .await?;
        let parquet_meta = sst_meta.parquet_metadata();
        let region_meta = sst_meta.region_metadata();
        let region_partition_expr_str = self
            .expected_metadata
            .as_ref()
            .and_then(|meta| meta.partition_expr.as_ref())
            .map(|expr| expr.as_str());
        let (_, is_same_region_partition) = Self::is_same_region_partition(
            region_partition_expr_str,
            self.file_handle.meta_ref().partition_expr.as_ref(),
        )?;
        // Skip auto convert when:
        // - compaction is enabled
        // - region partition expr is same with file partition expr (no need to auto convert)
        let skip_auto_convert = self.compaction && is_same_region_partition;

        // Build a compaction projection helper when:
        // - compaction is enabled
        // - region partition expr differs from file partition expr
        // - flat format is enabled
        // - primary key encoding is sparse
        //
        // This is applied after row-group filtering to align batches with flat output schema
        // before compat handling.
        let compaction_projection_mapper = if self.compaction
            && !is_same_region_partition
            && region_meta.primary_key_encoding == PrimaryKeyEncoding::Sparse
        {
            Some(CompactionProjectionMapper::try_new(&region_meta)?)
        } else {
            None
        };

        let mut read_format = if let Some(column_ids) = &self.projection {
            FlatReadFormat::new(
                region_meta.clone(),
                column_ids.iter().copied(),
                Some(parquet_meta.file_metadata().schema_descr().num_columns()),
                &file_path,
                skip_auto_convert,
            )?
        } else {
            // Lists all column ids to read, we always use the expected metadata if possible.
            let expected_meta = self.expected_metadata.as_ref().unwrap_or(&region_meta);
            let column_ids: Vec<_> = expected_meta
                .column_metadatas
                .iter()
                .map(|col| col.column_id)
                .collect();
            FlatReadFormat::new(
                region_meta.clone(),
                column_ids.iter().copied(),
                Some(parquet_meta.file_metadata().schema_descr().num_columns()),
                &file_path,
                skip_auto_convert,
            )?
        };
        if need_override_sequence(&parquet_meta) {
            read_format
                .set_override_sequence(self.file_handle.meta_ref().sequence.map(|x| x.get()));
        }

        // Computes the projection mask.
        let parquet_schema_desc = parquet_meta.file_metadata().schema_descr();
        let parquet_read_cols = ParquetReadColumns::from_deduped_root_indices(
            read_format.projection_indices().iter().copied(),
        );

        let projection_plan = build_projection_plan(&parquet_read_cols, parquet_schema_desc);

        let selection = self
            .row_groups_to_read(&read_format, &parquet_meta, &mut metrics.filter_metrics)
            .await;

        if selection.is_empty() {
            metrics.build_cost += start.elapsed();
            return Ok(None);
        }

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

        // Create ArrowReaderMetadata for async stream building.
        let arrow_reader_options =
            ArrowReaderOptions::new().with_schema(read_format.arrow_schema().clone());
        let arrow_metadata =
            ArrowReaderMetadata::try_new(parquet_meta.clone(), arrow_reader_options)
                .context(ReadDataPartSnafu)?;

        let dyn_filters = if let Some(predicate) = &self.predicate {
            predicate.dyn_filters().as_ref().clone()
        } else {
            vec![]
        };

        let codec = build_primary_key_codec(read_format.metadata());

        let filter_plan = build_reader_filter_plan(
            self.predicate.as_ref(),
            self.expected_metadata.as_deref(),
            self.pre_filter_mode,
            &read_format,
            parquet_meta.file_metadata().schema_descr(),
            &codec,
        );

        let output_schema = read_format.output_arrow_schema()?;

        let reader_builder = RowGroupReaderBuilder {
            file_handle: self.file_handle.clone(),
            file_path,
            parquet_meta,
            arrow_metadata,
            output_schema,
            object_store: self.object_store.clone(),
            projection: projection_plan,
            cache_strategy: self.cache_strategy.clone(),
            prefilter_builder: filter_plan.prefilter_builder,
        };

        let partition_filter = self.build_partition_filter(&read_format, &prune_schema)?;

        let context = FileRangeContext::new(
            reader_builder,
            RangeBase {
                filters: filter_plan.remaining_simple_filters,
                dyn_filters,
                read_format,
                expected_metadata: self.expected_metadata.clone(),
                prune_schema,
                codec,
                compat_batch: None,
                compaction_projection_mapper,
                pre_filter_mode: self.pre_filter_mode,
                partition_filter,
            },
        );

        metrics.build_cost += start.elapsed();

        Ok(Some((context, selection)))
    }

    fn is_same_region_partition(
        region_partition_expr_str: Option<&str>,
        file_partition_expr: Option<&PartitionExpr>,
    ) -> Result<(Option<PartitionExpr>, bool)> {
        let region_partition_expr = match region_partition_expr_str {
            Some(expr_str) => crate::region::parse_partition_expr(Some(expr_str))?,
            None => None,
        };

        let is_same = region_partition_expr.as_ref() == file_partition_expr;
        Ok((region_partition_expr, is_same))
    }

    /// Compare partition expressions from expected metadata and file metadata,
    /// and build a partition filter if they differ.
    fn build_partition_filter(
        &self,
        read_format: &FlatReadFormat,
        prune_schema: &Arc<datatypes::schema::Schema>,
    ) -> Result<Option<PartitionFilterContext>> {
        let region_partition_expr_str = self
            .expected_metadata
            .as_ref()
            .and_then(|meta| meta.partition_expr.as_ref());
        let file_partition_expr_ref = self.file_handle.meta_ref().partition_expr.as_ref();

        let (region_partition_expr, is_same_region_partition) = Self::is_same_region_partition(
            region_partition_expr_str.map(|s| s.as_str()),
            file_partition_expr_ref,
        )?;

        if is_same_region_partition {
            return Ok(None);
        }

        let Some(region_partition_expr) = region_partition_expr else {
            return Ok(None);
        };

        // Collect columns referenced by the partition expression.
        let mut referenced_columns = HashSet::new();
        region_partition_expr.collect_column_names(&mut referenced_columns);

        // Build a partition_schema containing only referenced columns.
        let partition_schema = Arc::new(datatypes::schema::Schema::new(
            prune_schema
                .column_schemas()
                .iter()
                .filter(|col| referenced_columns.contains(&col.name))
                .map(|col| {
                    if let Some(column_meta) = read_format.metadata().column_by_name(&col.name)
                        && column_meta.semantic_type == SemanticType::Tag
                        && col.data_type.is_string()
                    {
                        let field = Arc::new(Field::new(
                            &col.name,
                            col.data_type.as_arrow_type(),
                            col.is_nullable(),
                        ));
                        let dict_field = tag_maybe_to_dictionary_field(&col.data_type, &field);
                        let mut column = col.clone();
                        column.data_type =
                            ConcreteDataType::from_arrow_type(dict_field.data_type());
                        return column;
                    }

                    col.clone()
                })
                .collect::<Vec<_>>(),
        ));

        let region_partition_physical_expr = region_partition_expr
            .try_as_physical_expr(partition_schema.arrow_schema())
            .context(SerializePartitionExprSnafu)?;

        Ok(Some(PartitionFilterContext {
            region_partition_physical_expr,
            partition_schema,
        }))
    }

    /// Reads parquet metadata of specific file.
    /// Returns (fused metadata, cache_miss_flag).
    async fn read_parquet_metadata(
        &self,
        file_path: &str,
        file_size: u64,
        cache_metrics: &mut MetadataCacheMetrics,
        page_index_policy: PageIndexPolicy,
    ) -> Result<(Arc<CachedSstMeta>, bool)> {
        let start = Instant::now();
        let _t = READ_STAGE_ELAPSED
            .with_label_values(&["read_parquet_metadata"])
            .start_timer();

        let file_id = self.file_handle.file_id();
        // Tries to get from cache with metrics tracking.
        if let Some(metadata) = self
            .cache_strategy
            .get_sst_meta_data(file_id, cache_metrics, page_index_policy)
            .await
        {
            cache_metrics.metadata_load_cost += start.elapsed();
            return Ok((metadata, false));
        }

        // Cache miss, load metadata directly.
        let mut metadata_loader =
            MetadataLoader::new(self.object_store.clone(), file_path, file_size);
        metadata_loader.with_page_index_policy(page_index_policy);
        let metadata = metadata_loader.load(cache_metrics).await?;

        let metadata = Arc::new(CachedSstMeta::try_new(file_path, metadata)?);
        // Cache the metadata.
        self.cache_strategy
            .put_sst_meta_data(file_id, metadata.clone());

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
        read_format: &FlatReadFormat,
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

        // Compute skip_fields once for all pruning operations
        let skip_fields = self.pre_filter_mode.skip_fields();

        let mut output = self.row_groups_by_minmax(
            read_format,
            parquet_meta,
            row_group_size,
            num_rows as usize,
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
        metrics.rows_vector_selected += selection.row_count();
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

    /// Computes row groups selection after min-max pruning.
    fn row_groups_by_minmax(
        &self,
        read_format: &FlatReadFormat,
        parquet_meta: &ParquetMetaData,
        row_group_size: usize,
        total_row_count: usize,
        metrics: &mut ReaderFilterMetrics,
        skip_fields: bool,
    ) -> RowGroupSelection {
        let Some(predicate) = &self.predicate else {
            return RowGroupSelection::new(row_group_size, total_row_count);
        };

        let file_id = self.file_handle.file_id().file_id();
        let index_result_cache = self.cache_strategy.index_result_cache();
        let cached_minmax_key =
            if index_result_cache.is_some() && predicate.dyn_filters().is_empty() {
                // Cache min-max pruning results keyed by predicate expressions. This avoids repeatedly
                // building row-group pruning stats for identical predicates across queries.
                let mut exprs = predicate
                    .exprs()
                    .iter()
                    .map(|expr| format!("{expr:?}"))
                    .collect::<Vec<_>>();
                exprs.sort();
                let schema_version = self
                    .expected_metadata
                    .as_ref()
                    .map(|meta| meta.schema_version)
                    .unwrap_or_else(|| read_format.metadata().schema_version);
                Some(PredicateKey::new_minmax(
                    Arc::new(exprs),
                    schema_version,
                    skip_fields,
                ))
            } else {
                None
            };

        if let Some(index_result_cache) = index_result_cache
            && let Some(predicate_key) = cached_minmax_key.as_ref()
        {
            if let Some(result) = index_result_cache.get(predicate_key, file_id) {
                metrics.minmax_cache_hit += 1;
                let num_row_groups = parquet_meta.num_row_groups();
                metrics.rg_minmax_filtered +=
                    num_row_groups.saturating_sub(result.row_group_count());
                return (*result).clone();
            }

            metrics.minmax_cache_miss += 1;
        }

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
        let mask = predicate.prune_with_stats(&stats, prune_schema);
        let output = RowGroupSelection::from_full_row_group_ids(
            mask.iter()
                .enumerate()
                .filter_map(|(row_group, keep)| keep.then_some(row_group)),
            row_group_size,
            total_row_count,
        );

        metrics.rg_minmax_filtered += parquet_meta
            .num_row_groups()
            .saturating_sub(output.row_group_count());

        if let Some(index_result_cache) = index_result_cache
            && let Some(predicate_key) = cached_minmax_key
        {
            index_result_cache.put(predicate_key, file_id, Arc::new(output.clone()));
        }

        output
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
    /// Number of rows selected by vector index.
    pub(crate) rows_vector_selected: usize,
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
    /// Number of index result cache hits for minmax pruning.
    pub(crate) minmax_cache_hit: usize,
    /// Number of index result cache misses for minmax pruning.
    pub(crate) minmax_cache_miss: usize,

    /// Optional metrics for inverted index applier.
    pub(crate) inverted_index_apply_metrics: Option<InvertedIndexApplyMetrics>,
    /// Optional metrics for bloom filter index applier.
    pub(crate) bloom_filter_apply_metrics: Option<BloomFilterIndexApplyMetrics>,
    /// Optional metrics for fulltext index applier.
    pub(crate) fulltext_index_apply_metrics: Option<FulltextIndexApplyMetrics>,

    /// Number of pruner builder cache hits.
    pub(crate) pruner_cache_hit: usize,
    /// Number of pruner builder cache misses.
    pub(crate) pruner_cache_miss: usize,
    /// Duration spent waiting for pruner to build file ranges.
    pub(crate) pruner_prune_cost: Duration,
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
        self.rows_vector_selected += other.rows_vector_selected;
        self.rows_precise_filtered += other.rows_precise_filtered;

        self.fulltext_index_cache_hit += other.fulltext_index_cache_hit;
        self.fulltext_index_cache_miss += other.fulltext_index_cache_miss;
        self.inverted_index_cache_hit += other.inverted_index_cache_hit;
        self.inverted_index_cache_miss += other.inverted_index_cache_miss;
        self.bloom_filter_cache_hit += other.bloom_filter_cache_hit;
        self.bloom_filter_cache_miss += other.bloom_filter_cache_miss;
        self.minmax_cache_hit += other.minmax_cache_hit;
        self.minmax_cache_miss += other.minmax_cache_miss;

        self.pruner_cache_hit += other.pruner_cache_hit;
        self.pruner_cache_miss += other.pruner_cache_miss;
        self.pruner_prune_cost += other.pruner_prune_cost;

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
mod vector_index_tests {
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
    /// Memory size of metadata loaded for building file ranges.
    pub(crate) metadata_mem_size: isize,
    /// Number of file range builders created.
    pub(crate) num_range_builders: isize,
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
        self.metadata_mem_size += other.metadata_mem_size;
        self.num_range_builders += other.num_range_builders;
    }

    /// Reports total rows.
    pub(crate) fn observe_rows(&self, read_type: &str) {
        READ_ROWS_TOTAL
            .with_label_values(&[read_type])
            .inc_by(self.num_rows as u64);
    }
}

/// Builder to build a [ParquetRecordBatchStream] for a row group.
pub(crate) struct RowGroupReaderBuilder {
    /// SST file to read.
    ///
    /// Holds the file handle to avoid the file purge it.
    file_handle: FileHandle,
    /// Path of the file.
    file_path: String,
    /// Metadata of the parquet file.
    parquet_meta: Arc<ParquetMetaData>,
    /// Arrow reader metadata for building async stream.
    arrow_metadata: ArrowReaderMetadata,
    /// Projected output schema aligned with `projection.projected_root_presence`.
    output_schema: SchemaRef,
    /// Object store as an Operator.
    object_store: ObjectStore,
    /// Projection mask.
    projection: ProjectionMaskPlan,
    /// Cache.
    cache_strategy: CacheStrategy,
    /// Pre-built prefilter state. `None` if prefiltering is not applicable.
    prefilter_builder: Option<PrefilterContextBuilder>,
}

/// Context passed to [RowGroupReaderBuilder::build()] carrying all information
/// needed for prefiltering decisions.
pub(crate) struct RowGroupBuildContext<'a> {
    /// Index of the row group to read.
    pub(crate) row_group_idx: usize,
    /// Row selection for the row group. `None` means all rows.
    pub(crate) row_selection: Option<RowSelection>,
    /// Metrics for tracking fetch operations.
    pub(crate) fetch_metrics: Option<&'a ParquetFetchMetrics>,
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

    pub(crate) fn has_predicate_prefilter(&self) -> bool {
        self.prefilter_builder.is_some()
    }

    /// Builds a [ParquetRecordBatchStream] to read the row group at `row_group_idx`.
    ///
    /// If prefiltering is applicable (based on `build_ctx`), this performs a two-phase read:
    /// 1. Reads only the prefilter columns (e.g. PK column), applies filters to get a refined row selection
    /// 2. Reads the full projection with the refined row selection
    pub(crate) async fn build(
        &self,
        build_ctx: RowGroupBuildContext<'_>,
    ) -> Result<ProjectedRecordBatchStream> {
        let prefilter_ctx = self.prefilter_builder.as_ref().map(|b| b.build());

        let Some(mut prefilter_ctx) = prefilter_ctx else {
            // No prefilter applicable, build stream with full projection.
            let stream = self
                .build_with_projection(
                    build_ctx.row_group_idx,
                    build_ctx.row_selection,
                    self.projection.mask.clone(),
                    build_ctx.fetch_metrics,
                )
                .await?;
            return self.make_projected_stream(stream);
        };

        let prefilter_start = Instant::now();
        let prefilter_result = execute_prefilter(&mut prefilter_ctx, self, &build_ctx).await?;
        if let Some(metrics) = build_ctx.fetch_metrics {
            let mut data = metrics.data.lock().unwrap();
            data.prefilter_cost += prefilter_start.elapsed();
            data.prefilter_filtered_rows += prefilter_result.filtered_rows;
        }

        let refined_selection = Some(prefilter_result.refined_selection);

        let stream = self
            .build_with_projection(
                build_ctx.row_group_idx,
                refined_selection,
                self.projection.mask.clone(),
                build_ctx.fetch_metrics,
            )
            .await?;
        self.make_projected_stream(stream)
    }

    fn make_projected_stream(
        &self,
        stream: ParquetRecordBatchStream<SstAsyncFileReader>,
    ) -> Result<ProjectedRecordBatchStream> {
        let stream = ParquetErrorAdapter::new(stream, self.file_path.clone());
        ProjectedRecordBatchStream::new(
            stream,
            self.projection.projected_root_presence.clone(),
            self.output_schema.clone(),
        )
    }

    /// Builds a [ParquetRecordBatchStream] with a custom projection mask.
    pub(crate) async fn build_with_projection(
        &self,
        row_group_idx: usize,
        row_selection: Option<RowSelection>,
        projection: ProjectionMask,
        fetch_metrics: Option<&ParquetFetchMetrics>,
    ) -> Result<ParquetRecordBatchStream<SstAsyncFileReader>> {
        // Create async file reader with caching support.
        let async_reader = SstAsyncFileReader::new(
            self.file_handle.file_id(),
            self.file_path.clone(),
            self.object_store.clone(),
            self.cache_strategy.clone(),
            self.parquet_meta.clone(),
            row_group_idx,
        )
        .with_fetch_metrics(fetch_metrics.cloned());

        // Build the async stream using ArrowReaderBuilder API.
        let mut builder = ParquetRecordBatchStreamBuilder::new_with_metadata(
            async_reader,
            self.arrow_metadata.clone(),
        );
        builder = builder
            .with_row_groups(vec![row_group_idx])
            .with_projection(projection)
            .with_batch_size(DEFAULT_READ_BATCH_SIZE);

        if let Some(selection) = row_selection {
            builder = builder.with_row_selection(selection);
        }

        let stream = builder.build().context(ReadParquetSnafu {
            path: &self.file_path,
        })?;

        Ok(stream)
    }
}

#[derive(Clone)]
/// The filter to evaluate or the prune result of the default value.
pub(crate) enum MaybeFilter {
    /// The filter to evaluate.
    Filter(SimpleFilterEvaluator),
    /// The filter matches the default value.
    Matched,
    /// The filter is pruned.
    Pruned,
}

impl MaybeFilter {
    /// Returns the inner filter when it is available.
    pub(crate) fn as_filter(&self) -> Option<&SimpleFilterEvaluator> {
        match self {
            MaybeFilter::Filter(filter) => Some(filter),
            MaybeFilter::Matched | MaybeFilter::Pruned => None,
        }
    }
}

#[derive(Clone)]
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

/// Context to evaluate a physical expression for a parquet file.
#[derive(Clone)]
pub(crate) struct PhysicalFilterContext {
    /// Filter to evaluate.
    filter: Arc<dyn PhysicalExpr>,
    /// Id of the column to evaluate.
    column_id: ColumnId,
    /// Name of the column to evaluate.
    column_name: String,
    /// Semantic type of the column.
    semantic_type: SemanticType,
    /// Schema containing only the referenced column.
    schema: SchemaRef,
}

impl PhysicalFilterContext {
    /// Creates a context for the `expr`.
    ///
    /// Returns None if the expression doesn't reference exactly one column or the
    /// column to filter doesn't exist in the SST metadata or the expected metadata.
    pub(crate) fn new_opt(
        sst_meta: &RegionMetadataRef,
        expected_meta: Option<&RegionMetadata>,
        read_format: &FlatReadFormat,
        expr: &Expr,
    ) -> Option<Self> {
        if !Self::is_prefilter_candidate(expr) {
            return None;
        }
        let column_name = Self::single_column_name(expr)?;
        let column_metadata = match expected_meta {
            Some(meta) => {
                let column = meta.column_by_name(&column_name)?;
                let sst_column = sst_meta.column_by_id(column.column_id)?;
                // Physical expr requires the column name to match the SST column name.
                if sst_column.column_schema.name != column_name {
                    return None;
                }
                column
            }
            None => sst_meta.column_by_name(&column_name)?,
        };

        // The column must be present in the projected arrow schema for the
        // prefilter to be able to read it.
        let (_, field) = read_format.arrow_schema().column_with_name(&column_name)?;
        let field = field.clone();
        let schema = Arc::new(ArrowSchema::new(vec![field]));
        let physical_expr = Predicate::to_physical_expr(expr, &schema)
            .inspect_err(|e| {
                error!(e; "Unable to build physical filter for {expr}, schema: {schema:?}");
            })
            .ok()?;

        Some(Self {
            filter: physical_expr,
            column_id: column_metadata.column_id,
            column_name,
            semantic_type: column_metadata.semantic_type,
            schema,
        })
    }

    /// Returns true if the expression is a variant we want to evaluate as a
    /// physical prefilter. Binary exprs are intentionally excluded because
    /// [`SimpleFilterEvaluator`] already handles them.
    // TODO(yingwen): extend more expressions if necessary. For example, allow some cheap scalar functions (e.g. `lower`, `length`, date truncations)
    fn is_prefilter_candidate(expr: &Expr) -> bool {
        matches!(
            expr,
            Expr::InList(_) | Expr::IsNull(_) | Expr::IsNotNull(_) | Expr::Between(_)
        )
    }

    fn single_column_name(expr: &Expr) -> Option<String> {
        let mut columns = HashSet::new();
        if expr_to_columns(expr, &mut columns).is_err() {
            return None;
        }
        if columns.len() != 1 {
            return None;
        }
        columns.iter().next().map(|column| column.name.clone())
    }

    /// Returns the filter to evaluate.
    pub(crate) fn filter(&self) -> &Arc<dyn PhysicalExpr> {
        &self.filter
    }

    /// Returns the column id.
    pub(crate) fn column_id(&self) -> ColumnId {
        self.column_id
    }

    /// Returns the column name.
    pub(crate) fn column_name(&self) -> &str {
        &self.column_name
    }

    /// Returns the semantic type of the column.
    pub(crate) fn semantic_type(&self) -> SemanticType {
        self.semantic_type
    }

    /// Returns the schema containing only the referenced column.
    pub(crate) fn schema(&self) -> &SchemaRef {
        &self.schema
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
    reader: Option<FlatPruneReader>,
    /// Metrics for tracking row group fetch operations.
    fetch_metrics: ParquetFetchMetrics,
}

impl ParquetReader {
    #[tracing::instrument(
        skip_all,
        fields(
            region_id = %self.context.reader_builder().file_handle.region_id(),
            file_id = %self.context.reader_builder().file_handle.file_id()
        )
    )]
    pub async fn next_record_batch(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            if let Some(reader) = &mut self.reader {
                if let Some(batch) = reader.next_batch().await? {
                    return Ok(Some(batch));
                }
                self.reader = None;
                continue;
            }

            let Some((row_group_idx, row_selection)) = self.selection.pop_first() else {
                return Ok(None);
            };

            let skip_fields = self.context.pre_filter_mode().skip_fields();
            let parquet_reader = self
                .context
                .reader_builder()
                .build(self.context.build_context(
                    row_group_idx,
                    Some(row_selection),
                    Some(&self.fetch_metrics),
                ))
                .await?;
            self.reader = Some(FlatPruneReader::new_with_row_group_reader(
                self.context.clone(),
                FlatRowGroupReader::new(self.context.clone(), parquet_reader),
                skip_fields,
            ));
        }
    }
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
        let reader = if let Some((row_group_idx, row_selection)) = selection.pop_first() {
            let skip_fields = context.pre_filter_mode().skip_fields();
            let parquet_reader = context
                .reader_builder()
                .build(context.build_context(
                    row_group_idx,
                    Some(row_selection),
                    Some(&fetch_metrics),
                ))
                .await?;
            Some(FlatPruneReader::new_with_row_group_reader(
                context.clone(),
                FlatRowGroupReader::new(context.clone(), parquet_reader),
                skip_fields,
            ))
        } else {
            None
        };

        Ok(ParquetReader {
            context,
            selection,
            reader,
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

/// Reader to read a row group of a parquet file in flat format, returning RecordBatch.
pub(crate) struct FlatRowGroupReader {
    /// Context for file ranges.
    context: FileRangeContextRef,
    /// Inner parquet record batch stream.
    stream: ProjectedRecordBatchStream,
    /// Cached sequence array to override sequences.
    override_sequence: Option<ArrayRef>,
}

impl FlatRowGroupReader {
    /// Creates a new flat reader from file range.
    pub(crate) fn new(context: FileRangeContextRef, stream: ProjectedRecordBatchStream) -> Self {
        // The batch length from the reader should be less than or equal to DEFAULT_READ_BATCH_SIZE.
        let override_sequence = context
            .read_format()
            .new_override_sequence_array(DEFAULT_READ_BATCH_SIZE);

        Self {
            context,
            stream,
            override_sequence,
        }
    }

    /// Returns the next RecordBatch.
    pub(crate) async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        match self.stream.next().await {
            Some(batch_result) => {
                let record_batch = batch_result?;

                let record_batch = self
                    .context
                    .read_format()
                    .convert_batch(record_batch, self.override_sequence.as_ref())?;
                Ok(Some(record_batch))
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::fmt::{Debug, Formatter};
    use std::sync::{Arc, LazyLock};

    use datafusion::arrow::datatypes::DataType;
    use datafusion_common::ScalarValue;
    use datafusion_expr::expr::ScalarFunction;
    use datafusion_expr::{
        ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
        col, lit,
    };
    use datatypes::arrow::array::{ArrayRef, Int64Array};
    use datatypes::arrow::record_batch::RecordBatch;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use object_store::services::Memory;
    use parquet::arrow::ArrowWriter;
    use store_api::metadata::{ColumnMetadata, RegionMetadata, RegionMetadataBuilder};
    use store_api::region_request::PathType;
    use table::predicate::Predicate;

    use super::*;
    use crate::sst::parquet::metadata::MetadataLoader;
    use crate::test_util::sst_util::{sst_file_handle, sst_region_metadata};

    #[tokio::test(flavor = "current_thread")]
    async fn test_minmax_predicate_key_not_built_when_index_result_cache_disabled() {
        #[derive(Eq, PartialEq, Hash)]
        struct PanicDebugUdf;

        impl Debug for PanicDebugUdf {
            fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
                panic!("minmax predicate key should not format exprs when cache is disabled");
            }
        }

        impl ScalarUDFImpl for PanicDebugUdf {
            fn as_any(&self) -> &dyn Any {
                self
            }

            fn name(&self) -> &str {
                "panic_debug_udf"
            }

            fn signature(&self) -> &Signature {
                static SIGNATURE: LazyLock<Signature> =
                    LazyLock::new(|| Signature::variadic_any(Volatility::Immutable));
                &SIGNATURE
            }

            fn return_type(&self, _arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
                Ok(DataType::Int64)
            }

            fn invoke_with_args(
                &self,
                _args: ScalarFunctionArgs,
            ) -> datafusion_common::Result<ColumnarValue> {
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(1))))
            }
        }

        let object_store = ObjectStore::new(Memory::default()).unwrap().finish();
        let file_handle = sst_file_handle(0, 1);
        let table_dir = "test_table".to_string();
        let path_type = PathType::Bare;
        let file_path = file_handle.file_path(&table_dir, path_type);

        let col = Arc::new(Int64Array::from_iter_values([1, 2, 3])) as ArrayRef;
        let batch = RecordBatch::try_from_iter([("col", col)]).unwrap();
        let mut parquet_bytes = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut parquet_bytes, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let file_size = parquet_bytes.len() as u64;
        object_store.write(&file_path, parquet_bytes).await.unwrap();

        let region_metadata: RegionMetadataRef = Arc::new(sst_region_metadata());
        let read_format = FlatReadFormat::new(
            region_metadata.clone(),
            region_metadata
                .column_metadatas
                .iter()
                .map(|column| column.column_id),
            None,
            &file_path,
            false,
        )
        .unwrap();

        let mut cache_metrics = MetadataCacheMetrics::default();
        let loader = MetadataLoader::new(object_store.clone(), &file_path, file_size);
        let parquet_meta = loader.load(&mut cache_metrics).await.unwrap();

        let udf = Arc::new(ScalarUDF::new_from_impl(PanicDebugUdf));
        let predicate = Predicate::new(vec![Expr::ScalarFunction(ScalarFunction::new_udf(
            udf,
            vec![],
        ))]);
        let builder = ParquetReaderBuilder::new(table_dir, path_type, file_handle, object_store)
            .predicate(Some(predicate))
            .cache(CacheStrategy::Disabled);

        let row_group_size = parquet_meta.row_group(0).num_rows() as usize;
        let total_row_count = parquet_meta.file_metadata().num_rows() as usize;
        let mut metrics = ReaderFilterMetrics::default();
        let selection = builder.row_groups_by_minmax(
            &read_format,
            &parquet_meta,
            row_group_size,
            total_row_count,
            &mut metrics,
            false,
        );

        assert!(!selection.is_empty());
    }

    fn expected_metadata_with_reused_tag_name(
        old_metadata: &RegionMetadata,
    ) -> Arc<RegionMetadata> {
        let mut builder = RegionMetadataBuilder::new(old_metadata.region_id);
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "tag_0".to_string(),
                    ConcreteDataType::string_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Tag,
                column_id: 10,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "tag_1".to_string(),
                    ConcreteDataType::string_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Tag,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "field_0".to_string(),
                    ConcreteDataType::uint64_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Field,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts".to_string(),
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 3,
            })
            .primary_key(vec![10, 1]);

        Arc::new(builder.build().unwrap())
    }

    #[test]
    fn test_simple_filter_context_uses_default_value_for_mismatched_expected_metadata() {
        let metadata: RegionMetadataRef = Arc::new(sst_region_metadata());
        let expected_metadata = expected_metadata_with_reused_tag_name(metadata.as_ref());
        let ctx = SimpleFilterContext::new_opt(
            &metadata,
            Some(expected_metadata.as_ref()),
            &col("tag_0").eq(lit("a")),
        )
        .unwrap();
        assert!(matches!(
            ctx.filter(),
            MaybeFilter::Matched | MaybeFilter::Pruned
        ));
    }

    #[test]
    fn test_physical_filter_context_skips_renamed_column() {
        let metadata: RegionMetadataRef = Arc::new(sst_region_metadata());
        let expected_metadata = expected_metadata_with_reused_tag_name(metadata.as_ref());
        let read_format = FlatReadFormat::new(
            metadata.clone(),
            metadata.column_metadatas.iter().map(|c| c.column_id),
            None,
            "test",
            true,
        )
        .unwrap();

        let ctx = PhysicalFilterContext::new_opt(
            &metadata,
            Some(expected_metadata.as_ref()),
            &read_format,
            &col("tag_0").in_list(vec![lit("a"), lit("b")], false),
        );

        assert!(ctx.is_none());
    }

    #[test]
    fn test_physical_filter_context_only_accepts_prefilter_candidates() {
        let metadata: RegionMetadataRef = Arc::new(sst_region_metadata());
        let read_format = FlatReadFormat::new(
            metadata.clone(),
            metadata.column_metadatas.iter().map(|c| c.column_id),
            None,
            "test",
            true,
        )
        .unwrap();

        // InList is on the allowlist — should build a context.
        let in_list = col("tag_0").in_list(vec![lit("a"), lit("b")], false);
        assert!(PhysicalFilterContext::new_opt(&metadata, None, &read_format, &in_list).is_some());

        // NOT IN uses the same variant with `negated: true` — also accepted.
        let not_in = col("tag_0").in_list(vec![lit("a"), lit("b")], true);
        assert!(PhysicalFilterContext::new_opt(&metadata, None, &read_format, &not_in).is_some());

        // IS NULL / IS NOT NULL are accepted.
        let is_null = col("tag_0").is_null();
        assert!(PhysicalFilterContext::new_opt(&metadata, None, &read_format, &is_null).is_some());
        let is_not_null = col("tag_0").is_not_null();
        assert!(
            PhysicalFilterContext::new_opt(&metadata, None, &read_format, &is_not_null).is_some()
        );

        // BETWEEN is accepted.
        let between = col("field_0").between(lit(1_u64), lit(10_u64));
        assert!(PhysicalFilterContext::new_opt(&metadata, None, &read_format, &between).is_some());

        // Binary expr is handled by SimpleFilterEvaluator — rejected here.
        let binary = col("tag_0").eq(lit("a"));
        assert!(PhysicalFilterContext::new_opt(&metadata, None, &read_format, &binary).is_none());
    }
}
