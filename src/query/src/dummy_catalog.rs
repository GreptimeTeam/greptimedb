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

//! Dummy catalog for region server.

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use common_recordbatch::DfSendableRecordBatchStream;
use datafusion::execution::context::TaskContext;
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};

use api::v1::SemanticType;
use async_trait::async_trait;
use catalog::error::Result as CatalogResult;
use catalog::{CatalogManager, CatalogManagerRef};
use common_query::request::RAW_SERIES_READ_EXTENSION_KEY;
use common_query::series_frame::{pack_raw_series, unpack_raw_series};
use common_recordbatch::OrderOption;
use common_recordbatch::filter::SimpleFilterEvaluator;
use datafusion::catalog::{CatalogProvider, CatalogProviderList, SchemaProvider, Session};
use datafusion::datasource::TableProvider;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_common::DataFusionError;
use datafusion_expr::{Expr, TableProviderFilterPushDown, TableType};
use datatypes::arrow::datatypes::SchemaRef;
use datatypes::data_type::DataType;
use datatypes::types::json_type::JsonNativeType;
use futures::stream::BoxStream;
use session::context::{QueryContext, QueryContextRef};
use snafu::ResultExt;
use store_api::metadata::RegionMetadataRef;
use store_api::region_engine::RegionEngineRef;
use store_api::storage::{RegionId, ScanRequest, TimeSeriesDistribution, TimeSeriesRowSelector};
use table::TableRef;
use table::metadata::{TableId, TableInfoRef};
use table::table::adapter::{dictionary_encode_string_columns, supports_pk_dictionary_encoding};
use table::table::scan::RegionScanExec;

use crate::error::{GetRegionMetadataSnafu, Result};
use crate::options::{FlowIncrementalMode, FlowQueryExtensions};

/// One ordered frame stream over parallel raw-series scan partitions.
#[derive(Debug)]
struct MergedSeriesScanExec {
    input: Arc<table::table::scan::SeriesScanExec>,
    properties: Arc<PlanProperties>,
}

impl MergedSeriesScanExec {
    fn new(input: Arc<table::table::scan::SeriesScanExec>) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(input.schema()),
            Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        ));
        Self { input, properties }
    }
}

impl DisplayAs for MergedSeriesScanExec {
    fn fmt_as(&self, _format: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MergedSeriesScanExec: partitions=1")
    }
}

impl ExecutionPlan for MergedSeriesScanExec {
    fn name(&self) -> &str {
        "MergedSeriesScanExec"
    }
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &Arc<dyn datafusion_physical_expr::PhysicalExpr>,
        ) -> datafusion_common::Result<
            datafusion_common::tree_node::TreeNodeRecursion,
        >,
    ) -> datafusion_common::Result<datafusion_common::tree_node::TreeNodeRecursion> {
        Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn metrics(&self) -> Option<MetricsSet> {
        self.input.metrics()
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::error::Result<DfSendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "partition {partition} out of bounds for 1"
            )));
        }
        let count = self
            .input
            .properties()
            .output_partitioning()
            .partition_count();
        let schema = self.input.series_schema().clone();
        let streams = (0..count)
            .map(|p| {
                let batches = self.input.execute(p, context.clone())?;
                unpack_raw_series(batches, Some(schema.clone()))
            })
            .collect::<datafusion::error::Result<Vec<_>>>()?;
        let merged = promql::source::merge_ordered_raw_series(streams)
            .map_err(|e| DataFusionError::External(e.into_inner()))?;
        pack_raw_series(merged)
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Plan(
                "MergedSeriesScanExec is a leaf".into(),
            ));
        }
        Ok(self)
    }
    fn reset_state(self: Arc<Self>) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.input.clone().reset_state()?;
        Ok(self)
    }
}

/// Resolve to the given region (specified by [RegionId]) unconditionally.
#[derive(Clone, Debug)]
pub struct DummyCatalogList {
    catalog: DummyCatalogProvider,
}

impl DummyCatalogList {
    /// Creates a new catalog list with the given table provider.
    pub fn with_table_provider(table_provider: Arc<dyn TableProvider>) -> Self {
        let schema_provider = DummySchemaProvider {
            table: table_provider,
        };
        let catalog_provider = DummyCatalogProvider {
            schema: schema_provider,
        };
        Self {
            catalog: catalog_provider,
        }
    }
}

impl CatalogProviderList for DummyCatalogList {
    fn register_catalog(
        &self,
        _name: String,
        _catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        None
    }

    fn catalog_names(&self) -> Vec<String> {
        vec![]
    }

    fn catalog(&self, _name: &str) -> Option<Arc<dyn CatalogProvider>> {
        Some(Arc::new(self.catalog.clone()))
    }
}

/// A dummy catalog provider for [DummyCatalogList].
#[derive(Clone, Debug)]
struct DummyCatalogProvider {
    schema: DummySchemaProvider,
}

impl CatalogProvider for DummyCatalogProvider {
    fn schema_names(&self) -> Vec<String> {
        vec![]
    }

    fn schema(&self, _name: &str) -> Option<Arc<dyn SchemaProvider>> {
        Some(Arc::new(self.schema.clone()))
    }
}

/// A dummy schema provider for [DummyCatalogList].
#[derive(Clone, Debug)]
struct DummySchemaProvider {
    table: Arc<dyn TableProvider>,
}

#[async_trait]
impl SchemaProvider for DummySchemaProvider {
    fn table_names(&self) -> Vec<String> {
        vec![]
    }

    async fn table(
        &self,
        _name: &str,
    ) -> datafusion::error::Result<Option<Arc<dyn TableProvider>>> {
        Ok(Some(self.table.clone()))
    }

    fn table_exist(&self, _name: &str) -> bool {
        true
    }
}

/// For [TableProvider] and [DummyCatalogList]
#[derive(Clone)]
pub struct DummyTableProvider {
    region_id: RegionId,
    engine: RegionEngineRef,
    metadata: RegionMetadataRef,
    /// Keeping a mutable request makes it possible to change in the optimize phase.
    scan_request: Arc<Mutex<ScanRequest>>,
    query_ctx: Option<QueryContextRef>,
}

impl fmt::Debug for DummyTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DummyTableProvider")
            .field("region_id", &self.region_id)
            .field("metadata", &self.metadata)
            .field("scan_request", &self.scan_request)
            .finish()
    }
}

#[async_trait]
impl TableProvider for DummyTableProvider {
    fn schema(&self) -> SchemaRef {
        let schema = self.metadata.schema.arrow_schema();
        if !supports_pk_dictionary_encoding(self.engine.name()) {
            return schema.clone();
        }
        dictionary_encode_string_columns(schema, |index| {
            self.metadata.column_metadatas[index].semantic_type == SemanticType::Tag
        })
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let mut request = self.scan_request.lock().unwrap().clone();
        request.projection = projection.cloned();
        request.filters = filters.to_vec();
        request.limit = limit;
        // Distribution selects Mito's series scanner, so set it before `handle_query`.
        if self.raw_series_metric().is_some() {
            request.distribution = Some(TimeSeriesDistribution::PerSeries);
        }
        if let Some(query_ctx) = &self.query_ctx {
            let is_sink_scan = is_sink_scan(query_ctx, self.region_id)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            apply_cached_snapshot_to_request(query_ctx, self.region_id, is_sink_scan, &mut request);
        }

        let scanner = self
            .engine
            .handle_query(self.region_id, request.clone())
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        if request.snapshot_on_scan
            && let Some(query_ctx) = &self.query_ctx
            && let Some(snapshot_sequence) = scanner.snapshot_sequence()
        {
            bind_snapshot_bound_region_seq(query_ctx, self.region_id, snapshot_sequence)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        }

        let query_memory_tracker = self.engine.query_memory_tracker();
        let mut scan_exec = RegionScanExec::new(scanner, request, query_memory_tracker)?;
        if let Some(query_ctx) = &self.query_ctx {
            scan_exec.set_explain_verbose(query_ctx.explain_verbose());
        }
        // The only guard that keeps an ordinary read ordinary: a request that does not name a
        // metric for raw frames leaves the region's table provider without this key, and the leaf
        // below is then exactly the `RegionScanExec` the region has always built.
        let Some(metric_name) = self.raw_series_metric() else {
            return Ok(Arc::new(scan_exec));
        };
        self.raw_series_leaf(
            scan_exec,
            projection,
            limit,
            &metric_name,
            state.config_options().execution.target_partitions,
        )
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        let supported = filters
            .iter()
            .map(|e| {
                // Simple filter on primary key columns are precisely evaluated.
                if let Some(simple_filter) =
                    SimpleFilterEvaluator::try_new_with_column_type(e, &|name| {
                        self.metadata
                            .column_by_name(name)
                            .map(|column| column.column_schema.data_type.as_arrow_type())
                    })
                {
                    if self
                        .metadata
                        .column_by_name(simple_filter.column_name())
                        .and_then(|c| {
                            (c.semantic_type == SemanticType::Tag
                                || c.semantic_type == SemanticType::Timestamp)
                                .then_some(())
                        })
                        .is_some()
                    {
                        TableProviderFilterPushDown::Exact
                    } else {
                        TableProviderFilterPushDown::Inexact
                    }
                } else {
                    TableProviderFilterPushDown::Inexact
                }
            })
            .collect();
        Ok(supported)
    }
}

impl DummyTableProvider {
    /// Creates a new provider.
    pub fn new(region_id: RegionId, engine: RegionEngineRef, metadata: RegionMetadataRef) -> Self {
        let preserve_pk_dictionary_encoding = supports_pk_dictionary_encoding(engine.name());
        Self {
            region_id,
            engine,
            metadata,
            scan_request: Arc::new(Mutex::new(ScanRequest {
                preserve_pk_dictionary_encoding,
                ..Default::default()
            })),
            query_ctx: None,
        }
    }

    pub fn region_metadata(&self) -> RegionMetadataRef {
        self.metadata.clone()
    }

    /// Sets the ordering hint of the query to the provider.
    pub fn with_ordering_hint(&self, order_opts: &[OrderOption]) {
        self.scan_request.lock().unwrap().output_ordering = Some(order_opts.to_vec());
    }

    /// Sets the distribution hint of the query to the provider.
    pub fn with_distribution(&self, distribution: TimeSeriesDistribution) {
        self.scan_request.lock().unwrap().distribution = Some(distribution);
    }

    /// Sets the time series selector hint of the query to the provider.
    pub fn with_time_series_selector_hint(&self, selector: TimeSeriesRowSelector) {
        self.scan_request.lock().unwrap().series_row_selector = Some(selector);
    }

    /// Clones this provider for one logical table-scan use-site.
    ///
    /// The optimizer may attach different hints to scans that share the same
    /// catalog provider, so the per-scan request must not share its mutex.
    pub fn clone_for_scan(&self) -> Self {
        Self {
            scan_request: Arc::new(Mutex::new(self.scan_request.lock().unwrap().clone())),
            ..self.clone()
        }
    }

    pub fn with_sequence(&self, sequence: u64) {
        self.scan_request.lock().unwrap().memtable_max_sequence = Some(sequence);
    }

    pub(crate) fn with_json_type_hint(&self, hint: HashMap<String, JsonNativeType>) {
        self.scan_request.lock().unwrap().json_type_hint = hint;
    }

    /// Returns the metric this read should frame, when it asked for raw series frames.
    ///
    /// The region server writes this key on every read it handles, holding the metric derived from
    /// the plan's table scan for a read that opted into raw frames and an empty value otherwise.
    /// A read that does not name a metric — every read that existed before this option — keeps its
    /// ordinary rows.
    fn raw_series_metric(&self) -> Option<String> {
        self.query_ctx
            .as_ref()?
            .extension(RAW_SERIES_READ_EXTENSION_KEY)
            .filter(|metric| !metric.is_empty())
            .map(str::to_string)
    }

    /// Selects a raw-series leaf for a read that asked for frames, preparing the scan first.
    ///
    /// The scanner exists only after `handle_query`, and [`RegionScanExec::with_new_partitions`]
    /// *returns* the prepared exec the frame leaf then owns, so preparation and leaf selection
    /// cannot be split: there is no scanner to prepare before this point, and the leaf refuses an
    /// unprepared scan because its label binding resolves against prepared partition ranges.
    ///
    /// The prepared scan retains its N assigned partitions. The named raw-series conversion binds
    /// against the scanner's metadata snapshot, and [`MergedSeriesScanExec`] restores ordered raw
    /// series at the single-stream region-read boundary.
    ///
    /// This read asked for frames and is answered with frames or with an error naming why it could
    /// not be: it never silently receives rows. Answering with rows would be a different answer to
    /// a question that was asked exactly once, over a stream whose shape the caller's plan was
    /// built around; the caller could compare schemas to notice, but "looks like a frame" is not
    /// the same as "is the answer I asked for", and a caller reading a frame leaf gets no chance to
    /// ask again once operators above the scan have been planned over one shape. A named error is
    /// actionable, and the read can be retried as an ordinary one by clearing the request's
    /// [`RegionReadKind`](common_query::request::RegionReadKind).
    fn raw_series_leaf(
        &self,
        scan_exec: RegionScanExec,
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
        metric_name: &str,
        target_partitions: usize,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let region_id = self.region_id;
        if limit.is_some() {
            return Err(DataFusionError::NotImplemented(format!(
                "region {region_id} cannot answer a raw series request: a row limit cannot be \
                 applied to a raw series frame"
            )));
        }
        let field_names = self.raw_series_field_names(projection).ok_or_else(|| {
            DataFusionError::NotImplemented(format!(
                "region {region_id} cannot answer a raw series request: the scan projection selects \
                 a column that is not a raw series field"
            ))
        })?;
        let logical_tags = self.raw_series_logical_tags();
        let ranges = scan_exec.get_partition_ranges();
        // Keep scan partitions parallel; MergedSeriesScanExec restores the frame contract
        // (KeyThenTime and series contiguity) at the single-stream region-read boundary.
        let target = if target_partitions > 0 {
            target_partitions
        } else {
            std::thread::available_parallelism().map_or(1, usize::from)
        }
        .min(ranges.len().max(1));
        let partitions =
            crate::optimizer::parallelize_scan::ParallelizeScan::assign_partition_range(
                ranges, target,
            );
        let prepared = Arc::new(
            scan_exec
                .with_new_partitions(partitions, target)
                .map_err(|e| DataFusionError::External(e.into_inner()))?,
        );
        let frames = prepared
            .clone()
            .try_to_named_series_scan(metric_name, &logical_tags, &field_names)?
            .ok_or_else(|| {
                DataFusionError::NotImplemented(format!(
                    "region {region_id} cannot answer a raw series request for metric \
                     `{metric_name}`: engine `{}` declined to serve raw series from the prepared \
                     scan (raw series unsupported, dynamic filters, or requested names that do not \
                     bind)",
                    self.engine.name(),
                ))
            })?;
        Ok(Arc::new(MergedSeriesScanExec::new(frames)))
    }

    /// Returns this region's own tag columns as the frames' logical labels.
    ///
    /// A region describes the labels it stores, so nothing about them is carried by the request.
    /// Internal and reserved columns are storage bookkeeping rather than labels, and the metric
    /// name travels in the frame's label decode description instead of as a label column.
    fn raw_series_logical_tags(&self) -> Vec<String> {
        self.metadata
            .column_metadatas
            .iter()
            .filter(|column| column.semantic_type == SemanticType::Tag)
            .filter(|column| {
                let name = column.column_schema.name.as_str();
                name != "__name__"
                    && !store_api::storage::consts::is_internal_column(name)
                    && !store_api::metric_engine_consts::is_metric_engine_internal_column(name)
            })
            .map(|column| column.column_schema.name.clone())
            .collect()
    }

    /// Returns the projected field columns as the frames' ordered payload fields.
    ///
    /// The projection is already part of the request, so no field name is carried either. `None`
    /// means the projection selects a column a frame cannot carry as a field — an internal column
    /// or an index outside the region schema — which the caller turns into a named error rather
    /// than answering with a different shape. Tags and the timestamp need no entry: a key-encoded
    /// frame describes both itself.
    fn raw_series_field_names(&self, projection: Option<&Vec<usize>>) -> Option<Vec<String>> {
        let columns = &self.metadata.column_metadatas;
        let indices = match projection {
            Some(projection) => projection.clone(),
            None => (0..columns.len()).collect::<Vec<_>>(),
        };
        let mut names = Vec::with_capacity(indices.len());
        for index in indices {
            let column = columns.get(index)?;
            match column.semantic_type {
                SemanticType::Tag | SemanticType::Timestamp => {}
                SemanticType::Field => {
                    let name = column.column_schema.name.as_str();
                    if store_api::storage::consts::is_internal_column(name)
                        || store_api::metric_engine_consts::is_metric_engine_internal_column(name)
                    {
                        return None;
                    }
                    names.push(column.column_schema.name.clone());
                }
            }
        }
        Some(names)
    }

    /// Gets the scan request of the provider.
    #[cfg(test)]
    pub fn scan_request(&self) -> ScanRequest {
        self.scan_request.lock().unwrap().clone()
    }
}

pub struct DummyTableProviderFactory;

impl DummyTableProviderFactory {
    pub async fn create_table_provider(
        &self,
        region_id: RegionId,
        engine: RegionEngineRef,
        query_ctx: Option<QueryContextRef>,
    ) -> Result<DummyTableProvider> {
        let metadata =
            engine
                .get_metadata(region_id)
                .await
                .with_context(|_| GetRegionMetadataSnafu {
                    engine: engine.name(),
                    region_id,
                })?;

        let mut scan_request = if let Some(ctx) = query_ctx.as_ref() {
            scan_request_from_query_context(region_id, ctx)?
        } else {
            ScanRequest::default()
        };
        scan_request.preserve_pk_dictionary_encoding =
            supports_pk_dictionary_encoding(engine.name());

        Ok(DummyTableProvider {
            region_id,
            engine,
            metadata,
            scan_request: Arc::new(Mutex::new(scan_request)),
            query_ctx,
        })
    }
}

fn scan_request_from_query_context(
    region_id: RegionId,
    query_ctx: &QueryContext,
) -> Result<ScanRequest> {
    let decision = decide_flow_scan(query_ctx, region_id)?;
    Ok(build_scan_request(query_ctx, region_id, &decision))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FlowScanDecision {
    /// Whether this region is the flow sink-table scan.
    /// Sink scans intentionally bypass incremental and snapshot-binding semantics.
    is_sink_scan: bool,
    /// Whether this scan should bind a memtable upper bound when opening the scan.
    /// This is only the initial intent; if a cached bound already exists in `query_ctx`,
    /// we reuse that cached bound instead and clear this flag.
    snapshot_on_scan: bool,
    /// Optional lower exclusive memtable sequence bound for incremental reads.
    /// When set, only rows with sequence strictly greater than this bound are read from memtables.
    memtable_min_sequence: Option<u64>,
    /// Optional cached per-region snapshot already bound in `query_ctx`.
    /// When present, this becomes the effective memtable upper bound and suppresses
    /// binding a new snapshot on scan open.
    memtable_max_sequence: Option<u64>,
    /// Whether to skip SST files for memtable-only incremental source scans.
    skip_sst_files: bool,
    /// Whether this source scan must enforce the exact sequence range.
    exact_sequence_range: bool,
}

impl FlowScanDecision {
    fn plain_scan() -> Self {
        Self {
            is_sink_scan: true,
            snapshot_on_scan: false,
            memtable_min_sequence: None,
            memtable_max_sequence: None,
            skip_sst_files: false,
            exact_sequence_range: false,
        }
    }
}

fn decide_flow_scan(query_ctx: &QueryContext, region_id: RegionId) -> Result<FlowScanDecision> {
    let Some(flow_extensions) =
        FlowQueryExtensions::parse_flow_extensions(&query_ctx.extensions())?
    else {
        return Ok(FlowScanDecision {
            is_sink_scan: false,
            snapshot_on_scan: false,
            memtable_min_sequence: None,
            memtable_max_sequence: query_ctx.get_snapshot(region_id.as_u64()),
            skip_sst_files: false,
            exact_sequence_range: false,
        });
    };

    // Sink-table scans intentionally bypass all flow scan semantics. They should
    // behave like plain reads and must not participate in incremental lower bounds
    // or per-region snapshot binding/reuse.
    if flow_extensions.sink_table_id == Some(region_id.table_id()) {
        return Ok(FlowScanDecision::plain_scan());
    }

    let apply_incremental = flow_extensions.validate_for_scan(region_id)?;

    let memtable_min_sequence = if apply_incremental {
        flow_extensions
            .incremental_after_seqs
            .as_ref()
            .and_then(|seqs| seqs.get(&region_id.as_u64()))
            .copied()
    } else {
        None
    };

    let memtable_max_sequence = query_ctx.get_snapshot(region_id.as_u64());

    // `skip_sst_files` is only valid for memtable-only incremental deltas,
    // identified by a lower checkpoint bound. A snapshot upper bound without an
    // incremental lower bound is a fenced full-snapshot read and must keep SSTs
    // in the scan so mito can reject stale upper bounds after H has been flushed
    // into SSTs, instead of silently bypassing the stale-fence check. If a future
    // incremental delta also carries an upper bound, the lower-bound stale check
    // still proves whether memtable-only is safe.
    let skip_sst_files = apply_incremental
        && memtable_min_sequence.is_some()
        && flow_extensions.incremental_mode == Some(FlowIncrementalMode::MemtableOnly);

    Ok(FlowScanDecision {
        is_sink_scan: false,
        snapshot_on_scan: memtable_max_sequence.is_none()
            && flow_extensions.should_collect_region_watermark(),
        memtable_min_sequence,
        memtable_max_sequence,
        skip_sst_files,
        exact_sequence_range: apply_incremental
            && flow_extensions.incremental_mode == Some(FlowIncrementalMode::SequenceRange),
    })
}

fn build_scan_request(
    query_ctx: &QueryContext,
    region_id: RegionId,
    decision: &FlowScanDecision,
) -> ScanRequest {
    // Build the initial scan request from the final decision known at provider creation
    // time. A later scan may still refresh `memtable_max_sequence` if another source scan
    // has bound a snapshot into `query_ctx` after this provider was created.
    ScanRequest {
        sst_min_sequence: (!decision.is_sink_scan && !decision.exact_sequence_range)
            .then(|| query_ctx.sst_min_sequence(region_id.as_u64()))
            .flatten(),
        skip_sst_files: decision.skip_sst_files,
        snapshot_on_scan: decision.snapshot_on_scan,
        exact_sequence_range: decision.exact_sequence_range,
        memtable_min_sequence: decision.memtable_min_sequence,
        memtable_max_sequence: decision.memtable_max_sequence,
        ..Default::default()
    }
}

fn is_sink_scan(query_ctx: &QueryContext, region_id: RegionId) -> Result<bool> {
    Ok(
        FlowQueryExtensions::parse_flow_extensions(&query_ctx.extensions())?
            .is_some_and(|exts| exts.sink_table_id == Some(region_id.table_id())),
    )
}

fn apply_cached_snapshot_to_request(
    query_ctx: &QueryContext,
    region_id: RegionId,
    is_sink_scan: bool,
    scan_request: &mut ScanRequest,
) {
    if is_sink_scan {
        return;
    }

    if let Some(snapshot_sequence) = query_ctx.get_snapshot(region_id.as_u64()) {
        // Reuse the previously bound per-region snapshot instead of rebinding a new
        // upper bound on scan open. This refresh is still needed at scan time because
        // the provider's cached request may have been built before another source scan
        // bound the shared query-level snapshot into `query_ctx`.
        scan_request.memtable_max_sequence = Some(snapshot_sequence);
        scan_request.snapshot_on_scan = false;
    }
}

fn bind_snapshot_bound_region_seq(
    query_ctx: &QueryContext,
    region_id: RegionId,
    snapshot_sequence: u64,
) -> Result<u64> {
    if let Some(existing) = query_ctx.get_snapshot(region_id.as_u64()) {
        if existing != snapshot_sequence {
            return crate::error::ConflictingSnapshotSequenceSnafu {
                region_id,
                existing,
                new: snapshot_sequence,
            }
            .fail();
        }
        Ok(existing)
    } else {
        query_ctx.set_snapshot(region_id.as_u64(), snapshot_sequence);
        Ok(snapshot_sequence)
    }
}

#[async_trait]
impl TableProviderFactory for DummyTableProviderFactory {
    async fn create(
        &self,
        region_id: RegionId,
        engine: RegionEngineRef,
        ctx: Option<QueryContextRef>,
    ) -> Result<Arc<dyn TableProvider>> {
        let provider = self.create_table_provider(region_id, engine, ctx).await?;
        Ok(Arc::new(provider))
    }
}

#[async_trait]
pub trait TableProviderFactory: Send + Sync {
    async fn create(
        &self,
        region_id: RegionId,
        engine: RegionEngineRef,
        ctx: Option<QueryContextRef>,
    ) -> Result<Arc<dyn TableProvider>>;
}

pub type TableProviderFactoryRef = Arc<dyn TableProviderFactory>;

/// A dummy catalog manager that always returns empty results.
///
/// Used to fill the arg of `QueryEngineFactory::new_with_plugins` in datanode.
pub struct DummyCatalogManager;

impl DummyCatalogManager {
    /// Returns a new `CatalogManagerRef` instance.
    pub fn arc() -> CatalogManagerRef {
        Arc::new(Self)
    }
}

#[async_trait::async_trait]
impl CatalogManager for DummyCatalogManager {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn catalog_names(&self) -> CatalogResult<Vec<String>> {
        Ok(vec![])
    }

    async fn schema_names(
        &self,
        _catalog: &str,
        _query_ctx: Option<&QueryContext>,
    ) -> CatalogResult<Vec<String>> {
        Ok(vec![])
    }

    async fn table_names(
        &self,
        _catalog: &str,
        _schema: &str,
        _query_ctx: Option<&QueryContext>,
    ) -> CatalogResult<Vec<String>> {
        Ok(vec![])
    }

    async fn catalog_exists(&self, _catalog: &str) -> CatalogResult<bool> {
        Ok(false)
    }

    async fn schema_exists(
        &self,
        _catalog: &str,
        _schema: &str,
        _query_ctx: Option<&QueryContext>,
    ) -> CatalogResult<bool> {
        Ok(false)
    }

    async fn table_exists(
        &self,
        _catalog: &str,
        _schema: &str,
        _table: &str,
        _query_ctx: Option<&QueryContext>,
    ) -> CatalogResult<bool> {
        Ok(false)
    }

    async fn table(
        &self,
        _catalog: &str,
        _schema: &str,
        _table_name: &str,
        _query_ctx: Option<&QueryContext>,
    ) -> CatalogResult<Option<TableRef>> {
        Ok(None)
    }

    async fn table_id(
        &self,
        _catalog: &str,
        _schema: &str,
        _table_name: &str,
        _query_ctx: Option<&QueryContext>,
    ) -> CatalogResult<Option<TableId>> {
        Ok(None)
    }

    async fn table_info_by_id(&self, _table_id: TableId) -> CatalogResult<Option<TableInfoRef>> {
        Ok(None)
    }

    async fn tables_by_ids(
        &self,
        _catalog: &str,
        _schema: &str,
        _table_ids: &[TableId],
    ) -> CatalogResult<Vec<TableRef>> {
        Ok(vec![])
    }

    fn tables<'a>(
        &'a self,
        _catalog: &'a str,
        _schema: &'a str,
        _query_ctx: Option<&'a QueryContext>,
    ) -> BoxStream<'a, CatalogResult<TableRef>> {
        Box::pin(futures::stream::empty())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};

    use common_error::ext::ErrorExt;
    use common_error::status_code::StatusCode;
    use session::context::QueryContextBuilder;

    use super::*;
    use crate::error::Error;
    use crate::options::{
        FLOW_INCREMENTAL_AFTER_SEQS, FLOW_INCREMENTAL_MODE, FLOW_INCREMENTAL_MODE_SEQUENCE_RANGE,
        FLOW_RETURN_REGION_SEQ, FLOW_SINK_TABLE_ID,
    };

    #[test]
    fn test_nullable_label_filter_pushdown() {
        use datafusion_expr::{Cast, col, lit};
        use datatypes::arrow::datatypes::DataType as ArrowDataType;

        let provider = crate::optimizer::test_util::mock_table_provider(RegionId::new(1, 1));
        let cast_label = Expr::Cast(Cast::new(Box::new(col("k0")), ArrowDataType::Utf8));
        let cast_field = Expr::Cast(Cast::new(Box::new(col("v0")), ArrowDataType::Utf8));
        let filters = [
            col("k0").is_null().or(col("k0").not_eq(lit("lo"))),
            col("k0").is_null().or(cast_label.not_eq(lit("lo"))),
            col("k0").is_null().or(col("v0").not_eq(lit(1.0))),
            cast_field.eq(lit("1")),
            col("k0").in_list(vec![lit(""), lit("lo")], false),
            col("k0").in_list(vec![lit(""), lit("lo")], true),
        ];
        assert_eq!(
            provider
                .supports_filters_pushdown(&filters.iter().collect::<Vec<_>>())
                .unwrap(),
            vec![
                TableProviderFilterPushDown::Exact,
                TableProviderFilterPushDown::Exact,
                TableProviderFilterPushDown::Inexact,
                TableProviderFilterPushDown::Inexact,
                TableProviderFilterPushDown::Inexact,
                TableProviderFilterPushDown::Inexact,
            ]
        );
    }

    fn test_region_id() -> RegionId {
        RegionId::new(1024, 1)
    }

    #[test]
    fn test_scan_request_from_query_context_uses_snapshot_bound_intent() {
        let region_id = test_region_id();
        let query_ctx = QueryContextBuilder::default()
            .extensions(HashMap::from([(
                "flow.return_region_seq".to_string(),
                "true".to_string(),
            )]))
            .snapshot_seqs(Arc::new(RwLock::new(HashMap::from([(
                region_id.as_u64(),
                42_u64,
            )]))))
            .sst_min_sequences(Arc::new(RwLock::new(HashMap::from([(
                region_id.as_u64(),
                7_u64,
            )]))))
            .build();

        let request = scan_request_from_query_context(region_id, &query_ctx).unwrap();

        assert!(!request.snapshot_on_scan);
        assert_eq!(request.memtable_max_sequence, Some(42));
        assert_eq!(request.sst_min_sequence, Some(7));
    }

    #[test]
    fn test_terminal_watermark_context_source_and_sink_scan_semantics() {
        let region_id = test_region_id();
        let query_ctx = QueryContextBuilder::default()
            .extensions(HashMap::from([(
                FLOW_RETURN_REGION_SEQ.to_string(),
                "true".to_string(),
            )]))
            .build();

        let request = scan_request_from_query_context(region_id, &query_ctx).unwrap();

        assert!(request.snapshot_on_scan);
        assert_eq!(request.memtable_min_sequence, None);
        assert_eq!(request.memtable_max_sequence, None);
        assert_eq!(request.sst_min_sequence, None);

        let query_ctx = QueryContextBuilder::default()
            .extensions(HashMap::from([
                (FLOW_RETURN_REGION_SEQ.to_string(), "true".to_string()),
                (
                    FLOW_SINK_TABLE_ID.to_string(),
                    region_id.table_id().to_string(),
                ),
            ]))
            .snapshot_seqs(Arc::new(RwLock::new(HashMap::from([(
                region_id.as_u64(),
                88_u64,
            )]))))
            .sst_min_sequences(Arc::new(RwLock::new(HashMap::from([(
                region_id.as_u64(),
                77_u64,
            )]))))
            .build();

        let request = scan_request_from_query_context(region_id, &query_ctx).unwrap();

        assert!(!request.snapshot_on_scan);
        assert_eq!(request.memtable_min_sequence, None);
        assert_eq!(request.memtable_max_sequence, None);
        assert_eq!(request.sst_min_sequence, None);
    }

    #[test]
    fn test_scan_request_from_incremental_context_uses_snapshot_bound_intent() {
        let region_id = test_region_id();
        let query_ctx = QueryContextBuilder::default()
            .extensions(HashMap::from([(
                "flow.incremental_after_seqs".to_string(),
                format!(r#"{{"{}":10}}"#, region_id.as_u64()),
            )]))
            .build();

        let request = scan_request_from_query_context(region_id, &query_ctx).unwrap();

        assert!(request.snapshot_on_scan);
        assert_eq!(request.memtable_min_sequence, Some(10));
        assert_eq!(request.memtable_max_sequence, None);
    }

    #[test]
    fn test_scan_request_from_sequence_range_context_uses_exact_source_scan() {
        let region_id = test_region_id();
        let query_ctx = QueryContextBuilder::default()
            .extensions(HashMap::from([
                (
                    FLOW_INCREMENTAL_MODE.to_string(),
                    FLOW_INCREMENTAL_MODE_SEQUENCE_RANGE.to_string(),
                ),
                (
                    FLOW_INCREMENTAL_AFTER_SEQS.to_string(),
                    format!(r#"{{"{}":10}}"#, region_id.as_u64()),
                ),
            ]))
            .snapshot_seqs(Arc::new(RwLock::new(HashMap::from([(
                region_id.as_u64(),
                42_u64,
            )]))))
            .sst_min_sequences(Arc::new(RwLock::new(HashMap::from([(
                region_id.as_u64(),
                7_u64,
            )]))))
            .build();

        let request = scan_request_from_query_context(region_id, &query_ctx).unwrap();

        assert!(request.exact_sequence_range);
        assert!(!request.skip_sst_files);
        assert_eq!(request.memtable_min_sequence, Some(10));
        assert_eq!(request.memtable_max_sequence, Some(42));
        assert_eq!(request.sst_min_sequence, None);
    }

    #[test]
    fn test_scan_request_from_sequence_range_context_binds_snapshot_on_scan() {
        let region_id = test_region_id();
        let query_ctx = QueryContextBuilder::default()
            .extensions(HashMap::from([
                (
                    FLOW_INCREMENTAL_MODE.to_string(),
                    FLOW_INCREMENTAL_MODE_SEQUENCE_RANGE.to_string(),
                ),
                (
                    FLOW_INCREMENTAL_AFTER_SEQS.to_string(),
                    format!(r#"{{"{}":10}}"#, region_id.as_u64()),
                ),
            ]))
            .build();

        let request = scan_request_from_query_context(region_id, &query_ctx).unwrap();

        assert!(request.exact_sequence_range);
        assert!(request.snapshot_on_scan);
        assert_eq!(request.memtable_min_sequence, Some(10));
        assert_eq!(request.memtable_max_sequence, None);
        assert!(!request.skip_sst_files);
    }

    #[test]
    fn test_scan_request_from_query_context_keeps_snapshot_fields() {
        let region_id = test_region_id();
        let query_ctx = QueryContextBuilder::default()
            .snapshot_seqs(Arc::new(RwLock::new(HashMap::from([(
                region_id.as_u64(),
                100,
            )]))))
            .sst_min_sequences(Arc::new(RwLock::new(HashMap::from([(
                region_id.as_u64(),
                90,
            )]))))
            .build();

        let request = scan_request_from_query_context(region_id, &query_ctx).unwrap();
        assert_eq!(request.memtable_max_sequence, Some(100));
        assert_eq!(request.sst_min_sequence, Some(90));
        assert_eq!(request.memtable_min_sequence, None);
        assert!(!request.snapshot_on_scan);
        assert!(!request.skip_sst_files);
    }

    #[test]
    fn test_scan_request_from_query_context_reuses_existing_snapshot_for_incremental_scan() {
        let region_id = test_region_id();
        let query_ctx = QueryContextBuilder::default()
            .extensions(HashMap::from([
                (
                    FLOW_INCREMENTAL_MODE.to_string(),
                    "memtable_only".to_string(),
                ),
                (
                    FLOW_INCREMENTAL_AFTER_SEQS.to_string(),
                    format!(r#"{{"{}":10}}"#, region_id.as_u64()),
                ),
            ]))
            .snapshot_seqs(Arc::new(RwLock::new(HashMap::from([(
                region_id.as_u64(),
                42_u64,
            )]))))
            .build();

        let request = scan_request_from_query_context(region_id, &query_ctx).unwrap();

        assert_eq!(request.memtable_min_sequence, Some(10));
        assert_eq!(request.memtable_max_sequence, Some(42));
        assert!(!request.snapshot_on_scan);
        assert!(request.skip_sst_files);
    }

    #[test]
    fn test_apply_cached_snapshot_to_request_updates_cached_scan_request() {
        let region_id = test_region_id();
        let query_ctx = QueryContextBuilder::default()
            .snapshot_seqs(Arc::new(RwLock::new(HashMap::from([(
                region_id.as_u64(),
                88_u64,
            )]))))
            .build();
        let mut request = ScanRequest {
            snapshot_on_scan: true,
            ..Default::default()
        };

        apply_cached_snapshot_to_request(&query_ctx, region_id, false, &mut request);

        assert_eq!(request.memtable_max_sequence, Some(88));
        assert!(!request.snapshot_on_scan);
    }

    #[test]
    fn test_apply_cached_snapshot_to_request_preserves_exact_sequence_range() {
        let region_id = test_region_id();
        let query_ctx = QueryContextBuilder::default()
            .snapshot_seqs(Arc::new(RwLock::new(HashMap::from([(
                region_id.as_u64(),
                10_u64,
            )]))))
            .build();
        let mut request = ScanRequest {
            memtable_min_sequence: Some(10),
            snapshot_on_scan: true,
            exact_sequence_range: true,
            ..Default::default()
        };

        apply_cached_snapshot_to_request(&query_ctx, region_id, false, &mut request);

        assert_eq!(request.memtable_min_sequence, Some(10));
        assert_eq!(request.memtable_max_sequence, Some(10));
        assert!(request.exact_sequence_range);
        assert!(!request.skip_sst_files);
        assert_eq!(request.sst_min_sequence, None);
        assert!(!request.snapshot_on_scan);
    }

    #[test]
    fn test_apply_cached_snapshot_to_request_skips_sink_scan() {
        let region_id = test_region_id();
        let query_ctx = QueryContextBuilder::default()
            .snapshot_seqs(Arc::new(RwLock::new(HashMap::from([(
                region_id.as_u64(),
                88_u64,
            )]))))
            .build();
        let mut request = ScanRequest {
            snapshot_on_scan: true,
            ..Default::default()
        };

        apply_cached_snapshot_to_request(&query_ctx, region_id, true, &mut request);

        assert_eq!(request.memtable_max_sequence, None);
        assert!(request.snapshot_on_scan);
    }

    #[test]
    fn test_bind_snapshot_bound_region_seq_reuses_existing_snapshot() {
        let region_id = test_region_id();
        let query_ctx = QueryContextBuilder::default()
            .snapshot_seqs(Arc::new(RwLock::new(HashMap::from([(
                region_id.as_u64(),
                42_u64,
            )]))))
            .build();

        let err = bind_snapshot_bound_region_seq(&query_ctx, region_id, 99).unwrap_err();

        assert!(matches!(err, Error::ConflictingSnapshotSequence { .. }));
        assert_eq!(query_ctx.get_snapshot(region_id.as_u64()), Some(42));
    }

    #[test]
    fn test_bind_snapshot_bound_region_seq_sets_snapshot_once() {
        let region_id = test_region_id();
        let query_ctx = QueryContextBuilder::default().build();

        let seq = bind_snapshot_bound_region_seq(&query_ctx, region_id, 99).unwrap();

        assert_eq!(seq, 99);
        assert_eq!(query_ctx.get_snapshot(region_id.as_u64()), Some(99));
    }

    #[test]
    fn test_scan_request_from_query_context_applies_incremental_after_seq_for_source_scan() {
        let region_id = test_region_id();
        let query_ctx = QueryContextBuilder::default()
            .extensions(HashMap::from([
                (
                    FLOW_INCREMENTAL_MODE.to_string(),
                    "memtable_only".to_string(),
                ),
                (
                    FLOW_INCREMENTAL_AFTER_SEQS.to_string(),
                    format!(r#"{{"{}":55}}"#, region_id.as_u64()),
                ),
            ]))
            .build();

        let request = scan_request_from_query_context(region_id, &query_ctx).unwrap();
        assert_eq!(request.memtable_min_sequence, Some(55));
        assert_eq!(request.sst_min_sequence, None);
        assert!(request.skip_sst_files);
        assert!(!request.exact_sequence_range);
    }

    #[test]
    fn test_scan_request_from_sequence_range_context_excludes_sink_scan() {
        let region_id = test_region_id();
        let query_ctx = QueryContextBuilder::default()
            .extensions(HashMap::from([
                (
                    FLOW_INCREMENTAL_MODE.to_string(),
                    FLOW_INCREMENTAL_MODE_SEQUENCE_RANGE.to_string(),
                ),
                (
                    FLOW_INCREMENTAL_AFTER_SEQS.to_string(),
                    format!(r#"{{"{}":55}}"#, region_id.as_u64()),
                ),
                (
                    FLOW_SINK_TABLE_ID.to_string(),
                    region_id.table_id().to_string(),
                ),
            ]))
            .snapshot_seqs(Arc::new(RwLock::new(HashMap::from([(
                region_id.as_u64(),
                88_u64,
            )]))))
            .sst_min_sequences(Arc::new(RwLock::new(HashMap::from([(
                region_id.as_u64(),
                77_u64,
            )]))))
            .build();

        let request = scan_request_from_query_context(region_id, &query_ctx).unwrap();

        assert!(!request.exact_sequence_range);
        assert!(!request.skip_sst_files);
        assert_eq!(request.memtable_min_sequence, None);
        assert_eq!(request.memtable_max_sequence, None);
        assert_eq!(request.sst_min_sequence, None);
    }

    #[test]
    fn test_scan_request_from_query_context_does_not_apply_incremental_for_sink_table() {
        let region_id = test_region_id();
        let query_ctx = QueryContextBuilder::default()
            .extensions(HashMap::from([
                (
                    FLOW_INCREMENTAL_MODE.to_string(),
                    "memtable_only".to_string(),
                ),
                (
                    FLOW_INCREMENTAL_AFTER_SEQS.to_string(),
                    format!(r#"{{"{}":55}}"#, region_id.as_u64()),
                ),
                (
                    FLOW_SINK_TABLE_ID.to_string(),
                    region_id.table_id().to_string(),
                ),
            ]))
            .snapshot_seqs(Arc::new(RwLock::new(HashMap::from([(
                region_id.as_u64(),
                88_u64,
            )]))))
            .sst_min_sequences(Arc::new(RwLock::new(HashMap::from([(
                region_id.as_u64(),
                77_u64,
            )]))))
            .build();

        let request = scan_request_from_query_context(region_id, &query_ctx).unwrap();
        assert_eq!(request.memtable_min_sequence, None);
        assert_eq!(request.memtable_max_sequence, None);
        assert_eq!(request.sst_min_sequence, None);
        assert!(!request.skip_sst_files);
        assert!(!request.snapshot_on_scan);
    }

    #[test]
    fn test_scan_request_from_query_context_rejects_missing_memtable_only_region() {
        let region_id = test_region_id();
        let query_ctx = QueryContextBuilder::default()
            .extensions(HashMap::from([
                (
                    FLOW_INCREMENTAL_MODE.to_string(),
                    "memtable_only".to_string(),
                ),
                (
                    FLOW_INCREMENTAL_AFTER_SEQS.to_string(),
                    r#"{"9":55}"#.to_string(),
                ),
            ]))
            .build();

        let err = scan_request_from_query_context(region_id, &query_ctx).unwrap_err();
        assert!(matches!(err, Error::InvalidQueryContextExtension { .. }));
    }

    #[test]
    fn test_scan_request_from_sequence_range_rejects_missing_source_bound() {
        let region_id = test_region_id();
        let query_ctx = QueryContextBuilder::default()
            .extensions(HashMap::from([
                (
                    FLOW_INCREMENTAL_MODE.to_string(),
                    FLOW_INCREMENTAL_MODE_SEQUENCE_RANGE.to_string(),
                ),
                (
                    FLOW_INCREMENTAL_AFTER_SEQS.to_string(),
                    r#"{"9":55}"#.to_string(),
                ),
            ]))
            .build();

        let err = scan_request_from_query_context(region_id, &query_ctx).unwrap_err();
        assert!(matches!(err, Error::InvalidQueryContextExtension { .. }));
        assert_eq!(err.status_code(), StatusCode::InvalidArguments);
    }

    #[test]
    fn test_scan_request_from_query_context_rejects_invalid_incremental_json() {
        let region_id = test_region_id();
        let query_ctx = QueryContextBuilder::default()
            .extensions(HashMap::from([(
                FLOW_INCREMENTAL_AFTER_SEQS.to_string(),
                "not-json".to_string(),
            )]))
            .build();

        let err = scan_request_from_query_context(region_id, &query_ctx).unwrap_err();
        assert!(matches!(err, Error::InvalidQueryContextExtension { .. }));
        assert_eq!(err.status_code(), StatusCode::InvalidArguments);
    }

    #[test]
    fn test_scan_request_from_query_context_rejects_invalid_sink_table_id() {
        let region_id = test_region_id();
        let query_ctx = QueryContextBuilder::default()
            .extensions(HashMap::from([(
                FLOW_SINK_TABLE_ID.to_string(),
                "abc".to_string(),
            )]))
            .build();

        let err = scan_request_from_query_context(region_id, &query_ctx).unwrap_err();
        assert!(matches!(err, Error::InvalidQueryContextExtension { .. }));
        assert_eq!(err.status_code(), StatusCode::InvalidArguments);
    }

    // ---- raw series leaf selection ----

    use common_error::ext::{BoxedError, PlainError};
    use common_query::series_frame::{label_decode_spec, unpack_raw_series};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::execution::context::{SessionConfig, TaskContext};
    use datafusion::physical_plan::ExecutionPlanProperties;
    use datafusion::physical_plan::execution_plan::reset_plan_states;
    use datatypes::arrow::array::Float64Array;
    use datatypes::arrow::buffer::{Buffer, ScalarBuffer};
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use datatypes::series::SeriesOrdering;
    use datatypes::series::frame::LabelDecodeSpec;
    use datatypes::series::raw::{RawSeriesChunk, RawTimestamps};
    use datatypes::series::samples::{FieldSpec, SampleRef, Samples, SeriesFields, SeriesSchema};
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::region_engine::{
        PartitionRange, PrepareRequest, QueryScanContext, RawLabelSource, RawSeriesStream,
        RegionEngine, RegionRole, RegionScanner, RegionStatistic, RemapManifestsRequest,
        RemapManifestsResponse, ScannerProperties, SendableRawSeriesStream,
        SetRegionRoleStateResponse, SettableRegionRoleState, SyncRegionFromRequest,
        SyncRegionFromResponse,
    };
    use store_api::region_request::RegionRequest;
    use store_api::storage::SequenceNumber;
    use table::table::scan::REGION_SCAN_EXEC_NAME;

    /// `Result` here is the crate's one-argument alias, so the engine traits below name theirs.
    type RegionResult<T> = std::result::Result<T, BoxedError>;

    type FrameRegionSetup = (
        FrameRegionEngine,
        RegionMetadataRef,
        Arc<Mutex<Vec<usize>>>,
        Arc<Mutex<Vec<Option<TimeSeriesDistribution>>>>,
    );

    /// Scripted raw-series chunks not yet yielded or released by the scan streams.
    type PendingChunks = Arc<Mutex<usize>>;

    /// Suspends a scripted scan stream, once it has yielded this many chunks, until it is cleared.
    type PauseAfterChunks = Arc<Mutex<Option<usize>>>;

    /// A region whose scan either can or cannot serve raw series frames.
    ///
    /// The engine reports the metadata and hands out one scanner; whether that scanner has a raw
    /// series schema is the single difference between an eligible region and an ineligible one.
    struct FrameRegionEngine {
        metadata: RegionMetadataRef,
        serves_frames: bool,
        distributions: Arc<Mutex<Vec<Option<TimeSeriesDistribution>>>>,
        prepares: Arc<Mutex<Vec<usize>>>,
        partition_chunks: Arc<Vec<Vec<RawSeriesChunk>>>,
        pending: PendingChunks,
        pause_after: PauseAfterChunks,
    }

    impl FrameRegionEngine {
        /// The engine as a test builds it, with the chunks each scan partition emits.
        ///
        /// The returned counter holds the scripted chunks owned by live scan streams; it is zero
        /// when every stream has yielded or released its chunks. The returned pause handle
        /// suspends every scan stream once it has yielded the configured number of chunks.
        fn with_scripted_chunks(
            partition_chunks: Vec<Vec<RawSeriesChunk>>,
        ) -> (FrameRegionSetup, PendingChunks, PauseAfterChunks) {
            let (mut engine, metadata, prepares, distributions) = Self::new(true);
            let pending = Arc::new(Mutex::new(0));
            let pause_after = Arc::new(Mutex::new(None));
            engine.partition_chunks = Arc::new(partition_chunks);
            engine.pending = pending.clone();
            engine.pause_after = pause_after.clone();
            (
                (engine, metadata, prepares, distributions),
                pending,
                pause_after,
            )
        }

        fn new(serves_frames: bool) -> FrameRegionSetup {
            let metadata = Arc::new(frame_region_metadata());
            let prepares = Arc::new(Mutex::new(Vec::new()));
            let distributions = Arc::new(Mutex::new(Vec::new()));
            (
                Self {
                    metadata: metadata.clone(),
                    serves_frames,
                    distributions: distributions.clone(),
                    prepares: prepares.clone(),
                    partition_chunks: Arc::new(Vec::new()),
                    pending: Arc::new(Mutex::new(0)),
                    pause_after: Arc::new(Mutex::new(None)),
                },
                metadata,
                prepares,
                distributions,
            )
        }
    }

    /// A metric-like region: `host` tag, `ts` timestamp, and two fields so a projection has to
    /// choose which field the frame carries.
    fn frame_region_metadata() -> store_api::metadata::RegionMetadata {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(2048, 1));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "host",
                    ConcreteDataType::string_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Tag,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "value",
                    ConcreteDataType::float64_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Field,
                column_id: 3,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("aux", ConcreteDataType::float64_datatype(), true),
                semantic_type: SemanticType::Field,
                column_id: 4,
            })
            .primary_key(vec![1]);
        builder.build().unwrap()
    }

    fn projected_schema(
        metadata: &store_api::metadata::RegionMetadata,
        projection: Option<&Vec<usize>>,
    ) -> datatypes::schema::SchemaRef {
        let columns = metadata
            .schema
            .column_schemas()
            .iter()
            .enumerate()
            .filter(|(index, _)| projection.is_none_or(|projection| projection.contains(index)))
            .map(|(_, column)| column.clone())
            .collect::<Vec<_>>();
        Arc::new(datatypes::schema::Schema::new(columns))
    }

    fn frame_series_schema(
        metadata: &store_api::metadata::RegionMetadata,
        projection: Option<&Vec<usize>>,
    ) -> Arc<SeriesSchema> {
        let fields = metadata
            .column_metadatas
            .iter()
            .enumerate()
            .filter(|(index, _)| projection.is_none_or(|projection| projection.contains(index)))
            .filter(|(_, column)| column.semantic_type == SemanticType::Field)
            .map(|(_, column)| FieldSpec {
                name: Arc::from(column.column_schema.name.as_str()),
            })
            .collect::<Vec<_>>();
        Arc::new(SeriesSchema {
            fields: fields.into(),
            eval_grid: None,
        })
    }

    #[derive(Debug)]
    struct FrameScanner {
        metadata: RegionMetadataRef,
        projection: Option<Vec<usize>>,
        properties: ScannerProperties,
        serves_frames: bool,
        prepares: Arc<Mutex<Vec<usize>>>,
        /// Chunks each scan partition emits, indexed by partition. Empty unless a test scripts it.
        partition_chunks: Arc<Vec<Vec<RawSeriesChunk>>>,
        executed_partitions: Arc<Mutex<Vec<bool>>>,
        pending: PendingChunks,
        pause_after: PauseAfterChunks,
    }

    impl datafusion::physical_plan::DisplayAs for FrameScanner {
        fn fmt_as(
            &self,
            _format: datafusion::physical_plan::DisplayFormatType,
            f: &mut std::fmt::Formatter<'_>,
        ) -> std::fmt::Result {
            write!(f, "FrameScanner")
        }
    }

    impl RegionScanner for FrameScanner {
        fn name(&self) -> &str {
            "FrameScanner"
        }

        fn properties(&self) -> &ScannerProperties {
            &self.properties
        }

        fn schema(&self) -> datatypes::schema::SchemaRef {
            projected_schema(&self.metadata, self.projection.as_ref())
        }

        fn metadata(&self) -> RegionMetadataRef {
            self.metadata.clone()
        }

        fn prepare(&mut self, request: PrepareRequest) -> RegionResult<()> {
            self.prepares
                .lock()
                .unwrap()
                .push(request.target_partitions.unwrap_or(0));
            self.properties.prepare(request);
            Ok(())
        }

        fn scan_partition(
            &self,
            _ctx: &QueryScanContext,
            _metrics_set: &datafusion::physical_plan::metrics::ExecutionPlanMetricsSet,
            _partition: usize,
        ) -> RegionResult<common_recordbatch::SendableRecordBatchStream> {
            unimplemented!("these tests never execute the row leaf")
        }

        fn raw_series_schema(
            &self,
            _label_source: &RawLabelSource,
        ) -> RegionResult<Option<Arc<SeriesSchema>>> {
            if !self.serves_frames {
                return Ok(None);
            }
            Ok(Some(frame_series_schema(
                &self.metadata,
                self.projection.as_ref(),
            )))
        }

        fn raw_series_ordering(&self) -> SeriesOrdering {
            SeriesOrdering::KeyThenTime
        }

        fn scan_series_partition(
            &self,
            _ctx: &QueryScanContext,
            _metrics_set: &datafusion::physical_plan::metrics::ExecutionPlanMetricsSet,
            partition: usize,
            label_source: &RawLabelSource,
        ) -> RegionResult<Option<SendableRawSeriesStream>> {
            {
                let mut executed = self.executed_partitions.lock().unwrap();
                if executed[partition] {
                    return Err(BoxedError::new(PlainError::new(
                        "partition executed twice without reset".to_string(),
                        common_error::status_code::StatusCode::Unexpected,
                    )));
                }
                executed[partition] = true;
            }
            let chunks = self
                .partition_chunks
                .get(partition)
                .cloned()
                .unwrap_or_default();
            *self.pending.lock().unwrap() += chunks.len();
            Ok(Some(Box::pin(ScriptedRawStream {
                schema: frame_series_schema(&self.metadata, self.projection.as_ref()),
                source: label_source.clone(),
                decode: label_decode_spec(label_source),
                chunks,
                yielded: 0,
                pending: self.pending.clone(),
                pause_after: self.pause_after.clone(),
            })))
        }

        fn reset_state(&mut self) {
            self.executed_partitions.lock().unwrap().fill(false);
        }

        fn has_predicate_without_region(&self) -> bool {
            false
        }

        fn add_dyn_filter_to_predicate(
            &mut self,
            filter_exprs: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
        ) -> Vec<bool> {
            vec![false; filter_exprs.len()]
        }

        fn set_logical_region(&mut self, _logical_region: bool) {}

        fn set_query_load_region_id(&mut self, _region_id: RegionId) {}
    }

    /// A raw stream over the chunks scripted for its scan partition.
    struct ScriptedRawStream {
        schema: Arc<SeriesSchema>,
        source: RawLabelSource,
        decode: Option<LabelDecodeSpec>,
        chunks: Vec<RawSeriesChunk>,
        yielded: usize,
        pending: PendingChunks,
        /// When set, poll suspends after that many chunks without waking, so the remaining chunks
        /// stay owned by the stream and a consumer can cancel it mid-scan.
        pause_after: PauseAfterChunks,
    }

    impl RawSeriesStream for ScriptedRawStream {
        fn schema(&self) -> &Arc<SeriesSchema> {
            &self.schema
        }

        fn label_source(&self) -> &RawLabelSource {
            &self.source
        }

        fn label_decode_spec(&self) -> Option<&LabelDecodeSpec> {
            self.decode.as_ref()
        }

        fn ordering(&self) -> SeriesOrdering {
            SeriesOrdering::KeyThenTime
        }
    }

    impl futures::Stream for ScriptedRawStream {
        type Item = RegionResult<RawSeriesChunk>;

        fn poll_next(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            let this = self.as_mut().get_mut();
            if !this.chunks.is_empty() && *this.pause_after.lock().unwrap() == Some(this.yielded) {
                return std::task::Poll::Pending;
            }
            if this.chunks.is_empty() {
                std::task::Poll::Ready(None)
            } else {
                this.yielded += 1;
                *this.pending.lock().unwrap() -= 1;
                std::task::Poll::Ready(Some(Ok(this.chunks.remove(0))))
            }
        }
    }

    impl Drop for ScriptedRawStream {
        fn drop(&mut self) {
            *self.pending.lock().unwrap() -= self.chunks.len();
            self.chunks.clear();
        }
    }

    #[async_trait]
    impl RegionEngine for FrameRegionEngine {
        fn name(&self) -> &str {
            "FrameRegionEngine"
        }

        async fn handle_request(
            &self,
            _region_id: RegionId,
            _request: RegionRequest,
        ) -> RegionResult<api::region::RegionResponse> {
            unimplemented!("these tests never write")
        }

        async fn handle_query(
            &self,
            _region_id: RegionId,
            request: ScanRequest,
        ) -> RegionResult<store_api::region_engine::RegionScannerRef> {
            self.distributions
                .lock()
                .unwrap()
                .push(request.distribution);
            Ok(Box::new(FrameScanner {
                metadata: self.metadata.clone(),
                projection: request.projection.clone(),
                properties: ScannerProperties::new(frame_partition_ranges(), false, 0),
                serves_frames: self.serves_frames
                    && request.distribution == Some(TimeSeriesDistribution::PerSeries),
                prepares: self.prepares.clone(),
                partition_chunks: self.partition_chunks.clone(),
                executed_partitions: Arc::new(Mutex::new(vec![
                    false;
                    frame_partition_ranges().len()
                ])),
                pending: self.pending.clone(),
                pause_after: self.pause_after.clone(),
            }))
        }

        async fn get_metadata(&self, _region_id: RegionId) -> RegionResult<RegionMetadataRef> {
            Ok(self.metadata.clone())
        }

        fn region_statistic(&self, _region_id: RegionId) -> Option<RegionStatistic> {
            None
        }

        async fn get_committed_sequence(
            &self,
            _region_id: RegionId,
        ) -> RegionResult<SequenceNumber> {
            Ok(SequenceNumber::default())
        }

        async fn stop(&self) -> RegionResult<()> {
            Ok(())
        }

        fn set_region_role(&self, _region_id: RegionId, _role: RegionRole) -> RegionResult<()> {
            Ok(())
        }

        async fn set_region_role_state_gracefully(
            &self,
            _region_id: RegionId,
            _region_role_state: SettableRegionRoleState,
        ) -> RegionResult<SetRegionRoleStateResponse> {
            unimplemented!("these tests never change the region role")
        }

        async fn sync_region(
            &self,
            _region_id: RegionId,
            _request: SyncRegionFromRequest,
        ) -> RegionResult<SyncRegionFromResponse> {
            unimplemented!("these tests never sync")
        }

        async fn remap_manifests(
            &self,
            _request: RemapManifestsRequest,
        ) -> RegionResult<RemapManifestsResponse> {
            unimplemented!("these tests never remap manifests")
        }

        fn role(&self, _region_id: RegionId) -> Option<RegionRole> {
            None
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    /// Builds a region provider, optionally with the request's raw-series key naming `metric`.
    async fn frame_provider(
        serves_frames: bool,
        raw_metric: Option<&str>,
    ) -> (
        DummyTableProvider,
        Arc<Mutex<Vec<usize>>>,
        Arc<Mutex<Vec<Option<TimeSeriesDistribution>>>>,
    ) {
        let region_id = RegionId::new(2048, 1);
        let (engine, _, prepares, distributions) = FrameRegionEngine::new(serves_frames);
        let mut builder = QueryContextBuilder::default();
        if let Some(metric) = raw_metric {
            builder = builder.extensions(HashMap::from([(
                RAW_SERIES_READ_EXTENSION_KEY.to_string(),
                metric.to_string(),
            )]));
        }
        let provider = DummyTableProviderFactory
            .create_table_provider(
                region_id,
                Arc::new(engine) as RegionEngineRef,
                Some(Arc::new(builder.build())),
            )
            .await
            .unwrap();
        (provider, prepares, distributions)
    }

    /// Answers a frame read from a region whose scan streams are scripted.
    async fn scripted_plan(engine: FrameRegionEngine) -> Arc<dyn ExecutionPlan> {
        let provider = DummyTableProviderFactory
            .create_table_provider(
                RegionId::new(2048, 1),
                Arc::new(engine) as RegionEngineRef,
                Some(Arc::new(
                    QueryContextBuilder::default()
                        .extensions(HashMap::from([(
                            RAW_SERIES_READ_EXTENSION_KEY.to_string(),
                            "cpu_usage".to_string(),
                        )]))
                        .build(),
                )),
            )
            .await
            .unwrap();
        provider
            .scan(&test_session(), Some(&vec![2]), &[], None)
            .await
            .expect("an eligible region answers a frame read")
    }

    fn frame_partition_ranges() -> Vec<Vec<PartitionRange>> {
        (0..3)
            .map(|i| {
                vec![PartitionRange {
                    start: common_time::Timestamp::new(i, common_time::timestamp::TimeUnit::Second),
                    end: common_time::Timestamp::new(
                        i + 1,
                        common_time::timestamp::TimeUnit::Second,
                    ),
                    num_rows: 10,
                    identifier: i as usize,
                }]
            })
            .collect()
    }

    fn has_series_frame_metadata(schema: &datatypes::arrow::datatypes::Schema) -> bool {
        schema
            .metadata()
            .keys()
            .any(|key| key.starts_with("greptime.series."))
    }

    fn task_context() -> Arc<TaskContext> {
        datafusion::prelude::SessionContext::new().task_ctx()
    }

    fn test_session() -> datafusion::execution::SessionState {
        SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_target_partitions(3))
            .build()
    }

    #[tokio::test]
    async fn ordinary_read_keeps_the_region_scan_exec() {
        let (provider, prepares, distributions) = frame_provider(true, None).await;
        let state = test_session();
        let plan = provider
            .scan(&state, Some(&vec![2]), &[], None)
            .await
            .unwrap();

        assert_eq!(plan.name(), REGION_SCAN_EXEC_NAME);
        assert!(plan.downcast_ref::<RegionScanExec>().is_some());
        assert!(
            !has_series_frame_metadata(plan.schema().as_ref()),
            "an ordinary read must not declare a series frame"
        );
        // Nothing prepared the scan on the ordinary path, so its partitioning is untouched.
        assert!(prepares.lock().unwrap().is_empty());
        assert_eq!(*distributions.lock().unwrap(), vec![None]);
    }

    #[tokio::test]
    async fn raw_series_read_answers_with_a_frame_carrying_its_own_layout() {
        let (provider, prepares, distributions) = frame_provider(true, Some("cpu_usage")).await;
        let state = test_session();
        let plan = provider
            .scan(&state, Some(&vec![2]), &[], None)
            .await
            .unwrap();

        assert_eq!(plan.name(), "MergedSeriesScanExec");
        let merged = plan.downcast_ref::<MergedSeriesScanExec>().unwrap();
        let frames = merged.input.as_ref();
        // The conversion requires a prepared scan; the fixture has three ranges/partitions.
        assert_eq!(*prepares.lock().unwrap(), vec![3usize]);
        assert_eq!(
            *distributions.lock().unwrap(),
            vec![Some(TimeSeriesDistribution::PerSeries)]
        );
        assert_eq!(
            frames.properties().output_partitioning().partition_count(),
            3
        );
        assert_eq!(plan.output_partitioning().partition_count(), 1);
        // Fields come from the projection, labels and the metric from the region and its request.
        assert_eq!(
            frames
                .series_schema()
                .fields
                .iter()
                .map(|field| field.name.as_ref())
                .collect::<Vec<_>>(),
            vec!["value"]
        );
        let RawLabelSource::PrimaryKey { metric_name, .. } = frames.label_source() else {
            panic!("a prepared scan binds key-encoded labels");
        };
        assert_eq!(metric_name.as_ref(), "cpu_usage");

        // The stream schema declares the frame: layout, content, label mode and ordering, plus the
        // label decode description that names the metric.
        let declared = plan.schema();
        let metadata = declared.metadata();
        assert_eq!(
            metadata.get("greptime.series.layout").map(String::as_str),
            Some("v1")
        );
        assert_eq!(
            metadata.get("greptime.series.content").map(String::as_str),
            Some("raw")
        );
        assert_eq!(
            metadata.get("greptime.series.labels").map(String::as_str),
            Some("key")
        );
        assert_eq!(
            metadata.get("greptime.series.ordering").map(String::as_str),
            Some("read_key_then_time")
        );
        assert!(
            metadata["greptime.series.label_decode"].contains("cpu_usage"),
            "the decode description names the metric: {}",
            metadata["greptime.series.label_decode"]
        );

        // The region's own stream keeps the declared frame schema, so the frame survives the
        // region boundary as it is. `execute` yields the frame stream itself, which is what the
        // cross-process path sends on unchanged.
        let stream = plan.execute(0, task_context()).unwrap();
        assert_eq!(stream.schema(), declared);

        // The frame is the only decode input: unpacking it needs no binding from the caller.
        let unpacked = unpack_raw_series(stream, None).unwrap();
        assert_eq!(unpacked.schema().fields.len(), 1);
        assert_eq!(
            unpacked
                .schema()
                .fields
                .iter()
                .map(|field| field.name.as_ref())
                .collect::<Vec<_>>(),
            vec!["value"]
        );
        assert_eq!(unpacked.ordering(), SeriesOrdering::KeyThenTime);
    }

    /// One chunk of a raw series: `key` names the series, `ts` its sample times, `last` closes it.
    fn frame_chunk(ts: &[i64], key: &[u8], last: bool) -> RawSeriesChunk {
        let schema = SeriesSchema {
            fields: vec![FieldSpec {
                name: Arc::from("value"),
            }]
            .into(),
            eval_grid: None,
        };
        RawSeriesChunk::try_new(
            &schema,
            Buffer::from([key, &[0, 0]].concat()),
            None,
            RawTimestamps::try_new(ScalarBuffer::from(ts.to_vec())).unwrap(),
            SeriesFields::one(Samples::from_float(Float64Array::from(
                ts.iter().map(|time| Some(*time as f64)).collect::<Vec<_>>(),
            ))),
            last,
        )
        .unwrap()
    }

    /// Drains a plan's packed frame stream into the raw-series chunks it carries.
    async fn frame_chunks(plan: &Arc<dyn ExecutionPlan>) -> Vec<RawSeriesChunk> {
        let context = Arc::new(TaskContext::default());
        let unpacked = unpack_raw_series(plan.execute(0, context).unwrap(), None).unwrap();
        futures_util::TryStreamExt::try_collect::<Vec<_>>(unpacked)
            .await
            .unwrap()
    }

    /// Joins an error and its sources, so an assertion can see a cause a consumer wrapped.
    fn error_chain(error: &dyn std::error::Error) -> String {
        let mut chain = vec![error.to_string()];
        let mut source = error.source();
        while let Some(error) = source {
            chain.push(error.to_string());
            source = error.source();
        }
        chain.join(": ")
    }

    /// The complete payload of a raw-series stream: key, times, float samples and finality.
    fn frame_snapshot(chunks: &[RawSeriesChunk]) -> Vec<(Vec<u8>, Vec<i64>, Vec<f64>, bool)> {
        chunks
            .iter()
            .map(|chunk| {
                (
                    chunk.primary_key.to_vec(),
                    chunk.time.as_buffer().to_vec(),
                    chunk
                        .fields
                        .get(0)
                        .unwrap()
                        .iter()
                        .map(|sample| match sample {
                            SampleRef::Float(value) => value,
                            _ => panic!("the fixture stores float samples"),
                        })
                        .collect(),
                    chunk.is_last,
                )
            })
            .collect()
    }

    /// Partitions that would interleave if simply concatenated are merged into one ordered stream.
    ///
    /// Every partition is itself well formed: within it a series is closed before the next key
    /// starts, which is what `unpack_raw_series` requires of one stream. The encoded key `a` is
    /// split across partitions 0 and 1 with interleaved times and closed by both of them,
    /// partition 0 also carries `c`, and partition 1 also carries `b`. Concatenating the
    /// partitions gives `a, a, c, a, a, b`, which breaks the `KeyThenTime` contract every
    /// raw-series consumer requires. The single region-read stream must instead be
    /// `a@10, a@11, a@12, a@15, b@25, c@20`, keeping `a`'s chunks adjacent and closing it exactly
    /// once across both partitions.
    ///
    /// The same plan then replays: an execution without a reset fails, a reset reproduces the
    /// complete payload, and a partially consumed stream that is dropped and reset reproduces it
    /// again. For the cancellation the fixture suspends its streams after two chunks without
    /// waking, so the dropped stream still owns undrained chunks when it is cancelled.
    #[tokio::test]
    async fn raw_series_partitions_merge_into_one_ordered_stream() {
        let chunks = vec![
            vec![
                frame_chunk(&[10], b"a", false),
                frame_chunk(&[12], b"a", true),
                frame_chunk(&[20], b"c", true),
            ],
            vec![
                frame_chunk(&[11], b"a", false),
                frame_chunk(&[15], b"a", true),
                frame_chunk(&[25], b"b", true),
            ],
            Vec::new(),
        ];
        let ((engine, _, _, _), pending, pause_after) =
            FrameRegionEngine::with_scripted_chunks(chunks);
        let plan = scripted_plan(engine).await;
        // The scan keeps its three partitions; the region-read boundary sees one.
        assert_eq!(
            plan.downcast_ref::<MergedSeriesScanExec>()
                .unwrap()
                .input
                .properties()
                .output_partitioning()
                .partition_count(),
            3
        );
        assert_eq!(plan.output_partitioning().partition_count(), 1);

        let first = frame_snapshot(&frame_chunks(&plan).await);
        let expected = vec![
            (b"a\0\0".to_vec(), vec![10], vec![10.0], false),
            (b"a\0\0".to_vec(), vec![11], vec![11.0], false),
            (b"a\0\0".to_vec(), vec![12], vec![12.0], false),
            (b"a\0\0".to_vec(), vec![15], vec![15.0], true),
            (b"b\0\0".to_vec(), vec![25], vec![25.0], true),
            (b"c\0\0".to_vec(), vec![20], vec![20.0], true),
        ];
        assert_eq!(
            first, expected,
            "the merged stream must be ordered by key then time, keep each series' chunks \
             adjacent, and close every series exactly once"
        );
        // A scan that has ended is not replayable until it is reset: the hidden scanner refuses.
        let error = match plan.execute(0, task_context()) {
            Ok(_) => panic!("an unreset scan must not execute again"),
            Err(error) => error.to_string(),
        };
        assert!(
            error.contains("partition executed twice without reset"),
            "unexpected error: {error}"
        );

        // Resetting the wrapper resets the scan it hides, so the whole stream replays.
        let replay = reset_plan_states(plan.clone()).unwrap();
        assert_eq!(frame_snapshot(&frame_chunks(&replay).await), first);

        // A cancelled stream is dropped while the source still owns chunks, and the reset plan
        // replays the complete payload afterwards.
        *pause_after.lock().unwrap() = Some(2);
        let cancelled = reset_plan_states(replay).unwrap();
        let mut partial =
            unpack_raw_series(cancelled.execute(0, task_context()).unwrap(), None).unwrap();
        assert!(
            futures_util::TryStreamExt::try_next(&mut partial)
                .await
                .unwrap()
                .is_some(),
            "the cancelled stream must emit a chunk before cancellation"
        );
        let held = *pending.lock().unwrap();
        assert!(
            held > 0,
            "the paused stream still owns {held} undrained chunks"
        );
        drop(partial);
        assert_eq!(
            *pending.lock().unwrap(),
            0,
            "dropping the cancelled stream releases the rest of the source"
        );
        *pause_after.lock().unwrap() = None;
        let after_cancel = reset_plan_states(cancelled).unwrap();
        assert_eq!(frame_snapshot(&frame_chunks(&after_cancel).await), first);
    }

    /// A failure acquiring a later partition releases the partition already acquired.
    ///
    /// The wrapper acquires partitions in order, so a partition spent before it runs fails the
    /// read after an earlier partition was acquired. The acquired stream must be released with
    /// its chunks, the scan must stay one-shot until it is reset, and the reset plan must replay
    /// the complete payload from both partitions.
    #[tokio::test]
    async fn raw_series_later_partition_failure_releases_the_acquired_partition() {
        let chunks = vec![
            vec![frame_chunk(&[10], b"a", true)],
            vec![frame_chunk(&[20], b"b", true)],
            Vec::new(),
        ];
        let ((engine, _, _, _), pending, _) = FrameRegionEngine::with_scripted_chunks(chunks);
        let plan = scripted_plan(engine).await;
        let input = plan
            .downcast_ref::<MergedSeriesScanExec>()
            .unwrap()
            .input
            .clone();

        // Spend partition 1's single acquisition before the wrapper asks for it, then release it.
        drop(input.execute(1, task_context()).unwrap());
        assert_eq!(*pending.lock().unwrap(), 0);

        let error = match plan.execute(0, task_context()) {
            Ok(_) => panic!("a partition spent before the wrapper runs must fail the read"),
            Err(error) => error,
        };
        assert!(
            error_chain(&error).contains("partition executed twice without reset"),
            "unexpected error: {error}"
        );
        assert_eq!(
            *pending.lock().unwrap(),
            0,
            "the partition acquired before the failure must release its chunks"
        );
        // Partition 0 was really acquired: the scan refuses it instead of it never having run.
        let error = match input.execute(0, task_context()) {
            Ok(_) => panic!("the failed execution must have acquired partition 0"),
            Err(error) => error,
        };
        assert!(
            error_chain(&error).contains("partition executed twice without reset"),
            "unexpected error: {error}"
        );

        // Resetting the wrapper recovers the whole stream, including both partitions.
        let replay = reset_plan_states(plan).unwrap();
        assert_eq!(
            frame_snapshot(&frame_chunks(&replay).await),
            vec![
                (b"a\0\0".to_vec(), vec![10], vec![10.0], true),
                (b"b\0\0".to_vec(), vec![20], vec![20.0], true),
            ]
        );
    }

    /// A partition that ends before its final chunk fails the read and releases the rest.
    ///
    /// Partition 1's series `b` never closes, so the partition's own packing rejects the truncated
    /// stream at EOF before the merge can judge it. Partition 0 is suspended after two chunks, so
    /// it still owns `d` when that error arrives: the failed read must release the chunk it never
    /// consumed rather than leave it behind in the scanner. The read is not claimed to be empty
    /// before the error, only to fail and to own nothing afterwards.
    #[tokio::test]
    async fn raw_series_truncated_partition_fails_and_releases_the_stream() {
        let chunks = vec![
            vec![
                frame_chunk(&[10], b"a", true),
                frame_chunk(&[30], b"c", true),
                frame_chunk(&[40], b"d", true),
            ],
            vec![frame_chunk(&[20], b"b", false)],
            Vec::new(),
        ];
        let ((engine, _, _, _), pending, pause_after) =
            FrameRegionEngine::with_scripted_chunks(chunks);
        let plan = scripted_plan(engine).await;
        assert_eq!(*pending.lock().unwrap(), 0, "nothing is acquired yet");

        // Partition 0 yields `a` and `c`, then suspends while it still owns `d`; partition 1's
        // single open chunk is not held back, so its EOF reaches the read.
        *pause_after.lock().unwrap() = Some(2);
        let unpacked = unpack_raw_series(plan.execute(0, task_context()).unwrap(), None).unwrap();
        assert!(
            *pending.lock().unwrap() > 0,
            "the read must acquire partition 0's remaining chunks"
        );
        let error = match futures_util::TryStreamExt::try_collect::<Vec<_>>(unpacked).await {
            Ok(_) => panic!("a partition that ends before its final chunk must fail the read"),
            Err(error) => error,
        };
        assert!(
            error_chain(&error).contains("raw series stream ended before key was final"),
            "unexpected error: {error}"
        );
        assert_eq!(
            *pending.lock().unwrap(),
            0,
            "the failed read must release the chunk it never consumed"
        );
    }

    #[tokio::test]
    async fn raw_series_read_over_an_ineligible_region_fails_naming_the_reason() {
        let (provider, prepares, _) = frame_provider(false, Some("cpu_usage")).await;
        let state = test_session();
        let error = provider
            .scan(&state, Some(&vec![2]), &[], None)
            .await
            .expect_err("a read that asked for frames must not be answered with rows");

        // The region cannot serve frames and says so, naming the region, the metric and the engine
        // it asked. It never substitutes the ordinary row answer for the frame answer.
        let message = error.to_string();
        assert!(
            message.contains("cannot answer a raw series request"),
            "unexpected error: {message}"
        );
        assert!(message.contains("cpu_usage"), "unexpected error: {message}");
        assert!(
            message.contains(&RegionId::new(2048, 1).to_string()),
            "unexpected error: {message}"
        );
        assert!(
            message.contains("FrameRegionEngine"),
            "unexpected error: {message}"
        );
        // It failed after preparing the scan the conversion needs, not by skipping the attempt.
        assert_eq!(*prepares.lock().unwrap(), vec![3usize]);
    }

    #[tokio::test]
    async fn raw_series_read_falls_back_to_no_frames_only_by_being_asked_for_rows() {
        // The same ineligible region answers normally once the request is an ordinary one: the
        // way to get rows is to ask for rows, not to ask for frames and receive them.
        let (provider, _, _distributions) = frame_provider(false, None).await;
        let state = test_session();
        let plan = provider
            .scan(&state, Some(&vec![2]), &[], None)
            .await
            .unwrap();
        assert_eq!(plan.name(), REGION_SCAN_EXEC_NAME);
        assert!(!has_series_frame_metadata(plan.schema().as_ref()));
    }
}
