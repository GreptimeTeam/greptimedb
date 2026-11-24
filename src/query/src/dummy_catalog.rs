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
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::{Arc, Mutex};

use api::v1::SemanticType;
use async_trait::async_trait;
use catalog::error::Result as CatalogResult;
use catalog::{CatalogManager, CatalogManagerRef};
use common_recordbatch::OrderOption;
use common_recordbatch::filter::SimpleFilterEvaluator;
use datafusion::catalog::{CatalogProvider, CatalogProviderList, SchemaProvider, Session};
use datafusion::datasource::TableProvider;
use datafusion::physical_plan::ExecutionPlan;
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
use store_api::storage::{
    CursorShard, RegionId, ScanRequest, TimeSeriesDistribution, TimeSeriesRowSelector,
};
use table::TableRef;
use table::metadata::{TableId, TableInfoRef};
use table::table::adapter::{dictionary_encode_string_columns, supports_pk_dictionary_encoding};
use table::table::scan::RegionScanExec;

use crate::error::{GetRegionMetadataSnafu, Result};
use crate::options::{FlowIncrementalMode, FlowQueryExtensions, FlowRepairMode};

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
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let mut request = self.scan_request.lock().unwrap().clone();
        request.projection = projection.cloned();
        request.filters = filters.to_vec();
        request.limit = limit;
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

        // Bind what the region's scan stated about the snapshot it reads under: the partition
        // expression it covers and the creators of the files it had visible. The engine states all
        // of it from one version snapshot, so the expression a region read reports, the watermark
        // it reports and the file list it started from describe the same instant: a scan that
        // inherited a caller-fenced bound still states what it actually read under.
        if let Some(query_ctx) = &self.query_ctx
            && let Some(identity) = scanner.snapshot_identity()
        {
            query_ctx.bind_scan_partition_expr(self.region_id.as_u64(), identity.partition_expr);
            if let Some(creators) = identity.visible_foreign_file_creators {
                query_ctx.bind_scan_visible_file_creators(self.region_id.as_u64(), creators);
            }
        }

        let query_memory_tracker = self.engine.query_memory_tracker();
        let mut scan_exec = RegionScanExec::new(scanner, request, query_memory_tracker)?;
        if let Some(query_ctx) = &self.query_ctx {
            scan_exec.set_explain_verbose(query_ctx.explain_verbose());
        }
        Ok(Arc::new(scan_exec))
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
    /// Whether the read may cross an invalid boundary: it prunes at file granularity
    /// and forces the file barrier as each row's sequence.
    read_across_invalid_boundary: bool,
    /// Whether the read requires its snapshot to cover the region's flushed frontier:
    /// it must read the selected windows completely, so it gets no file pruning and no
    /// forced override. Both reads return rows a stale cursor can no longer explain.
    require_flushed_frontier: bool,
    /// Whether the scan reads physical row sequences and hides the rows a substitution
    /// produced, reporting the windows they occupy. Only the incremental delta asks for
    /// it: it is the read that must split an adopted file into consumed and unconsumed
    /// rows instead of trusting the handle's barrier.
    read_row_sequence: bool,
    /// The cursor slices of every creator region, used to filter the rows of a file this
    /// region adopted from another one. Only a physical-sequence read carries them.
    creator_cursor_shards: Option<Arc<BTreeMap<u64, Vec<CursorShard>>>>,
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
            read_across_invalid_boundary: false,
            require_flushed_frontier: false,
            read_row_sequence: false,
            creator_cursor_shards: None,
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
            read_across_invalid_boundary: false,
            require_flushed_frontier: false,
            read_row_sequence: false,
            creator_cursor_shards: None,
        });
    };

    // Sink-table scans intentionally bypass all flow scan semantics. They should
    // behave like plain reads and must not participate in incremental lower bounds
    // or per-region snapshot binding/reuse.
    if flow_extensions.sink_table_id == Some(region_id.table_id()) {
        return Ok(FlowScanDecision::plain_scan());
    }

    let apply_incremental = flow_extensions.validate_for_scan(region_id)?;

    // A region's own memtables and files carry numbers this region allocated, but a
    // region-level bound cannot tell which expression slice a row belongs to, so it
    // starts after the least advanced slice (`region_cursor`, not a per-row choice).
    let memtable_min_sequence = if apply_incremental {
        flow_extensions.region_cursor(region_id.as_u64())
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

    // Only an exact incremental delta splits an adopted file by its physical
    // sequences: without the row-level range there is no cursor to split it against,
    // and a repair read wants every row of its windows.
    let read_row_sequence = apply_incremental
        && flow_extensions.read_row_sequence
        && flow_extensions.incremental_mode == Some(FlowIncrementalMode::SequenceRange);

    Ok(FlowScanDecision {
        is_sink_scan: false,
        snapshot_on_scan: memtable_max_sequence.is_none()
            && flow_extensions.should_collect_region_watermark(),
        memtable_min_sequence,
        memtable_max_sequence,
        skip_sst_files,
        exact_sequence_range: apply_incremental
            && flow_extensions.incremental_mode == Some(FlowIncrementalMode::SequenceRange),
        // Flow repair reads are decided by the repair mode alone, never by
        // `apply_incremental`: a fenced repair DML advances no checkpoint, yet it must
        // still read rows an old cursor no longer explains, or recovery can never make progress.
        read_across_invalid_boundary: flow_extensions.repair_mode
            == Some(FlowRepairMode::Discovery),
        // The fenced repair DML reads the selected windows completely: no file-level
        // pruning and no forced sequence override, only crossing the invalid boundary
        // and the `B <= H` proof.
        require_flushed_frontier: flow_extensions.repair_mode == Some(FlowRepairMode::Fenced),
        read_row_sequence,
        // A physical-sequence read is the only read that compares a row of an adopted
        // file against the domain its number came from, so it is the only one that needs
        // the creator's cursor slices.
        creator_cursor_shards: read_row_sequence
            .then(|| flow_extensions.creator_cursor_shards())
            .flatten(),
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
        read_across_invalid_boundary: decision.read_across_invalid_boundary,
        require_flushed_frontier: decision.require_flushed_frontier,
        read_row_sequence: decision.read_row_sequence,
        creator_cursor_shards: decision.creator_cursor_shards.clone(),
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
        FLOW_READ_ROW_SEQUENCE, FLOW_RETURN_REGION_SEQ, FLOW_SINK_TABLE_ID,
        encode_incremental_after_seqs,
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

    /// The identity of one half-open expression slice `k0 >= lo AND k0 < hi`, in the form
    /// the cursor wire form carries it.
    fn test_slice_expr(lo: u64, hi: u64) -> String {
        use datatypes::value::Value;
        use partition::expr::{Operand, col as partition_col};

        partition_col("k0")
            .gt_eq(Operand::Value(Value::UInt64(lo)))
            .and(partition_col("k0").lt(Operand::Value(Value::UInt64(hi))))
            .as_json_str()
            .unwrap()
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

    /// The expression-keyed cursor an incremental delta carries: the holder's own rows
    /// start after the least advanced of its expressions, and every creator's expressions
    /// travel with the scan, because a physical read of an adopted file is filtered by the
    /// expression the file belongs to.
    #[test]
    fn test_scan_request_from_slice_cursors_keeps_creator_slices() {
        let region_id = test_region_id();
        let creator = RegionId::new(1024, 7);
        let narrow = test_slice_expr(0, 50);
        let wide = test_slice_expr(50, 100);
        let query_ctx = QueryContextBuilder::default()
            .extensions(HashMap::from([
                (
                    FLOW_INCREMENTAL_MODE.to_string(),
                    FLOW_INCREMENTAL_MODE_SEQUENCE_RANGE.to_string(),
                ),
                (FLOW_READ_ROW_SEQUENCE.to_string(), "true".to_string()),
                (
                    FLOW_INCREMENTAL_AFTER_SEQS.to_string(),
                    encode_incremental_after_seqs(&BTreeMap::from([
                        (
                            region_id.as_u64(),
                            vec![
                                CursorShard::new(Some(narrow.clone()), 10),
                                CursorShard::new(Some(wide.clone()), 3),
                            ],
                        ),
                        (
                            creator.as_u64(),
                            vec![CursorShard::new(Some(wide.clone()), 3)],
                        ),
                    ]))
                    .unwrap(),
                ),
            ]))
            .build();

        let request = scan_request_from_query_context(region_id, &query_ctx).unwrap();

        assert!(request.exact_sequence_range);
        assert!(request.read_row_sequence);
        assert_eq!(
            request.memtable_min_sequence,
            Some(3),
            "a region-level bound starts after the least advanced slice of the region"
        );
        assert_eq!(
            Some(Arc::new(BTreeMap::from([
                (
                    region_id.as_u64(),
                    vec![
                        CursorShard::new(Some(narrow), 10),
                        CursorShard::new(Some(wide.clone()), 3)
                    ]
                ),
                (creator.as_u64(), vec![CursorShard::new(Some(wide), 3)]),
            ]))),
            request.creator_cursor_shards
        );
    }

    /// The region-keyed form is the same statement as one whole-domain slice per region,
    /// which is what keeps a producer of the older shape working.
    #[test]
    fn test_scan_request_from_region_cursors_is_the_full_slice() {
        let region_id = test_region_id();
        let creator = RegionId::new(1024, 7);
        let query_ctx = QueryContextBuilder::default()
            .extensions(HashMap::from([
                (
                    FLOW_INCREMENTAL_MODE.to_string(),
                    FLOW_INCREMENTAL_MODE_SEQUENCE_RANGE.to_string(),
                ),
                (FLOW_READ_ROW_SEQUENCE.to_string(), "true".to_string()),
                (
                    FLOW_INCREMENTAL_AFTER_SEQS.to_string(),
                    format!(
                        r#"{{"{}":10,"{}":3}}"#,
                        region_id.as_u64(),
                        creator.as_u64()
                    ),
                ),
            ]))
            .build();

        let request = scan_request_from_query_context(region_id, &query_ctx).unwrap();

        assert_eq!(request.memtable_min_sequence, Some(10));
        assert_eq!(
            Some(Arc::new(BTreeMap::from([
                (region_id.as_u64(), vec![CursorShard::whole_domain(10)]),
                (creator.as_u64(), vec![CursorShard::whole_domain(3)]),
            ]))),
            request.creator_cursor_shards
        );
    }

    /// A read that does not split adopted files by their physical sequences carries no
    /// creator cursors: no other read compares a number against a foreign domain.
    #[test]
    fn test_scan_request_without_physical_sequence_read_carries_no_creator_cursors() {
        let region_id = test_region_id();
        let query_ctx = QueryContextBuilder::default()
            .extensions(HashMap::from([
                (
                    FLOW_INCREMENTAL_MODE.to_string(),
                    FLOW_INCREMENTAL_MODE_SEQUENCE_RANGE.to_string(),
                ),
                (
                    FLOW_INCREMENTAL_AFTER_SEQS.to_string(),
                    encode_incremental_after_seqs(&BTreeMap::from([(
                        region_id.as_u64(),
                        vec![CursorShard::new(Some(test_slice_expr(0, 50)), 10)],
                    )]))
                    .unwrap(),
                ),
            ]))
            .build();

        let request = scan_request_from_query_context(region_id, &query_ctx).unwrap();

        assert!(!request.read_row_sequence);
        assert_eq!(None, request.creator_cursor_shards);
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
}
