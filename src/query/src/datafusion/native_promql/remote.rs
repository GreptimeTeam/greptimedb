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

//! Frontend-side native instant selection over one frozen distributed route.
//!
//! [`try_select_remote`] runs at the existing experimental gate on the optimized logical plan and
//! the physical plan DataFusion built for it. It admits only the bounded instant shape
//! (`InstantManipulate -> SeriesDivide -> MergeSort -> MergeScan`) whose captured route can be
//! frozen and whose every region approves the same complete binding before any raw read:
//!
//! 1. the optimized logical shape, the physical wrapper chain, and the captured table are
//!    validated locally; any other shape returns the untouched original plan without an RPC;
//! 2. the whole frozen route and the bounded instant declaration are validated locally as well,
//!    so a route this frontend cannot serve and an instant the native mapping cannot evaluate
//!    also return the original plan without an RPC;
//! 3. every frozen region of the captured route is prepared over the exact ordinary remote plan,
//!    so no raw read can start before all of them answered;
//! 4. every approved binding is validated against the authorized frontend table metadata and
//!    against every other approved binding through the existing full-input raw merge contract;
//! 5. the leaf is built only after those preflights and is wrapped by the existing native instant
//!    evaluation, which revalidates the declared frame schema before it runs.
//!
//! Fallback is local and explicit: an unsupported shape, an unsupported approval, or any shape
//! the frontend cannot prove returns the original plan. A preparation RPC failure propagates, and
//! an approval that contradicts the authorized table is an error instead of a silent ordinary
//! execution, because the peer approved exactly this plan's binding. Unsigned reads a peer
//! already approved are either consumed exactly as bound or end as a terminal stream error; the
//! leaf never re-reads, deduplicates, or truncates a region.

use std::collections::HashSet;
use std::sync::Arc;

use api::v1::SemanticType;
use api::v1::region::RegionRequestHeader;
use arrow_schema::{Schema as ArrowSchema, SortOptions};
use common_catalog::consts::MITO_ENGINE;
use common_error::ext::BoxedError;
use common_meta::node_manager::bind_raw_series_query;
use common_query::request::{
    INITIAL_REMOTE_DYN_FILTER_REGISTRATIONS_EXTENSION_KEY, QueryRequest,
    RAW_SERIES_REQUEST_EXTENSION_KEY, RawSeriesRequest,
};
use common_query::series_frame::{
    pack_raw_series, raw_series_frame_ordering, raw_series_frame_schema_with_ordering,
    unpack_raw_series,
};
use common_recordbatch::adapter::DfRecordBatchStreamAdapter;
use common_telemetry::tracing_context::TracingContext;
use common_time::timestamp::TimeUnit;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::DefaultTableSource;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PhysicalExpr, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{DataFusionError, Result as DfResult};
use datafusion_expr::logical_plan::TableScan;
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNode};
use datatypes::data_type::ConcreteDataType;
use datatypes::series::SeriesOrdering;
use datatypes::series::frame::RawLabelMode;
use datatypes::series::samples::{FieldSpec, SeriesSchema};
use futures::{Stream, StreamExt};
use promql::extension_plan::{
    InstantManipulate, InstantManipulateExec, SeriesDivide, SeriesDivideExec,
};
use promql::source::merge_ordered_raw_series;
use session::ReadPreference;
use session::context::QueryContextRef;
use store_api::region_engine::{RawLabelSource, SendableRawSeriesStream};
use store_api::storage::consts::ReservedColumnId;
use store_api::storage::{ColumnId, RegionId};
use table::metadata::TableType;
use table::table::adapter::DfTableProviderAdapter;
use table::table_name::TableName;

use crate::datafusion::native_promql::table_scan_through_safe_nodes;
use crate::datafusion::raw_expr_supported;
use crate::dist_plan::{MergeScanExec, MergeScanLogicalPlan, MergeSortExec, MergeSortLogicalPlan};
use crate::region_query::{RegionQueryHandlerRef, RegionQueryTarget};

/// Selects the remote native path for one bounded instant over a frozen distributed route.
///
/// `Ok(None)` means the ordinary plan must run unchanged; every local check, every unsupported
/// approval, and every route this frontend cannot serve answers this shape. Errors are terminal.
pub(crate) async fn try_select_remote(
    logical: &LogicalPlan,
    original: Arc<dyn ExecutionPlan>,
) -> DfResult<Option<Arc<dyn ExecutionPlan>>> {
    let Some(shape) = RemoteShape::discover(logical) else {
        return Ok(None);
    };
    let Some(physical) = RemotePhysical::discover(&original) else {
        return Ok(None);
    };
    // The frozen handoff view of the captured merge scan is the only source of the route, the
    // ordinary remote plan, the table, the handler, and the query context. Nothing is copied into
    // a second route model.
    let Some(scan) = physical.merge_scan() else {
        return Ok(None);
    };
    let Some(source) = scan.native_raw_source() else {
        return Ok(None);
    };
    let Some(authorized) = authorize(&shape, source.table) else {
        return Ok(None);
    };
    if !physical_order_matches(&shape, &physical, &authorized) {
        return Ok(None);
    }
    // Every route rejection this frontend can decide locally happens before the first RPC, so a
    // later target this frontend cannot serve never leaves an earlier region read behind.
    if !route_targets_authorized(source.route.targets.as_slice(), &authorized) {
        return Ok(None);
    }
    // The bounded instant declaration is checked from the authorized table alone, so an instant
    // the native mapping cannot evaluate returns the ordinary plan without a peer round trip.
    if !instant_is_evaluable(&shape, &physical.instant_schema, &authorized)? {
        return Ok(None);
    }
    let Some(prepared) = prepare_regions(
        source.plan,
        source.route.targets.as_slice(),
        source.region_query_handler,
        source.query_ctx,
        &authorized,
    )
    .await?
    else {
        return Ok(None);
    };
    let Some((prepared, declared)) = derive_regions(prepared)? else {
        return Ok(None);
    };
    let Some(native) = build_native(
        &shape,
        &physical.instant_schema,
        source.region_query_handler,
        &authorized,
        prepared,
        declared,
    )?
    else {
        return Ok(None);
    };
    if physical.root_cooperative {
        return original
            .replace_children(
                vec![native],
                datafusion::physical_plan::ReplaceChildrenOptions::new(
                    datafusion::physical_plan::ChildrenPropertiesMode::Recompute,
                ),
            )
            .map(Some);
    }
    Ok(Some(native))
}

/// The bounded logical shape: one instant, one series divide, one merge sort and one merge scan.
struct RemoteShape<'a> {
    instant: &'a InstantManipulate,
    divide: &'a SeriesDivide,
    merge_sort: &'a MergeSortLogicalPlan,
    scan: &'a TableScan,
}

impl<'a> RemoteShape<'a> {
    /// Discovers the exact shape. Every other operator, and every shape without the captured
    /// scan, declines instead of being unwrapped or traversed.
    fn discover(logical: &'a LogicalPlan) -> Option<Self> {
        let instant_node = extension_node(logical)?;
        let instant = instant_node.as_any().downcast_ref::<InstantManipulate>()?;
        if instant.is_single_evaluation() {
            return None;
        }
        let inputs = instant_node.inputs();
        let [divide_input] = inputs.as_slice() else {
            return None;
        };
        let divide_node = extension_node(divide_input)?;
        let divide = divide_node.as_any().downcast_ref::<SeriesDivide>()?;
        let inputs = divide_node.inputs();
        let [sort_input] = inputs.as_slice() else {
            return None;
        };
        let sort_node = extension_node(sort_input)?;
        let merge_sort = sort_node.as_any().downcast_ref::<MergeSortLogicalPlan>()?;
        // A fetch would turn the merge into a bounded top-k the native path does not perform.
        if merge_sort.fetch.is_some() {
            return None;
        }
        let merge_scan_node = extension_node(&merge_sort.input)?;
        let merge_scan = merge_scan_node
            .as_any()
            .downcast_ref::<MergeScanLogicalPlan>()?;
        if merge_scan.is_placeholder() {
            return None;
        }
        // The remote input tree is only inspected here: the discovered scan is the authorization
        // subject, while the tree itself stays whole and is sent to every region unchanged.
        let scan = table_scan_through_safe_nodes(merge_scan.input())?;
        Some(Self {
            instant,
            divide,
            merge_sort,
            scan,
        })
    }
}

/// The exact ordinary physical wrapper chain above the captured merge scan.
struct RemotePhysical {
    /// Output schema of the ordinary instant operator, which the native path reproduces.
    instant_schema: SchemaRef,
    root_cooperative: bool,
    /// Hash repartition of the merged stream by the logical tags, present when the ordinary plan
    /// parallelizes above the merge sort.
    repartition: Option<Arc<dyn ExecutionPlan>>,
    /// The merge stage that consumes the per-region streams.
    merge_sort: Arc<dyn ExecutionPlan>,
    /// Partition-local sort inserted below the merge sort when the merge scan cannot preserve its
    /// input ordering.
    sort: Option<Arc<dyn ExecutionPlan>>,
    /// The captured merge scan. Its existing handoff view carries the frozen route, the ordinary
    /// remote plan, the table, the handler and the query context, so none of them is copied here.
    scan: Arc<dyn ExecutionPlan>,
}

impl RemotePhysical {
    /// Walks the known wrapper chain explicitly. Any other wrapper at any position declines.
    fn discover(original: &Arc<dyn ExecutionPlan>) -> Option<Self> {
        let (root, root_cooperative) = match original.downcast_ref::<CooperativeExec>() {
            Some(wrapper) => (Arc::clone(wrapper.input()), true),
            None => (Arc::clone(original), false),
        };
        let instant_schema = root.downcast_ref::<InstantManipulateExec>()?.schema();

        let divide = single_child(&root)?;
        divide.downcast_ref::<SeriesDivideExec>()?;
        let mut child = single_child(&divide)?;
        let repartition = if child.downcast_ref::<RepartitionExec>().is_some() {
            let repartition = Arc::clone(&child);
            child = single_child(&repartition)?;
            Some(repartition)
        } else {
            None
        };
        child.downcast_ref::<MergeSortExec>()?;
        let merge_sort = Arc::clone(&child);
        child = single_child(&merge_sort)?;
        let sort = if child.downcast_ref::<SortExec>().is_some() {
            let sort = Arc::clone(&child);
            child = single_child(&sort)?;
            Some(sort)
        } else {
            None
        };
        if child.downcast_ref::<CooperativeExec>().is_some() {
            child = single_child(&child)?;
        }
        child.downcast_ref::<MergeScanExec>()?;
        Some(Self {
            instant_schema,
            root_cooperative,
            repartition,
            merge_sort,
            sort,
            scan: child,
        })
    }

    /// The captured merge scan of this chain.
    fn merge_scan(&self) -> Option<&MergeScanExec> {
        self.scan.downcast_ref::<MergeScanExec>()
    }
}

/// Returns the only child of a plan, or `None` when the plan has any other child count.
fn single_child(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    let children = plan.children();
    let [child] = children.as_slice() else {
        return None;
    };
    Some(Arc::clone(child))
}

fn extension_node(plan: &LogicalPlan) -> Option<&Arc<dyn UserDefinedLogicalNode>> {
    match plan {
        LogicalPlan::Extension(extension) => Some(&extension.node),
        _ => None,
    }
}

/// One authorized frontend column with its authoritative physical identifier and type.
struct AuthorizedColumn {
    id: ColumnId,
    name: String,
    data_type: ConcreteDataType,
}

/// The frontend authorization of the captured table, derived from its own table metadata.
///
/// The authorized output is the whole table: the complete time index, every logical tag in
/// primary-key order, and every value field in its declared field order, each with the
/// authoritative name and type. Nothing is selected or narrowed here.
struct AuthorizedScan {
    table_id: u32,
    metric_name: String,
    tags: Vec<AuthorizedColumn>,
    fields: Vec<AuthorizedColumn>,
    time_index: AuthorizedColumn,
}

/// Authorizes the captured table of the discovered scan against the table of the frozen route.
///
/// `None` means the frontend cannot prove the identity, the physical identifiers, or the bounded
/// output of the captured table, so the ordinary plan must run unchanged.
fn authorize(shape: &RemoteShape<'_>, table: &TableName) -> Option<AuthorizedScan> {
    let scan = shape.scan;
    // The scan must read its entire authorized schema: no projection, no fetch, no statistics
    // request, and no residual filter the raw path cannot evaluate.
    if scan.projection.is_some()
        || scan.fetch.is_some()
        || !scan.statistics_requests.is_empty()
        || scan
            .filters
            .iter()
            .any(|filter| !raw_expr_supported(filter))
    {
        return None;
    }
    let source = scan.source.downcast_ref::<DefaultTableSource>()?;
    let provider = source
        .table_provider
        .downcast_ref::<DfTableProviderAdapter>()?;
    let captured = provider.table();
    let info = captured.table_info();
    // The captured table, not a name lookup, is the authorization subject: a re-resolved table
    // could be a replacement or a logical table of another physical route.
    if captured.table_type() != TableType::Base
        || info.meta.engine != MITO_ENGINE
        || info.catalog_name != table.catalog_name
        || info.schema_name != table.schema_name
        || info.name != table.table_name
    {
        return None;
    }
    // Authoritative name-to-id mapping of the captured table. Missing or misaligned identifiers
    // must decline instead of being invented from a name or an ordinal.
    let ids = info.name_to_ids()?;
    let schema = &info.meta.schema;

    let time_index = schema.timestamp_column()?;
    if time_index.data_type.as_timestamp()?.unit() != TimeUnit::Millisecond {
        return None;
    }

    let mut tags = Vec::with_capacity(info.meta.primary_key_indices.len());
    let mut seen_ids = HashSet::new();
    let mut seen_names = HashSet::new();
    for index in &info.meta.primary_key_indices {
        let column = schema.column_schemas().get(*index)?;
        // A hidden or internal key column would be decoded as a label the ordinary output does
        // not carry, so the bounded shape cannot be preserved.
        if column.name == "__name__"
            || store_api::storage::consts::is_internal_column(&column.name)
            || store_api::metric_engine_consts::is_metric_engine_internal_column(&column.name)
        {
            return None;
        }
        let id = *ids.get(&column.name)?;
        if ReservedColumnId::is_reserved(id)
            || !seen_ids.insert(id)
            || !seen_names.insert(column.name.clone())
        {
            return None;
        }
        tags.push(AuthorizedColumn {
            id,
            name: column.name.clone(),
            data_type: column.data_type.clone(),
        });
    }
    // An unkeyed series cannot be merged across regions into the ordinary per-region rows.
    if tags.is_empty() {
        return None;
    }

    // Every value field of the table is authorized in its declared field order: each one must be
    // an existing Float64 column with a distinct, non-reserved physical identifier.
    let field_names = info.meta.field_column_names().cloned().collect::<Vec<_>>();
    if field_names.is_empty() {
        return None;
    }
    let mut fields = Vec::with_capacity(field_names.len());
    for field_name in &field_names {
        if store_api::storage::consts::is_internal_column(field_name)
            || store_api::metric_engine_consts::is_metric_engine_internal_column(field_name)
        {
            return None;
        }
        let field_column = schema.column_schema_by_name(field_name)?;
        if field_column.data_type != ConcreteDataType::float64_datatype() {
            return None;
        }
        let field_id = *ids.get(field_name)?;
        if ReservedColumnId::is_reserved(field_id) || !seen_ids.insert(field_id) {
            return None;
        }
        fields.push(AuthorizedColumn {
            id: field_id,
            name: field_name.clone(),
            data_type: field_column.data_type.clone(),
        });
    }
    let time_index_id = *ids.get(&time_index.name)?;
    // Every authorized identifier must be a distinct, non-reserved physical column, so the
    // timestamp is inserted into the same `seen_ids` set as every tag and field identifier.
    if ReservedColumnId::is_reserved(time_index_id)
        || !seen_ids.insert(time_index_id)
        || !seen_names.insert(time_index.name.clone())
    {
        return None;
    }

    Some(AuthorizedScan {
        table_id: info.table_id(),
        metric_name: info.name.clone(),
        tags,
        fields,
        time_index: AuthorizedColumn {
            id: time_index_id,
            name: time_index.name.clone(),
            data_type: time_index.data_type.clone(),
        },
    })
}

/// Checks the whole frozen route before the first RPC.
///
/// The route must be non-empty and every target must belong to the authorized table. Because the
/// complete route is decided here, one target this frontend cannot serve returns the ordinary
/// plan instead of leaving earlier regions read.
fn route_targets_authorized(
    targets: &[(RegionId, RegionQueryTarget)],
    authorized: &AuthorizedScan,
) -> bool {
    !targets.is_empty()
        && targets
            .iter()
            .all(|(region_id, _)| region_id.table_id() == authorized.table_id)
}

/// Checks the bounded instant declaration from the authorized table alone, before the first RPC.
///
/// The declaration is built from the authorized frontend field names with no evaluation grid, so
/// this is the same declaration the final plan will carry; the approved peers still validate it
/// and the native plan is still built from their actual label binding, so this decision is
/// eligibility and not authorization. `Ok(false)` returns the ordinary plan without contacting a
/// peer; an error is terminal.
fn instant_is_evaluable(
    shape: &RemoteShape<'_>,
    instant_schema: &SchemaRef,
    authorized: &AuthorizedScan,
) -> DfResult<bool> {
    let series_schema = Arc::new(SeriesSchema {
        fields: authorized
            .fields
            .iter()
            .map(|field| FieldSpec {
                name: Arc::from(field.name.as_str()),
            })
            .collect::<Vec<_>>()
            .into(),
        eval_grid: None,
    });
    let logical_tags = authorized
        .tags
        .iter()
        .map(|tag| tag.name.clone())
        .collect::<Vec<_>>();
    shape.instant.is_native_mapping_supported(
        Arc::clone(instant_schema),
        series_schema,
        &logical_tags,
    )
}

/// Validates the physical ordering operators against the authorized keys.
///
/// The merge sort must require exactly the authorized order; the series divide must split by
/// exactly the authorized tags; a hash repartition, when present, must hash exactly the logical
/// tags; and the partition-local sort, when present, must sort by exactly the same order without
/// a fetch.
fn physical_order_matches(
    shape: &RemoteShape<'_>,
    physical: &RemotePhysical,
    authorized: &AuthorizedScan,
) -> bool {
    let Some(order) = expected_order(&physical.instant_schema, authorized) else {
        return false;
    };
    let tag_names = authorized
        .tags
        .iter()
        .map(|tag| tag.name.as_str())
        .collect::<Vec<_>>();
    if shape.divide.tags() != tag_names.as_slice() || !logical_sort_key_matches(shape, authorized) {
        return false;
    }
    let expected_options = SortOptions {
        descending: false,
        nulls_first: true,
    };

    let Some(merge_sort) = physical.merge_sort.downcast_ref::<MergeSortExec>() else {
        return false;
    };
    if merge_sort.fetch().is_some() {
        return false;
    }
    let requirements = merge_sort.required_input_ordering();
    let [Some(requirement)] = requirements.as_slice() else {
        return false;
    };
    let (alternatives, soft) = requirement.clone().into_alternatives();
    let [requirement] = alternatives.as_slice() else {
        return false;
    };
    if soft
        || requirement.len() != order.len()
        || !requirement.iter().zip(&order).all(|(entry, expected)| {
            entry.options == Some(expected_options) && column_matches(&entry.expr, expected)
        })
    {
        return false;
    }

    if let Some(repartition) = &physical.repartition {
        let Some(repartition) = repartition.downcast_ref::<RepartitionExec>() else {
            return false;
        };
        let Partitioning::Hash(expressions, partitions) = repartition.partitioning() else {
            return false;
        };
        if *partitions == 0
            || expressions.len() != authorized.tags.len()
            || !expressions
                .iter()
                .zip(&order)
                .all(|(expression, expected)| column_matches(expression, expected))
        {
            return false;
        }
    }

    if let Some(sort) = &physical.sort {
        let Some(sort) = sort.downcast_ref::<SortExec>() else {
            return false;
        };
        if sort.fetch().is_some()
            || sort.preserve_partitioning()
            || sort.expr().len() != order.len()
            || !sort.expr().iter().zip(&order).all(|(entry, expected)| {
                entry.options == expected_options && column_matches(&entry.expr, expected)
            })
        {
            return false;
        }
    }

    true
}

/// The exact physical order of the ordinary merged stream: every logical tag in primary-key
/// order, then the time index, ascending with nulls first.
fn expected_order(
    schema: &ArrowSchema,
    authorized: &AuthorizedScan,
) -> Option<Vec<(usize, String)>> {
    let mut order = Vec::with_capacity(authorized.tags.len() + 1);
    for name in authorized
        .tags
        .iter()
        .map(|tag| tag.name.as_str())
        .chain(std::iter::once(authorized.time_index.name.as_str()))
    {
        order.push((schema.index_of(name).ok()?, name.to_string()));
    }
    Some(order)
}

fn column_matches(expr: &Arc<dyn PhysicalExpr>, expected: &(usize, String)) -> bool {
    matches!(
        expr.downcast_ref::<Column>(),
        Some(column) if column.index() == expected.0 && column.name() == expected.1
    )
}

/// The logical merge sort must order by exactly the authorized tags followed by the time index.
fn logical_sort_key_matches(shape: &RemoteShape<'_>, authorized: &AuthorizedScan) -> bool {
    let expr = &shape.merge_sort.expr;
    expr.len() == authorized.tags.len() + 1
        && expr
            .iter()
            .zip(
                authorized
                    .tags
                    .iter()
                    .map(|tag| tag.name.as_str())
                    .chain(std::iter::once(authorized.time_index.name.as_str())),
            )
            .all(|(sort, name)| {
                sort.asc
                    && sort.nulls_first
                    && matches!(&sort.expr, Expr::Column(column) if column.name == name)
            })
}

/// One frozen region with the approved binding its bound execution revalidates.
#[derive(Clone)]
struct PreparedRegion {
    target: RegionQueryTarget,
    /// Marker-free ordinary request; every bound execution re-attaches the marker.
    request: QueryRequest,
    binding: RawSeriesRequest,
}

/// Prepares every frozen target over the exact ordinary remote plan.
///
/// All targets are prepared before the caller may read from any of them. `Ok(None)` is the frozen
/// unsupported answer (a missing approval or a contiguous ordering) and returns the ordinary plan
/// before any raw read. A preparation failure propagates as an error, and an approval that
/// contradicts the authorized table is terminal at this point.
async fn prepare_regions(
    plan: &LogicalPlan,
    targets: &[(RegionId, RegionQueryTarget)],
    handler: &RegionQueryHandlerRef,
    query_ctx: &QueryContextRef,
    authorized: &AuthorizedScan,
) -> DfResult<Option<Vec<PreparedRegion>>> {
    // The frozen targets are region leaders; a non-leader read preference would not be honoured
    // by serving them, so the ordinary plan keeps its own leader selection instead.
    if query_ctx.read_preference() != ReadPreference::Leader {
        return Ok(None);
    }
    // A request that already carries the reserved marker or initial remote dynamic-filter intent
    // cannot be prepared through the raw contract.
    if query_ctx
        .extension(RAW_SERIES_REQUEST_EXTENSION_KEY)
        .is_some()
        || query_ctx
            .extension(INITIAL_REMOTE_DYN_FILTER_REGISTRATIONS_EXTENSION_KEY)
            .is_some()
    {
        return Ok(None);
    }

    let tracing_context = TracingContext::from_current_span();
    let dbname = query_ctx.get_db_string();
    // The whole ordinary query context travels with every request, so catalog, schema, timezone,
    // snapshot and read preference stay exactly the ordinary ones.
    let region_context = api::v1::QueryContext::from(query_ctx.as_ref());

    let mut prepared = Vec::with_capacity(targets.len());
    for (region_id, target) in targets {
        let request = QueryRequest {
            header: Some(RegionRequestHeader {
                tracing_context: tracing_context.to_w3c(),
                dbname: dbname.clone(),
                query_context: Some(region_context.clone()),
            }),
            region_id: *region_id,
            plan: plan.clone(),
        };
        let Some(approved) = handler
            .prepare_named_raw_series_query(target, request.clone())
            .await
            .map_err(|error| DataFusionError::External(Box::new(BoxedError::new(error))))?
        else {
            return Ok(None);
        };
        // Only the read-key-then-time guarantee of a frozen region leader is mergeable.
        if approved.ordering != SeriesOrdering::KeyThenTime {
            return Ok(None);
        }
        if !binding_matches(&approved.binding, authorized, *region_id) {
            return Err(DataFusionError::Plan(format!(
                "native raw-series binding for region {region_id} contradicts the authorized table"
            )));
        }
        prepared.push(PreparedRegion {
            target: target.clone(),
            request,
            binding: approved.binding,
        });
    }
    Ok(Some(prepared))
}

/// Validates one approved binding against the authorized frontend table.
///
/// The approved binding describes the actual authorized region scanner, so it must describe
/// exactly the authorized output: the same region, metric name, every authorized field in
/// declared field order, millisecond time index, and the same physical key columns in the
/// authorized key order. Every approved column is compared against the authorized name and type
/// of the column with that identifier, because the existing cross-region merge only compares
/// peers with each other and cannot notice that all of them approve the same wrong type. Tag
/// names and ids are compared as the merge contract compares them (by column id, because the
/// label decoder resolves every logical tag by its physical identifier), while the key order is
/// compared exactly because it decides the order of the merged stream.
fn binding_matches(
    binding: &RawSeriesRequest,
    authorized: &AuthorizedScan,
    region_id: RegionId,
) -> bool {
    if binding.version != 1 || binding.physical_metadata.region_id != region_id {
        return false;
    }
    if binding.metric_name != authorized.metric_name
        || binding.logical_tags.len() != authorized.tags.len()
    {
        return false;
    }
    let mut seen_tags = HashSet::new();
    for tag in &binding.logical_tags {
        let Some(expected) = authorized
            .tags
            .iter()
            .find(|expected| expected.id == tag.column_id && expected.name == tag.name)
        else {
            return false;
        };
        // The approved metadata must describe the authorized tag itself, not just a name and an
        // identifier that agree with it.
        let Some(column) = binding.physical_metadata.column_by_id(tag.column_id) else {
            return false;
        };
        if column.semantic_type != SemanticType::Tag
            || column.column_schema.name != expected.name
            || column.column_schema.data_type != expected.data_type
            || !seen_tags.insert(tag.column_id)
        {
            return false;
        }
    }
    let expected_field_ids = authorized
        .fields
        .iter()
        .map(|field| field.id)
        .collect::<Vec<_>>();
    if binding.field_ids != expected_field_ids {
        return false;
    }
    let expected_key = authorized.tags.iter().map(|tag| tag.id).collect::<Vec<_>>();
    if binding.physical_metadata.primary_key != expected_key {
        return false;
    }

    let metadata = &binding.physical_metadata;
    for expected in &authorized.fields {
        let Some(field) = metadata.column_by_id(expected.id) else {
            return false;
        };
        if field.semantic_type != SemanticType::Field
            || field.column_schema.name != expected.name
            || field.column_schema.data_type != expected.data_type
        {
            return false;
        }
    }
    let time_index = metadata.time_index_column();
    time_index.column_id == authorized.time_index.id
        && time_index.column_schema.name == authorized.time_index.name
        && time_index.column_schema.data_type == authorized.time_index.data_type
        && time_index.semantic_type == SemanticType::Timestamp
}

/// The one raw frame every approved region binding declares, taken from the first real binding.
struct DeclaredFrame {
    /// Packed frame schema the leaf declares.
    schema: SchemaRef,
    series_schema: Arc<SeriesSchema>,
    label_source: RawLabelSource,
}

/// Derives the declared raw frame and validates the cross-region contract.
///
/// The existing full-input raw merge owns the compatibility rules (equal field names, equal
/// primary-key label bindings, identical key encodings, read-key-then-time ordering), so it is
/// called once on the empty declared streams of the actual bindings instead of duplicating its
/// checks. Every input that reaches it is a peer's approved binding, so a frame or binding the
/// shared codec rejects is a terminal error rather than a silent ordinary execution.
fn derive_regions(
    prepared: Vec<PreparedRegion>,
) -> DfResult<Option<(Vec<PreparedRegion>, DeclaredFrame)>> {
    let mut declared = None;
    let mut inputs: Vec<SendableRawSeriesStream> = Vec::with_capacity(prepared.len());
    for region in &prepared {
        let (_, series_schema, label_source) =
            bind_raw_series_query(region.request.clone(), &region.binding)
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let frame = raw_series_frame_schema_with_ordering(
            &series_schema,
            RawLabelMode::Key,
            SeriesOrdering::KeyThenTime,
        )?;
        let empty = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&frame),
            futures::stream::empty(),
        ));
        let raw = unpack_raw_series(
            empty,
            Arc::clone(&series_schema),
            label_source.clone(),
            SeriesOrdering::KeyThenTime,
        )?;
        if declared.is_none() {
            declared = Some(DeclaredFrame {
                schema: frame,
                series_schema,
                label_source,
            });
        }
        inputs.push(raw);
    }
    let Some(declared) = declared else {
        return Ok(None);
    };
    // The full-input merge validates that every approved binding declares one shared frame.
    merge_ordered_raw_series(inputs).map_err(|error| DataFusionError::External(Box::new(error)))?;
    Ok(Some((prepared, declared)))
}

/// Builds the private leaf and wraps it with the existing native instant evaluation.
fn build_native(
    shape: &RemoteShape<'_>,
    instant_schema: &SchemaRef,
    handler: &RegionQueryHandlerRef,
    authorized: &AuthorizedScan,
    prepared: Vec<PreparedRegion>,
    declared: DeclaredFrame,
) -> DfResult<Option<Arc<dyn ExecutionPlan>>> {
    let logical_tags = authorized
        .tags
        .iter()
        .map(|tag| tag.name.clone())
        .collect::<Vec<_>>();
    let leaf: Arc<dyn ExecutionPlan> = Arc::new(NativeRawMergeExec::new(
        prepared,
        Arc::clone(handler),
        declared.schema,
    ));
    // The existing native instant owns the complete global evaluation and validates the declared
    // frame schema without executing the leaf.
    shape.instant.try_to_native_execution_plan(
        leaf,
        Arc::clone(instant_schema),
        declared.label_source,
        declared.series_schema,
        &logical_tags,
    )
}

/// Single-partition leaf that owns every frozen target of one native raw merge.
///
/// `execute(0)` opens all frozen targets with their bound point-in-time requests and then
/// delegates the complete cross-region merge and framing to the existing full-input raw merge and
/// frame packer. Every request, open stream and buffer is owned by the returned stream, so
/// dropping it cancels in-flight work. Any open, schema, ordering, or stream error is terminal.
struct NativeRawMergeExec {
    inputs: Vec<PreparedRegion>,
    handler: RegionQueryHandlerRef,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl NativeRawMergeExec {
    fn new(inputs: Vec<PreparedRegion>, handler: RegionQueryHandlerRef, schema: SchemaRef) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            inputs,
            handler,
            schema,
            properties,
        }
    }

    /// Opens one frozen target under the existing bound raw-series contract.
    ///
    /// The bound execution revalidates the binding against the actual scanner and attaches the
    /// reserved marker, so a binding that stopped matching this point-in-time plan fails here
    /// instead of reading a differently bound region.
    async fn open(
        target: RegionQueryTarget,
        handler: RegionQueryHandlerRef,
        request: QueryRequest,
        binding: RawSeriesRequest,
    ) -> DfResult<SendableRawSeriesStream> {
        let (request, series_schema, label_source) = bind_raw_series_query(request, &binding)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let frames = handler
            .do_get(&target, request)
            .await
            .map_err(|error| DataFusionError::External(Box::new(BoxedError::new(error))))?;
        let ordering = raw_series_frame_ordering(frames.schema().arrow_schema().as_ref())?;
        if ordering != SeriesOrdering::KeyThenTime {
            return Err(DataFusionError::Execution(format!(
                "native raw merge requires read-key-then-time frames, got {ordering:?}"
            )));
        }
        let frames: SendableRecordBatchStream = Box::pin(DfRecordBatchStreamAdapter::new(frames));
        unpack_raw_series(frames, series_schema, label_source, ordering)
    }
}

impl std::fmt::Debug for NativeRawMergeExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeRawMergeExec")
            .field("regions", &self.inputs.len())
            .finish()
    }
}

impl DisplayAs for NativeRawMergeExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "NativeRawMergeExec: regions={}", self.inputs.len())
    }
}

impl ExecutionPlan for NativeRawMergeExec {
    fn name(&self) -> &str {
        "NativeRawMergeExec"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DfResult<TreeNodeRecursion>,
    ) -> DfResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Plan(
                "NativeRawMergeExec is a leaf".to_string(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "native raw merge has one partition, requested {partition}"
            )));
        }
        // The executing task carries the trace and database of the ordinary merge scan request.
        let dbname = context.task_id().unwrap_or_default();
        let tracing_context = TracingContext::from_json(context.session_id().as_str());
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            merged_stream(
                self.inputs.clone(),
                Arc::clone(&self.handler),
                dbname,
                tracing_context,
            ),
        )))
    }
}

/// Opens every frozen target and then packs the complete cross-region merge.
///
/// The merged stream is produced by the existing full-input raw merge, so the leaf adds no
/// merging, deduplication, or truncation rule of its own.
fn merged_stream(
    inputs: Vec<PreparedRegion>,
    handler: RegionQueryHandlerRef,
    dbname: String,
    tracing_context: TracingContext,
) -> impl Stream<Item = DfResult<RecordBatch>> + Send + 'static {
    async_stream::try_stream! {
        // Every frozen target is opened before the merge consumes any of them.
        let mut opened = Vec::with_capacity(inputs.len());
        for input in &inputs {
            let mut request = input.request.clone();
            if let Some(header) = request.header.as_mut() {
                header.dbname = dbname.clone();
                header.tracing_context = tracing_context.to_w3c();
            }
            opened.push(
                NativeRawMergeExec::open(
                    input.target.clone(),
                    Arc::clone(&handler),
                    request,
                    input.binding.clone(),
                )
                .await?,
            );
        }
        let merged = merge_ordered_raw_series(opened)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let mut packed = pack_raw_series(merged)?;
        while let Some(batch) = packed.next().await {
            yield batch?;
        }
    }
}

#[cfg(test)]
#[path = "remote_tests.rs"]
mod tests;
