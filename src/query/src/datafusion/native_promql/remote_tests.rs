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

//! Fixtures and tests for the remote native instant consumer.
//!
//! The fixtures build the ordinary logical shape and the ordinary physical wrapper chain of a
//! partitioned base table, so every rejection and every preflight is exercised against real
//! operators; only the region peers are fakes. A frozen route cannot be attached from this
//! module, because the capture type is private to the distributed planner, so the route-wide and
//! declaration-wide pre-RPC decisions are driven directly and `try_select_remote` itself is
//! covered for the shapes it must return unchanged without contacting a peer.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use api::v1::region::{RemoteDynFilterUnregister, RemoteDynFilterUpdate};
use arrow::array::Float64Array;
use arrow::buffer::Buffer;
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, TimeUnit as ArrowTimeUnit};
use async_trait::async_trait;
use common_catalog::consts::MITO_ENGINE;
use common_meta::peer::Peer;
use common_query::request::{PreparedNamedRawSeries, RawSeriesTag};
use common_recordbatch::SendableRecordBatchStream;
use common_recordbatch::adapter::RecordBatchStreamAdapter as DfToGreptimeAdapter;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion_common::Column as LogicalColumn;
use datafusion_expr::{Extension, LogicalPlanBuilder, SortExpr, col, lit};
use datatypes::schema::{ColumnSchema, Schema};
use datatypes::series::raw::{RawSeriesChunk, RawTimestamps};
use datatypes::series::samples::{SampleRef, Samples, SeriesFields};
use promql::extension_plan::{InstantManipulate, SeriesDivide};
use session::context::{QueryContext, QueryContextBuilder};
use store_api::metadata::{ColumnMetadata, RegionMetadata, RegionMetadataBuilder};
use store_api::region_engine::RawSeriesStream;
use table::TableRef;
use table::metadata::{TableInfoBuilder, TableMetaBuilder, TableVersion};
use table::test_util::EmptyTable;
use tokio::sync::oneshot;

use crate::dist_plan::RemoteDynFilterProducerId;
use crate::dist_plan::planner::CaptureNativeRawRoute;
use crate::error::UnimplementedSnafu;
use crate::region_query::RegionQueryHandler;

use super::*;

const TABLE_ID: u32 = 1024;
const OTHER_TABLE_ID: u32 = 2048;
const METRIC_NAME: &str = "native_time_parts";
const HOST_COLUMN_ID: ColumnId = 1;
const VALUE_COLUMN_ID: ColumnId = 2;
const TS_COLUMN_ID: ColumnId = 0;
const REGION_ONE_PEER: u64 = 11;
const REGION_TWO_PEER: u64 = 12;
const SNAPSHOT_SEQUENCE: u64 = 42;

fn region(number: u32) -> RegionId {
    RegionId::new(TABLE_ID, number)
}

fn peer(id: u64) -> Peer {
    Peer {
        id,
        addr: format!("127.0.0.1:{}", 4000 + id),
    }
}

fn table_name() -> TableName {
    TableName::new("greptime", "public", METRIC_NAME)
}

fn physical_schema() -> SchemaRef {
    physical_schema_with(ArrowDataType::Utf8)
}

/// The physical schema of the ordinary merged stream: timestamp at 0, tag at 1, field at 2.
fn physical_schema_with(tag_type: ArrowDataType) -> SchemaRef {
    Arc::new(ArrowSchema::new(vec![
        ArrowField::new(
            "ts",
            ArrowDataType::Timestamp(ArrowTimeUnit::Millisecond, None),
            false,
        ),
        ArrowField::new("host", tag_type, true),
        ArrowField::new("v", ArrowDataType::Float64, false),
    ]))
}

fn sort_options() -> SortOptions {
    SortOptions {
        descending: false,
        nulls_first: true,
    }
}

fn ordering() -> LexOrdering {
    LexOrdering::new(vec![
        PhysicalSortExpr::new(Arc::new(Column::new("host", 1)), sort_options()),
        PhysicalSortExpr::new(Arc::new(Column::new("ts", 0)), sort_options()),
    ])
    .unwrap()
}

/// Builds the authorized frontend table of the fixture.
fn fe_table_with(
    engine: &str,
    schema: Arc<Schema>,
    column_ids: Vec<ColumnId>,
    primary_key_indices: Vec<usize>,
    value_indices: Vec<usize>,
) -> TableRef {
    let meta = TableMetaBuilder::empty()
        .schema(schema)
        .primary_key_indices(primary_key_indices)
        .value_indices(value_indices)
        .column_ids(column_ids)
        .engine(engine)
        .next_column_id(3)
        .options(Default::default())
        .created_on(Default::default())
        .build()
        .unwrap();
    let info = TableInfoBuilder::default()
        .table_id(TABLE_ID)
        .table_version(0 as TableVersion)
        .name(METRIC_NAME.to_string())
        .schema_name("public".to_string())
        .catalog_name("greptime".to_string())
        .desc(None)
        .table_type(TableType::Base)
        .meta(meta)
        .build()
        .unwrap();
    EmptyTable::from_table_info(&info)
}

fn fe_schema(time_index: ConcreteDataType) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        ColumnSchema::new("ts", time_index, false).with_time_index(true),
        ColumnSchema::new("host", ConcreteDataType::string_datatype(), true),
        ColumnSchema::new("v", ConcreteDataType::float64_datatype(), false),
    ]))
}

fn fe_table(engine: &str, column_ids: Vec<ColumnId>) -> TableRef {
    fe_table_with(
        engine,
        fe_schema(ConcreteDataType::timestamp_millisecond_datatype()),
        column_ids,
        vec![1],
        vec![2],
    )
}

/// The authorized frontend table of the fixture with the given `host` tag type.
fn fe_table_with_tag_type(tag_type: ConcreteDataType) -> TableRef {
    let schema = Arc::new(Schema::new(vec![
        ColumnSchema::new(
            "ts",
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        )
        .with_time_index(true),
        ColumnSchema::new("host", tag_type, true),
        ColumnSchema::new("v", ConcreteDataType::float64_datatype(), false),
    ]));
    fe_table_with(
        MITO_ENGINE,
        schema,
        vec![TS_COLUMN_ID, HOST_COLUMN_ID, VALUE_COLUMN_ID],
        vec![1],
        vec![2],
    )
}

/// The region metadata one frozen leader would report for the fixture schema.
fn region_metadata(region_id: RegionId) -> Arc<RegionMetadata> {
    let mut builder = RegionMetadataBuilder::new(region_id);
    builder
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            semantic_type: SemanticType::Timestamp,
            column_id: TS_COLUMN_ID,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new("host", ConcreteDataType::string_datatype(), true),
            semantic_type: SemanticType::Tag,
            column_id: HOST_COLUMN_ID,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new("v", ConcreteDataType::float64_datatype(), false),
            semantic_type: SemanticType::Field,
            column_id: VALUE_COLUMN_ID,
        })
        .primary_key(vec![HOST_COLUMN_ID]);
    Arc::new(builder.build().unwrap())
}

/// The approved metadata column with the given physical identifier.
fn column(binding: &mut RawSeriesRequest, column_id: ColumnId) -> &mut ColumnMetadata {
    binding
        .physical_metadata
        .column_metadatas
        .iter_mut()
        .find(|column| column.column_id == column_id)
        .expect("the approved metadata carries the fixture columns")
}

fn tag_column(binding: &mut RawSeriesRequest) -> &mut ColumnMetadata {
    column(binding, HOST_COLUMN_ID)
}

fn field_column(binding: &mut RawSeriesRequest) -> &mut ColumnMetadata {
    column(binding, VALUE_COLUMN_ID)
}

fn time_column(binding: &mut RawSeriesRequest) -> &mut ColumnMetadata {
    column(binding, TS_COLUMN_ID)
}

/// The binding a peer approves for one frozen region.
fn binding(region_id: RegionId) -> RawSeriesRequest {
    RawSeriesRequest {
        version: 1,
        physical_metadata: (*region_metadata(region_id)).clone(),
        metric_name: METRIC_NAME.to_string(),
        logical_tags: vec![RawSeriesTag {
            column_id: HOST_COLUMN_ID,
            name: "host".to_string(),
        }],
        field_ids: vec![VALUE_COLUMN_ID],
    }
}

/// The ordered, deliberately non-lexical field names of the multifloat fixtures.
const MULTIFLOAT_FIELD_NAMES: [&str; 3] = ["value", "a_second", "m_third"];

/// The physical identifiers of the multifloat fields, in declared field order.
const MULTIFLOAT_FIELD_IDS: [ColumnId; 3] = [
    VALUE_COLUMN_ID,
    SECOND_VALUE_COLUMN_ID,
    THIRD_VALUE_COLUMN_ID,
];

/// The physical identifier of the second ordered field of the multifloat fixtures.
const SECOND_VALUE_COLUMN_ID: ColumnId = 3;

/// The physical identifier of the third ordered field of the multifloat fixtures.
const THIRD_VALUE_COLUMN_ID: ColumnId = 4;

/// A reserved physical identifier no field column may carry.
const RESERVED_FIELD_ID: ColumnId = store_api::storage::consts::ReservedColumnId::version();

/// The declared fields of a multifloat fixture with the given field count.
fn multifloat_fields(field_count: usize) -> Vec<(&'static str, ColumnId)> {
    MULTIFLOAT_FIELD_NAMES
        .iter()
        .copied()
        .zip(MULTIFLOAT_FIELD_IDS)
        .take(field_count)
        .collect()
}

/// The authorized frontend table of an ordered-field fixture, in declared field order.
fn multifloat_table(fields: &[(&str, ColumnId)]) -> TableRef {
    let mut columns = vec![
        ColumnSchema::new(
            "ts",
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        )
        .with_time_index(true),
        ColumnSchema::new("host", ConcreteDataType::string_datatype(), true),
    ];
    columns.extend(
        fields
            .iter()
            .map(|(name, _)| ColumnSchema::new(*name, ConcreteDataType::float64_datatype(), false)),
    );
    let mut column_ids = vec![TS_COLUMN_ID, HOST_COLUMN_ID];
    column_ids.extend(fields.iter().map(|(_, id)| *id));
    let value_indices = (2..2 + fields.len()).collect::<Vec<_>>();
    fe_table_with(
        MITO_ENGINE,
        Arc::new(Schema::new(columns)),
        column_ids,
        vec![1],
        value_indices,
    )
}

/// The region metadata one frozen leader reports for an ordered-field fixture.
fn multifloat_region_metadata(
    region_id: RegionId,
    fields: &[(&str, ColumnId)],
) -> Arc<RegionMetadata> {
    let mut builder = RegionMetadataBuilder::new(region_id);
    builder
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            semantic_type: SemanticType::Timestamp,
            column_id: TS_COLUMN_ID,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new("host", ConcreteDataType::string_datatype(), true),
            semantic_type: SemanticType::Tag,
            column_id: HOST_COLUMN_ID,
        });
    for (name, id) in fields {
        builder.push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(*name, ConcreteDataType::float64_datatype(), false),
            semantic_type: SemanticType::Field,
            column_id: *id,
        });
    }
    builder.primary_key(vec![HOST_COLUMN_ID]);
    Arc::new(builder.build().unwrap())
}

/// The binding one frozen peer approves for an ordered-field fixture: every declared field in
/// declared, non-lexical order.
fn multifloat_binding(region_id: RegionId, fields: &[(&str, ColumnId)]) -> RawSeriesRequest {
    RawSeriesRequest {
        version: 1,
        physical_metadata: (*multifloat_region_metadata(region_id, fields)).clone(),
        metric_name: METRIC_NAME.to_string(),
        logical_tags: vec![RawSeriesTag {
            column_id: HOST_COLUMN_ID,
            name: "host".to_string(),
        }],
        field_ids: fields.iter().map(|(_, id)| *id).collect(),
    }
}

/// One bounded instant declaration of the fixture.
#[derive(Clone, Copy)]
struct Instant {
    start: i64,
    end: i64,
    lookback: i64,
    interval: i64,
    offset: i64,
}

impl Instant {
    const ORDINARY: Self = Self {
        start: 1000,
        end: 4000,
        lookback: 0,
        interval: 1000,
        offset: 0,
    };

    fn with_offset(self, offset: i64) -> Self {
        Self { offset, ..self }
    }

    fn reversed(self) -> Self {
        Self {
            start: self.end,
            end: self.start,
            ..self
        }
    }

    fn without_interval(self) -> Self {
        Self {
            interval: 0,
            ..self
        }
    }
}

/// The ordinary optimized logical shape: instant, divide, merge sort, merge scan, table scan.
fn logical_plan(table: TableRef) -> LogicalPlan {
    logical_plan_with(table, Instant::ORDINARY)
}

fn logical_plan_with(table: TableRef, instant: Instant) -> LogicalPlan {
    let provider = Arc::new(DfTableProviderAdapter::new(table));
    let source = Arc::new(DefaultTableSource::new(provider));
    let scan = LogicalPlanBuilder::scan(METRIC_NAME, source, None)
        .unwrap()
        .build()
        .unwrap();
    // The analyzer assigns one producer id to every merge scan, so the ordinary shape carries
    // one even when no filter is ever pushed down to it.
    let merge_scan = MergeScanLogicalPlan::new(scan, false)
        .with_remote_dyn_filter_producer_id(RemoteDynFilterProducerId::new(1))
        .into_logical_plan();
    let merge_sort = MergeSortLogicalPlan::new(
        Arc::new(merge_scan),
        vec![
            SortExpr::new(Expr::Column(LogicalColumn::from_name("host")), true, true),
            SortExpr::new(Expr::Column(LogicalColumn::from_name("ts")), true, true),
        ],
        None,
    )
    .into_logical_plan();
    let divide = LogicalPlan::Extension(Extension {
        node: Arc::new(SeriesDivide::new(
            vec!["host".to_string()],
            "ts".to_string(),
            merge_sort,
        )),
    });
    LogicalPlan::Extension(Extension {
        node: Arc::new(InstantManipulate::new(
            instant.start,
            instant.end,
            instant.lookback,
            instant.interval,
            instant.offset,
            "ts".to_string(),
            vec!["host".to_string()],
            Some("v".to_string()),
            divide,
        )),
    })
}

/// The exact ordinary remote plan of the fixture: the captured scan under a pushed filter.
fn remote_plan(shape: &RemoteShape<'_>) -> LogicalPlan {
    LogicalPlanBuilder::from(LogicalPlan::TableScan(shape.scan.clone()))
        .filter(col("host").eq(lit("a")))
        .unwrap()
        .build()
        .unwrap()
}

/// The ordinary frozen route, in the exact dispatch order of the ordinary merge scan.
fn ordinary_route() -> Vec<(RegionId, RegionQueryTarget)> {
    vec![
        (region(1), RegionQueryTarget::new(peer(REGION_ONE_PEER))),
        (region(2), RegionQueryTarget::new(peer(REGION_TWO_PEER))),
    ]
}

/// The authorization of the ordinary fixture table against the ordinary frozen route.
fn authorized_table(shape: &RemoteShape<'_>) -> AuthorizedScan {
    authorize(shape, &table_name()).expect("the fixture table is authorized")
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Chain {
    /// One target partition: a partition-local sort under a root cooperative wrapper.
    P1,
    /// Four target partitions: a hash repartition of the logical tags above the merge sort.
    P4,
}

/// The ordinary physical wrapper chain over one captured merge scan.
struct OrdinaryPhysical {
    plan: Arc<dyn ExecutionPlan>,
    /// Output schema of the ordinary instant operator, read to check the wrapper walk against the
    /// ordinary plan.
    instant_schema: SchemaRef,
}

fn ordinary_physical(
    shape: &RemoteShape<'_>,
    handler: RegionQueryHandlerRef,
    query_ctx: QueryContextRef,
    chain: Chain,
    arrow_tag_type: ArrowDataType,
    captured_route: Option<Vec<(RegionId, RegionQueryTarget)>>,
) -> OrdinaryPhysical {
    let arrow_schema = physical_schema_with(arrow_tag_type);
    let regions = ordinary_route()
        .iter()
        .map(|(region_id, _)| *region_id)
        .collect::<Vec<_>>();
    let session_state = SessionStateBuilder::new().with_default_features().build();
    let merge_scan = MergeScanExec::new(
        &session_state,
        table_name(),
        regions,
        LogicalPlan::TableScan(shape.scan.clone()),
        arrow_schema.as_ref(),
        handler,
        query_ctx,
        1,
        // The analyzer assigns this identity to every merge scan; it says nothing about whether
        // any filter was pushed down to it.
        Some(RemoteDynFilterProducerId::new(1)),
        false,
    )
    .unwrap();
    // The distributed planner attaches the route it captured from the same physical route that
    // selected the regions. A case attaches the same artifact to drive the frontend gate over a
    // captured route; the real capture stays covered by the distributed planner and route tests.
    let merge_scan = match captured_route {
        Some(targets) => merge_scan.with_native_raw_route(CaptureNativeRawRoute { targets }),
        None => merge_scan,
    };

    let mut plan: Arc<dyn ExecutionPlan> = Arc::new(merge_scan);
    if chain == Chain::P1 {
        plan = Arc::new(CooperativeExec::new(plan));
        plan = Arc::new(SortExec::new(ordering(), plan));
    }
    plan = Arc::new(MergeSortExec::new(ordering(), plan, None));
    if chain == Chain::P4 {
        plan = Arc::new(
            RepartitionExec::try_new(
                plan,
                Partitioning::Hash(vec![Arc::new(Column::new("host", 1))], 4),
            )
            .unwrap(),
        );
    }
    plan = shape.divide.to_execution_plan(plan);
    plan = shape.instant.to_execution_plan(plan);
    if chain == Chain::P1 {
        plan = Arc::new(CooperativeExec::new(plan));
    }

    OrdinaryPhysical {
        instant_schema: plan.downcast_ref::<CooperativeExec>().map_or_else(
            || {
                plan.downcast_ref::<InstantManipulateExec>()
                    .unwrap()
                    .schema()
            },
            |wrapper| {
                wrapper
                    .input()
                    .downcast_ref::<InstantManipulateExec>()
                    .unwrap()
                    .schema()
            },
        ),
        plan,
    }
}

/// The ordinary chain with the wrapper walk's result over it.
fn admitted_physical(
    shape: &RemoteShape<'_>,
    handler: RegionQueryHandlerRef,
    query_ctx: QueryContextRef,
    chain: Chain,
) -> (RemotePhysical, OrdinaryPhysical) {
    let built = ordinary_physical(shape, handler, query_ctx, chain, ArrowDataType::Utf8, None);
    let physical =
        RemotePhysical::discover(&built.plan).expect("the ordinary chain is admitted by the walk");
    (physical, built)
}

/// The ordinary query context of the fixture, with one snapshot to carry.
fn query_context() -> QueryContextRef {
    let query_ctx = QueryContext::arc();
    query_ctx.set_snapshot(region(1).as_u64(), SNAPSHOT_SEQUENCE);
    query_ctx
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Call {
    Prepare(u64),
    RawRead(u64),
    OrdinaryRead,
}

/// One recorded call with the exact request the consumer sent when it made one.
struct RecordedCall {
    call: Call,
    request: Option<QueryRequest>,
}

/// One fake region leader with the approval and the raw payload it answers with.
struct FakeRegion {
    peer: u64,
    /// `None` is the frozen unsupported answer.
    approval: Option<RawSeriesRequest>,
    ordering: SeriesOrdering,
    /// `(read key, timestamps, field values)` of the frames this peer returns.
    chunks: Vec<(Vec<u8>, Vec<i64>, Vec<f64>)>,
    fail: bool,
    dropped: Arc<AtomicBool>,
}

impl FakeRegion {
    fn new(peer: u64, approval: Option<RawSeriesRequest>) -> Self {
        Self {
            peer,
            approval,
            ordering: SeriesOrdering::KeyThenTime,
            chunks: Vec::new(),
            fail: false,
            dropped: Arc::new(AtomicBool::new(false)),
        }
    }

    fn with_chunks(mut self, chunks: Vec<(Vec<u8>, Vec<i64>, Vec<f64>)>) -> Self {
        self.chunks = chunks;
        self
    }

    fn with_ordering(mut self, ordering: SeriesOrdering) -> Self {
        self.ordering = ordering;
        self
    }

    fn failing(mut self) -> Self {
        self.fail = true;
        self
    }
}

struct FakeHandler {
    regions: Vec<FakeRegion>,
    calls: Mutex<Vec<RecordedCall>>,
    /// Raw reads the test holds pending, keyed by peer. A read takes its gate while it waits, so
    /// cancelling the read drops the receiver and closes the test's sender.
    gates: Mutex<HashMap<u64, oneshot::Receiver<()>>>,
    /// Fails every preparation and every raw read.
    fails: bool,
}

impl FakeHandler {
    fn new(regions: Vec<FakeRegion>) -> Self {
        Self {
            regions,
            calls: Mutex::new(Vec::new()),
            gates: Mutex::new(HashMap::new()),
            fails: false,
        }
    }

    /// Fails every preparation and every raw read, so a plan that declines locally can be shown
    /// never to reach a peer at all.
    fn failing(regions: Vec<FakeRegion>) -> Self {
        Self {
            fails: true,
            ..Self::new(regions)
        }
    }

    /// Holds this peer's next raw read until the returned sender is released or dropped.
    fn hold_read(&self, peer: u64) -> oneshot::Sender<()> {
        let (sender, receiver) = oneshot::channel();
        self.gates.lock().unwrap().insert(peer, receiver);
        sender
    }

    fn record(&self, call: Call, request: Option<QueryRequest>) {
        self.calls
            .lock()
            .unwrap()
            .push(RecordedCall { call, request });
    }

    /// The calls in order, without their requests.
    fn calls(&self) -> Vec<Call> {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .map(|recorded| recorded.call)
            .collect()
    }

    /// The exact request of every preparation and raw read, in dispatch order.
    fn requests(&self) -> Vec<(Call, QueryRequest)> {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter_map(|recorded| {
                recorded
                    .request
                    .clone()
                    .map(|request| (recorded.call, request))
            })
            .collect()
    }

    fn region(&self, peer: &Peer) -> &FakeRegion {
        self.regions
            .iter()
            .find(|region| region.peer == peer.id)
            .expect("unknown peer")
    }

    fn unavailable(&self) -> crate::error::Error {
        UnimplementedSnafu {
            operation: "fake region failure".to_string(),
        }
        .build()
    }

    /// Builds the raw frames of one region through the shared frame packer.
    fn frames(&self, region: &FakeRegion) -> SendableRecordBatchStream {
        let approved = region
            .approval
            .as_ref()
            .expect("a reading region is approved");
        let (series_schema, label_source) = approved
            .bind(Arc::new(approved.physical_metadata.clone()))
            .expect("the approved binding is valid");
        let chunks = region
            .chunks
            .iter()
            .map(|(key, times, values)| {
                RawSeriesChunk::try_new(
                    &series_schema,
                    Buffer::from(key.clone()),
                    None,
                    RawTimestamps::try_new(times.clone().into()).unwrap(),
                    SeriesFields::one(Samples::from_float(Float64Array::from(values.clone()))),
                    true,
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let stream = TestRawSeriesStream {
            schema: series_schema,
            label_source,
            ordering: SeriesOrdering::KeyThenTime,
            chunks: chunks.into_iter().map(Ok).collect(),
            dropped: Arc::clone(&region.dropped),
        };
        let frames = pack_raw_series(Box::pin(stream)).unwrap();
        Box::pin(DfToGreptimeAdapter::try_new(frames).unwrap())
    }
}

#[async_trait]
impl RegionQueryHandler for FakeHandler {
    async fn select_target(
        &self,
        _read_preference: ReadPreference,
        _region_id: RegionId,
    ) -> crate::error::Result<RegionQueryTarget> {
        self.record(Call::OrdinaryRead, None);
        Err(self.unavailable())
    }

    async fn do_get(
        &self,
        target: &RegionQueryTarget,
        request: QueryRequest,
    ) -> crate::error::Result<SendableRecordBatchStream> {
        self.record(Call::RawRead(target.peer().id), Some(request));
        if self.fails {
            return Err(self.unavailable());
        }
        // The gate is taken out of the map before it is awaited, so no guard is held across the
        // await point and the handler future stays sendable.
        let gate = self.gates.lock().unwrap().remove(&target.peer().id);
        if let Some(gate) = gate {
            let _ = gate.await;
        }
        let region = self.region(target.peer());
        if region.fail {
            return Err(self.unavailable());
        }
        Ok(self.frames(region))
    }

    async fn prepare_named_raw_series_query(
        &self,
        target: &RegionQueryTarget,
        request: QueryRequest,
    ) -> crate::error::Result<Option<PreparedNamedRawSeries>> {
        self.record(Call::Prepare(target.peer().id), Some(request));
        if self.fails {
            return Err(self.unavailable());
        }
        let region = self.region(target.peer());
        Ok(region
            .approval
            .as_ref()
            .map(|binding| PreparedNamedRawSeries {
                binding: binding.clone(),
                ordering: region.ordering,
            }))
    }

    async fn handle_remote_dyn_filter_update(
        &self,
        _target: &RegionQueryTarget,
        _query_id: String,
        _update: RemoteDynFilterUpdate,
    ) -> crate::error::Result<()> {
        Ok(())
    }

    async fn handle_remote_dyn_filter_unregister(
        &self,
        _target: &RegionQueryTarget,
        _query_id: String,
        _unregister: RemoteDynFilterUnregister,
    ) -> crate::error::Result<()> {
        Ok(())
    }
}

/// A finite raw-series stream over test chunks that reports when it is dropped.
struct TestRawSeriesStream {
    schema: Arc<SeriesSchema>,
    label_source: RawLabelSource,
    ordering: SeriesOrdering,
    chunks: Vec<Result<RawSeriesChunk, BoxedError>>,
    dropped: Arc<AtomicBool>,
}

impl Stream for TestRawSeriesStream {
    type Item = Result<RawSeriesChunk, BoxedError>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.chunks.is_empty() {
            return Poll::Ready(None);
        }
        Poll::Ready(Some(self.chunks.remove(0)))
    }
}

impl RawSeriesStream for TestRawSeriesStream {
    fn schema(&self) -> &Arc<SeriesSchema> {
        &self.schema
    }

    fn label_source(&self) -> &RawLabelSource {
        &self.label_source
    }

    fn ordering(&self) -> SeriesOrdering {
        self.ordering
    }
}

impl Drop for TestRawSeriesStream {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}

/// The decoded field values of one chunk, rejecting non-float samples.
fn float_values(chunk: &RawSeriesChunk) -> Vec<Option<f64>> {
    chunk
        .fields
        .get(0)
        .expect("one field")
        .iter()
        .map(|sample| match sample {
            SampleRef::Float(value) => Some(value),
            SampleRef::Missing => None,
            SampleRef::Histogram(_) => panic!("the fixture has no histogram samples"),
        })
        .collect()
}

/// The decoded payload of one raw stream.
async fn payload(
    mut stream: SendableRawSeriesStream,
) -> Vec<(Vec<u8>, Vec<i64>, Vec<Option<f64>>, bool)> {
    let mut chunks = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.expect("the merged payload is valid");
        chunks.push((
            chunk.primary_key.as_slice().to_vec(),
            chunk.time.as_buffer().to_vec(),
            float_values(&chunk),
            chunk.is_last,
        ));
    }
    chunks
}

/// The declaration one built native plan carries, kept for payload assertions.
struct Declaration {
    frame: SchemaRef,
    series_schema: Arc<SeriesSchema>,
    label_source: RawLabelSource,
}

/// One fully built native plan with the fixtures its assertions need.
struct NativeFixture {
    plan: LogicalPlan,
    prepared: Vec<PreparedRegion>,
    declaration: Declaration,
    native: Arc<dyn ExecutionPlan>,
}

/// Prepares, derives, and builds the native plan of one approved two-region fixture.
async fn native_fixture(fake: &Arc<FakeHandler>, query_ctx: &QueryContextRef) -> NativeFixture {
    let table = fe_table(
        MITO_ENGINE,
        vec![TS_COLUMN_ID, HOST_COLUMN_ID, VALUE_COLUMN_ID],
    );
    let logical = logical_plan(table.clone());
    let shape = RemoteShape::discover(&logical).expect("the ordinary shape is discoverable");
    let handler: RegionQueryHandlerRef = fake.clone();
    let (physical, built) = admitted_physical(
        &shape,
        Arc::clone(&handler),
        Arc::clone(query_ctx),
        Chain::P1,
    );
    assert_eq!(
        built.instant_schema, physical.instant_schema,
        "the wrapper walk reads the schema of the ordinary instant operator"
    );
    let authorized = authorized_table(&shape);
    assert!(route_targets_authorized(&ordinary_route(), &authorized));
    let plan = remote_plan(&shape);
    let prepared = prepare_regions(&plan, &ordinary_route(), &handler, query_ctx, &authorized)
        .await
        .expect("preparation does not fail")
        .expect("every region is approved");
    let (prepared, declared) = derive_regions(prepared)
        .expect("the approved bindings agree")
        .expect("one shared frame");
    let declaration = Declaration {
        frame: Arc::clone(&declared.schema),
        series_schema: Arc::clone(&declared.series_schema),
        label_source: declared.label_source.clone(),
    };
    let native = build_native(
        &shape,
        &physical.instant_schema,
        &handler,
        &authorized,
        prepared.clone(),
        declared,
    )
    .expect("the native plan is built")
    .expect("the native instant accepts the declared frame");

    NativeFixture {
        plan,
        prepared,
        declaration,
        native,
    }
}

#[test]
fn native_raw_admits_the_ordinary_p1_and_p4_chains() {
    let table = fe_table(
        MITO_ENGINE,
        vec![TS_COLUMN_ID, HOST_COLUMN_ID, VALUE_COLUMN_ID],
    );
    let logical = logical_plan(table.clone());
    let shape = RemoteShape::discover(&logical).expect("the ordinary shape is discoverable");

    for chain in [Chain::P1, Chain::P4] {
        let handler: RegionQueryHandlerRef = Arc::new(FakeHandler::new(Vec::new()));
        let (physical, _) = admitted_physical(&shape, handler, QueryContext::arc(), chain);
        let authorized = authorized_table(&shape);
        assert!(
            physical_order_matches(&shape, &physical, &authorized),
            "{chain:?} must keep the authorized order"
        );
        let scan = physical
            .merge_scan()
            .expect("the walk ends in the captured scan");
        assert!(
            scan.native_raw_source().is_none(),
            "this fixture has no frozen route, which only the distributed planner attaches"
        );
    }
}

#[test]
fn native_raw_declines_chains_it_cannot_prove() {
    let table = fe_table(
        MITO_ENGINE,
        vec![TS_COLUMN_ID, HOST_COLUMN_ID, VALUE_COLUMN_ID],
    );
    let logical = logical_plan(table.clone());
    let shape = RemoteShape::discover(&logical).expect("the ordinary shape is discoverable");
    let handler: RegionQueryHandlerRef = Arc::new(FakeHandler::new(Vec::new()));
    let (physical, _) = admitted_physical(&shape, handler.clone(), QueryContext::arc(), Chain::P1);
    let authorized = authorized_table(&shape);
    assert!(physical_order_matches(&shape, &physical, &authorized));

    // A physical chain with any other wrapper above the merge scan is not admitted.
    let wrapped: Arc<dyn ExecutionPlan> = Arc::new(
        datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec::new(Arc::clone(
            &physical.merge_sort,
        )),
    );
    assert!(RemotePhysical::discover(&wrapped).is_none());

    // A merge sort that requires a fetch is a bounded top-k the native path does not perform.
    let (mut fetched, _) =
        admitted_physical(&shape, handler.clone(), QueryContext::arc(), Chain::P1);
    let leaf = Arc::clone(physical.merge_sort.children()[0]);
    fetched.merge_sort = Arc::new(MergeSortExec::new(ordering(), leaf, Some(1)));
    assert!(!physical_order_matches(&shape, &fetched, &authorized));

    // A merge sort that requires a different order cannot prove the authorized key order.
    let (mut descending, _) =
        admitted_physical(&shape, handler.clone(), QueryContext::arc(), Chain::P1);
    let leaf = Arc::clone(physical.merge_sort.children()[0]);
    descending.merge_sort = Arc::new(MergeSortExec::new(
        LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new("host", 1)),
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        )])
        .unwrap(),
        leaf,
        None,
    ));
    assert!(!physical_order_matches(&shape, &descending, &authorized));

    // A hash repartition of a non-tag column does not dispatch the authorized series.
    let (mut wrong_hash, _) =
        admitted_physical(&shape, handler.clone(), QueryContext::arc(), Chain::P1);
    wrong_hash.repartition = Some(Arc::new(
        RepartitionExec::try_new(
            Arc::clone(&wrong_hash.merge_sort),
            Partitioning::Hash(vec![Arc::new(Column::new("v", 2))], 4),
        )
        .unwrap(),
    ));
    assert!(!physical_order_matches(&shape, &wrong_hash, &authorized));

    // A divide that splits by other tags cannot reuse the authorized order.
    let other = LogicalPlan::TableScan(shape.scan.clone());
    let merge_scan = MergeScanLogicalPlan::new(other, false).into_logical_plan();
    let merge_sort = MergeSortLogicalPlan::new(
        Arc::new(merge_scan),
        vec![
            SortExpr::new(Expr::Column(LogicalColumn::from_name("host")), true, true),
            SortExpr::new(Expr::Column(LogicalColumn::from_name("ts")), true, true),
        ],
        None,
    )
    .into_logical_plan();
    let divide = SeriesDivide::new(vec!["v".to_string()], "ts".to_string(), merge_sort);
    let other_shape = RemoteShape {
        instant: shape.instant,
        divide: &divide,
        merge_sort: shape.merge_sort,
        scan: shape.scan,
    };
    assert!(!physical_order_matches(
        &other_shape,
        &physical,
        &authorized
    ));
}

#[test]
fn authorize_declines_tables_it_cannot_identify() {
    let logical_table = fe_table(
        MITO_ENGINE,
        vec![TS_COLUMN_ID, HOST_COLUMN_ID, VALUE_COLUMN_ID],
    );

    // Missing column ids cannot be invented.
    let unknown_ids = fe_table(MITO_ENGINE, Vec::new());
    let logical = logical_plan(unknown_ids);
    let shape = RemoteShape::discover(&logical).expect("discoverable");
    assert!(authorize(&shape, &table_name()).is_none());

    // Another engine has no native raw contract.
    let memory = fe_table(
        "memory",
        vec![TS_COLUMN_ID, HOST_COLUMN_ID, VALUE_COLUMN_ID],
    );
    let logical = logical_plan(memory);
    let shape = RemoteShape::discover(&logical).expect("discoverable");
    assert!(authorize(&shape, &table_name()).is_none());

    // A route table that does not match the captured table is not authorized.
    let logical = logical_plan(logical_table);
    let shape = RemoteShape::discover(&logical).expect("discoverable");
    let replacement = TableName::new("greptime", "public", "replacement");
    assert!(authorize(&shape, &replacement).is_none());

    // A non-millisecond time index cannot be evaluated natively.
    let seconds = fe_table_with(
        MITO_ENGINE,
        fe_schema(ConcreteDataType::timestamp_second_datatype()),
        vec![TS_COLUMN_ID, HOST_COLUMN_ID, VALUE_COLUMN_ID],
        vec![1],
        vec![2],
    );
    let logical = logical_plan(seconds);
    let shape = RemoteShape::discover(&logical).expect("discoverable");
    assert!(authorize(&shape, &table_name()).is_none());
}

#[test]
fn multifloat_ordered_fields_are_authorized_and_bound() {
    // A second field is no longer outside the bounded instant evaluation, so the reclaimed
    // rejection becomes an accepted control for the declared field order.
    for field_count in [2, 3] {
        let fields = multifloat_fields(field_count);
        let logical = logical_plan(multifloat_table(&fields));
        let shape = RemoteShape::discover(&logical).expect("discoverable");
        let authorized = authorize(&shape, &table_name()).unwrap_or_else(|| {
            panic!("{field_count} ordered Float64 fields are authorized, not declined")
        });
        let approved = multifloat_binding(region(1), &fields);
        assert_eq!(
            approved.field_ids,
            MULTIFLOAT_FIELD_IDS[..field_count].to_vec(),
            "every declared field travels in declared, non-lexical order"
        );
        assert!(
            binding_matches(&approved, &authorized, region(1)),
            "{field_count} complete ordered approvals are accepted"
        );
    }
}

#[test]
fn multifloat_binding_mutations_and_illegal_tables_are_rejected() {
    let fields = multifloat_fields(3);
    let logical = logical_plan(multifloat_table(&fields));
    let shape = RemoteShape::discover(&logical).expect("discoverable");
    let authorized = authorize(&shape, &table_name()).expect("three ordered fields are authorized");
    let approved = multifloat_binding(region(1), &fields);
    assert!(binding_matches(&approved, &authorized, region(1)));

    let mut only_first = approved.clone();
    only_first.field_ids = vec![VALUE_COLUMN_ID];
    let mut only_second = approved.clone();
    only_second.field_ids = vec![SECOND_VALUE_COLUMN_ID];
    let mut missing_last = approved.clone();
    missing_last.field_ids = vec![VALUE_COLUMN_ID, SECOND_VALUE_COLUMN_ID];
    let mut extra = approved.clone();
    extra.field_ids = vec![
        VALUE_COLUMN_ID,
        SECOND_VALUE_COLUMN_ID,
        THIRD_VALUE_COLUMN_ID,
        99,
    ];
    let mut duplicated = approved.clone();
    duplicated.field_ids = vec![
        VALUE_COLUMN_ID,
        SECOND_VALUE_COLUMN_ID,
        SECOND_VALUE_COLUMN_ID,
    ];
    let mut lexically_ordered = approved.clone();
    lexically_ordered.field_ids = vec![
        SECOND_VALUE_COLUMN_ID,
        THIRD_VALUE_COLUMN_ID,
        VALUE_COLUMN_ID,
    ];
    let mut reversed = approved.clone();
    reversed.field_ids = vec![
        THIRD_VALUE_COLUMN_ID,
        SECOND_VALUE_COLUMN_ID,
        VALUE_COLUMN_ID,
    ];
    let mut renamed = approved.clone();
    column(&mut renamed, SECOND_VALUE_COLUMN_ID)
        .column_schema
        .name = "renamed".to_string();
    let mut wrong_semantics = approved.clone();
    column(&mut wrong_semantics, SECOND_VALUE_COLUMN_ID).semantic_type = SemanticType::Tag;
    let mut wrong_type = approved.clone();
    column(&mut wrong_type, THIRD_VALUE_COLUMN_ID)
        .column_schema
        .data_type = ConcreteDataType::timestamp_millisecond_datatype();

    for (reason, mutation) in [
        ("an approval that approves only the first field", only_first),
        (
            "an approval that approves only the second field",
            only_second,
        ),
        ("an approval that drops the last field", missing_last),
        ("an approval with an extra field identifier", extra),
        ("an approval with a duplicated field identifier", duplicated),
        (
            "an approval ordered by field name instead of declared order",
            lexically_ordered,
        ),
        ("an approval in reverse declared order", reversed),
        ("an approval with a renamed field column", renamed),
        (
            "an approval with a field column of another semantic type",
            wrong_semantics,
        ),
        (
            "an approval with a field column of another type",
            wrong_type,
        ),
    ] {
        assert!(
            !binding_matches(&mutation, &authorized, region(1)),
            "{reason}"
        );
    }

    // The field identifiers of the table itself must stay distinct, non-reserved and free of
    // internal columns, so no ordered binding can be authorized from them.
    let illegal_tables: [(&str, &[(&str, ColumnId)]); 5] = [
        (
            "a second field that collides with the timestamp identifier",
            &[("value", VALUE_COLUMN_ID), ("a_second", TS_COLUMN_ID)][..],
        ),
        (
            "a second field that collides with another field identifier",
            &[("value", VALUE_COLUMN_ID), ("a_second", VALUE_COLUMN_ID)][..],
        ),
        (
            "an internal field name",
            &[
                ("value", VALUE_COLUMN_ID),
                ("__sequence", SECOND_VALUE_COLUMN_ID),
            ][..],
        ),
        (
            "a metric-engine internal field name",
            &[
                ("value", VALUE_COLUMN_ID),
                ("__tsid", SECOND_VALUE_COLUMN_ID),
            ][..],
        ),
        (
            "a reserved field identifier",
            &[("value", VALUE_COLUMN_ID), ("a_second", RESERVED_FIELD_ID)][..],
        ),
    ];
    for (reason, fields) in illegal_tables {
        let logical = logical_plan(multifloat_table(fields));
        let shape = RemoteShape::discover(&logical).expect("discoverable");
        assert!(authorize(&shape, &table_name()).is_none(), "{reason}");
    }
}

#[tokio::test]
async fn multifloat_approvals_prepare_and_a_contradicting_second_field_is_terminal() {
    let fields = multifloat_fields(2);
    let logical = logical_plan(multifloat_table(&fields));
    let shape = RemoteShape::discover(&logical).expect("discoverable");
    let authorized = authorize(&shape, &table_name()).expect("two ordered fields are authorized");
    let plan = remote_plan(&shape);
    let query_ctx = query_context();

    // The accepted control: every frozen region approves the same complete ordered binding.
    let accepting = Arc::new(FakeHandler::new(vec![
        FakeRegion::new(
            REGION_ONE_PEER,
            Some(multifloat_binding(region(1), &fields)),
        ),
        FakeRegion::new(
            REGION_TWO_PEER,
            Some(multifloat_binding(region(2), &fields)),
        ),
    ]));
    let handler: RegionQueryHandlerRef = accepting.clone();
    let prepared = prepare_regions(&plan, &ordinary_route(), &handler, &query_ctx, &authorized)
        .await
        .expect("an ordered-field approval is not terminal")
        .expect("every region approved the complete ordered binding");
    assert_eq!(prepared.len(), 2);
    assert_eq!(
        accepting.calls(),
        vec![
            Call::Prepare(REGION_ONE_PEER),
            Call::Prepare(REGION_TWO_PEER)
        ],
        "both ordered approvals are collected before any raw read"
    );

    // A contradiction in the second approved field stays terminal instead of becoming a silent
    // ordinary execution, and it is reported before any raw read starts.
    let mut contradicted = multifloat_binding(region(2), &fields);
    column(&mut contradicted, SECOND_VALUE_COLUMN_ID)
        .column_schema
        .data_type = ConcreteDataType::int32_datatype();
    let fake = Arc::new(FakeHandler::new(vec![
        FakeRegion::new(
            REGION_ONE_PEER,
            Some(multifloat_binding(region(1), &fields)),
        ),
        FakeRegion::new(REGION_TWO_PEER, Some(contradicted)),
    ]));
    let handler: RegionQueryHandlerRef = fake.clone();
    assert!(
        prepare_regions(&plan, &ordinary_route(), &handler, &query_ctx, &authorized)
            .await
            .is_err(),
        "a contradicting second field is terminal"
    );
    assert_eq!(
        fake.calls(),
        vec![
            Call::Prepare(REGION_ONE_PEER),
            Call::Prepare(REGION_TWO_PEER)
        ],
        "the contradiction is reported after both preparations and before any raw read"
    );
}

#[test]
fn binding_matches_rejects_contradicting_approvals() {
    let table = fe_table(
        MITO_ENGINE,
        vec![TS_COLUMN_ID, HOST_COLUMN_ID, VALUE_COLUMN_ID],
    );
    let logical = logical_plan(table);
    let shape = RemoteShape::discover(&logical).expect("discoverable");
    let authorized = authorized_table(&shape);
    let approved = binding(region(1));
    assert!(binding_matches(&approved, &authorized, region(1)));

    assert!(!binding_matches(&approved, &authorized, region(2)));

    let mut other_metric = approved.clone();
    other_metric.metric_name = "other".to_string();
    assert!(!binding_matches(&other_metric, &authorized, region(1)));

    let mut other_tag = approved.clone();
    other_tag.logical_tags[0].column_id = VALUE_COLUMN_ID;
    assert!(!binding_matches(&other_tag, &authorized, region(1)));

    let mut renamed_tag = approved.clone();
    renamed_tag.logical_tags[0].name = "zone".to_string();
    assert!(!binding_matches(&renamed_tag, &authorized, region(1)));

    let mut other_field = approved.clone();
    other_field.field_ids = vec![TS_COLUMN_ID];
    assert!(!binding_matches(&other_field, &authorized, region(1)));

    let mut extra_field = approved.clone();
    extra_field.field_ids = vec![VALUE_COLUMN_ID, TS_COLUMN_ID];
    assert!(!binding_matches(&extra_field, &authorized, region(1)));

    // The approved metadata must describe the authorized column itself: a peer whose tag column
    // carries the authorized name and identifier but another type is not the authorized scanner.
    let mut other_tag_semantics = approved.clone();
    tag_column(&mut other_tag_semantics).semantic_type = SemanticType::Field;
    assert!(!binding_matches(
        &other_tag_semantics,
        &authorized,
        region(1)
    ));

    let mut other_tag_type = approved.clone();
    tag_column(&mut other_tag_type).column_schema.data_type = ConcreteDataType::int64_datatype();
    assert!(!binding_matches(&other_tag_type, &authorized, region(1)));

    let mut other_field_type = approved.clone();
    field_column(&mut other_field_type).column_schema.data_type =
        ConcreteDataType::int32_datatype();
    assert!(!binding_matches(&other_field_type, &authorized, region(1)));

    let mut other_time_type = approved.clone();
    time_column(&mut other_time_type).column_schema.data_type =
        ConcreteDataType::timestamp_nanosecond_datatype();
    assert!(!binding_matches(&other_time_type, &authorized, region(1)));

    // The raw read key order decides the order of the merged stream.
    let mut other_key_order = approved.clone();
    other_key_order.physical_metadata.primary_key = vec![VALUE_COLUMN_ID];
    assert!(!binding_matches(&other_key_order, &authorized, region(1)));

    let mut unknown_version = approved;
    unknown_version.version = 2;
    assert!(!binding_matches(&unknown_version, &authorized, region(1)));
}

#[test]
fn the_whole_route_decides_before_any_peer_round_trip() {
    let table = fe_table(
        MITO_ENGINE,
        vec![TS_COLUMN_ID, HOST_COLUMN_ID, VALUE_COLUMN_ID],
    );
    let logical = logical_plan(table);
    let shape = RemoteShape::discover(&logical).expect("discoverable");
    let authorized = authorized_table(&shape);

    // An empty route has nothing to read.
    assert!(!route_targets_authorized(&[], &authorized));
    // A later target of another table must reject the whole route, not only after the earlier
    // regions were already read.
    let mut foreign = ordinary_route();
    foreign.push((
        RegionId::new(OTHER_TABLE_ID, 3),
        RegionQueryTarget::new(peer(13)),
    ));
    assert!(!route_targets_authorized(&foreign, &authorized));
    // The ordinary route is servable.
    assert!(route_targets_authorized(&ordinary_route(), &authorized));
}

#[test]
fn the_instant_declaration_decides_locally() {
    let table = fe_table(
        MITO_ENGINE,
        vec![TS_COLUMN_ID, HOST_COLUMN_ID, VALUE_COLUMN_ID],
    );

    let ordinary = logical_plan(table.clone());
    let shape = RemoteShape::discover(&ordinary).expect("discoverable");
    let authorized = authorized_table(&shape);
    assert!(
        instant_is_evaluable(&shape, &physical_schema(), &authorized).unwrap(),
        "the ordinary declaration is evaluable"
    );

    // An offset moves the declaration outside the bounded evaluation.
    let offset = logical_plan_with(table.clone(), Instant::ORDINARY.with_offset(500));
    let shape = RemoteShape::discover(&offset).expect("discoverable");
    assert!(!instant_is_evaluable(&shape, &physical_schema(), &authorized).unwrap());

    // A reversed range has no evaluation grid.
    let reversed = logical_plan_with(table.clone(), Instant::ORDINARY.reversed());
    let shape = RemoteShape::discover(&reversed).expect("discoverable");
    assert!(!instant_is_evaluable(&shape, &physical_schema(), &authorized).unwrap());

    // A tag that both the authorized table and the ordinary output declare as a non-string is
    // not a bounded tag column, so the declaration decides on the tag type itself.
    let int_tag = logical_plan(fe_table_with_tag_type(ConcreteDataType::int64_datatype()));
    let shape = RemoteShape::discover(&int_tag).expect("discoverable");
    let int_authorized = authorized_table(&shape);
    assert!(
        !instant_is_evaluable(
            &shape,
            &physical_schema_with(ArrowDataType::Int64),
            &int_authorized
        )
        .unwrap()
    );

    // An invalid step is a terminal error instead of a silent ordinary execution.
    let invalid = logical_plan_with(table, Instant::ORDINARY.without_interval());
    let shape = RemoteShape::discover(&invalid).expect("discoverable");
    assert!(instant_is_evaluable(&shape, &physical_schema(), &authorized).is_err());
}

#[tokio::test]
async fn try_select_remote_returns_the_original_plan_without_a_round_trip() {
    let table = fe_table(
        MITO_ENGINE,
        vec![TS_COLUMN_ID, HOST_COLUMN_ID, VALUE_COLUMN_ID],
    );
    let logical = logical_plan(table.clone());
    let shape = RemoteShape::discover(&logical).expect("discoverable");
    let fake = Arc::new(FakeHandler::failing(vec![]));
    let handler: RegionQueryHandlerRef = fake.clone();
    let built = ordinary_physical(
        &shape,
        handler,
        query_context(),
        Chain::P1,
        ArrowDataType::Utf8,
        None,
    );

    // This chain carries no frozen route, and every shape outside the bounded instant is
    // declined: the ordinary plan must come back untouched and no peer may be contacted at all.
    let selected = try_select_remote(&logical, Arc::clone(&built.plan))
        .await
        .expect("declining is not an error");
    assert!(selected.is_none(), "the ordinary plan must run unchanged");
    assert!(
        fake.calls().is_empty(),
        "a declined plan never prepares or reads a region"
    );

    let empty = LogicalPlanBuilder::empty(false).build().unwrap();
    let selected = try_select_remote(&empty, Arc::clone(&built.plan))
        .await
        .expect("declining is not an error");
    assert!(selected.is_none());
    assert!(fake.calls().is_empty());
}

/// One route-bearing case of the frontend selection gate.
struct GateCase {
    /// The case name of every assertion.
    name: &'static str,
    /// The bounded instant declaration the gate must judge.
    instant: Instant,
    /// The `host` tag type of the authorized frontend table and of the ordinary output.
    tag_type: ConcreteDataType,
    /// The matching tag type of the ordinary merged stream.
    arrow_tag_type: ArrowDataType,
    /// The frozen route the captured scan carries.
    route: Vec<(RegionId, RegionQueryTarget)>,
    /// `true` when the declaration is admissible, so the case must reach the armed peer.
    reaches_a_peer: bool,
}

#[tokio::test]
async fn the_selection_gate_declines_locally_and_reaches_an_armed_peer_once() {
    let mut foreign_route = ordinary_route();
    foreign_route.push((
        RegionId::new(OTHER_TABLE_ID, 3),
        RegionQueryTarget::new(peer(13)),
    ));
    let string = ConcreteDataType::string_datatype();
    let cases = [
        GateCase {
            name: "an offset declaration",
            instant: Instant::ORDINARY.with_offset(500),
            tag_type: string.clone(),
            arrow_tag_type: ArrowDataType::Utf8,
            route: ordinary_route(),
            reaches_a_peer: false,
        },
        GateCase {
            name: "a reversed range",
            instant: Instant::ORDINARY.reversed(),
            tag_type: string.clone(),
            arrow_tag_type: ArrowDataType::Utf8,
            route: ordinary_route(),
            reaches_a_peer: false,
        },
        GateCase {
            name: "a non-string tag in the table and the output",
            instant: Instant::ORDINARY,
            tag_type: ConcreteDataType::int64_datatype(),
            arrow_tag_type: ArrowDataType::Int64,
            route: ordinary_route(),
            reaches_a_peer: false,
        },
        GateCase {
            name: "a later region of another table",
            instant: Instant::ORDINARY,
            tag_type: string,
            arrow_tag_type: ArrowDataType::Utf8,
            route: foreign_route,
            reaches_a_peer: false,
        },
        GateCase {
            name: "an admissible declaration",
            instant: Instant::ORDINARY,
            tag_type: ConcreteDataType::string_datatype(),
            arrow_tag_type: ArrowDataType::Utf8,
            route: ordinary_route(),
            reaches_a_peer: true,
        },
    ];

    for case in cases {
        let GateCase {
            name,
            instant,
            tag_type,
            arrow_tag_type,
            route,
            reaches_a_peer,
        } = case;
        let logical = logical_plan_with(fe_table_with_tag_type(tag_type), instant);
        let shape = RemoteShape::discover(&logical)
            .unwrap_or_else(|| panic!("{name}: the ordinary shape is discoverable"));
        // Every peer call fails, so a case that declines locally must never produce one and the
        // admissible case shows that this fixture really reaches the peer.
        let fake = Arc::new(FakeHandler::failing(vec![]));
        let handler: RegionQueryHandlerRef = fake.clone();
        let built = ordinary_physical(
            &shape,
            handler,
            query_context(),
            Chain::P1,
            arrow_tag_type,
            Some(route),
        );
        let selected = try_select_remote(&logical, Arc::clone(&built.plan)).await;
        if reaches_a_peer {
            assert!(
                selected.is_err(),
                "{name}: the armed peer is reached and its failure is terminal"
            );
            assert_eq!(
                fake.calls(),
                vec![Call::Prepare(REGION_ONE_PEER)],
                "{name}: an admissible declaration reaches the peer exactly once"
            );
        } else {
            assert!(
                selected.expect("declining is not an error").is_none(),
                "{name}: the ordinary plan must run unchanged"
            );
            assert!(
                fake.calls().is_empty(),
                "{name}: a locally declined case never contacts a peer"
            );
        }
    }
}

#[tokio::test]
async fn reserved_intent_in_the_query_context_declines_before_the_first_peer_call() {
    let table = fe_table(
        MITO_ENGINE,
        vec![TS_COLUMN_ID, HOST_COLUMN_ID, VALUE_COLUMN_ID],
    );
    let logical = logical_plan(table);
    let shape = RemoteShape::discover(&logical).expect("discoverable");

    // `None` is the unmarked control of the same route-bearing case: it must select the native
    // plan, so the empty call list of a marked case is decided by the marker itself.
    let cases = [
        (None, true),
        (Some((RAW_SERIES_REQUEST_EXTENSION_KEY, "")), false),
        (
            Some((RAW_SERIES_REQUEST_EXTENSION_KEY, "not-a-binding")),
            false,
        ),
        (
            Some((INITIAL_REMOTE_DYN_FILTER_REGISTRATIONS_EXTENSION_KEY, "")),
            false,
        ),
        (
            Some((
                INITIAL_REMOTE_DYN_FILTER_REGISTRATIONS_EXTENSION_KEY,
                "not-a-registration",
            )),
            false,
        ),
    ];

    for (marker, reaches_a_peer) in cases {
        // Every peer approves and would serve this route, so only the incoming marker can
        // decide the case.
        let fake = Arc::new(FakeHandler::new(vec![
            FakeRegion::new(REGION_ONE_PEER, Some(binding(region(1)))),
            FakeRegion::new(REGION_TWO_PEER, Some(binding(region(2)))),
        ]));
        let handler: RegionQueryHandlerRef = fake.clone();
        let mut query_ctx = QueryContextBuilder::default();
        if let Some((key, value)) = marker {
            query_ctx = query_ctx.set_extension(key.to_string(), value.to_string());
        }
        let built = ordinary_physical(
            &shape,
            handler,
            Arc::new(query_ctx.build()),
            Chain::P1,
            ArrowDataType::Utf8,
            Some(ordinary_route()),
        );

        let selected = try_select_remote(&logical, Arc::clone(&built.plan)).await;
        if reaches_a_peer {
            assert!(
                selected
                    .expect("the unmarked route prepares successfully")
                    .is_some(),
                "the unmarked route must select native execution"
            );
            assert_eq!(
                fake.calls(),
                vec![
                    Call::Prepare(REGION_ONE_PEER),
                    Call::Prepare(REGION_TWO_PEER)
                ],
                "the unmarked route prepares every region without opening a raw read"
            );
        } else {
            assert!(
                selected.expect("declining is not an error").is_none(),
                "{marker:?}: reserved intent keeps the ordinary plan"
            );
            assert!(
                fake.calls().is_empty(),
                "{marker:?}: reserved intent is decided before the first peer call"
            );
        }
    }
}

#[tokio::test]
async fn every_region_prepares_before_the_first_raw_read_and_the_payload_is_raw() {
    let regions = vec![
        FakeRegion::new(REGION_ONE_PEER, Some(binding(region(1)))).with_chunks(vec![(
            b"a".to_vec(),
            vec![1000, 2000],
            vec![1.0, 2.0],
        )]),
        FakeRegion::new(REGION_TWO_PEER, Some(binding(region(2)))).with_chunks(vec![(
            b"b".to_vec(),
            vec![3000],
            vec![3.0],
        )]),
    ];
    let handler = Arc::new(FakeHandler::new(regions));
    let query_ctx = query_context();
    let fixture = native_fixture(&handler, &query_ctx).await;

    assert_eq!(
        handler.calls(),
        vec![
            Call::Prepare(REGION_ONE_PEER),
            Call::Prepare(REGION_TWO_PEER)
        ],
        "no raw read may start before the complete prepared set"
    );
    assert_eq!(fixture.native.name(), "NativeInstantManipulateExec");
    assert!(
        fixture.native.children()[0].name() == "NativeRawMergeExec",
        "the native instant wraps the private raw merge leaf"
    );

    let leaf = Arc::clone(fixture.native.children()[0]);
    assert_eq!(
        leaf.schema(),
        fixture.declaration.frame,
        "the leaf declares exactly the frame the approved bindings derived"
    );
    let frames = leaf.execute(0, Arc::new(TaskContext::default())).unwrap();
    let raw = unpack_raw_series(
        frames,
        Arc::clone(&fixture.declaration.series_schema),
        fixture.declaration.label_source.clone(),
        SeriesOrdering::KeyThenTime,
    )
    .unwrap();
    assert_eq!(
        payload(raw).await,
        vec![
            (
                b"a".to_vec(),
                vec![1000, 2000],
                vec![Some(1.0), Some(2.0)],
                true
            ),
            (b"b".to_vec(), vec![3000], vec![Some(3.0)], true),
        ]
    );
    assert_eq!(
        handler.calls(),
        vec![
            Call::Prepare(REGION_ONE_PEER),
            Call::Prepare(REGION_TWO_PEER),
            Call::RawRead(REGION_ONE_PEER),
            Call::RawRead(REGION_TWO_PEER),
        ],
        "each frozen region is read exactly once and never replayed"
    );

    // Every preparation carries the exact ordinary remote plan and the whole ordinary query
    // context without the reserved marker; every bound read carries the marker of the binding
    // that was actually approved for that region.
    let requests = handler.requests();
    let expected = [
        (Call::Prepare(REGION_ONE_PEER), region(1), false),
        (Call::Prepare(REGION_TWO_PEER), region(2), false),
        (Call::RawRead(REGION_ONE_PEER), region(1), true),
        (Call::RawRead(REGION_TWO_PEER), region(2), true),
    ];
    assert_eq!(requests.len(), expected.len());
    for ((call, request), (expected_call, expected_region, bound)) in requests.iter().zip(expected)
    {
        assert_eq!(*call, expected_call);
        assert_eq!(request.region_id, expected_region);
        assert_eq!(request.plan, fixture.plan, "the exact plan travels");
        let header = request
            .header
            .as_ref()
            .expect("every request carries a header");
        let context = header
            .query_context
            .as_ref()
            .expect("the whole query context travels");
        assert_eq!(
            context
                .snapshot_seqs
                .as_ref()
                .and_then(|sequences| { sequences.snapshot_seqs.get(&region(1).as_u64()) }),
            Some(&SNAPSHOT_SEQUENCE),
            "the ordinary snapshot travels"
        );
        assert_eq!(
            QueryContext::try_from(context.clone())
                .expect("the transported context decodes")
                .read_preference(),
            ReadPreference::Leader,
            "the ordinary read preference travels"
        );
        if bound {
            let expected_marker = binding(expected_region).to_extension_value().unwrap();
            assert_eq!(
                context
                    .extensions
                    .get(RAW_SERIES_REQUEST_EXTENSION_KEY)
                    .map(String::as_str),
                Some(expected_marker.as_str()),
                "a bound read carries the approved binding"
            );
            assert!(
                header.dbname.is_empty(),
                "the executing task identity replaces the preparation database"
            );
        } else {
            assert!(
                !context
                    .extensions
                    .contains_key(RAW_SERIES_REQUEST_EXTENSION_KEY),
                "a preparation is marker-free"
            );
            assert_eq!(header.dbname, query_ctx.get_db_string());
        }
    }
    assert_eq!(fixture.prepared.len(), 2);
    for region in &handler.regions {
        assert!(
            region.dropped.load(Ordering::SeqCst),
            "the leaf owns and drops every region stream"
        );
    }
}

#[tokio::test]
async fn a_contiguous_approval_returns_the_original_plan_without_a_read() {
    let regions = vec![
        FakeRegion::new(REGION_ONE_PEER, Some(binding(region(1))))
            .with_ordering(SeriesOrdering::Contiguous),
        FakeRegion::new(REGION_TWO_PEER, Some(binding(region(2)))),
    ];
    let fake = Arc::new(FakeHandler::new(regions));
    let handler: RegionQueryHandlerRef = fake.clone();
    let query_ctx = query_context();
    let table = fe_table(
        MITO_ENGINE,
        vec![TS_COLUMN_ID, HOST_COLUMN_ID, VALUE_COLUMN_ID],
    );
    let logical = logical_plan(table);
    let shape = RemoteShape::discover(&logical).unwrap();
    let authorized = authorized_table(&shape);
    let plan = remote_plan(&shape);
    assert!(
        prepare_regions(&plan, &ordinary_route(), &handler, &query_ctx, &authorized)
            .await
            .expect("a contiguous approval is not a failure")
            .is_none(),
        "only read-key-then-time approvals are mergeable"
    );
    assert_eq!(
        fake.calls(),
        vec![Call::Prepare(REGION_ONE_PEER)],
        "the unsupported answer stops the preparation before any raw read"
    );
}

#[tokio::test]
async fn an_unsupported_approval_returns_the_original_plan_without_a_read() {
    let regions = vec![
        FakeRegion::new(REGION_ONE_PEER, Some(binding(region(1)))),
        FakeRegion::new(REGION_TWO_PEER, None),
    ];
    let fake = Arc::new(FakeHandler::new(regions));
    let handler: RegionQueryHandlerRef = fake.clone();
    let query_ctx = query_context();
    let table = fe_table(
        MITO_ENGINE,
        vec![TS_COLUMN_ID, HOST_COLUMN_ID, VALUE_COLUMN_ID],
    );
    let logical = logical_plan(table);
    let shape = RemoteShape::discover(&logical).unwrap();
    let authorized = authorized_table(&shape);
    let plan = remote_plan(&shape);
    assert!(
        prepare_regions(&plan, &ordinary_route(), &handler, &query_ctx, &authorized)
            .await
            .expect("an unsupported answer is not a failure")
            .is_none(),
        "an unsupported shape keeps the original plan"
    );
    assert_eq!(
        fake.calls(),
        vec![
            Call::Prepare(REGION_ONE_PEER),
            Call::Prepare(REGION_TWO_PEER)
        ]
    );
}

#[tokio::test]
async fn a_contradicting_approval_is_terminal_and_never_reads() {
    let mut contradicted = binding(region(2));
    contradicted.metric_name = "other".to_string();
    let regions = vec![
        FakeRegion::new(REGION_ONE_PEER, Some(binding(region(1)))),
        FakeRegion::new(REGION_TWO_PEER, Some(contradicted)),
    ];
    let fake = Arc::new(FakeHandler::new(regions));
    let handler: RegionQueryHandlerRef = fake.clone();
    let query_ctx = query_context();
    let table = fe_table(
        MITO_ENGINE,
        vec![TS_COLUMN_ID, HOST_COLUMN_ID, VALUE_COLUMN_ID],
    );
    let logical = logical_plan(table);
    let shape = RemoteShape::discover(&logical).unwrap();
    let authorized = authorized_table(&shape);
    let plan = remote_plan(&shape);
    assert!(
        prepare_regions(&plan, &ordinary_route(), &handler, &query_ctx, &authorized)
            .await
            .is_err(),
        "an approval that contradicts the authorized table is terminal"
    );
    assert_eq!(
        fake.calls(),
        vec![
            Call::Prepare(REGION_ONE_PEER),
            Call::Prepare(REGION_TWO_PEER)
        ],
        "a contradicting approval is reported before any raw read"
    );
}

#[tokio::test]
async fn incompatible_region_bindings_are_terminal() {
    let table = fe_table(
        MITO_ENGINE,
        vec![TS_COLUMN_ID, HOST_COLUMN_ID, VALUE_COLUMN_ID],
    );
    let logical = logical_plan(table);
    let shape = RemoteShape::discover(&logical).unwrap();
    let plan = LogicalPlan::TableScan(shape.scan.clone());
    let prepared = vec![
        PreparedRegion {
            target: RegionQueryTarget::new(peer(REGION_ONE_PEER)),
            request: QueryRequest {
                header: None,
                region_id: region(1),
                plan: plan.clone(),
            },
            binding: binding(region(1)),
        },
        PreparedRegion {
            target: RegionQueryTarget::new(peer(REGION_TWO_PEER)),
            request: QueryRequest {
                header: None,
                region_id: region(2),
                plan,
            },
            binding: {
                let mut other = binding(region(2));
                other.metric_name = "other".to_string();
                other
            },
        },
    ];
    assert!(
        derive_regions(prepared).is_err(),
        "peers that approved different identities cannot merge"
    );
}

#[tokio::test]
async fn a_binding_the_shared_contract_rejects_is_terminal_not_a_fallback() {
    let table = fe_table(
        MITO_ENGINE,
        vec![TS_COLUMN_ID, HOST_COLUMN_ID, VALUE_COLUMN_ID],
    );
    let logical = logical_plan(table);
    let shape = RemoteShape::discover(&logical).unwrap();
    let plan = LogicalPlan::TableScan(shape.scan.clone());

    // A prepared request whose region does not match its approval is a terminal error: a peer
    // already approved a read, so the frontend must not silently run the ordinary plan.
    let mismatched = vec![PreparedRegion {
        target: RegionQueryTarget::new(peer(REGION_ONE_PEER)),
        request: QueryRequest {
            header: None,
            region_id: region(1),
            plan: plan.clone(),
        },
        binding: binding(region(2)),
    }];
    assert!(derive_regions(mismatched).is_err());

    // A prepared request that already carries the reserved marker cannot be bound again.
    let marked = vec![PreparedRegion {
        target: RegionQueryTarget::new(peer(REGION_ONE_PEER)),
        request: QueryRequest {
            header: Some(RegionRequestHeader {
                tracing_context: Default::default(),
                dbname: String::new(),
                query_context: Some(api::v1::QueryContext {
                    extensions: HashMap::from([(
                        RAW_SERIES_REQUEST_EXTENSION_KEY.to_string(),
                        "{}".to_string(),
                    )]),
                    ..Default::default()
                }),
            }),
            region_id: region(1),
            plan,
        },
        binding: binding(region(1)),
    }];
    assert!(derive_regions(marked).is_err());
}

#[tokio::test]
async fn a_raw_read_failure_is_terminal_and_never_replays() {
    let regions = vec![
        FakeRegion::new(REGION_ONE_PEER, Some(binding(region(1)))).with_chunks(vec![(
            b"a".to_vec(),
            vec![1000],
            vec![1.0],
        )]),
        FakeRegion::new(REGION_TWO_PEER, Some(binding(region(2)))).failing(),
    ];
    let handler = Arc::new(FakeHandler::new(regions));
    let fixture = native_fixture(&handler, &query_context()).await;
    let leaf = Arc::clone(fixture.native.children()[0]);
    let mut frames = leaf.execute(0, Arc::new(TaskContext::default())).unwrap();
    assert!(
        matches!(frames.next().await, Some(Err(_))),
        "an open failure must be reported as a stream error"
    );
    assert!(
        frames.next().await.is_none(),
        "the failed stream ends instead of retrying"
    );
    assert_eq!(
        handler.calls(),
        vec![
            Call::Prepare(REGION_ONE_PEER),
            Call::Prepare(REGION_TWO_PEER),
            Call::RawRead(REGION_ONE_PEER),
            Call::RawRead(REGION_TWO_PEER),
        ],
        "a failed raw read is never replayed and never falls back"
    );
}

#[tokio::test]
async fn dropping_the_prepared_set_starts_no_raw_read() {
    let regions = vec![
        FakeRegion::new(REGION_ONE_PEER, Some(binding(region(1)))),
        FakeRegion::new(REGION_TWO_PEER, Some(binding(region(2)))),
    ];
    let fake = Arc::new(FakeHandler::new(regions));
    let handler: RegionQueryHandlerRef = fake.clone();
    let query_ctx = query_context();
    let table = fe_table(
        MITO_ENGINE,
        vec![TS_COLUMN_ID, HOST_COLUMN_ID, VALUE_COLUMN_ID],
    );
    let logical = logical_plan(table);
    let shape = RemoteShape::discover(&logical).unwrap();
    let authorized = authorized_table(&shape);
    let plan = remote_plan(&shape);
    let prepared = prepare_regions(&plan, &ordinary_route(), &handler, &query_ctx, &authorized)
        .await
        .unwrap()
        .unwrap();
    drop(prepared);
    assert_eq!(
        fake.calls(),
        vec![
            Call::Prepare(REGION_ONE_PEER),
            Call::Prepare(REGION_TWO_PEER)
        ],
        "an unexecuted prepared set never opens a raw read"
    );
    for region in &fake.regions {
        assert!(!region.dropped.load(Ordering::SeqCst));
    }
}

#[tokio::test]
async fn cancelling_the_output_stream_drops_pending_and_open_reads() {
    let regions = vec![
        FakeRegion::new(REGION_ONE_PEER, Some(binding(region(1)))).with_chunks(vec![(
            b"a".to_vec(),
            vec![1000],
            vec![1.0],
        )]),
        FakeRegion::new(REGION_TWO_PEER, Some(binding(region(2)))),
    ];
    let handler = Arc::new(FakeHandler::new(regions));
    let released = handler.hold_read(REGION_TWO_PEER);
    let fixture = native_fixture(&handler, &query_context()).await;
    let leaf = Arc::clone(fixture.native.children()[0]);
    let mut frames = leaf.execute(0, Arc::new(TaskContext::default())).unwrap();

    // The first poll opens the first region and holds the second read pending.
    assert!(futures::poll!(frames.next()).is_pending());
    assert_eq!(
        handler.calls(),
        vec![
            Call::Prepare(REGION_ONE_PEER),
            Call::Prepare(REGION_TWO_PEER),
            Call::RawRead(REGION_ONE_PEER),
            Call::RawRead(REGION_TWO_PEER),
        ]
    );
    assert!(
        !handler.regions[0].dropped.load(Ordering::SeqCst),
        "the opened stream is still owned by the running output"
    );

    drop(frames);
    assert!(
        handler.regions[0].dropped.load(Ordering::SeqCst),
        "cancelling the output drops the already-opened region stream"
    );
    assert!(
        released.is_closed(),
        "cancelling the output drops the pending region read"
    );
    assert_eq!(
        handler.calls(),
        vec![
            Call::Prepare(REGION_ONE_PEER),
            Call::Prepare(REGION_TWO_PEER),
            Call::RawRead(REGION_ONE_PEER),
            Call::RawRead(REGION_TWO_PEER),
        ],
        "cancellation never replays a read"
    );
}

#[test]
fn native_raw_merge_is_a_single_partition_leaf() {
    let handler: RegionQueryHandlerRef = Arc::new(FakeHandler::new(vec![]));
    let leaf = Arc::new(NativeRawMergeExec::new(vec![], handler, physical_schema()));
    assert!(leaf.children().is_empty());
    assert_eq!(leaf.name(), "NativeRawMergeExec");
    assert!(
        Arc::clone(&leaf)
            .replace_children(
                vec![Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
                    physical_schema(),
                ))],
                datafusion::physical_plan::ReplaceChildrenOptions::new(
                    datafusion::physical_plan::ChildrenPropertiesMode::Recompute,
                ),
            )
            .is_err()
    );
    assert!(
        leaf.execute(1, Arc::new(TaskContext::default())).is_err(),
        "the native raw merge owns exactly one partition"
    );
}
