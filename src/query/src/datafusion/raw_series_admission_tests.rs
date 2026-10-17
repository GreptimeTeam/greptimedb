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

use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use api::region::RegionResponse;
use api::v1::SemanticType;
use async_trait::async_trait;
use common_error::ext::BoxedError;
use common_query::OutputData;
use common_query::request::{
    INITIAL_REMOTE_DYN_FILTER_REGISTRATIONS_EXTENSION_KEY, RawSeriesRequest, RawSeriesTag,
};
use common_recordbatch::{SendableRecordBatchStream, util};
use datafusion::datasource::{DefaultTableSource, MemTable};
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::display::{DisplayAs, DisplayFormatType};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_common::ScalarValue;
use datafusion_expr::{LogicalPlan, LogicalPlanBuilder, SortExpr, col, lit};
use datatypes::arrow::array::{AsArray, BinaryArray, Float64Array, Int64Array, ListArray};
use datatypes::arrow::buffer::Buffer;
use datatypes::schema::SchemaRef;
use datatypes::series::SeriesOrdering;
use datatypes::series::raw::{RawSeriesChunk, RawTimestamps};
use datatypes::series::samples::{FieldSpec, Samples, SeriesFields, SeriesSchema};
use futures::Stream;
use session::context::{QueryContext, QueryContextBuilder};
use store_api::metadata::RegionMetadataRef;
use store_api::region_engine::{
    PrepareRequest, QueryScanContext, RawLabelSource, RawSeriesStream, RegionEngine,
    RegionEngineRef, RegionRole, RegionScanner, RegionScannerRef, RegionStatistic,
    RemapManifestsRequest, RemapManifestsResponse, ScannerProperties, SendableRawSeriesStream,
    SetRegionRoleStateResponse, SettableRegionRoleState, SyncRegionFromRequest,
    SyncRegionFromResponse,
};
use store_api::region_request::RegionRequest;
use store_api::storage::{RegionId, ScanRequest, SequenceNumber, TimeSeriesRowSelector};

use crate::QueryEngineRef;
use crate::datafusion::QUERY_PARALLELISM_HINT;
use crate::dummy_catalog::DummyTableProvider;
use crate::options::QueryOptions;
use crate::query_engine::QueryEngineFactory;

async fn engine() -> QueryEngineRef {
    QueryEngineFactory::new(
        catalog::memory::new_memory_catalog_manager().unwrap(),
        None,
        None,
        None,
        None,
        false,
        QueryOptions::default(),
    )
    .query_engine()
}

fn mock_scan() -> (LogicalPlan, RawSeriesRequest) {
    let provider = crate::optimizer::test_util::mock_table_provider(RegionId::new(1, 1));
    let metadata = provider.region_metadata();
    let source = Arc::new(DefaultTableSource::new(Arc::new(provider)));
    let scan = LogicalPlanBuilder::scan("raw", source, Some(vec![2]))
        .unwrap()
        .build()
        .unwrap();
    let binding = RawSeriesRequest {
        version: 1,
        physical_metadata: metadata.as_ref().clone(),
        metric_name: "m".to_string(),
        logical_tags: vec![],
        field_ids: vec![3],
    };
    (scan, binding)
}

async fn assert_rejected(
    engine: &QueryEngineRef,
    plan: LogicalPlan,
    ctx: Arc<QueryContext>,
    binding: RawSeriesRequest,
) {
    assert!(
        engine
            .prepare_raw_series(plan.clone(), ctx.clone(), binding.clone())
            .await
            .unwrap()
            .is_none()
    );
    let error = match engine.execute_raw_series(plan, ctx, binding).await {
        Err(error) => error,
        Ok(_) => panic!("unsupported raw-series plan was accepted"),
    };
    assert!(matches!(
        error,
        crate::error::Error::Unimplemented { operation, .. }
            if operation == "unsupported native raw-series plan"
    ));
}

#[tokio::test]
async fn raw_series_admission_rejects_invalid_parallelism_hints() {
    let engine = engine().await;
    let (scan, binding) = mock_scan();
    // Positive hints are no longer a hint-level rejection: the private preparation context
    // pins the scan to one partition, so a positive hint reaches the plan-shape gates instead.
    for hint in ["invalid", "0"] {
        let ctx = Arc::new(
            QueryContextBuilder::default()
                .set_extension(QUERY_PARALLELISM_HINT.to_string(), hint.to_string())
                .build(),
        );
        assert_rejected(&engine, scan.clone(), ctx, binding.clone()).await;
    }
}

#[tokio::test]
async fn raw_series_admission_rejects_initial_dynamic_filter_intent() {
    let engine = engine().await;
    let (scan, binding) = mock_scan();
    let ctx = Arc::new(
        QueryContextBuilder::default()
            .set_extension(
                INITIAL_REMOTE_DYN_FILTER_REGISTRATIONS_EXTENSION_KEY.to_string(),
                "malformed marker".to_string(),
            )
            .build(),
    );
    assert_rejected(&engine, scan.clone(), ctx, binding.clone()).await;
}

/// A region engine that serves one region with a non-executing raw-series scanner.
struct RawSeriesEngine {
    metadata: RegionMetadataRef,
    raw_capable: bool,
}

#[async_trait]
impl RegionEngine for RawSeriesEngine {
    fn name(&self) -> &str {
        "RawSeriesEngine"
    }

    async fn handle_request(
        &self,
        _region_id: RegionId,
        _request: RegionRequest,
    ) -> std::result::Result<RegionResponse, BoxedError> {
        unimplemented!("raw-series admission never writes")
    }

    async fn handle_query(
        &self,
        region_id: RegionId,
        _request: ScanRequest,
    ) -> std::result::Result<RegionScannerRef, BoxedError> {
        assert_eq!(region_id, self.metadata.region_id);
        Ok(Box::new(RawSeriesScanner {
            properties: ScannerProperties::default(),
            metadata: self.metadata.clone(),
            raw_capable: self.raw_capable,
        }))
    }

    async fn get_metadata(
        &self,
        region_id: RegionId,
    ) -> std::result::Result<RegionMetadataRef, BoxedError> {
        assert_eq!(region_id, self.metadata.region_id);
        Ok(self.metadata.clone())
    }

    fn region_statistic(&self, _region_id: RegionId) -> Option<RegionStatistic> {
        None
    }

    async fn get_committed_sequence(
        &self,
        _region_id: RegionId,
    ) -> std::result::Result<SequenceNumber, BoxedError> {
        Ok(SequenceNumber::default())
    }

    async fn stop(&self) -> std::result::Result<(), BoxedError> {
        Ok(())
    }

    fn set_region_role(
        &self,
        _region_id: RegionId,
        _role: RegionRole,
    ) -> std::result::Result<(), BoxedError> {
        unimplemented!("raw-series admission never changes the region role")
    }

    async fn set_region_role_state_gracefully(
        &self,
        _region_id: RegionId,
        _region_role_state: SettableRegionRoleState,
    ) -> std::result::Result<SetRegionRoleStateResponse, BoxedError> {
        unimplemented!("raw-series admission never changes the region role")
    }

    async fn sync_region(
        &self,
        _region_id: RegionId,
        _request: SyncRegionFromRequest,
    ) -> std::result::Result<SyncRegionFromResponse, BoxedError> {
        unimplemented!("raw-series admission never syncs a region")
    }

    async fn remap_manifests(
        &self,
        _request: RemapManifestsRequest,
    ) -> std::result::Result<RemapManifestsResponse, BoxedError> {
        unimplemented!("raw-series admission never remaps manifests")
    }

    fn role(&self, _region_id: RegionId) -> Option<RegionRole> {
        None
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// The single sample every raw execution of this scanner emits: one series, one timestamp, and
/// one float value per declared field.
const RAW_SAMPLE_KEY: &[u8] = b"complete-key";
const RAW_SAMPLE_TIMESTAMP: i64 = 10;
const RAW_SAMPLE_VALUE: f64 = 1.5;

/// A scanner that reports raw-series capability without polluting any execution state.
#[derive(Debug)]
struct RawSeriesScanner {
    properties: ScannerProperties,
    metadata: RegionMetadataRef,
    raw_capable: bool,
}

impl RawSeriesScanner {
    /// The complete raw-series schema this scanner declares for its own metadata.
    fn declared_raw_schema(&self) -> Arc<SeriesSchema> {
        let fields = self
            .metadata
            .column_metadatas
            .iter()
            .filter(|column| column.semantic_type == SemanticType::Field)
            .map(|column| FieldSpec {
                name: Arc::from(column.column_schema.name.as_str()),
            })
            .collect::<Vec<_>>();
        Arc::new(SeriesSchema {
            fields: fields.into(),
            eval_grid: None,
        })
    }
}

/// One planned raw-series chunk, yielded once and finished afterwards.
#[derive(Debug)]
struct RawSeriesSampleStream {
    schema: Arc<SeriesSchema>,
    label_source: RawLabelSource,
    chunk: Option<RawSeriesChunk>,
}

impl Stream for RawSeriesSampleStream {
    type Item = std::result::Result<RawSeriesChunk, BoxedError>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.chunk.take().map(Ok))
    }
}

impl RawSeriesStream for RawSeriesSampleStream {
    fn schema(&self) -> &Arc<SeriesSchema> {
        &self.schema
    }

    fn label_source(&self) -> &RawLabelSource {
        &self.label_source
    }

    fn ordering(&self) -> SeriesOrdering {
        SeriesOrdering::KeyThenTime
    }
}

impl DisplayAs for RawSeriesScanner {
    fn fmt_as(&self, _format: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RawSeriesScanner")
    }
}

impl RegionScanner for RawSeriesScanner {
    fn name(&self) -> &str {
        "RawSeriesScanner"
    }

    fn properties(&self) -> &ScannerProperties {
        &self.properties
    }

    fn schema(&self) -> SchemaRef {
        self.metadata.schema.clone()
    }

    fn metadata(&self) -> RegionMetadataRef {
        self.metadata.clone()
    }

    fn prepare(&mut self, request: PrepareRequest) -> std::result::Result<(), BoxedError> {
        self.properties.prepare(request);
        Ok(())
    }

    fn scan_partition(
        &self,
        _ctx: &QueryScanContext,
        _metrics: &ExecutionPlanMetricsSet,
        _partition: usize,
    ) -> std::result::Result<SendableRecordBatchStream, BoxedError> {
        unimplemented!("raw-series admission never executes an ordinary scan")
    }

    fn raw_series_schema(
        &self,
        _source: &RawLabelSource,
    ) -> std::result::Result<Option<Arc<SeriesSchema>>, BoxedError> {
        if !self.raw_capable {
            return Ok(None);
        }
        Ok(Some(self.declared_raw_schema()))
    }

    /// Executes the raw path for the focused admission case without changing scanner state: one
    /// series with one sample per declared field, so a prepared plan's visible output can be
    /// observed directly.
    fn scan_series_partition(
        &self,
        _ctx: &QueryScanContext,
        _metrics: &ExecutionPlanMetricsSet,
        _partition: usize,
        label_source: &RawLabelSource,
    ) -> std::result::Result<Option<SendableRawSeriesStream>, BoxedError> {
        if !self.raw_capable {
            return Ok(None);
        }
        let schema = self.declared_raw_schema();
        let fields = SeriesFields::try_from_vec(
            schema
                .fields
                .iter()
                .map(|_| Samples::from_float(Float64Array::from(vec![Some(RAW_SAMPLE_VALUE)])))
                .collect(),
        )
        .expect("the declared raw schema must accept one sample per field");
        let chunk = RawSeriesChunk::try_new(
            &schema,
            Buffer::from(RAW_SAMPLE_KEY),
            None,
            RawTimestamps::try_new(vec![RAW_SAMPLE_TIMESTAMP].into())
                .expect("the sample timestamp must be strictly increasing"),
            fields,
            true,
        )
        .expect("the sample chunk must match the declared raw schema");
        Ok(Some(Box::pin(RawSeriesSampleStream {
            schema,
            label_source: label_source.clone(),
            chunk: Some(chunk),
        })))
    }

    fn raw_series_ordering(&self) -> SeriesOrdering {
        SeriesOrdering::KeyThenTime
    }

    fn has_predicate_without_region(&self) -> bool {
        false
    }

    fn add_dyn_filter_to_predicate(
        &mut self,
        filter_exprs: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Vec<bool> {
        filter_exprs.iter().map(|_| false).collect()
    }

    fn set_logical_region(&mut self, logical_region: bool) {
        self.properties.set_logical_region(logical_region);
    }

    fn set_query_load_region_id(&mut self, region_id: RegionId) {
        self.properties.set_query_load_region_id(region_id);
    }
}

/// One authorized region scan over the mock metadata `k0: string tag, ts: timestamp, v0: f64 field`.
struct RawSeriesFixture {
    scan: LogicalPlan,
    provider: DummyTableProvider,
    metadata: RegionMetadataRef,
    region_id: RegionId,
}

fn raw_series_fixture(raw_capable: bool) -> RawSeriesFixture {
    let region_id = RegionId::new(1, 1);
    let metadata = crate::optimizer::test_util::mock_table_provider(region_id).region_metadata();
    let engine: RegionEngineRef = Arc::new(RawSeriesEngine {
        metadata: metadata.clone(),
        raw_capable,
    });
    let provider = DummyTableProvider::new(region_id, engine, metadata.clone());
    let source = Arc::new(DefaultTableSource::new(Arc::new(provider.clone())));
    let scan = LogicalPlanBuilder::scan("cpu", source, None)
        .unwrap()
        .build()
        .unwrap();
    RawSeriesFixture {
        scan,
        provider,
        metadata,
        region_id,
    }
}

impl RawSeriesFixture {
    /// The frozen remote shape: sort over a timestamp filter over a full-schema scan.
    fn wrapped(&self) -> LogicalPlan {
        let lower = lit(ScalarValue::TimestampMillisecond(Some(-298_999), None));
        let upper = lit(ScalarValue::TimestampMillisecond(Some(3_000), None));
        LogicalPlanBuilder::from(self.scan.clone())
            .filter(col("ts").gt_eq(lower).and(col("ts").lt_eq(upper)))
            .unwrap()
            .sort(vec![
                SortExpr::new(col("k0"), true, true),
                SortExpr::new(col("ts"), true, true),
            ])
            .unwrap()
            .build()
            .unwrap()
    }

    /// The expectation a bound caller derives from the same authorized scan.
    fn binding(&self) -> RawSeriesRequest {
        RawSeriesRequest {
            version: 1,
            physical_metadata: self.metadata.as_ref().clone(),
            metric_name: "cpu".to_string(),
            logical_tags: vec![RawSeriesTag {
                column_id: 1,
                name: "k0".to_string(),
            }],
            field_ids: vec![3],
        }
    }
}

#[tokio::test]
async fn named_raw_series_preparation_derives_the_actual_binding() {
    let fixture = raw_series_fixture(true);
    let engine = engine().await;
    let ctx = Arc::new(
        QueryContextBuilder::default()
            .set_extension(QUERY_PARALLELISM_HINT.to_string(), "4".to_string())
            .set_extension("unrelated.extension".to_string(), "kept".to_string())
            .build(),
    );

    for plan in [fixture.scan.clone(), fixture.wrapped()] {
        let rendered = plan.to_string();
        let prepared = engine
            .prepare_named_raw_series(plan.clone(), ctx.clone())
            .await
            .unwrap()
            .expect("the authorized wrapped shape must be approved");
        assert_eq!(prepared.ordering, SeriesOrdering::KeyThenTime);
        assert_eq!(prepared.binding.version, 1);
        assert_eq!(prepared.binding.metric_name, "cpu");
        assert_eq!(
            prepared.binding.physical_metadata.region_id,
            fixture.region_id
        );
        assert_eq!(prepared.binding.logical_tags.len(), 1);
        assert_eq!(prepared.binding.logical_tags[0].column_id, 1);
        assert_eq!(prepared.binding.logical_tags[0].name, "k0");
        assert_eq!(prepared.binding.field_ids, [3]);
        assert_eq!(
            engine
                .prepare_raw_series(plan.clone(), ctx.clone(), fixture.binding())
                .await
                .unwrap(),
            Some(SeriesOrdering::KeyThenTime),
            "bound preparation must share the approved shape"
        );
        assert_eq!(plan.to_string(), rendered);
    }

    assert_eq!(ctx.extension(QUERY_PARALLELISM_HINT), Some("4"));
    assert_eq!(ctx.extension("unrelated.extension"), Some("kept"));
    assert!(fixture.provider.scan_request().distribution.is_none());
    assert!(
        fixture
            .provider
            .scan_request()
            .series_row_selector
            .is_none()
    );
}

#[tokio::test]
async fn named_raw_series_preparation_approves_the_entire_unprojected_schema() {
    let fixture = raw_series_fixture(true);
    let engine = engine().await;
    let ctx = Arc::new(
        QueryContextBuilder::default()
            .set_extension(QUERY_PARALLELISM_HINT.to_string(), "2".to_string())
            .build(),
    );
    // The authorized region schema is the complete visible tag and field set of this scan.
    let expected_tags = fixture
        .metadata
        .column_metadatas
        .iter()
        .filter(|column| column.semantic_type == SemanticType::Tag)
        .map(|column| (column.column_id, column.column_schema.name.clone()))
        .collect::<Vec<_>>();
    let expected_fields = fixture
        .metadata
        .column_metadatas
        .iter()
        .filter(|column| column.semantic_type == SemanticType::Field)
        .map(|column| column.column_schema.name.clone())
        .collect::<Vec<_>>();
    assert!(!expected_tags.is_empty());
    assert!(!expected_fields.is_empty());

    // A scan without a projection reads its whole region, so the approval and the reported
    // binding must cover every visible tag and field instead of a convenient subset.
    let plan = fixture.wrapped();
    let prepared = engine
        .prepare_named_raw_series(plan.clone(), ctx.clone())
        .await
        .unwrap()
        .expect("an unprojected authorized scan must be approved as a whole");
    assert_eq!(prepared.ordering, SeriesOrdering::KeyThenTime);
    assert_eq!(prepared.binding.version, 1);
    assert_eq!(prepared.binding.metric_name, "cpu");
    assert_eq!(
        prepared.binding.physical_metadata.region_id,
        fixture.region_id
    );
    assert_eq!(
        prepared
            .binding
            .logical_tags
            .iter()
            .map(|tag| (tag.column_id, tag.name.clone()))
            .collect::<Vec<_>>(),
        expected_tags,
        "the approved binding must keep every authorized tag in schema order"
    );
    assert_eq!(
        prepared
            .binding
            .field_ids
            .iter()
            .map(|id| {
                fixture
                    .metadata
                    .column_by_id(*id)
                    .expect("approved fields must exist in the actual metadata")
                    .column_schema
                    .name
                    .clone()
            })
            .collect::<Vec<_>>(),
        expected_fields,
        "the approved binding must keep every authorized field in schema order"
    );

    // The bound path approves the same plan, and executing it emits exactly that visible data.
    let binding = prepared.binding.clone();
    assert_eq!(
        engine
            .prepare_raw_series(plan.clone(), ctx.clone(), binding.clone())
            .await
            .unwrap(),
        Some(SeriesOrdering::KeyThenTime)
    );
    let OutputData::Stream(stream) = engine
        .execute_raw_series(plan, ctx, binding)
        .await
        .unwrap()
        .data
    else {
        panic!("a prepared raw-series plan must stream record batches");
    };
    let batches = util::collect(stream).await.unwrap();
    assert_eq!(batches.len(), 1);
    let batch = batches.first().unwrap();
    assert_eq!(batch.num_rows(), 1);

    // The emitted frame keeps the whole authorized schema: the identity, the time axis, and every
    // authorized field, in order.
    let frame = batch.df_record_batch();
    assert_eq!(
        frame
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>(),
        ["read_key", "is_last", "time", "fields"]
    );
    assert_eq!(
        common_query::series_frame::raw_series_frame_ordering(&frame.schema()).unwrap(),
        SeriesOrdering::KeyThenTime
    );
    assert_eq!(
        frame
            .column_by_name("read_key")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0),
        RAW_SAMPLE_KEY
    );
    assert_eq!(
        frame
            .column_by_name("time")
            .unwrap()
            .as_struct()
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec(),
        vec![RAW_SAMPLE_TIMESTAMP]
    );
    let payload = frame.column_by_name("fields").unwrap().as_struct();
    assert_eq!(
        payload
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>(),
        expected_fields,
        "an unprojected approval must emit every authorized field"
    );
    for (index, name) in expected_fields.iter().enumerate() {
        assert_eq!(
            payload
                .column(index)
                .as_struct()
                .column(0)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .values()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .values()
                .to_vec(),
            vec![RAW_SAMPLE_VALUE],
            "field {name} must be visible in the emitted frame"
        );
    }

    assert_eq!(fixture.provider.scan_request().distribution, None);
}

#[tokio::test]
async fn raw_series_preparation_declines_residual_and_unknown_wrapper_shapes() {
    let fixture = raw_series_fixture(true);
    let engine = engine().await;
    let ctx = Arc::new(
        QueryContextBuilder::default()
            .set_extension(QUERY_PARALLELISM_HINT.to_string(), "1".to_string())
            .build(),
    );

    // A sort on a field column and an inexact field filter both survive physical planning above
    // the scan, so they must reject the plan instead of being unwrapped.
    let residual_sort = LogicalPlanBuilder::from(fixture.scan.clone())
        .sort(vec![SortExpr::new(col("v0"), false, true)])
        .unwrap()
        .build()
        .unwrap();
    let residual_filter = LogicalPlanBuilder::from(fixture.scan.clone())
        .filter(col("v0").gt(lit(0.0)))
        .unwrap()
        .build()
        .unwrap();
    // Unsupported operators, a non-identity projection, and a partial projection are not safe.
    let limited = LogicalPlanBuilder::from(fixture.scan.clone())
        .limit(0, Some(1))
        .unwrap()
        .build()
        .unwrap();
    let non_identity = LogicalPlanBuilder::from(fixture.scan.clone())
        .project(vec![col("k0"), col("ts"), col("v0") + lit(1.0)])
        .unwrap()
        .build()
        .unwrap();
    let partial_projection = LogicalPlanBuilder::scan(
        "cpu",
        Arc::new(DefaultTableSource::new(Arc::new(fixture.provider.clone()))),
        Some(vec![2]),
    )
    .unwrap()
    .build()
    .unwrap();
    // A table source that is not the authorized region provider is not a raw-series candidate.
    let foreign_source = {
        let schema = fixture.metadata.schema.arrow_schema();
        let table = MemTable::try_new(Arc::clone(schema), vec![vec![]]).unwrap();
        LogicalPlanBuilder::scan(
            "cpu",
            Arc::new(DefaultTableSource::new(Arc::new(table))),
            None,
        )
        .unwrap()
        .build()
        .unwrap()
    };

    for plan in [
        residual_sort,
        residual_filter,
        limited,
        non_identity,
        partial_projection,
        foreign_source,
    ] {
        assert!(
            engine
                .prepare_named_raw_series(plan.clone(), ctx.clone())
                .await
                .unwrap()
                .is_none(),
            "an unsupported shape must not be approved: {plan}"
        );
        assert!(
            engine
                .prepare_raw_series(plan, ctx.clone(), fixture.binding())
                .await
                .unwrap()
                .is_none()
        );
    }

    // A selector changes the requested rows, so the provider cannot be cloned for raw series.
    let selector_fixture = raw_series_fixture(true);
    selector_fixture
        .provider
        .with_time_series_selector_hint(TimeSeriesRowSelector::LastRow { after_merge: false });
    assert!(
        engine
            .prepare_named_raw_series(selector_fixture.wrapped(), ctx.clone())
            .await
            .unwrap()
            .is_none()
    );

    // A scanner without raw-series capability declines instead of falling back to an ordinary scan.
    let incapable = raw_series_fixture(false);
    assert!(
        engine
            .prepare_named_raw_series(incapable.wrapped(), ctx.clone())
            .await
            .unwrap()
            .is_none()
    );

    // Initial remote dynamic-filter intent and unusable hints decline before planning.
    let dynamic = Arc::new(
        QueryContextBuilder::default()
            .set_extension(
                INITIAL_REMOTE_DYN_FILTER_REGISTRATIONS_EXTENSION_KEY.to_string(),
                "malformed marker".to_string(),
            )
            .build(),
    );
    assert!(
        engine
            .prepare_named_raw_series(fixture.wrapped(), dynamic)
            .await
            .unwrap()
            .is_none()
    );
    for hint in ["invalid", "0"] {
        let ctx = Arc::new(
            QueryContextBuilder::default()
                .set_extension(QUERY_PARALLELISM_HINT.to_string(), hint.to_string())
                .build(),
        );
        assert!(
            engine
                .prepare_named_raw_series(fixture.wrapped(), ctx)
                .await
                .unwrap()
                .is_none()
        );
    }
}
