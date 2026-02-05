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

//! Vector index scan exec wrapper for vector search.

use std::any::Any;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use ahash::RandomState;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::memory::{LazyBatchGenerator, LazyMemoryExec};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::hash_utils::create_hashes;
use datafusion_common::internal_err;
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::{EquivalenceProperties, LexOrdering, Partitioning, PhysicalExpr};
use datatypes::arrow::array::ArrayRef;
use datatypes::arrow::compute::concat_batches;
use datatypes::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datatypes::arrow::record_batch::RecordBatch;
use futures::StreamExt;
use parking_lot::RwLock;
use store_api::region_engine::RegionEngineRef;
use store_api::storage::ScanRequest;
use table::table::scan::RegionScanExec;

use crate::metrics::{VECTOR_SCAN_MAX_K, VECTOR_SCAN_ROUNDS_TOTAL, VECTOR_SCAN_TOTAL_ROWS};
#[derive(Clone)]
pub(crate) struct VectorScanConfig {
    pub(crate) engine: RegionEngineRef,
    pub(crate) region_id: store_api::storage::RegionId,
    pub(crate) base_request: ScanRequest,
    pub(crate) query_memory_permit: Option<Arc<common_recordbatch::MemoryPermit>>,
    pub(crate) explain_verbose: bool,
}

#[derive(Clone)]
pub(crate) struct ProjectionConfig {
    pub(crate) exprs: Vec<ProjectionExpr>,
    pub(crate) schema: ArrowSchemaRef,
}

#[derive(Clone)]
pub(crate) struct VectorTopKConfig {
    pub(crate) sort_exprs: LexOrdering,
    pub(crate) fetch: usize,
    pub(crate) skip: usize,
    pub(crate) predicate: Option<Arc<dyn PhysicalExpr>>,
    pub(crate) projection: Option<ProjectionConfig>,
    pub(crate) projection_before_filter: bool,
}

pub struct VectorSearchScanExec {
    config: VectorScanConfig,
    initial_scan: Arc<dyn ExecutionPlan>,
    schema: ArrowSchemaRef,
    metric: ExecutionPlanMetricsSet,
    properties: PlanProperties,
}

impl std::fmt::Debug for VectorSearchScanExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VectorSearchScanExec")
            .field("region_id", &self.config.region_id)
            .finish()
    }
}

impl VectorSearchScanExec {
    pub fn new(
        engine: RegionEngineRef,
        region_id: store_api::storage::RegionId,
        base_request: ScanRequest,
        initial_scan: Arc<dyn ExecutionPlan>,
        schema: ArrowSchemaRef,
        query_memory_permit: Option<Arc<common_recordbatch::MemoryPermit>>,
        explain_verbose: bool,
    ) -> Self {
        let eq_props = EquivalenceProperties::new(schema.clone());
        let properties = PlanProperties::new(
            eq_props,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            config: VectorScanConfig {
                engine,
                region_id,
                base_request,
                query_memory_permit,
                explain_verbose,
            },
            initial_scan,
            schema,
            metric: ExecutionPlanMetricsSet::new(),
            properties,
        }
    }

    fn base_k(&self) -> usize {
        self.config
            .base_request
            .vector_search
            .as_ref()
            .map(|search| search.k)
            .unwrap_or(0)
    }

    fn target_rows(&self, base_k: usize) -> usize {
        self.config.base_request.limit.unwrap_or(base_k)
    }

    pub(crate) fn config(&self) -> VectorScanConfig {
        self.config.clone()
    }

    pub(crate) async fn build_scan_exec(
        &self,
        current_k: usize,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        build_scan_exec_from_config(&self.config, current_k).await
    }
}

const MAX_ROUNDS: usize = 5;
const K_GROWTH_FACTOR: usize = 2;
const MAX_K_MULTIPLIER: usize = 32;
const FILTER_K_FACTOR: usize = 2;

async fn build_scan_exec_from_config(
    config: &VectorScanConfig,
    current_k: usize,
) -> DfResult<Arc<dyn ExecutionPlan>> {
    let mut request = config.base_request.clone();
    if let Some(search) = request.vector_search.as_mut() {
        search.k = current_k;
        request.limit = None;
    } else {
        request.limit = Some(current_k);
    }

    let mut scan_exec = RegionScanExec::new(
        config
            .engine
            .handle_query(config.region_id, request.clone())
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?,
        request,
        config.query_memory_permit.clone(),
    )?;
    if config.explain_verbose {
        scan_exec.set_explain_verbose(true);
    }

    Ok(Arc::new(scan_exec))
}

fn apply_skip(batch: RecordBatch, skip: usize) -> RecordBatch {
    if skip == 0 {
        return batch;
    }
    if skip >= batch.num_rows() {
        return RecordBatch::new_empty(batch.schema());
    }
    batch.slice(skip, batch.num_rows() - skip)
}

#[derive(Debug)]
struct SingleBatchGenerator {
    batch: Option<RecordBatch>,
}

impl SingleBatchGenerator {
    fn new(batch: RecordBatch) -> Self {
        Self { batch: Some(batch) }
    }
}

impl std::fmt::Display for SingleBatchGenerator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "single_batch_generator")
    }
}

impl LazyBatchGenerator for SingleBatchGenerator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn generate_next_batch(&mut self) -> DfResult<Option<RecordBatch>> {
        Ok(self.batch.take())
    }
}

async fn build_topk_batch_with_sort(
    batch: RecordBatch,
    sort_exprs: &LexOrdering,
    limit: usize,
    context: Arc<TaskContext>,
) -> DfResult<RecordBatch> {
    let schema = batch.schema();
    let generator = Arc::new(RwLock::new(SingleBatchGenerator::new(batch)));
    let memory_exec = LazyMemoryExec::try_new(schema, vec![generator])?;
    let sort_exec =
        SortExec::new(sort_exprs.clone(), Arc::new(memory_exec)).with_fetch(Some(limit));
    let mut stream = sort_exec.execute(0, context)?;
    let mut batches = Vec::new();
    while let Some(batch) = stream.next().await.transpose()? {
        batches.push(batch);
    }
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(sort_exec.schema()));
    }
    concat_batches(&sort_exec.schema(), &batches).map_err(DataFusionError::from)
}

fn build_topk_hashes(
    batch: &RecordBatch,
    sort_exprs: &LexOrdering,
    random_state: &RandomState,
) -> DfResult<Vec<u64>> {
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(sort_exprs.len());
    for sort_expr in sort_exprs {
        let value = sort_expr.expr.evaluate(batch)?;
        let array = match value {
            ColumnarValue::Array(array) => array,
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(batch.num_rows())?,
        };
        arrays.push(array);
    }
    let mut hashes = vec![0u64; batch.num_rows()];
    create_hashes(&arrays, random_state, &mut hashes)?;
    Ok(hashes)
}

impl ExecutionPlan for VectorSearchScanExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.initial_scan]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!(
                "VectorSearchScanExec expects exactly one child, got {}",
                children.len()
            );
        }
        Ok(Arc::new(Self {
            config: self.config.clone(),
            initial_scan: children[0].clone(),
            schema: self.schema.clone(),
            metric: self.metric.clone(),
            properties: self.properties.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        self.initial_scan.execute(partition, context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metric.clone_inner())
    }

    fn name(&self) -> &str {
        "VectorSearchScanExec"
    }
}

impl DisplayAs for VectorSearchScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                let base_k = self.base_k();
                let target_rows = self.target_rows(base_k);
                write!(
                    f,
                    "VectorSearchScanExec: region_id={}, base_k={}, target_rows={}",
                    self.config.region_id, base_k, target_rows
                )
            }
        }
    }
}

pub struct VectorTopKExec {
    config: VectorScanConfig,
    input: Arc<dyn ExecutionPlan>,
    schema: ArrowSchemaRef,
    metric: ExecutionPlanMetricsSet,
    properties: PlanProperties,
    topk: VectorTopKConfig,
    rounds: Arc<AtomicUsize>,
}

impl std::fmt::Debug for VectorTopKExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VectorTopKExec")
            .field("region_id", &self.config.region_id)
            .field("fetch", &self.topk.fetch)
            .field("skip", &self.topk.skip)
            .finish()
    }
}

impl VectorTopKExec {
    pub fn try_new(
        config: VectorScanConfig,
        input: Arc<dyn ExecutionPlan>,
        topk: VectorTopKConfig,
    ) -> DfResult<Self> {
        let schema = topk
            .projection
            .as_ref()
            .map(|projection| projection.schema.clone())
            .unwrap_or_else(|| input.schema());
        let eq_props = EquivalenceProperties::new_with_orderings(
            schema.clone(),
            vec![topk.sort_exprs.clone()],
        );
        let properties = PlanProperties::new(
            eq_props,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Ok(Self {
            config,
            input,
            schema,
            metric: ExecutionPlanMetricsSet::new(),
            properties,
            topk,
            rounds: Arc::new(AtomicUsize::new(0)),
        })
    }

    fn base_k(&self) -> usize {
        self.config
            .base_request
            .vector_search
            .as_ref()
            .map(|search| search.k)
            .unwrap_or(0)
    }

    #[cfg(test)]
    pub(crate) fn topk(&self) -> &VectorTopKConfig {
        &self.topk
    }
}

impl ExecutionPlan for VectorTopKExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!(
                "VectorTopKExec expects exactly one child, got {}",
                children.len()
            );
        }
        Ok(Arc::new(Self::try_new(
            self.config.clone(),
            children[0].clone(),
            self.topk.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!(
                "VectorTopKExec invalid partition. Expected 0, got {}",
                partition
            );
        }

        let schema = self.schema.clone();
        let schema_for_stream = schema.clone();
        let base_k = self.base_k();
        let max_k = base_k.saturating_mul(MAX_K_MULTIPLIER);
        let vector_scan = self.input.clone();
        let config = self.config.clone();
        let sort_exprs = self.topk.sort_exprs.clone();
        let fetch = self.topk.fetch;
        let skip = self.topk.skip;
        let predicate = self.topk.predicate.clone();
        let projection = self.topk.projection.clone();
        let projection_before_filter = self.topk.projection_before_filter;
        let rounds = self.rounds.clone();
        let rounds_metric = datafusion::physical_plan::metrics::MetricBuilder::new(&self.metric)
            .global_counter("vector_topk_rounds");
        let limit = fetch.saturating_add(skip);

        let random_state = RandomState::new();
        let output = async move {
            if limit == 0 {
                return Ok(RecordBatch::new_empty(schema.clone()));
            }

            let mut round = 0usize;
            let mut current_k = base_k.max(limit);
            if predicate.is_some() {
                current_k = current_k.saturating_mul(FILTER_K_FACTOR).min(max_k);
            }
            let mut prev_hashes: Option<Vec<u64>> = None;
            let record_metrics = |round: usize, current_k: usize, output_rows: usize| {
                rounds.store(round + 1, Ordering::Relaxed);
                rounds_metric.add(round + 1);
                VECTOR_SCAN_ROUNDS_TOTAL.inc_by((round + 1) as u64);
                VECTOR_SCAN_MAX_K.observe(current_k as f64);
                VECTOR_SCAN_TOTAL_ROWS.inc_by(output_rows as u64);
            };

            loop {
                let mut scan_exec: Arc<dyn ExecutionPlan> =
                    match vector_scan.as_any().downcast_ref::<VectorSearchScanExec>() {
                        Some(vector_scan) => vector_scan.build_scan_exec(current_k).await?,
                        None => build_scan_exec_from_config(&config, current_k).await?,
                    };

                if let Some(projection) = &projection
                    && projection_before_filter
                {
                    scan_exec = Arc::new(ProjectionExec::try_new(
                        projection.exprs.clone(),
                        scan_exec,
                    )?) as Arc<dyn ExecutionPlan>;
                }

                if let Some(predicate) = &predicate {
                    scan_exec = Arc::new(FilterExec::try_new(predicate.clone(), scan_exec)?)
                        as Arc<dyn ExecutionPlan>;
                }

                if let Some(projection) = &projection
                    && !projection_before_filter
                {
                    scan_exec = Arc::new(ProjectionExec::try_new(
                        projection.exprs.clone(),
                        scan_exec,
                    )?) as Arc<dyn ExecutionPlan>;
                }

                let mut batches: Vec<RecordBatch> = Vec::new();
                let partition_count = scan_exec.properties().partitioning.partition_count();
                for partition in 0..partition_count {
                    let mut stream = scan_exec.execute(partition, context.clone())?;
                    while let Some(batch) = stream.next().await.transpose()? {
                        batches.push(batch);
                    }
                }

                if batches.is_empty() {
                    if current_k >= max_k || round + 1 >= MAX_ROUNDS {
                        record_metrics(round, current_k, 0);
                        return Ok(RecordBatch::new_empty(schema.clone()));
                    }
                    let next_k = current_k.saturating_mul(K_GROWTH_FACTOR).min(max_k);
                    if next_k == current_k {
                        record_metrics(round, current_k, 0);
                        return Ok(RecordBatch::new_empty(schema.clone()));
                    }
                    current_k = next_k;
                    round += 1;
                    continue;
                }

                let scan_schema = scan_exec.schema();
                let batch = concat_batches(&scan_schema, &batches)?;

                let topk_batch =
                    build_topk_batch_with_sort(batch, &sort_exprs, limit, context.clone()).await?;
                let hashes = build_topk_hashes(&topk_batch, &sort_exprs, &random_state)?;

                if let Some(prev) = &prev_hashes
                    && prev == &hashes
                {
                    let output = apply_skip(topk_batch, skip);
                    record_metrics(round, current_k, output.num_rows());
                    return Ok(output);
                }

                prev_hashes = Some(hashes);

                if current_k >= max_k || round + 1 >= MAX_ROUNDS {
                    let output = apply_skip(topk_batch, skip);
                    record_metrics(round, current_k, output.num_rows());
                    return Ok(output);
                }

                let next_k = current_k.saturating_mul(K_GROWTH_FACTOR).min(max_k);
                if next_k == current_k {
                    let output = apply_skip(topk_batch, skip);
                    record_metrics(round, current_k, output.num_rows());
                    return Ok(output);
                }

                current_k = next_k;
                round += 1;
            }
        };

        Ok(Box::pin(
            datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                schema_for_stream,
                futures::stream::once(output),
            ),
        ))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metric.clone_inner())
    }

    fn name(&self) -> &str {
        "VectorTopKExec"
    }
}

impl DisplayAs for VectorTopKExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                let rounds = self.rounds.load(Ordering::Relaxed);
                write!(
                    f,
                    "VectorTopKExec: region_id={}, fetch={}, skip={}, rounds={}",
                    self.config.region_id, self.topk.fetch, self.topk.skip, rounds
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::region_engine::RegionEngineRef;
    use store_api::storage::{
        ConcreteDataType, RegionId, ScanRequest, VectorDistanceMetric, VectorSearchRequest,
    };

    use crate::optimizer::test_util::MetaRegionEngine;
    use crate::test_util::MockInputExec;
    use crate::vector_scan::VectorSearchScanExec;

    fn build_engine(region_id: RegionId) -> RegionEngineRef {
        let mut builder = RegionMetadataBuilder::new(region_id);
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: datatypes::schema::ColumnSchema::new(
                    "k0",
                    ConcreteDataType::string_datatype(),
                    true,
                ),
                semantic_type: api::v1::SemanticType::Tag,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: datatypes::schema::ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: api::v1::SemanticType::Timestamp,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: datatypes::schema::ColumnSchema::new(
                    "v0",
                    ConcreteDataType::float64_datatype(),
                    false,
                ),
                semantic_type: api::v1::SemanticType::Field,
                column_id: 3,
            })
            .primary_key(vec![1]);
        let metadata = Arc::new(builder.build().unwrap());
        Arc::new(MetaRegionEngine::with_metadata(metadata))
    }

    fn build_exec(limit: Option<usize>, k: usize) -> VectorSearchScanExec {
        let region_id = RegionId::new(1, 0);
        let engine = build_engine(region_id);
        let request = ScanRequest {
            limit,
            vector_search: Some(VectorSearchRequest {
                column_id: 1,
                query_vector: vec![0.0, 0.0],
                k,
                metric: VectorDistanceMetric::L2sq,
            }),
            ..Default::default()
        };
        let schema = Arc::new(Schema::new(vec![Field::new(
            "vec_id",
            DataType::Int32,
            false,
        )]));
        let mock_input = Arc::new(MockInputExec::new(vec![Vec::new()], schema.clone()));
        VectorSearchScanExec::new(engine, region_id, request, mock_input, schema, None, false)
    }

    #[test]
    fn test_target_rows_prefers_limit() {
        let exec = build_exec(Some(5), 2);
        assert_eq!(exec.base_k(), 2);
        assert_eq!(exec.target_rows(2), 5);
    }

    #[test]
    fn test_target_rows_fallback_to_k() {
        let exec = build_exec(None, 3);
        assert_eq!(exec.base_k(), 3);
        assert_eq!(exec.target_rows(3), 3);
    }
}
