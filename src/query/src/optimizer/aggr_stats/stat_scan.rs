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

#![allow(dead_code)]

use std::any::Any;
use std::sync::{Arc, Mutex};

use arrow_schema::{DataType, Field, SchemaRef};
use async_stream::try_stream;
use common_query::aggr_stats::StatsCandidateFile;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use datafusion::scalar::ScalarValue;
use datafusion_common::{DataFusionError, Result};
use datafusion_physical_expr::EquivalenceProperties;
use datatypes::arrow::array::{ArrayRef, StructArray};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::data_type::ConcreteDataType;
use futures::StreamExt;
use store_api::metadata::RegionMetadataRef;
use store_api::region_engine::{QueryScanContext, RegionScannerRef, SendableFileStatsStream};

use super::support_aggr::SupportStatAggr;

fn build_state_row(
    candidate: &StatsCandidateFile,
    schema: &SchemaRef,
    requirements: &[SupportStatAggr],
) -> Result<Vec<ScalarValue>> {
    if schema.fields().len() != requirements.len() {
        return Err(DataFusionError::Internal(format!(
            "StatsScanExec schema/requirement mismatch: {} fields, {} requirements",
            schema.fields().len(),
            requirements.len()
        )));
    }

    let mut state_row = Vec::with_capacity(requirements.len());
    for (field, requirement) in schema.fields().iter().zip(requirements) {
        let state = build_state_scalar(candidate, field.as_ref(), requirement)?;
        state_row.push(state);
    }

    Ok(state_row)
}

fn build_state_scalar(
    candidate: &StatsCandidateFile,
    field: &Field,
    requirement: &SupportStatAggr,
) -> Result<ScalarValue> {
    let DataType::Struct(state_fields) = field.data_type() else {
        return Err(DataFusionError::Internal(format!(
            "StatsScanExec expects struct state field, got {:?} for {}",
            field.data_type(),
            field.name()
        )));
    };
    if state_fields.len() != 1 {
        return Err(DataFusionError::Internal(format!(
            "StatsScanExec only supports single-field state in v1, got {} fields for {}",
            state_fields.len(),
            field.name()
        )));
    }

    let inner_field = state_fields[0].as_ref();
    let output_type = ConcreteDataType::from_arrow_type(inner_field.data_type());
    let Some(value) = candidate.stat_value(requirement)? else {
        return Err(DataFusionError::Internal(format!(
            "StatsScanExec built an ineligible stats candidate for requirement {:?}",
            requirement
        )));
    };

    let scalar = value.try_to_scalar_value(&output_type).map_err(|error| {
        DataFusionError::Internal(format!(
            "StatsScanExec failed to convert state value for {}: {}",
            field.name(),
            error
        ))
    })?;
    let state_array = scalar.to_array().map_err(|error| {
        DataFusionError::Internal(format!(
            "StatsScanExec failed to build state array for {}: {}",
            field.name(),
            error
        ))
    })?;
    Ok(ScalarValue::Struct(Arc::new(StructArray::new(
        state_fields.clone(),
        vec![state_array],
        None,
    ))))
}

fn build_batch_from_candidates(
    schema: &SchemaRef,
    requirements: &[SupportStatAggr],
    files: &[StatsCandidateFile],
) -> Result<Option<RecordBatch>> {
    let mut columns = (0..requirements.len())
        .map(|_| Vec::<ScalarValue>::new())
        .collect::<Vec<_>>();

    for file in files {
        let row = build_state_row(file, schema, requirements)?;
        for (index, scalar) in row.into_iter().enumerate() {
            columns[index].push(scalar);
        }
    }

    if columns.first().is_none_or(|column| column.is_empty()) {
        return Ok(None);
    }

    let arrays = columns
        .into_iter()
        .map(|values| {
            ScalarValue::iter_to_array(values).map_err(|error| {
                DataFusionError::Internal(format!(
                    "StatsScanExec failed to materialize state array: {}",
                    error
                ))
            })
        })
        .collect::<Result<Vec<ArrayRef>>>()?;

    RecordBatch::try_new(schema.clone(), arrays)
        .map(Some)
        .map_err(|error| {
            DataFusionError::Internal(format!(
                "StatsScanExec failed to build record batch: {}",
                error
            ))
        })
}

async fn build_batch_from_scan_stats(
    schema: &SchemaRef,
    requirements: &[SupportStatAggr],
    region_metadata: &RegionMetadataRef,
    mut scan_stats: SendableFileStatsStream,
) -> Result<Option<RecordBatch>> {
    let mut files = Vec::new();
    while let Some(file_stats) = scan_stats.next().await {
        let file_stats = file_stats.map_err(|error| DataFusionError::External(error.into()))?;
        let Some(candidate) = StatsCandidateFile::from_file_stats(
            &file_stats,
            region_metadata.partition_expr.as_deref(),
            requirements,
            &region_metadata.schema,
        )?
        else {
            continue;
        };
        files.push(candidate);
    }
    build_batch_from_candidates(schema, requirements, &files)
}

/// Physical execution plan for runtime stats-backed partial aggregates.
///
/// This node obtains scanner-owned file stats during `execute()` and materializes partial
/// aggregate state from those stats without doing optimizer-time I/O.
pub struct StatsScanExec {
    schema: SchemaRef,
    requirements: Vec<SupportStatAggr>,
    scanner: Arc<Mutex<RegionScannerRef>>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl std::fmt::Debug for StatsScanExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StatsScanExec")
            .field("schema", &self.schema)
            .field("requirements", &self.requirements)
            .finish()
    }
}

impl StatsScanExec {
    pub fn new(
        schema: SchemaRef,
        requirements: Vec<SupportStatAggr>,
        scanner: Arc<Mutex<RegionScannerRef>>,
    ) -> Self {
        Self {
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema.clone()),
                datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
            schema,
            requirements,
            scanner,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    pub fn requirements(&self) -> &[SupportStatAggr] {
        &self.requirements
    }
}

impl DisplayAs for StatsScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "StatsScanExec: requirements={}", self.requirements.len())
            }
            DisplayFormatType::TreeRender => write!(f, "StatsScanExec"),
        }
    }
}

impl ExecutionPlan for StatsScanExec {
    fn name(&self) -> &str {
        "StatsScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Internal(format!(
                "StatsScanExec expects no children, got {}",
                children.len()
            )));
        }

        Ok(Arc::new(Self {
            schema: self.schema.clone(),
            requirements: self.requirements.clone(),
            scanner: self.scanner.clone(),
            properties: self.properties.clone(),
            metrics: self.metrics.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "StatsScanExec expects a single partition, got {}",
                partition
            )));
        }

        let schema = self.schema.clone();
        let requirements = self.requirements.clone();
        let (region_metadata, scan_stats) = {
            let scanner = self.scanner.lock().unwrap();
            let region_metadata = scanner.metadata();
            let scan_stats = scanner
                .scan_stats(&QueryScanContext::default())
                .map_err(|error| DataFusionError::External(error.into()))?;
            (region_metadata, scan_stats)
        };
        let stream = try_stream! {
            if let Some(batch) = build_batch_from_scan_stats(
                &schema,
                &requirements,
                &region_metadata,
                scan_stats,
            )
            .await? {
                yield batch;
            }
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            Box::pin(stream),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use api::v1::SemanticType;
    use bytes::Bytes;
    use datafusion::functions_aggregate::average::avg_udaf;
    use datafusion::functions_aggregate::count::count_udaf;
    use datafusion::functions_aggregate::min_max::{max_udaf, min_udaf};
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use datafusion::parquet::file::properties::WriterProperties;
    use datafusion_physical_expr::aggregate::{AggregateExprBuilder, AggregateFunctionExpr};
    use datafusion_physical_expr::expressions::Column as PhysicalColumn;
    use datatypes::arrow::array::Int64Array;
    use datatypes::schema::ColumnSchema;
    use futures::StreamExt;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder, RegionMetadataRef};
    use store_api::region_engine::{
        FileStatsItem, RowGroupStatsItem, ScannerProperties, SendableFileStatsStream,
        SupportStatAggr,
    };
    use store_api::storage::RegionId;

    use super::*;

    #[derive(Debug)]
    struct StaticStatsScanner {
        schema: datatypes::schema::SchemaRef,
        metadata: RegionMetadataRef,
        properties: ScannerProperties,
        files: Vec<FileStatsItem>,
    }

    impl store_api::region_engine::RegionScanner for StaticStatsScanner {
        fn name(&self) -> &str {
            "StaticStatsScanner"
        }

        fn properties(&self) -> &ScannerProperties {
            &self.properties
        }

        fn schema(&self) -> datatypes::schema::SchemaRef {
            self.schema.clone()
        }

        fn metadata(&self) -> RegionMetadataRef {
            self.metadata.clone()
        }

        fn prepare(
            &mut self,
            request: store_api::region_engine::PrepareRequest,
        ) -> std::result::Result<(), common_error::ext::BoxedError> {
            self.properties.prepare(request);
            Ok(())
        }

        fn scan_partition(
            &self,
            _ctx: &QueryScanContext,
            _metrics_set: &ExecutionPlanMetricsSet,
            _partition: usize,
        ) -> std::result::Result<
            common_recordbatch::SendableRecordBatchStream,
            common_error::ext::BoxedError,
        > {
            Ok(Box::pin(common_recordbatch::EmptyRecordBatchStream::new(
                self.schema.clone(),
            )))
        }

        fn scan_stats(
            &self,
            _ctx: &QueryScanContext,
        ) -> std::result::Result<SendableFileStatsStream, common_error::ext::BoxedError> {
            Ok(Box::pin(futures::stream::iter(
                self.files.clone().into_iter().map(Ok),
            )))
        }

        fn has_predicate_without_region(&self) -> bool {
            false
        }

        fn add_dyn_filter_to_predicate(
            &mut self,
            filter_exprs: Vec<Arc<dyn datafusion::physical_plan::PhysicalExpr>>,
        ) -> Vec<bool> {
            vec![false; filter_exprs.len()]
        }

        fn set_logical_region(&mut self, logical_region: bool) {
            self.properties.set_logical_region(logical_region);
        }
    }

    impl datafusion::physical_plan::DisplayAs for StaticStatsScanner {
        fn fmt_as(
            &self,
            _t: datafusion::physical_plan::DisplayFormatType,
            f: &mut std::fmt::Formatter<'_>,
        ) -> std::fmt::Result {
            write!(f, "StaticStatsScanner")
        }
    }

    fn single_state_field(
        name: &str,
        inner_name: &str,
        inner_type: DataType,
        inner_nullable: bool,
    ) -> Field {
        Field::new(
            name,
            DataType::Struct(vec![Field::new(inner_name, inner_type, inner_nullable)].into()),
            true,
        )
    }

    fn build_region_metadata(partition_expr: Option<&str>) -> RegionMetadataRef {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
        builder.push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "value",
                datatypes::data_type::ConcreteDataType::int64_datatype(),
                true,
            ),
            semantic_type: SemanticType::Field,
            column_id: 1,
        });
        builder.push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "ts",
                datatypes::data_type::ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            semantic_type: SemanticType::Timestamp,
            column_id: 2,
        });

        let mut metadata = builder.build_without_validation().unwrap();
        metadata.set_partition_expr(partition_expr.map(str::to_string));
        Arc::new(metadata)
    }

    fn build_row_groups(chunks: &[Vec<Option<i64>>]) -> Vec<RowGroupStatsItem> {
        let arrow_schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "value",
            DataType::Int64,
            true,
        )]));
        let mut buffer = Cursor::new(Vec::new());
        let props = WriterProperties::builder().build();
        let mut writer =
            ArrowWriter::try_new(&mut buffer, arrow_schema.clone(), Some(props)).unwrap();

        for chunk in chunks {
            let batch = RecordBatch::try_new(
                arrow_schema.clone(),
                vec![Arc::new(Int64Array::from(chunk.clone()))],
            )
            .unwrap();
            writer.write(&batch).unwrap();
        }
        writer.close().unwrap();

        let metadata = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(buffer.into_inner()))
            .unwrap()
            .metadata()
            .clone();
        metadata
            .row_groups()
            .iter()
            .enumerate()
            .map(|(row_group_index, metadata)| RowGroupStatsItem {
                row_group_index,
                metadata: Arc::new(metadata.clone()),
            })
            .collect()
    }

    fn build_datafusion_aggr_expr(
        aggr: Arc<datafusion_expr::AggregateUDF>,
        alias: &str,
    ) -> Arc<AggregateFunctionExpr> {
        Arc::new(
            AggregateExprBuilder::new(aggr, vec![Arc::new(PhysicalColumn::new("value", 0))])
                .schema(Arc::new(arrow_schema::Schema::new(vec![Field::new(
                    "value",
                    DataType::Int64,
                    true,
                )])))
                .alias(alias)
                .build()
                .unwrap(),
        )
    }

    async fn collect_single_batch(exec: &StatsScanExec) -> RecordBatch {
        let stream = exec.execute(0, Arc::new(TaskContext::default())).unwrap();
        let batches = stream.map(|batch| batch.unwrap()).collect::<Vec<_>>().await;
        assert_eq!(batches.len(), 1);
        batches.into_iter().next().unwrap()
    }

    fn assert_struct_state_matches_field<'a>(
        batch: &'a RecordBatch,
        column_index: usize,
        expected_field: &Field,
    ) -> &'a StructArray {
        let struct_array = batch
            .column(column_index)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(struct_array.fields().len(), 1);
        assert_eq!(struct_array.fields()[0].as_ref(), expected_field);
        struct_array
    }

    #[tokio::test]
    async fn stats_scan_exec_matches_datafusion_count_state_field() {
        let aggr_expr = build_datafusion_aggr_expr(count_udaf(), "count(value)");
        let state_fields = aggr_expr.state_fields().unwrap();
        assert_eq!(state_fields.len(), 1);

        let inner_field = state_fields[0].as_ref().clone();
        let schema = Arc::new(arrow_schema::Schema::new(vec![single_state_field(
            "count_state",
            inner_field.name(),
            inner_field.data_type().clone(),
            inner_field.is_nullable(),
        )]));
        let region_metadata = build_region_metadata(Some("host = 'a'"));
        let scanner = StaticStatsScanner {
            schema: region_metadata.schema.clone(),
            metadata: region_metadata,
            properties: ScannerProperties::default(),
            files: vec![FileStatsItem {
                num_rows: Some(5),
                file_partition_expr: Some("host = 'a'".to_string()),
                row_groups: build_row_groups(&[vec![Some(1), None, Some(9), Some(3), None]]),
            }],
        };

        let exec = StatsScanExec::new(
            schema,
            vec![SupportStatAggr::CountNonNull {
                column_name: "value".to_string(),
            }],
            Arc::new(Mutex::new(Box::new(scanner) as RegionScannerRef)),
        );

        let batch = collect_single_batch(&exec).await;

        let struct_array = assert_struct_state_matches_field(&batch, 0, &inner_field);
        let values = struct_array
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), 3);
    }

    #[tokio::test]
    async fn stats_scan_exec_matches_datafusion_min_state_field() {
        let aggr_expr = build_datafusion_aggr_expr(min_udaf(), "min(value)");
        let state_fields = aggr_expr.state_fields().unwrap();
        assert_eq!(state_fields.len(), 1);

        let inner_field = state_fields[0].as_ref().clone();
        let schema = Arc::new(arrow_schema::Schema::new(vec![single_state_field(
            "min_state",
            inner_field.name(),
            inner_field.data_type().clone(),
            inner_field.is_nullable(),
        )]));
        let region_metadata = build_region_metadata(Some("host = 'a'"));
        let scanner = StaticStatsScanner {
            schema: region_metadata.schema.clone(),
            metadata: region_metadata,
            properties: ScannerProperties::default(),
            files: vec![FileStatsItem {
                num_rows: Some(6),
                file_partition_expr: Some("host = 'a'".to_string()),
                row_groups: build_row_groups(&[
                    vec![Some(4), Some(8), None],
                    vec![Some(-3), Some(7), Some(2)],
                ]),
            }],
        };

        let exec = StatsScanExec::new(
            schema,
            vec![SupportStatAggr::MinValue {
                column_name: "value".to_string(),
            }],
            Arc::new(Mutex::new(Box::new(scanner) as RegionScannerRef)),
        );

        let batch = collect_single_batch(&exec).await;

        let struct_array = assert_struct_state_matches_field(&batch, 0, &inner_field);
        let values = struct_array
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), -3);
    }

    #[tokio::test]
    async fn stats_scan_exec_matches_datafusion_max_state_field() {
        let aggr_expr = build_datafusion_aggr_expr(max_udaf(), "max(value)");
        let state_fields = aggr_expr.state_fields().unwrap();
        assert_eq!(state_fields.len(), 1);

        let inner_field = state_fields[0].as_ref().clone();
        let schema = Arc::new(arrow_schema::Schema::new(vec![single_state_field(
            "max_state",
            inner_field.name(),
            inner_field.data_type().clone(),
            inner_field.is_nullable(),
        )]));
        let region_metadata = build_region_metadata(Some("host = 'a'"));
        let scanner = StaticStatsScanner {
            schema: region_metadata.schema.clone(),
            metadata: region_metadata,
            properties: ScannerProperties::default(),
            files: vec![FileStatsItem {
                num_rows: Some(6),
                file_partition_expr: Some("host = 'a'".to_string()),
                row_groups: build_row_groups(&[
                    vec![Some(4), Some(8), None],
                    vec![Some(-3), Some(11), Some(2)],
                ]),
            }],
        };

        let exec = StatsScanExec::new(
            schema,
            vec![SupportStatAggr::MaxValue {
                column_name: "value".to_string(),
            }],
            Arc::new(Mutex::new(Box::new(scanner) as RegionScannerRef)),
        );

        let batch = collect_single_batch(&exec).await;

        let struct_array = assert_struct_state_matches_field(&batch, 0, &inner_field);
        let values = struct_array
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), 11);
    }

    #[tokio::test]
    async fn stats_scan_exec_rejects_datafusion_avg_multi_field_state() {
        let aggr_expr = build_datafusion_aggr_expr(avg_udaf(), "avg(value)");
        let state_fields = aggr_expr.state_fields().unwrap();
        assert!(state_fields.len() > 1);

        let schema = Arc::new(arrow_schema::Schema::new(vec![Field::new(
            "avg_state",
            DataType::Struct(
                state_fields
                    .iter()
                    .map(|field| field.as_ref().clone())
                    .collect::<Vec<_>>()
                    .into(),
            ),
            true,
        )]));
        let region_metadata = build_region_metadata(Some("host = 'a'"));
        let scanner = StaticStatsScanner {
            schema: region_metadata.schema.clone(),
            metadata: region_metadata,
            properties: ScannerProperties::default(),
            files: vec![FileStatsItem {
                num_rows: Some(5),
                file_partition_expr: Some("host = 'a'".to_string()),
                row_groups: build_row_groups(&[vec![Some(1), None, Some(9), Some(3), None]]),
            }],
        };

        let exec = StatsScanExec::new(
            schema,
            vec![SupportStatAggr::CountNonNull {
                column_name: "value".to_string(),
            }],
            Arc::new(Mutex::new(Box::new(scanner) as RegionScannerRef)),
        );

        let mut stream = exec.execute(0, Arc::new(TaskContext::default())).unwrap();
        let error = stream.next().await.unwrap().unwrap_err();

        assert!(
            error
                .to_string()
                .contains("only supports single-field state in v1")
        );
    }

    #[tokio::test]
    async fn stats_scan_exec_emits_state_rows_for_eligible_files() {
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            single_state_field("count_state", "count[count]", DataType::Int64, false),
            single_state_field("max_state", "max[max]", DataType::Int64, true),
        ]));
        let requirements = vec![
            SupportStatAggr::CountNonNull {
                column_name: "value".to_string(),
            },
            SupportStatAggr::MaxValue {
                column_name: "value".to_string(),
            },
        ];

        let region_metadata = build_region_metadata(Some("host = 'a'"));
        let eligible_row_groups = build_row_groups(&[
            vec![Some(1), None, Some(9), Some(3), None],
            vec![Some(2), Some(8), Some(7), None, Some(4)],
        ]);

        let scanner = StaticStatsScanner {
            schema: region_metadata.schema.clone(),
            metadata: region_metadata,
            properties: ScannerProperties::default(),
            files: vec![
                FileStatsItem {
                    num_rows: Some(10),
                    file_partition_expr: Some("host = 'a'".to_string()),
                    row_groups: eligible_row_groups.clone(),
                },
                FileStatsItem {
                    num_rows: Some(5),
                    file_partition_expr: Some("host = 'a'".to_string()),
                    row_groups: vec![],
                },
                FileStatsItem {
                    num_rows: Some(10),
                    file_partition_expr: Some("host = 'b'".to_string()),
                    row_groups: eligible_row_groups,
                },
            ],
        };

        let exec = StatsScanExec::new(
            schema,
            requirements,
            Arc::new(Mutex::new(Box::new(scanner) as RegionScannerRef)),
        );

        let stream = exec.execute(0, Arc::new(TaskContext::default())).unwrap();
        let batches = stream.map(|batch| batch.unwrap()).collect::<Vec<_>>().await;

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 1);

        let count_state = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let count_values = count_state
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(count_values.value(0), 7);

        let max_state = batch
            .column(1)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let max_values = max_state
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(max_values.value(0), 9);
    }

    #[tokio::test]
    async fn stats_scan_exec_emits_no_batches_when_all_files_fallback() {
        let schema = Arc::new(arrow_schema::Schema::new(vec![single_state_field(
            "count_state",
            "count[count]",
            DataType::Int64,
            false,
        )]));
        let region_metadata = build_region_metadata(Some("host = 'a'"));
        let scanner = StaticStatsScanner {
            schema: region_metadata.schema.clone(),
            metadata: region_metadata,
            properties: ScannerProperties::default(),
            files: vec![
                FileStatsItem {
                    num_rows: Some(5),
                    file_partition_expr: Some("host = 'a'".to_string()),
                    row_groups: vec![],
                },
                FileStatsItem {
                    num_rows: Some(5),
                    file_partition_expr: Some("host = 'b'".to_string()),
                    row_groups: build_row_groups(&[vec![Some(1), Some(2), Some(3)]]),
                },
            ],
        };

        let exec = StatsScanExec::new(
            schema,
            vec![SupportStatAggr::CountNonNull {
                column_name: "value".to_string(),
            }],
            Arc::new(Mutex::new(Box::new(scanner) as RegionScannerRef)),
        );

        let stream = exec.execute(0, Arc::new(TaskContext::default())).unwrap();
        let batches = stream.map(|batch| batch.unwrap()).collect::<Vec<_>>().await;

        assert!(batches.is_empty());
    }

    #[tokio::test]
    async fn stats_scan_exec_count_rows_uses_file_num_rows_without_row_groups() {
        let schema = Arc::new(arrow_schema::Schema::new(vec![single_state_field(
            "count_state",
            "count[count]",
            DataType::Int64,
            false,
        )]));
        let region_metadata = build_region_metadata(Some("host = 'a'"));
        let scanner = StaticStatsScanner {
            schema: region_metadata.schema.clone(),
            metadata: region_metadata,
            properties: ScannerProperties::default(),
            files: vec![
                FileStatsItem {
                    num_rows: Some(7),
                    file_partition_expr: Some("host = 'a'".to_string()),
                    row_groups: vec![],
                },
                FileStatsItem {
                    num_rows: Some(3),
                    file_partition_expr: Some("host = 'a'".to_string()),
                    row_groups: vec![],
                },
            ],
        };

        let exec = StatsScanExec::new(
            schema,
            vec![SupportStatAggr::CountRows],
            Arc::new(Mutex::new(Box::new(scanner) as RegionScannerRef)),
        );

        let batch = collect_single_batch(&exec).await;
        assert_eq!(batch.num_rows(), 2);

        let count_state = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let count_values = count_state
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(count_values.value(0), 7);
        assert_eq!(count_values.value(1), 3);
    }
}
