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

//! Fan-in for already-authorized native raw-series region reads.

use std::sync::Arc;

use std::task::{Context, Poll};

use common_error::ext::{BoxedError, PlainError};
use common_error::status_code::StatusCode;
use common_query::request::{QueryRequest, RawSeriesRead, RawSeriesReadColumn};
use common_recordbatch::DfSendableRecordBatchStream;
use common_recordbatch::adapter::DfRecordBatchStreamAdapter;
use common_telemetry::tracing_context::TracingContext;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    apply_expression_roots,
};
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{DataFusionError, Result, Statistics};
use datafusion_physical_expr::EquivalenceProperties;
use datafusion_physical_expr::PhysicalExpr;
use datatypes::arrow::datatypes::Schema as ArrowSchema;
use datatypes::series::SeriesOrdering;
use datatypes::series::frame::{
    FrameLabelMode, LabelDecodeColumn, LabelDecodeEncoding, LabelDecodeSpec, LabelDecodeTag,
};
use datatypes::series::raw::RawSeriesChunk;
use datatypes::series::samples::{FieldSpec, SeriesSchema};
use greptime_proto::v1::region::RegionRequestHeader;
use promql::source::merge_ordered_raw_series;
use session::context::QueryContextRef;
use store_api::region_engine::{RawLabelSource, RawSeriesStream, SendableRawSeriesStream};
use store_api::storage::RegionId;
use table::metadata::TableInfo;

use crate::dist_plan::merge_scan::validate_remote_schema;
use crate::region_query::{RegionQueryHandlerRef, RegionQueryTarget};

/// Bind the authorized logical table metadata into its physical raw-series read and decode schema.
pub(crate) fn bind_raw_series_table(
    table: &TableInfo,
    metric_name: &str,
    tag_names: &[String],
    field_names: &[String],
    region_id: RegionId,
) -> Result<(RawSeriesRead, SeriesSchema, LabelDecodeSpec)> {
    let fail = |message: &str| DataFusionError::Plan(message.to_string());
    let schema = &table.meta.schema;
    let ids = table
        .name_to_ids()
        .ok_or_else(|| fail("raw-series table metadata has incomplete column ids"))?;
    let bind = |index: usize| -> Result<RawSeriesReadColumn> {
        let column = schema
            .column_schemas()
            .get(index)
            .ok_or_else(|| fail("raw-series column index is invalid"))?;
        let column_id = *ids
            .get(&column.name)
            .ok_or_else(|| fail("raw-series column id is missing"))?;
        Ok(RawSeriesReadColumn {
            column_id,
            name: column.name.clone(),
            data_type: column.data_type.clone(),
            nullable: column.is_nullable(),
        })
    };
    let time_index = schema
        .timestamp_index()
        .ok_or_else(|| fail("raw-series table has no time index"))?;
    let primary_key = table
        .meta
        .primary_key_indices
        .iter()
        .map(|&index| bind(index))
        .collect::<Result<Vec<_>>>()?;
    let key_indices = table
        .meta
        .primary_key_indices
        .iter()
        .copied()
        .collect::<std::collections::HashSet<_>>();
    let mut logical_tags = Vec::with_capacity(tag_names.len());
    let mut tags = Vec::with_capacity(tag_names.len());
    for name in tag_names {
        let index = schema
            .column_schemas()
            .iter()
            .position(|column| column.name == *name)
            .ok_or_else(|| fail("raw-series tag column is missing"))?;
        if !key_indices.contains(&index) {
            return Err(fail("raw-series tag is not a primary-key column"));
        }
        let column = bind(index)?;
        logical_tags.push(LabelDecodeTag {
            column_id: column.column_id,
            name: column.name.clone(),
        });
        tags.push(column);
    }
    let mut fields = Vec::with_capacity(field_names.len());
    let mut series_fields = Vec::with_capacity(field_names.len());
    for name in field_names {
        let index = schema
            .column_schemas()
            .iter()
            .position(|column| column.name == *name)
            .ok_or_else(|| fail("raw-series field column is missing"))?;
        if key_indices.contains(&index) || index == time_index {
            return Err(fail("raw-series field has an invalid table role"));
        }
        let column = bind(index)?;
        if !column.data_type.is_numeric() {
            return Err(fail("raw-series field has an unsupported data type"));
        }
        series_fields.push(FieldSpec {
            name: Arc::from(column.name.as_str()),
        });
        fields.push(column);
    }
    if fields.is_empty() {
        return Err(fail("raw-series read requires fields"));
    }
    let primary_key_decode = primary_key
        .iter()
        .map(|column| LabelDecodeColumn {
            column_id: column.column_id,
            name: column.name.clone(),
            physical_type: column.data_type.clone(),
        })
        .collect();
    let mut decode = LabelDecodeSpec {
        version: datatypes::series::frame::LABEL_DECODE_VERSION,
        metric_name: metric_name.to_string(),
        encoding: LabelDecodeEncoding::Dense,
        primary_key: primary_key_decode,
        logical_tags,
    };
    decode.canonicalize();
    decode
        .validate()
        .map_err(|error| DataFusionError::Plan(error.to_string()))?;
    let request = RawSeriesRead {
        version: 1,
        region_id,
        metric_name: metric_name.to_string(),
        primary_key,
        time_index: bind(time_index)?,
        tags,
        fields,
    };
    Ok((
        request,
        SeriesSchema {
            fields: series_fields.into(),
            eval_grid: None,
        },
        decode,
    ))
}

/// One frozen destination in an authorized native raw-series fan-in.
pub(crate) type FrozenRawRegion = (RegionId, RegionQueryTarget);

/// A leaf physical operator that fans in regions under an already-authorized read binding.
pub(crate) struct NativeRawFanInExec {
    template: RawSeriesRead,
    expected_raw_schema: Arc<SeriesSchema>,
    expected_decode: LabelDecodeSpec,
    regions: Vec<FrozenRawRegion>,
    plan: datafusion_expr::LogicalPlan,
    handler: RegionQueryHandlerRef,
    query_ctx: QueryContextRef,
    schema: datatypes::arrow::datatypes::SchemaRef,
    remote_schema: datatypes::arrow::datatypes::SchemaRef,
    properties: Arc<PlanProperties>,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use table::metadata::{TableInfoBuilder, TableMetaBuilder};

    use futures_util::{Stream, StreamExt};

    use super::*;

    fn table_info(primary_key_indices: Vec<usize>) -> TableInfo {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new("host", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new("value", ConcreteDataType::float64_datatype(), true),
        ]));
        let meta = TableMetaBuilder::empty()
            .schema(schema)
            .primary_key_indices(primary_key_indices)
            .value_indices(vec![1, 2])
            .column_ids(vec![41, 42, 43])
            .next_column_id(44)
            .build()
            .unwrap();
        TableInfoBuilder::new("cpu_usage", meta).build().unwrap()
    }

    #[test]
    fn binds_raw_read_from_table_metadata() {
        let table = table_info(vec![0]);
        let (read, schema, decode) = bind_raw_series_table(
            &table,
            "cpu_usage",
            &["host".to_string()],
            &["value".to_string()],
            RegionId::new(4, 9),
        )
        .unwrap();
        assert_eq!(read.primary_key[0].column_id, 41);
        assert_eq!(read.time_index.column_id, 42);
        assert_eq!(read.tags[0].name, "host");
        assert_eq!(read.fields[0].column_id, 43);
        assert_eq!(schema.fields[0].name.as_ref(), "value");
        assert_eq!(
            decode.primary_key[0].physical_type,
            ConcreteDataType::string_datatype()
        );
        assert_eq!(decode.logical_tags[0].column_id, 41);
    }

    #[test]
    fn binds_raw_read_from_tagless_table() {
        let table = table_info(vec![]);
        let (read, schema, decode) = bind_raw_series_table(
            &table,
            "cpu_usage",
            &[],
            &["value".to_string()],
            RegionId::new(4, 9),
        )
        .unwrap();
        assert!(read.primary_key.is_empty());
        assert!(read.tags.is_empty());
        assert!(decode.primary_key.is_empty());
        assert!(decode.logical_tags.is_empty());
        assert_eq!(schema.fields[0].name.as_ref(), "value");
        let frame_schema = fan_in_frame_schema(&schema, &decode).unwrap();
        assert_eq!(
            datatypes::series::frame::series_frame_label_decode(&frame_schema)
                .unwrap()
                .as_ref(),
            Some(&decode)
        );
    }

    struct UnusedHandler;

    #[async_trait::async_trait]
    impl crate::region_query::RegionQueryHandler for UnusedHandler {
        async fn select_target(
            &self,
            _: session::ReadPreference,
            _: RegionId,
        ) -> crate::error::Result<crate::region_query::RegionQueryTarget> {
            unreachable!()
        }
        async fn do_get(
            &self,
            _: &crate::region_query::RegionQueryTarget,
            _: QueryRequest,
        ) -> crate::error::Result<common_recordbatch::SendableRecordBatchStream> {
            unreachable!()
        }
        async fn handle_remote_dyn_filter_update(
            &self,
            _: &crate::region_query::RegionQueryTarget,
            _: String,
            _: api::v1::region::RemoteDynFilterUpdate,
        ) -> crate::error::Result<()> {
            unreachable!()
        }
        async fn handle_remote_dyn_filter_unregister(
            &self,
            _: &crate::region_query::RegionQueryTarget,
            _: String,
            _: api::v1::region::RemoteDynFilterUnregister,
        ) -> crate::error::Result<()> {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn empty_fan_in_returns_empty_stream_with_declared_schema() {
        let table = table_info(vec![0]);
        let (template, raw_schema, decode) = bind_raw_series_table(
            &table,
            "cpu_usage",
            &["host".to_string()],
            &["value".to_string()],
            RegionId::new(4, 9),
        )
        .unwrap();
        let exec = NativeRawFanInExec::try_new(
            template,
            Arc::new(raw_schema),
            decode,
            vec![],
            datafusion::logical_expr::LogicalPlanBuilder::empty(true)
                .build()
                .unwrap(),
            Arc::new(UnusedHandler),
            session::context::QueryContext::arc(),
        )
        .unwrap();
        let expected_schema = exec.schema();
        let context = Arc::new(TaskContext::default());

        let mut stream = exec.execute(0, context.clone()).unwrap();
        assert_eq!(stream.schema(), expected_schema);
        assert!(futures_util::StreamExt::next(&mut stream).await.is_none());
        assert!(exec.execute(1, context).is_err());
    }

    #[test]
    fn fan_in_declares_key_schema_and_rejects_empty_wrong_layout() {
        let table = table_info(vec![0]);
        let (_, raw_schema, decode) = bind_raw_series_table(
            &table,
            "cpu_usage",
            &["host".to_string()],
            &["value".to_string()],
            RegionId::new(4, 9),
        )
        .unwrap();
        let expected = fan_in_frame_schema(&raw_schema, &decode).unwrap();
        let actual_decode = datatypes::series::frame::series_frame_label_decode(&expected).unwrap();
        assert_eq!(actual_decode.as_ref(), Some(&decode));

        // Schema validation also applies to an empty stream: no batch is needed to expose a
        // frame whose label layout violates the authorized remote binding.
        let wrong = datatypes::series::frame::series_frame_schema(
            &raw_schema,
            FrameLabelMode::Explicit,
            None,
        )
        .unwrap();
        assert!(validate_remote_schema(&expected, &wrong, "test empty stream").is_err());
    }

    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    use async_trait::async_trait;
    use common_meta::peer::Peer;
    use common_recordbatch::{RecordBatch, RecordBatchStream};
    use session::ReadPreference;
    use snafu::IntoError;

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    struct GuardedEmptyStream {
        schema: Arc<datatypes::schema::Schema>,
        fail_next: bool,
        _drop_flag: DropFlag,
    }

    impl Stream for GuardedEmptyStream {
        type Item = common_recordbatch::error::Result<RecordBatch>;

        fn poll_next(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Self::Item>> {
            if self.fail_next {
                self.fail_next = false;
                Poll::Ready(Some(Err(common_recordbatch::error::ExternalSnafu
                    .into_error(BoxedError::new(PlainError::new(
                        "late upstream failure".to_string(),
                        StatusCode::RequestOutdated,
                    ))))))
            } else {
                Poll::Ready(None)
            }
        }
    }

    impl RecordBatchStream for GuardedEmptyStream {
        fn schema(&self) -> Arc<datatypes::schema::Schema> {
            self.schema.clone()
        }
        fn output_ordering(&self) -> Option<&[common_recordbatch::OrderOption]> {
            None
        }
        fn metrics(&self) -> Option<common_recordbatch::adapter::RecordBatchMetrics> {
            None
        }
    }

    enum HandlerStep {
        Empty(Arc<datatypes::schema::Schema>, Arc<AtomicBool>),
        LateError(Arc<datatypes::schema::Schema>, Arc<AtomicBool>),
        Fail,
        Pending(Arc<AtomicBool>),
    }

    struct LifecycleHandler {
        steps: Mutex<Vec<HandlerStep>>,
        seen: Mutex<Vec<(RegionId, u64)>>,
    }

    #[async_trait]
    impl crate::region_query::RegionQueryHandler for LifecycleHandler {
        async fn select_target(
            &self,
            _: ReadPreference,
            region_id: RegionId,
        ) -> crate::error::Result<RegionQueryTarget> {
            Ok(RegionQueryTarget::new(Peer {
                id: region_id.as_u64(),
                addr: "127.0.0.1:1".into(),
            }))
        }
        async fn do_get(
            &self,
            target: &RegionQueryTarget,
            request: QueryRequest,
        ) -> crate::error::Result<common_recordbatch::SendableRecordBatchStream> {
            self.seen
                .lock()
                .unwrap()
                .push((request.region_id, target.peer().id));
            let step = self.steps.lock().unwrap().remove(0);
            match step {
                HandlerStep::Empty(schema, dropped) => Ok(Box::pin(GuardedEmptyStream {
                    schema,
                    fail_next: false,
                    _drop_flag: DropFlag(dropped),
                })),
                HandlerStep::LateError(schema, dropped) => Ok(Box::pin(GuardedEmptyStream {
                    schema,
                    fail_next: true,
                    _drop_flag: DropFlag(dropped),
                })),
                HandlerStep::Fail => Err(crate::error::UnimplementedSnafu {
                    operation: "test region request failure".to_string(),
                }
                .build()),
                HandlerStep::Pending(dropped) => {
                    let _guard = DropFlag(dropped);
                    futures_util::future::pending::<
                        crate::error::Result<common_recordbatch::SendableRecordBatchStream>,
                    >()
                    .await
                }
            }
        }
        async fn handle_remote_dyn_filter_update(
            &self,
            _: &RegionQueryTarget,
            _: String,
            _: api::v1::region::RemoteDynFilterUpdate,
        ) -> crate::error::Result<()> {
            unreachable!()
        }
        async fn handle_remote_dyn_filter_unregister(
            &self,
            _: &RegionQueryTarget,
            _: String,
            _: api::v1::region::RemoteDynFilterUnregister,
        ) -> crate::error::Result<()> {
            unreachable!()
        }
    }

    fn lifecycle_exec(
        steps: Vec<HandlerStep>,
        ids: [RegionId; 2],
    ) -> (NativeRawFanInExec, Arc<LifecycleHandler>) {
        let (template, raw_schema, decode) = bind_raw_series_table(
            &table_info(vec![0]),
            "cpu_usage",
            &["host".to_string()],
            &["value".to_string()],
            ids[0],
        )
        .unwrap();
        let schema = fan_in_frame_schema(&raw_schema, &decode).unwrap();
        let declared = Arc::new(Schema::try_from(schema).unwrap());
        let handler = Arc::new(LifecycleHandler {
            steps: Mutex::new(steps),
            seen: Mutex::new(vec![]),
        });
        let regions = ids
            .into_iter()
            .map(|id| {
                (
                    id,
                    RegionQueryTarget::new(Peer {
                        id: id.as_u64(),
                        addr: "127.0.0.1:1".into(),
                    }),
                )
            })
            .collect();
        let steps = std::mem::replace(&mut *handler.steps.lock().unwrap(), vec![]);
        let steps = steps
            .into_iter()
            .map(|step| match step {
                HandlerStep::Empty(_, flag) => HandlerStep::Empty(declared.clone(), flag),
                HandlerStep::LateError(_, flag) => HandlerStep::LateError(declared.clone(), flag),
                other => other,
            })
            .collect();
        *handler.steps.lock().unwrap() = steps;
        let exec = NativeRawFanInExec::try_new(
            template,
            Arc::new(raw_schema),
            decode,
            regions,
            datafusion::logical_expr::LogicalPlanBuilder::empty(true)
                .build()
                .unwrap(),
            handler.clone(),
            session::context::QueryContext::arc(),
        )
        .unwrap();
        (exec, handler)
    }

    #[tokio::test]
    async fn later_region_request_error_releases_previously_opened_stream() {
        let first_dropped = Arc::new(AtomicBool::new(false));
        let ids = [RegionId::new(4, 1), RegionId::new(4, 2)];
        let (exec, handler) = lifecycle_exec(
            vec![
                HandlerStep::Empty(Arc::new(Schema::new(vec![])), first_dropped.clone()),
                HandlerStep::Fail,
            ],
            ids,
        );
        let mut output = exec.execute(0, Arc::new(TaskContext::default())).unwrap();
        assert_eq!(output.schema(), exec.schema());
        assert!(output.next().await.unwrap().is_err());
        assert!(output.next().await.is_none());
        assert!(first_dropped.load(Ordering::SeqCst));
        assert_eq!(
            handler.seen.lock().unwrap().as_slice(),
            &[(ids[0], ids[0].as_u64()), (ids[1], ids[1].as_u64())]
        );
    }

    #[tokio::test]
    async fn late_region_stream_error_is_not_silently_treated_as_eof() {
        let upstream_dropped = Arc::new(AtomicBool::new(false));
        let other_dropped = Arc::new(AtomicBool::new(false));
        let ids = [RegionId::new(4, 1), RegionId::new(4, 2)];
        let (exec, _) = lifecycle_exec(
            vec![
                HandlerStep::LateError(Arc::new(Schema::new(vec![])), upstream_dropped.clone()),
                HandlerStep::Empty(Arc::new(Schema::new(vec![])), other_dropped.clone()),
            ],
            ids,
        );
        let mut output = exec.execute(0, Arc::new(TaskContext::default())).unwrap();
        assert!(output.next().await.unwrap().is_err());
        assert!(output.next().await.is_none());
        assert!(upstream_dropped.load(Ordering::SeqCst));
        assert!(other_dropped.load(Ordering::SeqCst));
    }

    #[test]
    fn dropping_output_cancels_pending_request_and_releases_open_stream() {
        let first_dropped = Arc::new(AtomicBool::new(false));
        let request_dropped = Arc::new(AtomicBool::new(false));
        let ids = [RegionId::new(4, 1), RegionId::new(4, 2)];
        let (exec, _) = lifecycle_exec(
            vec![
                HandlerStep::Empty(Arc::new(Schema::new(vec![])), first_dropped.clone()),
                HandlerStep::Pending(request_dropped.clone()),
            ],
            ids,
        );
        let mut output = exec.execute(0, Arc::new(TaskContext::default())).unwrap();
        let waker = futures_util::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        assert!(matches!(
            output.as_mut().poll_next(&mut context),
            Poll::Pending
        ));
        drop(output);
        assert!(request_dropped.load(Ordering::SeqCst));
        assert!(first_dropped.load(Ordering::SeqCst));
    }

    #[test]
    fn rejects_invalid_column_roles_and_missing_id_map() {
        let table = table_info(vec![0]);
        assert!(
            bind_raw_series_table(
                &table,
                "cpu_usage",
                &["value".to_string()],
                &["value".to_string()],
                RegionId::new(4, 9)
            )
            .is_err()
        );
        let mut missing_ids = table;
        missing_ids.meta.column_ids.clear();
        assert!(
            bind_raw_series_table(
                &missing_ids,
                "cpu_usage",
                &["host".to_string()],
                &["value".to_string()],
                RegionId::new(4, 9)
            )
            .is_err()
        );
    }
}

fn fan_in_frame_schema(
    raw_schema: &SeriesSchema,
    decode: &LabelDecodeSpec,
) -> Result<datatypes::arrow::datatypes::SchemaRef> {
    let basic = datatypes::series::frame::series_frame_schema(
        raw_schema,
        FrameLabelMode::Key,
        Some(decode),
    )
    .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let mut metadata = basic.metadata().clone();
    metadata.insert(
        "greptime.series.ordering".into(),
        "read_key_then_time".into(),
    );
    Ok(Arc::new(ArrowSchema::new_with_metadata(
        basic.fields().clone(),
        metadata,
    )))
}

impl NativeRawFanInExec {
    pub(crate) fn try_new(
        template: RawSeriesRead,
        expected_raw_schema: Arc<SeriesSchema>,
        expected_decode: LabelDecodeSpec,
        regions: Vec<FrozenRawRegion>,
        plan: datafusion_expr::LogicalPlan,
        handler: RegionQueryHandlerRef,
        query_ctx: QueryContextRef,
    ) -> Result<Self> {
        if expected_decode.encoding != datatypes::series::frame::LabelDecodeEncoding::Dense
            || template.version != 1
            || template.metric_name != expected_decode.metric_name
            || template.primary_key.len() != expected_decode.primary_key.len()
        {
            return Err(DataFusionError::Plan(
                "native raw fan-in requires dense keys".into(),
            ));
        }
        let remote_schema = fan_in_frame_schema(&expected_raw_schema, &expected_decode)?;
        let schema = remote_schema.clone();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            template,
            expected_raw_schema,
            expected_decode,
            regions,
            plan,
            handler,
            query_ctx,
            schema,
            remote_schema,
            properties,
        })
    }

    fn stream(
        &self,
        context: Arc<TaskContext>,
        partition: usize,
    ) -> Result<DfSendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "invalid NativeRawFanInExec partition {partition}"
            )));
        }
        if self.regions.is_empty() {
            return Ok(Box::pin(
                datafusion::physical_plan::EmptyRecordBatchStream::new(self.schema.clone()),
            ));
        }
        let expected_schema = self.expected_raw_schema.clone();
        let template = self.template.clone();
        let expected_decode = self.expected_decode.clone();
        let regions = self.regions.clone();
        let plan = self.plan.clone();
        let handler = self.handler.clone();
        let query_ctx = self.query_ctx.clone();
        let task_id = context.task_id().unwrap_or_default();
        let tracing = TracingContext::from_json(context.session_id().as_str());
        let frame_schema = self.remote_schema.clone();
        let pending = Box::pin(async move {
            let mut streams = Vec::with_capacity(regions.len());
            for (region_id, target) in regions {
                let mut region_ctx = query_ctx.fork();
                let mut read = template.clone();
                read.region_id = region_id;
                region_ctx.set_extension(
                    RawSeriesRead::EXTENSION_KEY,
                    read.encode().map_err(|error| {
                        BoxedError::new(PlainError::new(error.to_string(), StatusCode::Unexpected))
                    })?,
                );
                let request = QueryRequest {
                    header: Some(RegionRequestHeader {
                        tracing_context: tracing.to_w3c(),
                        dbname: task_id.clone(),
                        query_context: Some((&region_ctx).into()),
                    }),
                    region_id,
                    plan: plan.clone(),
                };
                let stream = handler
                    .do_get(&target, request)
                    .await
                    .map_err(BoxedError::new)?;
                validate_remote_schema(
                    &frame_schema,
                    stream.schema().arrow_schema().as_ref(),
                    "native raw advertised stream",
                )
                .map_err(|error| {
                    BoxedError::new(PlainError::new(error.to_string(), StatusCode::Unexpected))
                })?;
                let stream_schema = stream.schema().arrow_schema().clone();
                let df_stream = Box::pin(DfRecordBatchStreamAdapter::new(stream));
                let raw = common_query::series_frame::unpack_raw_series(
                    df_stream,
                    Some(expected_schema.clone()),
                )
                .map_err(|error| {
                    BoxedError::new(PlainError::new(error.to_string(), StatusCode::Unexpected))
                })?;
                let actual_decode =
                    datatypes::series::frame::series_frame_label_decode(stream_schema.as_ref())
                        .map_err(|error| {
                            BoxedError::new(PlainError::new(
                                error.to_string(),
                                StatusCode::Unexpected,
                            ))
                        })?;
                if actual_decode.as_ref() != Some(&expected_decode) {
                    return Err(BoxedError::new(PlainError::new(
                        "native raw label decode mismatch".to_string(),
                        StatusCode::Unexpected,
                    )));
                }
                streams.push(raw);
            }
            merge_ordered_raw_series(streams)
        });
        let raw_stream = FanInRawStream {
            expected_schema: self.expected_raw_schema.clone(),
            expected_decode: self.expected_decode.clone(),
            pending,
            merged: None,
            finished: false,
        };
        common_query::series_frame::pack_raw_series(Box::pin(raw_stream))
            .map_err(|error| DataFusionError::External(Box::new(error)))
    }
}

struct FanInRawStream {
    expected_schema: Arc<SeriesSchema>,
    expected_decode: LabelDecodeSpec,
    pending: std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = std::result::Result<SendableRawSeriesStream, BoxedError>,
                > + Send,
        >,
    >,
    merged: Option<SendableRawSeriesStream>,
    finished: bool,
}

impl futures_util::Stream for FanInRawStream {
    type Item = std::result::Result<RawSeriesChunk, BoxedError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }
        if let Some(merged) = self.merged.as_mut() {
            let result = merged.as_mut().poll_next(cx);
            if matches!(result, Poll::Ready(None)) {
                self.finished = true;
            }
            return result;
        }
        match self.pending.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => {
                self.finished = true;
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(Ok(merged)) => {
                self.merged = Some(merged);
                self.merged.as_mut().unwrap().as_mut().poll_next(cx)
            }
        }
    }
}

impl RawSeriesStream for FanInRawStream {
    fn schema(&self) -> &Arc<SeriesSchema> {
        self.merged
            .as_ref()
            .map_or(&self.expected_schema, |stream| stream.schema())
    }
    fn label_source(&self) -> &RawLabelSource {
        &store_api::region_engine::NO_BOUND_LABEL_SOURCE
    }
    fn label_decode_spec(&self) -> Option<&LabelDecodeSpec> {
        Some(&self.expected_decode)
    }
    fn ordering(&self) -> SeriesOrdering {
        SeriesOrdering::KeyThenTime
    }
}

impl std::fmt::Debug for NativeRawFanInExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeRawFanInExec")
            .field("regions", &self.regions.len())
            .finish()
    }
}

impl ExecutionPlan for NativeRawFanInExec {
    fn name(&self) -> &str {
        "NativeRawFanInExec"
    }
    fn schema(&self) -> datatypes::arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        apply_expression_roots(std::iter::empty::<Arc<dyn PhysicalExpr>>(), f)
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<DfSendableRecordBatchStream> {
        self.stream(context, partition)
    }
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }
    fn statistics_from_inputs(
        &self,
        _input_stats: &[Arc<Statistics>],
        _args: &datafusion::physical_plan::StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        Ok(Arc::new(Statistics::new_unknown(self.schema.as_ref())))
    }
}

impl DisplayAs for NativeRawFanInExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "NativeRawFanInExec: regions={}", self.regions.len())
    }
}
