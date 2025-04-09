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

use std::sync::Arc;

use arrow_schema::SchemaRef as ArrowSchemaRef;
use common_catalog::consts::INFORMATION_SCHEMA_RUNTIME_METRICS_TABLE_ID;
use common_error::ext::BoxedError;
use common_recordbatch::adapter::RecordBatchStreamAdapter;
use common_recordbatch::{RecordBatch, SendableRecordBatchStream};
use common_time::util::current_time_millis;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter as DfRecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream as DfPartitionStream;
use datafusion::physical_plan::SendableRecordBatchStream as DfSendableRecordBatchStream;
use datatypes::prelude::{ConcreteDataType, MutableVector};
use datatypes::scalars::ScalarVectorBuilder;
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use datatypes::vectors::{
    ConstantVector, Float64VectorBuilder, StringVectorBuilder, TimestampMillisecondVector,
    VectorRef,
};
use itertools::Itertools;
use snafu::ResultExt;
use store_api::storage::{ScanRequest, TableId};

use crate::error::{CreateRecordBatchSnafu, InternalSnafu, Result};
use crate::system_schema::information_schema::{InformationTable, RUNTIME_METRICS};

#[derive(Debug)]
pub(super) struct InformationSchemaMetrics {
    schema: SchemaRef,
}

const METRIC_NAME: &str = "metric_name";
const METRIC_VALUE: &str = "value";
const METRIC_LABELS: &str = "labels";
const PEER_ADDR: &str = "peer_addr";
const PEER_TYPE: &str = "peer_type";
const TIMESTAMP: &str = "timestamp";

/// The `information_schema.runtime_metrics` virtual table.
/// It provides the GreptimeDB runtime metrics for the users by SQL.
impl InformationSchemaMetrics {
    pub(super) fn new() -> Self {
        Self {
            schema: Self::schema(),
        }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            ColumnSchema::new(METRIC_NAME, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(METRIC_VALUE, ConcreteDataType::float64_datatype(), false),
            ColumnSchema::new(METRIC_LABELS, ConcreteDataType::string_datatype(), true),
            ColumnSchema::new(PEER_ADDR, ConcreteDataType::string_datatype(), true),
            ColumnSchema::new(PEER_TYPE, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(
                TIMESTAMP,
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
        ]))
    }

    fn builder(&self) -> InformationSchemaMetricsBuilder {
        InformationSchemaMetricsBuilder::new(self.schema.clone())
    }
}

impl InformationTable for InformationSchemaMetrics {
    fn table_id(&self) -> TableId {
        INFORMATION_SCHEMA_RUNTIME_METRICS_TABLE_ID
    }

    fn table_name(&self) -> &'static str {
        RUNTIME_METRICS
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn to_stream(&self, request: ScanRequest) -> Result<SendableRecordBatchStream> {
        let schema = self.schema.arrow_schema().clone();
        let mut builder = self.builder();
        let stream = Box::pin(DfRecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move {
                builder
                    .make_metrics(Some(request))
                    .await
                    .map(|x| x.into_df_record_batch())
                    .map_err(Into::into)
            }),
        ));

        Ok(Box::pin(
            RecordBatchStreamAdapter::try_new(stream)
                .map_err(BoxedError::new)
                .context(InternalSnafu)?,
        ))
    }
}

struct InformationSchemaMetricsBuilder {
    schema: SchemaRef,

    metric_names: StringVectorBuilder,
    metric_values: Float64VectorBuilder,
    metric_labels: StringVectorBuilder,
    peer_addrs: StringVectorBuilder,
    peer_types: StringVectorBuilder,
}

impl InformationSchemaMetricsBuilder {
    fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            metric_names: StringVectorBuilder::with_capacity(42),
            metric_values: Float64VectorBuilder::with_capacity(42),
            metric_labels: StringVectorBuilder::with_capacity(42),
            peer_addrs: StringVectorBuilder::with_capacity(42),
            peer_types: StringVectorBuilder::with_capacity(42),
        }
    }

    fn add_metric(
        &mut self,
        metric_name: &str,
        labels: String,
        metric_value: f64,
        peer: Option<&str>,
        peer_type: &str,
    ) {
        self.metric_names.push(Some(metric_name));
        self.metric_values.push(Some(metric_value));
        self.metric_labels.push(Some(&labels));
        self.peer_addrs.push(peer);
        self.peer_types.push(Some(peer_type));
    }

    async fn make_metrics(&mut self, _request: Option<ScanRequest>) -> Result<RecordBatch> {
        let metric_families = prometheus::gather();

        let write_request =
            common_telemetry::metric::convert_metric_to_write_request(metric_families, None, 0);

        for ts in write_request.timeseries {
            //Safety: always has `__name__` label
            let metric_name = ts
                .labels
                .iter()
                .find_map(|label| {
                    if label.name == "__name__" {
                        Some(label.value.clone())
                    } else {
                        None
                    }
                })
                .unwrap();

            self.add_metric(
                &metric_name,
                ts.labels
                    .into_iter()
                    .filter_map(|label| {
                        if label.name == "__name__" {
                            None
                        } else {
                            Some(format!("{}={}", label.name, label.value))
                        }
                    })
                    .join(", "),
                // Safety: always has a sample
                ts.samples[0].value,
                // The peer column is always `None` for standalone
                None,
                "STANDALONE",
            );
        }

        // FIXME(dennis): fetching other peers metrics
        self.finish()
    }

    fn finish(&mut self) -> Result<RecordBatch> {
        let rows_num = self.metric_names.len();

        let timestamps = Arc::new(ConstantVector::new(
            Arc::new(TimestampMillisecondVector::from_slice([
                current_time_millis(),
            ])),
            rows_num,
        ));

        let columns: Vec<VectorRef> = vec![
            Arc::new(self.metric_names.finish()),
            Arc::new(self.metric_values.finish()),
            Arc::new(self.metric_labels.finish()),
            Arc::new(self.peer_addrs.finish()),
            Arc::new(self.peer_types.finish()),
            timestamps,
        ];

        RecordBatch::new(self.schema.clone(), columns).context(CreateRecordBatchSnafu)
    }
}

impl DfPartitionStream for InformationSchemaMetrics {
    fn schema(&self) -> &ArrowSchemaRef {
        self.schema.arrow_schema()
    }

    fn execute(&self, _: Arc<TaskContext>) -> DfSendableRecordBatchStream {
        let schema = self.schema.arrow_schema().clone();
        let mut builder = self.builder();
        Box::pin(DfRecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move {
                builder
                    .make_metrics(None)
                    .await
                    .map(|x| x.into_df_record_batch())
                    .map_err(Into::into)
            }),
        ))
    }
}

#[cfg(test)]
mod tests {
    use common_recordbatch::RecordBatches;

    use super::*;

    #[tokio::test]
    async fn test_make_metrics() {
        let metrics = InformationSchemaMetrics::new();

        let stream = metrics.to_stream(ScanRequest::default()).unwrap();

        let batches = RecordBatches::try_collect(stream).await.unwrap();

        let result_literal = batches.pretty_print().unwrap();

        assert!(result_literal.contains(METRIC_NAME));
        assert!(result_literal.contains(METRIC_VALUE));
        assert!(result_literal.contains(METRIC_LABELS));
        assert!(result_literal.contains(PEER_ADDR));
        assert!(result_literal.contains(PEER_TYPE));
        assert!(result_literal.contains(TIMESTAMP));
    }
}
