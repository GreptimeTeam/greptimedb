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

use std::fmt;
use std::sync::Arc;

use api::v1::SemanticType;
use common_error::ext::BoxedError;
use common_recordbatch::SendableRecordBatchStream;
use common_telemetry::{debug, error, tracing};
use datafusion::logical_expr;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use snafu::{OptionExt, ResultExt};
use store_api::metadata::{RegionMetadataBuilder, RegionMetadataRef};
use store_api::metric_engine_consts::DATA_SCHEMA_TABLE_ID_COLUMN_NAME;
use store_api::region_engine::{
    PrepareRequest, QueryScanContext, RegionEngine, RegionScanner, RegionScannerRef,
    ScannerProperties,
};
use store_api::storage::{RegionId, ScanRequest, SequenceNumber};

use crate::engine::MetricEngineInner;
use crate::error::{
    InvalidMetadataSnafu, InvalidRequestSnafu, LogicalRegionNotFoundSnafu, MitoReadOperationSnafu,
    Result,
};
use crate::metrics::MITO_OPERATION_ELAPSED;
use crate::utils;

impl MetricEngineInner {
    #[tracing::instrument(skip_all)]
    pub async fn read_region(
        &self,
        region_id: RegionId,
        request: ScanRequest,
    ) -> Result<RegionScannerRef> {
        let is_reading_physical_region = self.is_physical_region(region_id);

        if is_reading_physical_region {
            debug!(
                "Metric region received read request {request:?} on physical region {region_id:?}"
            );
            self.read_physical_region(region_id, request).await
        } else {
            self.read_logical_region(region_id, request).await
        }
    }

    /// Proxy the read request to underlying physical region (mito engine).
    async fn read_physical_region(
        &self,
        region_id: RegionId,
        request: ScanRequest,
    ) -> Result<RegionScannerRef> {
        let _timer = MITO_OPERATION_ELAPSED
            .with_label_values(&["read_physical"])
            .start_timer();

        self.mito
            .handle_query(region_id, request)
            .await
            .context(MitoReadOperationSnafu)
    }

    async fn read_logical_region(
        &self,
        logical_region_id: RegionId,
        request: ScanRequest,
    ) -> Result<RegionScannerRef> {
        let _timer = MITO_OPERATION_ELAPSED
            .with_label_values(&["read"])
            .start_timer();

        let physical_region_id = self.get_physical_region_id(logical_region_id).await?;
        let data_region_id = utils::to_data_region_id(physical_region_id);
        let logical_metadata = self
            .logical_region_metadata(physical_region_id, logical_region_id)
            .await?;
        let physical_metadata = self
            .mito
            .get_metadata(data_region_id)
            .await
            .context(MitoReadOperationSnafu)?;
        let request = Self::transform_request_with_metadata(
            logical_region_id,
            request,
            &logical_metadata,
            &physical_metadata,
        )?;
        let mut scanner = self
            .mito
            .handle_query(data_region_id, request)
            .await
            .context(MitoReadOperationSnafu)?;
        scanner.set_logical_region(true);
        scanner.set_query_load_region_id(data_region_id);

        Ok(Box::new(LogicalRegionScanner {
            inner: scanner,
            metadata: logical_metadata,
        }))
    }

    pub async fn get_last_seq_num(&self, region_id: RegionId) -> Result<SequenceNumber> {
        let region_id = if self.is_physical_region(region_id) {
            region_id
        } else {
            let physical_region_id = self.get_physical_region_id(region_id).await?;
            utils::to_data_region_id(physical_region_id)
        };
        self.mito
            .get_committed_sequence(region_id)
            .await
            .context(MitoReadOperationSnafu)
    }

    pub async fn load_region_metadata(&self, region_id: RegionId) -> Result<RegionMetadataRef> {
        let is_reading_physical_region =
            self.state.read().unwrap().exist_physical_region(region_id);

        if is_reading_physical_region {
            self.mito
                .get_metadata(region_id)
                .await
                .context(MitoReadOperationSnafu)
        } else {
            let physical_region_id = self.get_physical_region_id(region_id).await?;
            self.logical_region_metadata(physical_region_id, region_id)
                .await
        }
    }

    /// Returns true if it's a physical region.
    pub fn is_physical_region(&self, region_id: RegionId) -> bool {
        self.state.read().unwrap().exist_physical_region(region_id)
    }

    async fn get_physical_region_id(&self, logical_region_id: RegionId) -> Result<RegionId> {
        let state = &self.state.read().unwrap();
        state
            .get_physical_region_id(logical_region_id)
            .with_context(|| {
                error!("Trying to read an nonexistent region {logical_region_id}");
                LogicalRegionNotFoundSnafu {
                    region_id: logical_region_id,
                }
            })
    }

    /// Transform the [ScanRequest] from logical region to physical data region.
    #[cfg(test)]
    async fn transform_request(
        &self,
        physical_region_id: RegionId,
        logical_region_id: RegionId,
        request: ScanRequest,
    ) -> Result<ScanRequest> {
        let logical_metadata = self
            .logical_region_metadata(physical_region_id, logical_region_id)
            .await?;
        let physical_metadata = self
            .mito
            .get_metadata(utils::to_data_region_id(physical_region_id))
            .await
            .context(MitoReadOperationSnafu)?;
        Self::transform_request_with_metadata(
            logical_region_id,
            request,
            &logical_metadata,
            &physical_metadata,
        )
    }

    fn transform_request_with_metadata(
        logical_region_id: RegionId,
        mut request: ScanRequest,
        logical_metadata: &RegionMetadataRef,
        physical_metadata: &RegionMetadataRef,
    ) -> Result<ScanRequest> {
        let logical_projection = match request.projection_input.as_ref() {
            Some(projection_input) => projection_input.projection.clone(),
            None => (0..logical_metadata.column_metadatas.len()).collect(),
        };
        let mut physical_projection = Vec::with_capacity(logical_projection.len());
        for logical_index in logical_projection {
            let logical_column = logical_metadata
                .column_metadatas
                .get(logical_index)
                .with_context(|| InvalidRequestSnafu {
                    region_id: logical_region_id,
                    reason: format!("projection index {logical_index} is out of bound"),
                })?;
            let name = &logical_column.column_schema.name;
            let physical_index =
                physical_metadata
                    .column_index_by_name(name)
                    .with_context(|| InvalidRequestSnafu {
                        region_id: logical_region_id,
                        reason: format!("column {name} is missing from physical metadata"),
                    })?;
            physical_projection.push(physical_index);
        }

        // Top-level projections are indices; nested paths are column names.
        request.projection_input.get_or_insert_default().projection = physical_projection;
        request.filters.push(
            logical_expr::col(DATA_SCHEMA_TABLE_ID_COLUMN_NAME)
                .eq(logical_expr::lit(logical_region_id.table_id())),
        );
        Ok(request)
    }

    pub async fn logical_region_metadata(
        &self,
        physical_region_id: RegionId,
        logical_region_id: RegionId,
    ) -> Result<RegionMetadataRef> {
        let logical_columns = self
            .load_logical_columns(physical_region_id, logical_region_id)
            .await?;

        let primary_keys = logical_columns
            .iter()
            .filter_map(|col| {
                if col.semantic_type == SemanticType::Tag {
                    Some(col.column_id)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        let mut logical_metadata_builder = RegionMetadataBuilder::new(logical_region_id);
        for col in logical_columns {
            logical_metadata_builder.push_column_metadata(col);
        }
        logical_metadata_builder.primary_key(primary_keys);
        let logical_metadata = logical_metadata_builder
            .build()
            .context(InvalidMetadataSnafu)?;

        Ok(Arc::new(logical_metadata))
    }
}

#[derive(Debug)]
struct LogicalRegionScanner {
    inner: RegionScannerRef,
    metadata: RegionMetadataRef,
}

impl DisplayAs for LogicalRegionScanner {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        self.inner.fmt_as(t, f)
    }
}

impl RegionScanner for LogicalRegionScanner {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn properties(&self) -> &ScannerProperties {
        self.inner.properties()
    }

    fn schema(&self) -> datatypes::schema::SchemaRef {
        self.inner.schema()
    }

    fn metadata(&self) -> RegionMetadataRef {
        self.metadata.clone()
    }

    fn prepare(&mut self, request: PrepareRequest) -> std::result::Result<(), BoxedError> {
        self.inner.prepare(request)
    }

    fn scan_partition(
        &self,
        ctx: &QueryScanContext,
        metrics_set: &ExecutionPlanMetricsSet,
        partition: usize,
    ) -> std::result::Result<SendableRecordBatchStream, BoxedError> {
        self.inner.scan_partition(ctx, metrics_set, partition)
    }

    fn has_predicate_without_region(&self) -> bool {
        self.inner.has_predicate_without_region()
    }

    fn add_dyn_filter_to_predicate(
        &mut self,
        filter_exprs: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Vec<bool> {
        self.inner.add_dyn_filter_to_predicate(filter_exprs)
    }

    fn set_logical_region(&mut self, logical_region: bool) {
        self.inner.set_logical_region(logical_region);
    }

    fn set_query_load_region_id(&mut self, region_id: RegionId) {
        self.inner.set_query_load_region_id(region_id);
    }

    fn snapshot_sequence(&self) -> Option<SequenceNumber> {
        self.inner.snapshot_sequence()
    }
}

#[cfg(test)]
impl MetricEngineInner {
    pub async fn scan_to_stream(
        &self,
        region_id: RegionId,
        request: ScanRequest,
    ) -> Result<common_recordbatch::SendableRecordBatchStream, common_error::ext::BoxedError> {
        let scanner = self
            .read_region(region_id, request)
            .await
            .map_err(common_error::ext::BoxedError::new)?;
        let metrics_set = datafusion::physical_plan::metrics::ExecutionPlanMetricsSet::new();
        let streams = (0..scanner.properties().num_partitions())
            .map(|partition| {
                scanner.scan_partition(
                    &store_api::region_engine::QueryScanContext::default(),
                    &metrics_set,
                    partition,
                )
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        common_recordbatch::util::ChainedRecordBatchStream::new(streams)
            .map(|stream| Box::pin(stream) as _)
            .map_err(common_error::ext::BoxedError::new)
    }
}

#[cfg(test)]
mod test {
    use store_api::region_request::RegionRequest;

    use super::*;
    use crate::test_util::{
        TestEnv, alter_logical_region_add_tag_columns, create_logical_region_request,
    };

    #[tokio::test]
    async fn test_transform_scan_req() {
        let env = TestEnv::new().await;
        env.init_metric_region().await;

        let logical_region_id = env.default_logical_region_id();
        let physical_region_id = env.default_physical_region_id();

        // create another logical region
        let logical_region_id2 = RegionId::new(1112345678, 999);
        let create_request =
            create_logical_region_request(&["123", "456", "789"], physical_region_id, "blabla");
        env.metric()
            .handle_request(logical_region_id2, RegionRequest::Create(create_request))
            .await
            .unwrap();

        // add columns to the first logical region
        let alter_request =
            alter_logical_region_add_tag_columns(123456, &["987", "798", "654", "321"]);
        env.metric()
            .handle_request(logical_region_id, RegionRequest::Alter(alter_request))
            .await
            .unwrap();

        // check explicit projection
        let projection_input = Some(vec![0, 1, 2, 3, 4, 5, 6].into());
        let scan_req = ScanRequest {
            projection_input,
            filters: vec![],
            ..Default::default()
        };

        let scan_req = env
            .metric()
            .inner
            .transform_request(physical_region_id, logical_region_id, scan_req)
            .await
            .unwrap();

        assert_eq!(
            scan_req.projection_indices().unwrap(),
            &[11, 10, 9, 8, 0, 1, 4]
        );
        assert_eq!(scan_req.filters.len(), 1);
        assert_eq!(
            scan_req.filters[0],
            logical_expr::col(DATA_SCHEMA_TABLE_ID_COLUMN_NAME)
                .eq(logical_expr::lit(logical_region_id.table_id()))
        );

        // check default projection
        let scan_req = ScanRequest::default();
        let scan_req = env
            .metric()
            .inner
            .transform_request(physical_region_id, logical_region_id, scan_req)
            .await
            .unwrap();
        assert_eq!(
            scan_req.projection_indices().unwrap(),
            &[11, 10, 9, 8, 0, 1, 4]
        );
    }
}
