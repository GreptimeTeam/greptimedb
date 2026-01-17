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

use api::v1::SemanticType;
use common_telemetry::{debug, error, tracing};
use datafusion::logical_expr::{self, Expr};
use snafu::{OptionExt, ResultExt};
use store_api::metadata::{RegionMetadataBuilder, RegionMetadataRef};
use store_api::metric_engine_consts::{
    DATA_SCHEMA_TABLE_ID_COLUMN_NAME, is_metric_engine_internal_column,
};
use store_api::region_engine::{RegionEngine, RegionScannerRef};
use store_api::storage::{RegionId, ScanRequest, SequenceNumber};

use crate::engine::MetricEngineInner;
use crate::error::{
    InvalidMetadataSnafu, LogicalRegionNotFoundSnafu, MitoReadOperationSnafu, Result,
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
        let request = self
            .transform_request(physical_region_id, logical_region_id, request)
            .await?;
        let mut scanner = self
            .mito
            .handle_query(data_region_id, request)
            .await
            .context(MitoReadOperationSnafu)?;
        scanner.set_logical_region(true);

        Ok(scanner)
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
    async fn transform_request(
        &self,
        physical_region_id: RegionId,
        logical_region_id: RegionId,
        mut request: ScanRequest,
    ) -> Result<ScanRequest> {
        // transform projection
        let physical_projection = if let Some(projection) = &request.projection {
            self.transform_projection(physical_region_id, logical_region_id, projection)
                .await?
        } else {
            self.default_projection(physical_region_id, logical_region_id)
                .await?
        };

        request.projection = Some(physical_projection);

        // add table filter
        request
            .filters
            .push(self.table_id_filter(logical_region_id));

        Ok(request)
    }

    /// Generate a filter on the table id column.
    fn table_id_filter(&self, logical_region_id: RegionId) -> Expr {
        logical_expr::col(DATA_SCHEMA_TABLE_ID_COLUMN_NAME)
            .eq(logical_expr::lit(logical_region_id.table_id()))
    }

    /// Transform the projection from logical region to physical region.
    ///
    /// This method will not preserve internal columns.
    pub async fn transform_projection(
        &self,
        physical_region_id: RegionId,
        logical_region_id: RegionId,
        origin_projection: &[usize],
    ) -> Result<Vec<usize>> {
        // project on logical columns
        let all_logical_columns = self
            .load_logical_column_names(physical_region_id, logical_region_id)
            .await?;
        let projected_logical_names = origin_projection
            .iter()
            .map(|i| all_logical_columns[*i].clone())
            .collect::<Vec<_>>();

        // generate physical projection
        let mut physical_projection = Vec::with_capacity(origin_projection.len());
        let data_region_id = utils::to_data_region_id(physical_region_id);
        let physical_metadata = self
            .mito
            .get_metadata(data_region_id)
            .await
            .context(MitoReadOperationSnafu)?;

        for name in projected_logical_names {
            // Safety: logical columns is a strict subset of physical columns
            physical_projection.push(physical_metadata.column_index_by_name(&name).unwrap());
        }

        Ok(physical_projection)
    }

    /// Default projection for a logical region. Includes non-internal columns
    pub async fn default_projection(
        &self,
        physical_region_id: RegionId,
        logical_region_id: RegionId,
    ) -> Result<Vec<usize>> {
        let logical_columns = self
            .load_logical_column_names(physical_region_id, logical_region_id)
            .await?;
        let mut projection = Vec::with_capacity(logical_columns.len());
        let data_region_id = utils::to_data_region_id(physical_region_id);
        let physical_metadata = self
            .mito
            .get_metadata(data_region_id)
            .await
            .context(MitoReadOperationSnafu)?;
        for name in logical_columns
            .into_iter()
            .filter(|name| !is_metric_engine_internal_column(name))
        {
            // Safety: logical columns is a strict subset of physical columns
            projection.push(physical_metadata.column_index_by_name(&name).unwrap());
        }

        Ok(projection)
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

#[cfg(test)]
impl MetricEngineInner {
    pub async fn scan_to_stream(
        &self,
        region_id: RegionId,
        request: ScanRequest,
    ) -> Result<common_recordbatch::SendableRecordBatchStream, common_error::ext::BoxedError> {
        let is_reading_physical_region = self.is_physical_region(region_id);

        if is_reading_physical_region {
            self.mito
                .scan_to_stream(region_id, request)
                .await
                .map_err(common_error::ext::BoxedError::new)
        } else {
            let physical_region_id = self
                .get_physical_region_id(region_id)
                .await
                .map_err(common_error::ext::BoxedError::new)?;
            let request = self
                .transform_request(physical_region_id, region_id, request)
                .await
                .map_err(common_error::ext::BoxedError::new)?;
            self.mito
                .scan_to_stream(physical_region_id, request)
                .await
                .map_err(common_error::ext::BoxedError::new)
        }
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
        let scan_req = ScanRequest {
            projection: Some(vec![0, 1, 2, 3, 4, 5, 6]),
            filters: vec![],
            ..Default::default()
        };

        let scan_req = env
            .metric()
            .inner
            .transform_request(physical_region_id, logical_region_id, scan_req)
            .await
            .unwrap();

        assert_eq!(scan_req.projection.unwrap(), vec![11, 10, 9, 8, 0, 1, 4]);
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
        assert_eq!(scan_req.projection.unwrap(), vec![11, 10, 9, 8, 0, 1, 4]);
    }
}
