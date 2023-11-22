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

use common_query::logical_plan::Expr;
use common_recordbatch::SendableRecordBatchStream;
use common_telemetry::{error, info, tracing};
use datafusion::logical_expr;
use snafu::{OptionExt, ResultExt};
use store_api::region_engine::RegionEngine;
use store_api::storage::{RegionId, ScanRequest};

use crate::consts::DATA_SCHEMA_TABLE_ID_COLUMN_NAME;
use crate::engine::MetricEngineInner;
use crate::error::{LogicalRegionNotFoundSnafu, MitoReadOperationSnafu, Result};
use crate::utils;

impl MetricEngineInner {
    #[tracing::instrument(skip_all)]
    pub async fn read_region(
        &self,
        region_id: RegionId,
        request: ScanRequest,
    ) -> Result<SendableRecordBatchStream> {
        let is_reading_physical_region = self
            .state
            .read()
            .await
            .physical_regions()
            .contains_key(&region_id);

        if is_reading_physical_region {
            info!(
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
    ) -> Result<SendableRecordBatchStream> {
        self.mito
            .handle_query(region_id, request)
            .await
            .context(MitoReadOperationSnafu)
    }

    async fn read_logical_region(
        &self,
        logical_region_id: RegionId,
        mut request: ScanRequest,
    ) -> Result<SendableRecordBatchStream> {
        let physical_region_id = {
            let state = &self.state.read().await;
            state
                .get_physical_region_id(logical_region_id)
                .with_context(|| {
                    error!("Trying to alter an nonexistent region {logical_region_id}");
                    LogicalRegionNotFoundSnafu {
                        region_id: logical_region_id,
                    }
                })?
        };
        let data_region_id = utils::to_data_region_id(physical_region_id);
        self.transform_request(physical_region_id, logical_region_id, &mut request);
        self.mito
            .handle_query(data_region_id, request)
            .await
            .context(MitoReadOperationSnafu)
    }

    /// Transform the [ScanRequest] from logical region to physical data region.
    async fn transform_request(
        &self,
        physical_region_id: RegionId,
        logical_region_id: RegionId,
        request: &mut ScanRequest,
    ) -> Result<()> {
        // transform projection
        if let Some(projection) = &request.projection {
            let physical_projection = self
                .transform_projection(physical_region_id, logical_region_id, &projection)
                .await?;
            request.projection = Some(physical_projection);
        }

        // add table filter
        request
            .filters
            .push(self.table_id_filter(logical_region_id));

        Ok(())
    }

    /// Generate a filter on the table id column.
    fn table_id_filter(&self, logical_region_id: RegionId) -> Expr {
        logical_expr::col(DATA_SCHEMA_TABLE_ID_COLUMN_NAME)
            .eq(logical_expr::lit(logical_region_id.table_id()))
            .into()
    }

    /// Transform the projection from logical region to physical region.
    pub async fn transform_projection(
        &self,
        physical_region_id: RegionId,
        logical_region_id: RegionId,
        origin_projection: &[usize],
    ) -> Result<Vec<usize>> {
        // project on logical columns
        let logical_columns = self
            .load_logical_columns(physical_region_id, logical_region_id)
            .await?;

        // generate physical projection
        let mut physical_projection = Vec::with_capacity(origin_projection.len());
        let data_region_id = utils::to_data_region_id(physical_region_id);
        let physical_metadata = self
            .mito
            .get_metadata(data_region_id)
            .await
            .context(MitoReadOperationSnafu)?;
        for logical_proj in origin_projection {
            let column_id = logical_columns[*logical_proj].column_id;
            // Safety: logical columns is a strict subset of physical columns
            physical_projection.push(physical_metadata.column_index_by_id(column_id).unwrap());
        }

        Ok(physical_projection)
    }
}
