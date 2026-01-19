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

use api::v1::region::{DeleteRequest, InsertRequest};
use api::v1::{PartitionExprVersion, Rows};
use common_time::Timestamp;
use partition::manager::PartitionRuleManager;
use snafu::ResultExt;
use store_api::storage::RegionId;
use table::metadata::TableInfo;

use crate::error::{Result, SplitDeleteSnafu, SplitInsertSnafu};

pub struct Partitioner<'a> {
    partition_manager: &'a PartitionRuleManager,
}

impl<'a> Partitioner<'a> {
    pub fn new(partition_manager: &'a PartitionRuleManager) -> Self {
        Self { partition_manager }
    }

    pub async fn partition_insert_requests(
        &self,
        table_info: &TableInfo,
        rows: Rows,
    ) -> Result<Vec<InsertRequest>> {
        let table_id = table_info.table_id();

        // TTL filtering now happens after flownode mirroring in insert.rs
        // This preserves instant TTL data for flownodes while filtering before datanode writes

        let requests = self
            .partition_manager
            .split_rows(table_info, rows)
            .await
            .context(SplitInsertSnafu)?
            .into_iter()
            .map(
                |(region_number, (rows, partition_expr_version))| InsertRequest {
                    region_id: RegionId::new(table_id, region_number).into(),
                    rows: Some(rows),
                    partition_expr_version: partition_expr_version
                        .map(|value| PartitionExprVersion { value }),
                },
            )
            .collect();
        Ok(requests)
    }


    pub async fn partition_delete_requests(
        &self,
        table_info: &TableInfo,
        rows: Rows,
    ) -> Result<Vec<DeleteRequest>> {
        let table_id = table_info.table_id();

        let requests = self
            .partition_manager
            .split_rows(table_info, rows)
            .await
            .context(SplitDeleteSnafu)?
            .into_iter()
            .map(
                |(region_number, (rows, partition_expr_version))| DeleteRequest {
                    region_id: RegionId::new(table_id, region_number).into(),
                    rows: Some(rows),
                    partition_expr_version: partition_expr_version
                        .map(|value| PartitionExprVersion { value }),
                },
            )
            .collect();
        Ok(requests)
    }
}

// TTL filtering tests have been moved to:
// - src/operator/src/req_convert/common/ttl_filter.rs for core filtering logic
// - Integration tests will verify TTL filtering happens after flownode mirroring
