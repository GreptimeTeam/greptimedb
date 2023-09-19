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
use api::v1::Rows;
use partition::manager::PartitionRuleManager;
use snafu::ResultExt;
use store_api::storage::{RegionId, TableId};

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
        table_id: TableId,
        rows: Rows,
    ) -> Result<Vec<InsertRequest>> {
        let requests = self
            .partition_manager
            .split_rows(table_id, rows)
            .await
            .context(SplitInsertSnafu)?
            .into_iter()
            .map(|(region_number, rows)| InsertRequest {
                region_id: RegionId::new(table_id, region_number).into(),
                rows: Some(rows),
            })
            .collect();
        Ok(requests)
    }

    pub async fn partition_delete_requests(
        &self,
        table_id: TableId,
        rows: Rows,
    ) -> Result<Vec<DeleteRequest>> {
        let requests = self
            .partition_manager
            .split_rows(table_id, rows)
            .await
            .context(SplitDeleteSnafu)?
            .into_iter()
            .map(|(region_number, rows)| DeleteRequest {
                region_id: RegionId::new(table_id, region_number).into(),
                rows: Some(rows),
            })
            .collect();
        Ok(requests)
    }
}
