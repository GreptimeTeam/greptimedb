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

use std::collections::HashMap;

use api::v1::region::InsertRequests as RegionInsertRequests;
use api::v1::RowInsertRequests;
use partition::manager::PartitionRuleManager;
use snafu::OptionExt;
use table::metadata::TableId;

use crate::error::{Result, TableNotFoundSnafu};
use crate::req_convert::common::partitioner::Partitioner;

pub struct RowToRegion<'a> {
    table_name_to_ids: HashMap<String, TableId>,
    partition_manager: &'a PartitionRuleManager,
}

impl<'a> RowToRegion<'a> {
    pub fn new(
        table_name_to_ids: HashMap<String, TableId>,
        partition_manager: &'a PartitionRuleManager,
    ) -> Self {
        Self {
            table_name_to_ids,
            partition_manager,
        }
    }

    pub async fn convert(&self, requests: RowInsertRequests) -> Result<RegionInsertRequests> {
        let mut region_request = Vec::with_capacity(requests.inserts.len());
        for request in requests.inserts {
            let table_id = self.get_table_id(&request.table_name)?;
            let requests = Partitioner::new(self.partition_manager)
                .partition_insert_requests(table_id, request.rows.unwrap_or_default())
                .await?;

            region_request.extend(requests);
        }

        Ok(RegionInsertRequests {
            requests: region_request,
        })
    }

    fn get_table_id(&self, table_name: &str) -> Result<TableId> {
        self.table_name_to_ids
            .get(table_name)
            .cloned()
            .context(TableNotFoundSnafu { table_name })
    }
}
