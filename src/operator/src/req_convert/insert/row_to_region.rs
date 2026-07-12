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

use ahash::{HashMap, HashSet};
use api::v1::RowInsertRequests;
use api::v1::region::InsertRequests as RegionInsertRequests;
use partition::manager::PartitionRuleManager;
use snafu::OptionExt;
use table::metadata::{TableId, TableInfoRef};

use crate::error::{Result, TableNotFoundSnafu};
use crate::insert::InstantAndNormalInsertRequests;
use crate::req_convert::common::partitioner::Partitioner;

pub struct RowToRegion<'a> {
    tables_info: HashMap<String, TableInfoRef>,
    instant_table_ids: HashSet<TableId>,
    partition_manager: &'a PartitionRuleManager,
}

impl<'a> RowToRegion<'a> {
    pub fn new(
        tables_info: HashMap<String, TableInfoRef>,
        instant_table_ids: HashSet<TableId>,
        partition_manager: &'a PartitionRuleManager,
    ) -> Self {
        Self {
            tables_info,
            instant_table_ids,
            partition_manager,
        }
    }

    pub async fn convert(
        &self,
        requests: RowInsertRequests,
    ) -> Result<InstantAndNormalInsertRequests> {
        let mut region_request = Vec::with_capacity(requests.inserts.len());
        let mut instant_request = Vec::with_capacity(requests.inserts.len());
        for request in requests.inserts {
            let Some(rows) = request.rows else { continue };

            let table_info = self.get_table_info(&request.table_name)?;
            let table_id = table_info.table_id();

            let requests = Partitioner::new(self.partition_manager)
                .partition_insert_requests(table_info, rows)
                .await?;

            if self.instant_table_ids.contains(&table_id) {
                instant_request.extend(requests);
            } else {
                region_request.extend(requests);
            }
        }

        Ok(InstantAndNormalInsertRequests {
            normal_requests: RegionInsertRequests {
                requests: region_request,
            },
            instant_requests: RegionInsertRequests {
                requests: instant_request,
            },
        })
    }

    fn get_table_info(&self, table_name: &str) -> Result<&TableInfoRef> {
        self.tables_info
            .get(table_name)
            .context(TableNotFoundSnafu { table_name })
    }
}
