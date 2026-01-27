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

use api::v1::Rows;
use api::v1::region::{DeleteRequest, InsertRequest};
use common_function::utils::partition_rule_version;
use partition::manager::{PartitionInfo, PartitionRuleManager};
use snafu::ResultExt;
use store_api::storage::{RegionId, RegionNumber};
use table::metadata::TableInfo;

use crate::error::{
    FindTablePartitionRuleSnafu, Result, SerializePartitionExprSnafu, SplitDeleteSnafu,
    SplitInsertSnafu,
};

pub struct Partitioner<'a> {
    partition_manager: &'a PartitionRuleManager,
}

impl<'a> Partitioner<'a> {
    pub fn new(partition_manager: &'a PartitionRuleManager) -> Self {
        Self { partition_manager }
    }

    fn build_partition_rule_versions(
        partitions: Vec<PartitionInfo>,
    ) -> Result<std::collections::HashMap<RegionNumber, u64>> {
        let mut versions = std::collections::HashMap::with_capacity(partitions.len());
        for partition in partitions {
            let expr_json = match &partition.partition_expr {
                Some(expr) => Some(expr.as_json_str().context(SerializePartitionExprSnafu)?),
                None => None,
            };
            let version = partition_rule_version(expr_json.as_deref());
            versions.insert(partition.id.region_number(), version);
        }
        Ok(versions)
    }

    pub async fn partition_insert_requests(
        &self,
        table_info: &TableInfo,
        rows: Rows,
    ) -> Result<Vec<InsertRequest>> {
        let table_id = table_info.table_id();
        let partition_rule_versions = Self::build_partition_rule_versions(
            self.partition_manager
                .find_table_partitions(table_id)
                .await
                .context(FindTablePartitionRuleSnafu {
                    table_name: table_info.name.clone(),
                })?,
        )?;
        let requests = self
            .partition_manager
            .split_rows(table_info, rows)
            .await
            .context(SplitInsertSnafu)?
            .into_iter()
            .map(|(region_number, rows)| InsertRequest {
                region_id: RegionId::new(table_id, region_number).into(),
                rows: Some(rows),
                partition_rule_version: partition_rule_versions
                    .get(&region_number)
                    .copied()
                    .unwrap_or_default(),
            })
            .collect();
        Ok(requests)
    }

    pub async fn partition_delete_requests(
        &self,
        table_info: &TableInfo,
        rows: Rows,
    ) -> Result<Vec<DeleteRequest>> {
        let table_id = table_info.table_id();
        let partition_rule_versions = Self::build_partition_rule_versions(
            self.partition_manager
                .find_table_partitions(table_id)
                .await
                .context(FindTablePartitionRuleSnafu {
                    table_name: table_info.name.clone(),
                })?,
        )?;
        let requests = self
            .partition_manager
            .split_rows(table_info, rows)
            .await
            .context(SplitDeleteSnafu)?
            .into_iter()
            .map(|(region_number, rows)| DeleteRequest {
                region_id: RegionId::new(table_id, region_number).into(),
                rows: Some(rows),
                partition_rule_version: partition_rule_versions
                    .get(&region_number)
                    .copied()
                    .unwrap_or_default(),
            })
            .collect();
        Ok(requests)
    }
}
