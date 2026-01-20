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

use ahash::{HashMap, HashSet};
use api::v1::RowInsertRequests;
use api::v1::region::InsertRequests as RegionInsertRequests;
use common_time::{TimeToLive, Timestamp};
use partition::manager::PartitionRuleManager;
use snafu::OptionExt;
use store_api::storage::RegionId;
use table::metadata::{TableId, TableInfo, TableInfoRef};

use crate::error::{Result, TableNotFoundSnafu};
use crate::insert::InstantAndNormalInsertRequests;
use crate::metrics::DIST_INGEST_ROWS_FILTERED_TTL_COUNTER;
use crate::req_convert::common::partitioner::Partitioner;
use crate::req_convert::common::ttl_filter::filter_expired_rows;

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

/// Filter expired rows from normal insert requests based on TTL.
/// This happens AFTER instant classification to preserve instant TTL data for flownodes.
pub fn filter_normal_requests_by_ttl(
    requests: RegionInsertRequests,
    table_infos: &HashMap<TableId, Arc<TableInfo>>,
) -> Result<RegionInsertRequests> {
    let mut filtered_requests = Vec::with_capacity(requests.requests.len());
    let now = Timestamp::current_millis();

    for mut request in requests.requests {
        let region_id = RegionId::from_u64(request.region_id);
        let table_id = region_id.table_id();
        let table_info = table_infos
            .get(&table_id)
            .with_context(|| TableNotFoundSnafu {
                table_name: format!("table_id: {}", table_id),
            })?;

        let ttl = &table_info.meta.options.ttl;
        let Some(ttl) = ttl else {
            filtered_requests.push(request);
            continue;
        };

        // Skip filtering for instant TTL.
        if matches!(ttl, TimeToLive::Instant) {
            filtered_requests.push(request);
            continue;
        }

        let Some(timestamp_col) = table_info.meta.schema.timestamp_column() else {
            // No timestamp column, skip filtering (safe default)
            filtered_requests.push(request);
            continue;
        };

        let Some(rows_data) = &mut request.rows else {
            continue;
        };

        // Filter expired rows from each region's rows
        let Some((timestamp_index_in_rows, _)) = rows_data
            .schema
            .iter()
            .enumerate()
            .find(|(_, c)| c.column_name == timestamp_col.name)
        else {
            // Timestamp not found in row, simply fallback
            filtered_requests.push(request);
            continue;
        };
        let (filtered_rows, filtered_count) = filter_expired_rows(
            std::mem::take(&mut rows_data.rows),
            timestamp_index_in_rows,
            ttl,
            &now,
        );
        rows_data.rows = filtered_rows;
        if filtered_count > 0 {
            DIST_INGEST_ROWS_FILTERED_TTL_COUNTER.inc_by(filtered_count as u64);
        }
        if !rows_data.rows.is_empty() {
            filtered_requests.push(request);
        }
    }

    Ok(RegionInsertRequests {
        requests: filtered_requests,
    })
}
