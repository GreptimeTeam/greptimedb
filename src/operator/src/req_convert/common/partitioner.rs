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

use crate::error::{Result, SplitDeleteSnafu, SplitInsertSnafu, TtlFilterSnafu};
use crate::req_convert::common::ttl_filter::filter_expired_rows;

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

        // Filter expired rows before partitioning
        let rows = self.filter_expired_rows_from_insert(table_info, rows)?;

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

    /// Filter expired rows based on TTL settings before inserting
    fn filter_expired_rows_from_insert(
        &self,
        table_info: &TableInfo,
        rows: Rows,
    ) -> Result<Rows> {
        // Get TTL from table options
        let ttl = &table_info.meta.options.ttl;

        // If no TTL, return rows unchanged
        if ttl.is_none() {
            return Ok(rows);
        }

        // Get timestamp column index
        let timestamp_index = match table_info.meta.schema.timestamp_index() {
            Some(idx) => idx,
            None => {
                // No timestamp column, cannot apply TTL filtering
                // This is a safe default - keep all rows
                return Ok(rows);
            }
        };

        // Get current timestamp for TTL comparison
        // Use the same time unit as the timestamp column
        let timestamp_column = &table_info.meta.schema.column_schemas()[timestamp_index];
        let time_unit = match timestamp_column.data_type {
            datatypes::data_type::ConcreteDataType::Timestamp(ts_type) => ts_type.unit(),
            _ => common_time::timestamp::TimeUnit::Millisecond, // Default fallback
        };
        let now = Timestamp::current_time(time_unit);

        // Extract rows vec from Rows
        let rows_vec = rows.rows;

        // Filter expired rows
        let (filtered_rows, filtered_count) = filter_expired_rows(
            rows_vec,
            timestamp_index,
            ttl,
            &now,
        ).context(TtlFilterSnafu {
            table_name: table_info.name.to_string(),
        })?;

        // Track metrics
        if filtered_count > 0 {
            crate::metrics::DIST_INGEST_ROWS_FILTERED_TTL_COUNTER
                .with_label_values(&[&table_info.name.to_string()])
                .inc_by(filtered_count as u64);
        }

        // Reconstruct Rows with filtered data
        Ok(Rows {
            schema: rows.schema,
            rows: filtered_rows,
        })
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

#[cfg(test)]
mod tests {
    use super::*;
    use api::v1::{Row, Value};
    use api::v1::value::ValueData;
    use common_time::ttl::TimeToLive;
    use common_time::timestamp::TimeUnit;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use std::sync::Arc;
    use std::time::Duration as StdDuration;
    use table::metadata::{TableInfoBuilder, TableMetaBuilder};
    use table::requests::TableOptions;

    #[tokio::test]
    async fn test_partition_filters_expired_rows() {
        // Build table info with TTL of 1 hour
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new("id", ConcreteDataType::int64_datatype(), false),
            ColumnSchema::new("ts", ConcreteDataType::timestamp_second_datatype(), false)
                .with_time_index(true),
        ]));

        // Set TTL on options
        let mut options = TableOptions::default();
        options.ttl = Some(TimeToLive::Duration(StdDuration::from_secs(3600))); // 1 hour

        let table_meta = TableMetaBuilder::empty()
            .schema(schema.clone())
            .primary_key_indices(vec![0])
            .engine("mito")
            .next_column_id(2)
            .options(options)
            .build()
            .unwrap();

        let table_info = TableInfoBuilder::default()
            .table_id(1)
            .table_version(1)
            .name("test_catalog.test_schema.test_table".to_string())
            .meta(table_meta)
            .build()
            .unwrap();

        // Create rows: some expired, some valid
        // Use fixed timestamps relative to current time
        let now = common_time::Timestamp::current_time(TimeUnit::Second);
        let current_seconds = now.value(); // in seconds (since we're using timestamp_second_datatype)

        // Create expired row (2 hours old, beyond 1 hour TTL)
        let expired_timestamp = current_seconds - 7200;
        // Create valid row (30 minutes old, within 1 hour TTL)
        let valid_timestamp = current_seconds - 1800;

        let rows = Rows {
            schema: vec![],
            rows: vec![
                Row {
                    values: vec![
                        Value { value_data: Some(ValueData::I64Value(1)) },
                        Value { value_data: Some(ValueData::TimestampSecondValue(expired_timestamp)) },
                    ],
                },
                Row {
                    values: vec![
                        Value { value_data: Some(ValueData::I64Value(2)) },
                        Value { value_data: Some(ValueData::TimestampSecondValue(valid_timestamp)) },
                    ],
                },
            ],
        };

        // Create a mock partition manager (we won't actually partition, just test filtering)
        // We'll test the filter_expired_rows_from_insert method directly
        let partition_manager = create_mock_partition_manager();
        let partitioner = Partitioner::new(&partition_manager);

        // Test the filtering
        let filtered_rows = partitioner
            .filter_expired_rows_from_insert(&table_info, rows)
            .unwrap();

        // Verify: only 1 row should remain (the valid one)
        assert_eq!(filtered_rows.rows.len(), 1);
        // Verify it's the correct row (id=2)
        assert_eq!(
            filtered_rows.rows[0].values[0].value_data,
            Some(ValueData::I64Value(2))
        );
    }

    fn create_mock_partition_manager() -> PartitionRuleManager {
        // Create a minimal partition manager for testing
        // This is a simplified setup - in real scenarios, it would need proper initialization
        use common_meta::kv_backend::memory::MemoryKvBackend;
        use common_meta::cache::new_table_route_cache;
        use moka::future::Cache;

        let kv_backend = Arc::new(MemoryKvBackend::new());
        let cache_backend: Cache<u32, Arc<common_meta::cache::TableRoute>> = Cache::new(128);
        let cache = Arc::new(new_table_route_cache(
            "test".to_string(),
            cache_backend,
            kv_backend.clone(),
        ));
        PartitionRuleManager::new(kv_backend, cache)
    }
}
