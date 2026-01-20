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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration as StdDuration;

    use ahash::HashMap;
    use api::v1::region::InsertRequest as RegionInsertRequest;
    use api::v1::value::ValueData;
    use api::v1::{ColumnDataType, Row, Rows, SemanticType, Value};
    use common_time::TimeToLive;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, SchemaBuilder};
    use store_api::storage::RegionId;
    use table::metadata::{TableInfoBuilder, TableMetaBuilder};
    use table::requests::TableOptions;

    use super::*;

    fn build_test_table_info(table_id: u32, ttl: Option<TimeToLive>) -> Arc<TableInfo> {
        let column_schemas = vec![
            ColumnSchema::new("id", ConcreteDataType::int64_datatype(), false),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new("value", ConcreteDataType::float64_datatype(), true),
        ];
        let schema = SchemaBuilder::try_from(column_schemas)
            .unwrap()
            .build()
            .unwrap();

        let options = TableOptions {
            ttl,
            ..Default::default()
        };

        let meta = TableMetaBuilder::empty()
            .schema(Arc::new(schema))
            .primary_key_indices(vec![0])
            .engine("mito")
            .next_column_id(3)
            .options(options)
            .build()
            .unwrap();

        Arc::new(
            TableInfoBuilder::default()
                .table_id(table_id)
                .name(format!("test_table_{}", table_id))
                .meta(meta)
                .build()
                .unwrap(),
        )
    }

    fn build_column_schema() -> Vec<api::v1::ColumnSchema> {
        vec![
            api::v1::ColumnSchema {
                column_name: "id".to_string(),
                datatype: ColumnDataType::Int64 as i32,
                semantic_type: SemanticType::Tag as i32,
                ..Default::default()
            },
            api::v1::ColumnSchema {
                column_name: "ts".to_string(),
                datatype: ColumnDataType::TimestampMillisecond as i32,
                semantic_type: SemanticType::Timestamp as i32,
                ..Default::default()
            },
            api::v1::ColumnSchema {
                column_name: "value".to_string(),
                datatype: ColumnDataType::Float64 as i32,
                semantic_type: SemanticType::Field as i32,
                ..Default::default()
            },
        ]
    }

    fn build_row(id: i64, ts_millis: i64, value: f64) -> Row {
        Row {
            values: vec![
                Value {
                    value_data: Some(ValueData::I64Value(id)),
                },
                Value {
                    value_data: Some(ValueData::TimestampMillisecondValue(ts_millis)),
                },
                Value {
                    value_data: Some(ValueData::F64Value(value)),
                },
            ],
        }
    }

    fn build_region_insert_request(
        table_id: u32,
        region_num: u32,
        rows: Vec<Row>,
    ) -> RegionInsertRequest {
        let region_id = RegionId::new(table_id, region_num);
        RegionInsertRequest {
            region_id: region_id.as_u64(),
            rows: Some(Rows {
                schema: build_column_schema(),
                rows,
            }),
        }
    }

    #[test]
    fn test_filter_normal_requests_by_ttl_no_ttl() {
        // Table without TTL should not filter any rows
        let table_id = 1;
        let table_info = build_test_table_info(table_id, None);
        let mut table_infos = HashMap::default();
        table_infos.insert(table_id, table_info);

        let now = Timestamp::current_millis();
        let rows = vec![
            build_row(1, now.value() - 1000, 1.0),
            build_row(2, now.value() - 2000, 2.0),
        ];

        let requests = RegionInsertRequests {
            requests: vec![build_region_insert_request(table_id, 1, rows)],
        };

        let result = filter_normal_requests_by_ttl(requests, &table_infos).unwrap();
        assert_eq!(result.requests.len(), 1);
        assert_eq!(result.requests[0].rows.as_ref().unwrap().rows.len(), 2);
    }

    #[test]
    fn test_filter_normal_requests_by_ttl_instant_ttl() {
        // Instant TTL should not filter rows (handled separately)
        let table_id = 1;
        let table_info = build_test_table_info(table_id, Some(TimeToLive::Instant));
        let mut table_infos = HashMap::default();
        table_infos.insert(table_id, table_info);

        let now = Timestamp::current_millis();
        let rows = vec![
            build_row(1, now.value() - 1000, 1.0),
            build_row(2, now.value() - 2000, 2.0),
        ];

        let requests = RegionInsertRequests {
            requests: vec![build_region_insert_request(table_id, 1, rows)],
        };

        let result = filter_normal_requests_by_ttl(requests, &table_infos).unwrap();
        assert_eq!(result.requests.len(), 1);
        assert_eq!(result.requests[0].rows.as_ref().unwrap().rows.len(), 2);
    }

    #[test]
    fn test_filter_normal_requests_by_ttl_forever_ttl() {
        // Forever TTL should not filter any rows
        let table_id = 1;
        let table_info = build_test_table_info(table_id, Some(TimeToLive::Forever));
        let mut table_infos = HashMap::default();
        table_infos.insert(table_id, table_info);

        let now = Timestamp::current_millis();
        let rows = vec![
            build_row(1, now.value() - 1000, 1.0),
            build_row(2, now.value() - 1000 * 60 * 60 * 24 * 365, 2.0), // 1 year ago
        ];

        let requests = RegionInsertRequests {
            requests: vec![build_region_insert_request(table_id, 1, rows)],
        };

        let result = filter_normal_requests_by_ttl(requests, &table_infos).unwrap();
        assert_eq!(result.requests.len(), 1);
        assert_eq!(result.requests[0].rows.as_ref().unwrap().rows.len(), 2);
    }

    #[test]
    fn test_filter_normal_requests_by_ttl_duration_ttl() {
        // TTL of 1 hour - should filter rows older than 1 hour
        let table_id = 1;
        let ttl = TimeToLive::Duration(StdDuration::from_secs(3600)); // 1 hour
        let table_info = build_test_table_info(table_id, Some(ttl));
        let mut table_infos = HashMap::default();
        table_infos.insert(table_id, table_info);

        let now = Timestamp::current_millis();
        let one_hour_ms = 3600 * 1000;
        let rows = vec![
            build_row(1, now.value() - 100, 1.0), // recent - keep
            build_row(2, now.value() - one_hour_ms - 1000, 2.0), // expired - filter
            build_row(3, now.value() - one_hour_ms / 2, 3.0), // within TTL - keep
            build_row(4, now.value() - one_hour_ms * 2, 4.0), // expired - filter
        ];

        let requests = RegionInsertRequests {
            requests: vec![build_region_insert_request(table_id, 1, rows)],
        };

        let result = filter_normal_requests_by_ttl(requests, &table_infos).unwrap();
        assert_eq!(result.requests.len(), 1);

        let filtered_rows = &result.requests[0].rows.as_ref().unwrap().rows;
        assert_eq!(filtered_rows.len(), 2);

        // Verify the kept rows are the recent ones (id=1 and id=3)
        let ids: Vec<i64> = filtered_rows
            .iter()
            .filter_map(|r| {
                if let Some(ValueData::I64Value(id)) = &r.values[0].value_data {
                    Some(*id)
                } else {
                    None
                }
            })
            .collect();
        assert!(ids.contains(&1));
        assert!(ids.contains(&3));
    }

    #[test]
    fn test_filter_normal_requests_by_ttl_all_expired() {
        // All rows expired - request should be removed entirely
        let table_id = 1;
        let ttl = TimeToLive::Duration(StdDuration::from_secs(60)); // 1 minute
        let table_info = build_test_table_info(table_id, Some(ttl));
        let mut table_infos = HashMap::default();
        table_infos.insert(table_id, table_info);

        let now = Timestamp::current_millis();
        let one_hour_ms = 3600 * 1000;
        let rows = vec![
            build_row(1, now.value() - one_hour_ms, 1.0), // expired
            build_row(2, now.value() - one_hour_ms * 2, 2.0), // expired
        ];

        let requests = RegionInsertRequests {
            requests: vec![build_region_insert_request(table_id, 1, rows)],
        };

        let result = filter_normal_requests_by_ttl(requests, &table_infos).unwrap();
        // All rows filtered, request should be removed
        assert_eq!(result.requests.len(), 0);
    }

    #[test]
    fn test_filter_normal_requests_by_ttl_multiple_tables() {
        // Test with multiple tables having different TTL settings
        let table_id_1 = 1;
        let table_id_2 = 2;

        let ttl_1 = TimeToLive::Duration(StdDuration::from_secs(3600)); // 1 hour
        let table_info_1 = build_test_table_info(table_id_1, Some(ttl_1));
        let table_info_2 = build_test_table_info(table_id_2, None); // No TTL

        let mut table_infos = HashMap::default();
        table_infos.insert(table_id_1, table_info_1);
        table_infos.insert(table_id_2, table_info_2);

        let now = Timestamp::current_millis();
        let two_hours_ms = 2 * 3600 * 1000;

        // Table 1: one expired row, one valid
        let rows_1 = vec![
            build_row(1, now.value() - 100, 1.0),          // keep
            build_row(2, now.value() - two_hours_ms, 2.0), // expired
        ];

        // Table 2: all rows should be kept (no TTL)
        let rows_2 = vec![
            build_row(3, now.value() - two_hours_ms, 3.0),
            build_row(4, now.value() - two_hours_ms * 10, 4.0),
        ];

        let requests = RegionInsertRequests {
            requests: vec![
                build_region_insert_request(table_id_1, 1, rows_1),
                build_region_insert_request(table_id_2, 1, rows_2),
            ],
        };

        let result = filter_normal_requests_by_ttl(requests, &table_infos).unwrap();
        assert_eq!(result.requests.len(), 2);

        // Find results by table
        let req_1 = result
            .requests
            .iter()
            .find(|r| RegionId::from_u64(r.region_id).table_id() == table_id_1)
            .unwrap();
        let req_2 = result
            .requests
            .iter()
            .find(|r| RegionId::from_u64(r.region_id).table_id() == table_id_2)
            .unwrap();

        // Table 1: only 1 row kept
        assert_eq!(req_1.rows.as_ref().unwrap().rows.len(), 1);
        // Table 2: all 2 rows kept
        assert_eq!(req_2.rows.as_ref().unwrap().rows.len(), 2);
    }

    #[test]
    fn test_filter_normal_requests_by_ttl_empty_rows() {
        // Request with no rows should be handled gracefully
        let table_id = 1;
        let ttl = TimeToLive::Duration(StdDuration::from_secs(3600));
        let table_info = build_test_table_info(table_id, Some(ttl));
        let mut table_infos = HashMap::default();
        table_infos.insert(table_id, table_info);

        let region_id = RegionId::new(table_id, 1);
        let requests = RegionInsertRequests {
            requests: vec![RegionInsertRequest {
                region_id: region_id.as_u64(),
                rows: None,
            }],
        };

        let result = filter_normal_requests_by_ttl(requests, &table_infos).unwrap();
        // Empty request should be skipped
        assert_eq!(result.requests.len(), 0);
    }

    #[test]
    fn test_filter_normal_requests_by_ttl_table_not_found() {
        // Request for unknown table should return error
        let table_id = 1;
        let unknown_table_id = 999;
        let table_info = build_test_table_info(table_id, None);
        let mut table_infos = HashMap::default();
        table_infos.insert(table_id, table_info);

        let rows = vec![build_row(1, 1000, 1.0)];
        let requests = RegionInsertRequests {
            requests: vec![build_region_insert_request(unknown_table_id, 1, rows)],
        };

        let result = filter_normal_requests_by_ttl(requests, &table_infos);
        assert!(result.is_err());
    }
}
