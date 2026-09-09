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
        skip_wal: bool,
    ) -> Result<InstantAndNormalInsertRequests> {
        let mut region_request = Vec::with_capacity(requests.inserts.len());
        let mut instant_request = Vec::with_capacity(requests.inserts.len());
        for request in requests.inserts {
            let Some(rows) = request.rows else { continue };

            let table_info = self.get_table_info(&request.table_name)?;
            let table_id = table_info.table_id();

            let requests = Partitioner::new(self.partition_manager)
                .partition_insert_requests(table_info, rows, skip_wal)
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::helper::tag_column_schema;
    use api::v1::value::ValueData;
    use api::v1::{ColumnDataType, Row, RowInsertRequest, Rows, Value};

    use super::*;
    use crate::tests::{
        create_partition_rule_manager, new_test_table_info, prepare_mocked_backend,
    };

    #[tokio::test]
    async fn test_partitioned_insert_skip_wal_normal_and_instant() {
        let backend = prepare_mocked_backend().await;
        let partition_manager = create_partition_rule_manager(backend).await;
        let table_info = Arc::new(new_test_table_info(1, "table_1", [1, 2, 3].into_iter()));
        for instant in [false, true] {
            let instant_table_ids = if instant {
                HashSet::from_iter([1])
            } else {
                HashSet::default()
            };
            let converter = RowToRegion::new(
                HashMap::from_iter([("table_1".to_string(), table_info.clone())]),
                instant_table_ids,
                &partition_manager,
            );
            for skip_wal in [false, true] {
                let requests = RowInsertRequests {
                    inserts: vec![RowInsertRequest {
                        table_name: "table_1".to_string(),
                        rows: Some(Rows {
                            schema: vec![tag_column_schema("a", ColumnDataType::Int32)],
                            rows: [1, 11, 101]
                                .into_iter()
                                .map(|value| Row {
                                    values: vec![Value {
                                        value_data: Some(ValueData::I32Value(value)),
                                    }],
                                })
                                .collect(),
                        }),
                    }],
                };
                let result = converter.convert(requests, skip_wal).await.unwrap();
                let (selected, other) = if instant {
                    (result.instant_requests, result.normal_requests)
                } else {
                    (result.normal_requests, result.instant_requests)
                };
                assert!(other.requests.is_empty());
                assert_eq!(selected.requests.len(), 3);
                assert!(
                    selected
                        .requests
                        .iter()
                        .all(|request| request.skip_wal == skip_wal)
                );
                assert_eq!(
                    selected
                        .requests
                        .iter()
                        .map(|request| request.rows.as_ref().unwrap().rows.len())
                        .sum::<usize>(),
                    3
                );
            }
        }
    }
}
