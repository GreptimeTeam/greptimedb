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

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use clap::Parser;
use common_error::ext::BoxedError;
use common_meta::key::table_info::{TableInfoKey, TableInfoValue};
use common_meta::key::table_route::{TableRouteKey, TableRouteValue};
use common_meta::key::{MetadataKey, MetadataValue};
use common_meta::kv_backend::KvBackendRef;
use common_meta::range_stream::{DEFAULT_PAGE_SIZE, PaginationStream};
use common_meta::rpc::store::{PutRequest, RangeRequest};
use common_telemetry::{info, warn};
use futures::StreamExt;
use partition::expr::PartitionExpr;
use store_api::storage::TableId;
use table::metadata::TableType;

use crate::{StoreConfig, Tool};

/// CLI command to repair partition column metadata mismatches.
#[derive(Parser)]
pub struct RepairPartitionColumnCommand {
    #[clap(flatten)]
    store_config: StoreConfig,

    /// Whether to actually do the update in the underlying metadata store, or not.
    #[clap(long)]
    dry_run: bool,

    /// The maximum count of update times.
    #[clap(long)]
    update_limit: Option<u32>,
}

impl RepairPartitionColumnCommand {
    pub(super) async fn build(&self) -> Result<RepairPartitionColumnTool, BoxedError> {
        let kv_backend = self.store_config.build().await?;
        Ok(RepairPartitionColumnTool {
            kv_backend,
            dry_run: self.dry_run,
            update_limit: self.update_limit,
        })
    }
}

/// Repair tool that reconciles partition columns between table info and routes.
pub(crate) struct RepairPartitionColumnTool {
    kv_backend: KvBackendRef,
    dry_run: bool,
    update_limit: Option<u32>,
}

impl RepairPartitionColumnTool {
    async fn do_repair(
        &self,
        table_infos: HashMap<TableId, TableInfoValue>,
        table_routes: HashMap<TableId, TableRouteValue>,
    ) -> Result<(), BoxedError> {
        let mut update_count = 0;
        for (table_id, table_info_value) in &table_infos {
            let table_meta = &table_info_value.table_info.meta;
            let mut partition_columns = Vec::with_capacity(table_meta.partition_key_indices.len());
            for i in &table_meta.partition_key_indices {
                if let Some(x) = table_meta.schema.column_schemas.get(*i) {
                    partition_columns.push(&x.name);
                } else {
                    warn!(
                        "Partition column not found by index: {i}, table: {}, id: {}",
                        table_info_value.table_name(),
                        table_id
                    );
                }
            }

            let Some(TableRouteValue::Physical(table_route)) = table_routes.get(table_id) else {
                continue;
            };
            let mut partition_expr_columns = HashSet::new();
            for region_route in &table_route.region_routes {
                let partition_expr_result =
                    PartitionExpr::from_json_str(&region_route.region.partition_expr());
                let partition_expr = match partition_expr_result {
                    Ok(Some(expr)) => expr,
                    Ok(None) => {
                        // No partition expression found, which might be valid.
                        continue;
                    }
                    Err(e) => {
                        warn!(
                            e;
                            "Failed to deserialize partition expression for region: {:?}, table: {}",
                            region_route.region.id,
                            table_info_value.table_name()
                        );
                        continue;
                    }
                };
                partition_expr.collect_column_names(&mut partition_expr_columns);
            }

            let mut partition_expr_columns = partition_expr_columns.iter().collect::<Vec<_>>();
            partition_expr_columns.sort();
            partition_columns.sort();
            if partition_expr_columns != partition_columns {
                warn!(
                    "Columns in partition exprs: {:?} do not match partition columns: {:?} in table ‘{}’",
                    partition_expr_columns,
                    partition_columns,
                    table_info_value.table_name(),
                );

                if let Some(update_limit) = self.update_limit
                    && update_count >= update_limit
                {
                    warn!(
                        "Reached update limit: {update_limit}. Stopping further table metadata updates."
                    );
                    return Ok(());
                }
                self.update_partition_columns(partition_expr_columns, table_info_value)
                    .await?;
                update_count += 1;
            }
        }
        Ok(())
    }

    async fn update_partition_columns(
        &self,
        partition_expr_columns: Vec<&String>,
        table_info_value: &TableInfoValue,
    ) -> Result<(), BoxedError> {
        let column_schemas = &table_info_value.table_info.meta.schema.column_schemas;
        let mut partition_column_indices = Vec::with_capacity(partition_expr_columns.len());
        for column_name in &partition_expr_columns {
            if let Some((i, _)) = column_schemas
                .iter()
                .enumerate()
                .find(|(_, x)| &x.name == *column_name)
            {
                partition_column_indices.push(i);
            } else {
                warn!(
                    "Partition column '{}' from partition expression not found in table schema '{}'. Skipping this column for update.",
                    column_name,
                    table_info_value.table_name()
                );
            }
        }

        info!(
            "Updating partition columns to {:?} (by column indices: {:?}) in table '{}'",
            partition_expr_columns,
            partition_column_indices,
            table_info_value.table_name(),
        );
        if self.dry_run {
            info!("Dry run enabled, do nothing");
            return Ok(());
        }

        let mut new_table_info = table_info_value.table_info.clone();
        new_table_info.meta.partition_key_indices = partition_column_indices;
        let table_info = table_info_value.update(new_table_info);

        let request = PutRequest::new()
            .with_key(TableInfoKey::new(table_info.table_info.ident.table_id).to_bytes())
            .with_value(table_info.try_as_raw_value().map_err(BoxedError::new)?);
        let _ = self
            .kv_backend
            .put(request)
            .await
            .map_err(BoxedError::new)?;
        Ok(())
    }
}

#[async_trait]
impl Tool for RepairPartitionColumnTool {
    async fn do_work(&self) -> Result<(), BoxedError> {
        let key_values = PaginationStream::new(
            self.kv_backend.clone(),
            RangeRequest::new().with_range(vec![0], vec![0]),
            DEFAULT_PAGE_SIZE,
            Ok,
        )
        .into_stream();
        let mut key_values = Box::pin(key_values);

        let mut table_infos = HashMap::new();
        let mut table_routes = HashMap::new();
        while let Some(result) = key_values.next().await {
            match result {
                Ok(kv) => {
                    if let Ok(key) = TableInfoKey::from_bytes(kv.key()) {
                        let Ok(value) = TableInfoValue::try_from_raw_value(&kv.value) else {
                            warn!("Skip corrupted key: {key}");
                            continue;
                        };
                        if value.table_info.table_type == TableType::Base {
                            table_infos.insert(value.table_info.ident.table_id, value);
                        }
                    } else if let Ok(key) = TableRouteKey::from_bytes(kv.key()) {
                        let Ok(value) = TableRouteValue::try_from_raw_value(&kv.value) else {
                            warn!("Skip corrupted key: {key}");
                            continue;
                        };
                        if value.is_physical() {
                            table_routes.insert(key.table_id, value);
                        }
                    }
                }
                Err(e) => {
                    warn!(e; "Failed to get next key")
                }
            }
        }

        self.do_repair(table_infos, table_routes).await
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use common_meta::kv_backend::KvBackend;
    use common_meta::kv_backend::memory::MemoryKvBackend;

    use super::*;

    #[tokio::test]
    async fn test_repair_partition_column() {
        common_telemetry::init_default_ut_logging();

        let kv_backend = Arc::new(MemoryKvBackend::new());

        let table_info_key = TableInfoKey::new(1282).to_bytes();
        let table_info_value = r#"{"table_info":{"ident":{"table_id":1282,"version":2},"name":"foo","desc":null,"catalog_name":"greptime","schema_name":"public","meta":{"schema":{"column_schemas":[{"name":"c0","data_type":{"Int32":{}},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}},{"name":"c1","data_type":{"String":null},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}},{"name":"c2","data_type":{"String":null},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}},{"name":"c3","data_type":{"String":null},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}},{"name":"c4","data_type":{"Int32":{}},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}},{"name":"c5","data_type":{"Int32":{}},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}},{"name":"c6","data_type":{"Int32":{}},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}},{"name":"c7","data_type":{"Int32":{}},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}},{"name":"c8","data_type":{"Int32":{}},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}},{"name":"c9","data_type":{"Int32":{}},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}},{"name":"c10","data_type":{"Timestamp":{"Nanosecond":null}},"is_nullable":false,"is_time_index":true,"default_constraint":null,"metadata":{"greptime:time_index":"true"}}],"timestamp_index":10,"version":2},"primary_key_indices":[4,7],"value_indices":[0,1,2,3,5,6,8,9,10],"engine":"mito","next_column_id":11,"region_numbers":[0,1,2],"options":{"write_buffer_size":null,"ttl":"14days","skip_wal":false,"extra_options":{"append_mode":"true"}},"created_on":"2025-09-25T01:39:28.702584510Z","partition_key_indices":[3]},"table_type":"Base"},"version":2}"#;
        kv_backend
            .put(
                PutRequest::new()
                    .with_key(table_info_key.clone())
                    .with_value(table_info_value),
            )
            .await
            .unwrap();

        let table_route_key = TableRouteKey::new(1282).to_bytes();
        let table_route_value = r#"{"type":"physical","region_routes":[{"region":{"id":5506148073472,"name":"","partition":{"column_list":["c4"],"value_list":["{\"Expr\":{\"lhs\":{\"Column\":\"c4\"},\"op\":\"Lt\",\"rhs\":{\"Value\":{\"Int32\":1}}}}"]},"attrs":{}},"leader_peer":{"id":12,"addr":"192.168.1.1:3001"},"follower_peers":[],"leader_down_since":null},{"region":{"id":5506148073473,"name":"","partition":{"column_list":["c4"],"value_list":["{\"Expr\":{\"lhs\":{\"Expr\":{\"lhs\":{\"Column\":\"c4\"},\"op\":\"GtEq\",\"rhs\":{\"Value\":{\"Int32\":1}}}},\"op\":\"And\",\"rhs\":{\"Expr\":{\"lhs\":{\"Column\":\"c4\"},\"op\":\"Lt\",\"rhs\":{\"Value\":{\"Int32\":2}}}}}}"]},"attrs":{}},"leader_peer":{"id":13,"addr":"192.168.1.2:3001"},"follower_peers":[],"leader_down_since":null},{"region":{"id":5506148073474,"name":"","partition":{"column_list":["c4"],"value_list":["{\"Expr\":{\"lhs\":{\"Column\":\"c4\"},\"op\":\"GtEq\",\"rhs\":{\"Value\":{\"Int32\":2}}}}"]},"attrs":{}},"leader_peer":{"id":10,"addr":"192.168.1.3:3001"},"follower_peers":[],"leader_down_since":null}],"version":0}"#;
        kv_backend
            .put(
                PutRequest::new()
                    .with_key(table_route_key)
                    .with_value(table_route_value),
            )
            .await
            .unwrap();

        let tool = RepairPartitionColumnTool {
            kv_backend: kv_backend.clone(),
            dry_run: true,
            update_limit: None,
        };
        tool.do_work().await.unwrap();
        let actual = String::from_utf8(
            kv_backend
                .get(&table_info_key)
                .await
                .unwrap()
                .unwrap()
                .value,
        )
        .unwrap();
        assert_eq!(actual, table_info_value);

        let tool = RepairPartitionColumnTool {
            kv_backend: kv_backend.clone(),
            dry_run: false,
            update_limit: Some(0),
        };
        tool.do_work().await.unwrap();
        let actual = String::from_utf8(
            kv_backend
                .get(&table_info_key)
                .await
                .unwrap()
                .unwrap()
                .value,
        )
        .unwrap();
        assert_eq!(actual, table_info_value);

        let tool = RepairPartitionColumnTool {
            kv_backend: kv_backend.clone(),
            dry_run: false,
            update_limit: Some(1),
        };
        tool.do_work().await.unwrap();
        let actual = String::from_utf8(
            kv_backend
                .get(&table_info_key)
                .await
                .unwrap()
                .unwrap()
                .value,
        )
        .unwrap();
        let expected = r#"{"table_info":{"ident":{"table_id":1282,"version":2},"name":"foo","desc":null,"catalog_name":"greptime","schema_name":"public","meta":{"schema":{"column_schemas":[{"name":"c0","data_type":{"Int32":{}},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}},{"name":"c1","data_type":{"String":{"size_type":"Utf8"}},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}},{"name":"c2","data_type":{"String":{"size_type":"Utf8"}},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}},{"name":"c3","data_type":{"String":{"size_type":"Utf8"}},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}},{"name":"c4","data_type":{"Int32":{}},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}},{"name":"c5","data_type":{"Int32":{}},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}},{"name":"c6","data_type":{"Int32":{}},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}},{"name":"c7","data_type":{"Int32":{}},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}},{"name":"c8","data_type":{"Int32":{}},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}},{"name":"c9","data_type":{"Int32":{}},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}},{"name":"c10","data_type":{"Timestamp":{"Nanosecond":null}},"is_nullable":false,"is_time_index":true,"default_constraint":null,"metadata":{"greptime:time_index":"true"}}],"timestamp_index":10,"version":2},"primary_key_indices":[4,7],"value_indices":[0,1,2,3,5,6,8,9,10],"engine":"mito","next_column_id":11,"options":{"write_buffer_size":null,"ttl":"14days","skip_wal":false,"extra_options":{"append_mode":"true"}},"created_on":"2025-09-25T01:39:28.702584510Z","updated_on":"2025-09-25T01:39:28.702584510Z","partition_key_indices":[4],"column_ids":[]},"table_type":"Base"},"version":3}"#;
        assert_eq!(actual, expected);
    }
}
