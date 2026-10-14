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

use futures::future::BoxFuture;
use moka::future::Cache;
use snafu::OptionExt;
use store_api::metric_engine_consts::METRIC_ENGINE_NAME;
use store_api::storage::TableId;
use table::metadata::{TableInfo, TableInfoRef};

use crate::cache::{CacheContainer, InitStrategy, Initializer, TableRoute, TableRouteCacheRef};
use crate::error;
use crate::error::Result;
use crate::instruction::CacheIdent;
use crate::key::table_info::{TableInfoManager, TableInfoManagerRef};
use crate::kv_backend::KvBackendRef;

/// [TableInfoCache] caches the [TableId] to [TableInfo] mapping.
pub type TableInfoCache = CacheContainer<TableId, Arc<TableInfo>, CacheIdent>;

pub type TableInfoCacheRef = Arc<TableInfoCache>;

/// Logical table metadata resolved against its physical table's current partition columns.
pub struct ResolvedLogicalTableInfo {
    /// Table metadata with partition indices mapped by column name into the logical schema.
    pub table_info: TableInfoRef,
    /// Physical partition columns absent from the logical schema, treated as null when reading.
    pub missing_partition_columns: Vec<String>,
}

/// Resolves inherited partition metadata without changing the raw cached table metadata.
///
/// Returns `None` for non-metric tables or when no physical table can be resolved.
/// Catalog reads and write retries must use the same logical-to-physical column mapping.
pub async fn resolve_logical_table_info(
    table_info: &TableInfoRef,
    table_route_cache: &TableRouteCacheRef,
    table_info_cache: &TableInfoCacheRef,
) -> Result<Option<ResolvedLogicalTableInfo>> {
    if table_info.meta.engine != METRIC_ENGINE_NAME {
        return Ok(None);
    }
    let Some(table_route) = table_route_cache.get(table_info.table_id()).await? else {
        return Ok(None);
    };
    let TableRoute::Logical(logical_route) = table_route.as_ref() else {
        return Ok(None);
    };
    let Some(physical_table_info) = table_info_cache
        .get(logical_route.physical_table_id())
        .await?
    else {
        return Ok(None);
    };
    let mut resolved = (**table_info).clone();
    let mut missing_partition_columns = Vec::new();
    resolved.meta.partition_key_indices = physical_table_info
        .meta
        .partition_key_indices
        .iter()
        .filter_map(|&physical_index| {
            physical_table_info
                .meta
                .schema
                .column_schemas()
                .get(physical_index)
                .and_then(|column| {
                    let index = table_info.meta.schema.column_index_by_name(&column.name);
                    if index.is_none() {
                        missing_partition_columns.push(column.name.clone());
                    }
                    index
                })
        })
        .collect();
    Ok(Some(ResolvedLogicalTableInfo {
        table_info: Arc::new(resolved),
        missing_partition_columns,
    }))
}

/// Constructs a [TableInfoCache].
pub fn new_table_info_cache(
    name: String,
    cache: Cache<TableId, Arc<TableInfo>>,
    kv_backend: KvBackendRef,
) -> TableInfoCache {
    let table_info_manager = Arc::new(TableInfoManager::new(kv_backend));
    let init = init_factory(table_info_manager);

    CacheContainer::with_strategy(
        name,
        cache,
        Box::new(invalidator),
        init,
        filter,
        InitStrategy::VersionChecked,
    )
}

fn init_factory(table_info_manager: TableInfoManagerRef) -> Initializer<TableId, Arc<TableInfo>> {
    Arc::new(move |table_id| {
        let table_info_manager = table_info_manager.clone();
        Box::pin(async move {
            let table_info = table_info_manager
                .get(*table_id)
                .await?
                .context(error::ValueNotExistSnafu {})?
                .into_inner()
                .table_info;
            Ok(Some(Arc::new(table_info)))
        })
    })
}

fn invalidator<'a>(
    cache: &'a Cache<TableId, Arc<TableInfo>>,
    idents: &'a [&CacheIdent],
) -> BoxFuture<'a, Result<()>> {
    Box::pin(async move {
        for ident in idents {
            if let CacheIdent::TableId(table_id) = ident {
                cache.invalidate(table_id).await
            }
        }
        Ok(())
    })
}

fn filter(ident: &CacheIdent) -> bool {
    matches!(ident, CacheIdent::TableId(_))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use moka::future::CacheBuilder;

    use super::*;
    use crate::cache::new_table_route_cache;
    use crate::ddl::test_util::create_table::test_create_table_task;
    use crate::key::TableMetadataManager;
    use crate::key::table_route::TableRouteValue;
    use crate::kv_backend::memory::MemoryKvBackend;

    #[tokio::test]
    async fn test_cache() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let table_metadata_manager = TableMetadataManager::new(mem_kv.clone());
        let cache = CacheBuilder::new(128).build();
        let cache = new_table_info_cache("test".to_string(), cache, mem_kv.clone());

        let result = cache.get(1024).await.unwrap();
        assert!(result.is_none());
        let task = test_create_table_task("my_table", 1024);
        table_metadata_manager
            .create_table_metadata(
                task.table_info.clone(),
                TableRouteValue::physical(vec![]),
                HashMap::new(),
            )
            .await
            .unwrap();
        let table_info = cache.get(1024).await.unwrap().unwrap();
        assert_eq!(*table_info, task.table_info);

        assert!(cache.contains_key(&1024));
        cache
            .invalidate(&[CacheIdent::TableId(1024)])
            .await
            .unwrap();
        assert!(!cache.contains_key(&1024));
    }

    #[tokio::test]
    async fn test_resolve_logical_table_info() {
        let cases = [
            ("non_metric", false, true, vec![3, 0], None),
            ("physical_metric", true, false, vec![3, 0], None),
            (
                "reordered",
                true,
                true,
                vec![2, 3, 0, 1],
                Some((vec![2, 3, 0], vec![])),
            ),
            (
                "missing_column",
                true,
                true,
                vec![1, 3, 0],
                Some((vec![2, 0], vec!["rack"])),
            ),
            (
                "all_columns_missing",
                true,
                true,
                vec![3],
                Some((vec![], vec!["host", "zone", "rack"])),
            ),
        ];
        for (name, metric, logical, column_order, expected) in cases {
            let backend = Arc::new(MemoryKvBackend::default());
            let metadata = TableMetadataManager::new(backend.clone());
            let mut physical_info = test_create_table_task("physical", 1024).table_info;
            physical_info.meta.engine = METRIC_ENGINE_NAME.to_string();
            let columns = vec![
                ColumnSchema::new("host", ConcreteDataType::string_datatype(), true),
                ColumnSchema::new("zone", ConcreteDataType::string_datatype(), true),
                ColumnSchema::new("rack", ConcreteDataType::string_datatype(), true),
                ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                )
                .with_time_index(true),
            ];
            physical_info.meta.schema = Arc::new(Schema::new(columns.clone()));
            physical_info.meta.primary_key_indices = vec![0, 1, 2];
            physical_info.meta.partition_key_indices = vec![0, 1, 2];
            metadata
                .create_table_metadata(
                    physical_info.clone(),
                    TableRouteValue::physical(vec![]),
                    HashMap::new(),
                )
                .await
                .unwrap();
            let mut input_info = test_create_table_task(name, 1025).table_info;
            if metric {
                input_info.meta.engine = METRIC_ENGINE_NAME.to_string();
            }
            input_info.meta.schema = Arc::new(Schema::new(
                column_order.iter().map(|&i| columns[i].clone()).collect(),
            ));
            input_info.meta.primary_key_indices = column_order
                .iter()
                .enumerate()
                .filter_map(|(index, &physical_index)| (physical_index != 3).then_some(index))
                .collect();
            input_info.meta.partition_key_indices.clear();
            metadata
                .create_table_metadata(
                    input_info.clone(),
                    if logical {
                        TableRouteValue::logical(1024)
                    } else {
                        TableRouteValue::physical(vec![])
                    },
                    HashMap::new(),
                )
                .await
                .unwrap();
            let info_cache = Arc::new(new_table_info_cache(
                "test_info".to_string(),
                CacheBuilder::new(128).build(),
                backend.clone(),
            ));
            let route_cache = Arc::new(new_table_route_cache(
                "test_route".to_string(),
                CacheBuilder::new(128).build(),
                backend,
            ));
            let cached = info_cache.get(1025).await.unwrap().unwrap();
            let resolved = resolve_logical_table_info(&cached, &route_cache, &info_cache)
                .await
                .unwrap();
            if let Some((indices, missing)) = expected {
                let resolved = resolved.unwrap();
                let mut expected_info = input_info.clone();
                expected_info.meta.partition_key_indices = indices;
                assert_eq!(*resolved.table_info, expected_info, "{name}");
                assert_eq!(resolved.missing_partition_columns, missing, "{name}");
            } else {
                assert!(resolved.is_none(), "{name}");
            }
            assert_eq!(*cached, input_info, "{name}");
            assert_eq!(
                *info_cache.get(1025).await.unwrap().unwrap(),
                input_info,
                "{name}"
            );
            assert_eq!(
                *info_cache.get(1024).await.unwrap().unwrap(),
                physical_info,
                "{name}"
            );
        }
    }
}
