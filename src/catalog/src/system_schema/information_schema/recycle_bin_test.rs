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

use cache::{build_fundamental_cache_registry, with_default_composite_cache_registry};
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, INFORMATION_SCHEMA_NAME};
use common_meta::cache::{CacheRegistryBuilder, LayeredCacheRegistryBuilder};
use common_meta::key::table_name::TableNameKey;
use common_meta::key::table_route::TableRouteValue;
use common_meta::key::tombstone::TombstoneManager;
use common_meta::key::{MetadataKey, TableMetadataManagerRef};
use common_meta::kv_backend::KvBackendRef;
use common_meta::kv_backend::memory::MemoryKvBackend;
use common_meta::kv_backend::test_util::MockKvBackendBuilder;
use common_meta::rpc::store::PutRequest;
use common_meta::wal_provider::RegionWalOptions;
use datafusion::logical_expr::{col, lit};
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::SchemaBuilder;
use futures_util::TryStreamExt;
use store_api::storage::{ScanRequest, TableId};
use table::TableRef;
use table::metadata::{TableInfo, TableInfoBuilder, TableMeta, TableType};
use table::table_name::TableName;

use crate::CatalogManager;
use crate::information_schema::NoopInformationExtension;
use crate::kvbackend::{KvBackendCatalogManager, KvBackendCatalogManagerBuilder};
use crate::memory::new_memory_catalog_manager;

const RECYCLE_BIN: &str = "recycle_bin";

fn kv_catalog_manager_with_backend(backend: KvBackendRef) -> Arc<KvBackendCatalogManager> {
    let layered_cache_builder = LayeredCacheRegistryBuilder::default()
        .add_cache_registry(CacheRegistryBuilder::default().build());
    let fundamental_cache_registry = build_fundamental_cache_registry(backend.clone());
    let layered_cache_registry = Arc::new(
        with_default_composite_cache_registry(
            layered_cache_builder.add_cache_registry(fundamental_cache_registry),
        )
        .unwrap()
        .build(),
    );
    KvBackendCatalogManagerBuilder::new(
        Arc::new(NoopInformationExtension),
        backend,
        layered_cache_registry,
    )
    .build()
}

fn kv_catalog_manager() -> Arc<KvBackendCatalogManager> {
    kv_catalog_manager_with_backend(Arc::new(MemoryKvBackend::default()))
}

fn table_info(catalog: &str, schema: &str, table: &str, table_id: TableId) -> TableInfo {
    let schema_ref = Arc::new(
        SchemaBuilder::try_from_columns(vec![])
            .unwrap()
            .build()
            .unwrap(),
    );
    TableInfoBuilder::default()
        .table_id(table_id)
        .name(table)
        .catalog_name(catalog)
        .schema_name(schema)
        .table_version(0)
        .table_type(TableType::Base)
        .meta(TableMeta {
            schema: schema_ref,
            primary_key_indices: vec![],
            value_indices: vec![],
            engine: common_catalog::consts::MITO_ENGINE.to_string(),
            next_column_id: 0,
            options: Default::default(),
            created_on: Default::default(),
            updated_on: Default::default(),
            partition_key_indices: vec![],
            column_ids: vec![],
        })
        .build()
        .unwrap()
}

async fn drop_table(
    manager: &TableMetadataManagerRef,
    catalog: &str,
    schema: &str,
    table: &str,
    table_id: TableId,
    dropped_at: Option<i64>,
) {
    let name = TableName::new(catalog, schema, table);
    let route = TableRouteValue::physical(vec![]);
    let wal_options = RegionWalOptions::default();
    manager
        .create_table_metadata(
            table_info(catalog, schema, table, table_id),
            route.clone(),
            wal_options.clone(),
        )
        .await
        .unwrap();
    manager
        .delete_table_metadata_with_retention(
            table_id,
            &name,
            &route,
            &wal_options,
            dropped_at,
            dropped_at.map(|v| v + 100),
        )
        .await
        .unwrap();
}

async fn information_table(manager: &Arc<dyn CatalogManager>, name: &str) -> TableRef {
    manager
        .table(DEFAULT_CATALOG_NAME, INFORMATION_SCHEMA_NAME, name, None)
        .await
        .unwrap()
        .unwrap_or_else(|| panic!("information_schema.{name} is not registered"))
}

async fn scan(table: &TableRef, filters: Vec<datafusion::logical_expr::Expr>) -> String {
    let batches = table
        .scan_to_stream(ScanRequest {
            filters,
            ..Default::default()
        })
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    batches
        .iter()
        .map(|batch| batch.pretty_print())
        .collect::<Vec<_>>()
        .join("\n")
}

#[tokio::test]
async fn recycle_bin_is_registered_with_stable_schema() {
    let manager: Arc<dyn CatalogManager> = kv_catalog_manager();
    let table = information_table(&manager, RECYCLE_BIN).await;
    let schema = table.schema();

    assert_eq!(44, table.table_info().table_id());
    let expected = [
        ("object_id", ConcreteDataType::uint64_datatype(), false),
        ("object_type", ConcreteDataType::string_datatype(), false),
        (
            "original_object_name",
            ConcreteDataType::string_datatype(),
            false,
        ),
        (
            "original_catalog_name",
            ConcreteDataType::string_datatype(),
            false,
        ),
        (
            "original_schema_name",
            ConcreteDataType::string_datatype(),
            false,
        ),
        (
            "dropped_at",
            ConcreteDataType::timestamp_millisecond_datatype(),
            true,
        ),
        ("dropped_by", ConcreteDataType::string_datatype(), true),
        (
            "retention_expires_at",
            ConcreteDataType::timestamp_millisecond_datatype(),
            true,
        ),
        ("purge_status", ConcreteDataType::string_datatype(), false),
        ("restorable", ConcreteDataType::boolean_datatype(), false),
        (
            "restored_at",
            ConcreteDataType::timestamp_millisecond_datatype(),
            true,
        ),
        ("restored_by", ConcreteDataType::string_datatype(), true),
    ];
    assert_eq!(expected.len(), schema.num_columns());
    for (column, (name, data_type, nullable)) in schema.column_schemas().iter().zip(expected) {
        assert_eq!(name, column.name);
        assert_eq!(data_type, column.data_type);
        assert_eq!(nullable, column.is_nullable());
    }
}

#[tokio::test]
async fn recycle_bin_scan_returns_scoped_sorted_rows_and_nulls() {
    let manager = kv_catalog_manager();
    let metadata = manager.table_metadata_manager_ref().clone();
    drop_table(&metadata, DEFAULT_CATALOG_NAME, "z", "gone", 9, Some(1234)).await;
    drop_table(&metadata, DEFAULT_CATALOG_NAME, "a", "legacy", 2, None).await;
    drop_table(&metadata, "other", "a", "hidden", 1, Some(1)).await;

    let manager: Arc<dyn CatalogManager> = manager;
    let output = scan(&information_table(&manager, RECYCLE_BIN).await, vec![]).await;
    assert!(output.contains("2         | TABLE"), "{output}");
    assert!(output.contains("9         | TABLE"), "{output}");
    assert!(output.contains("gone"), "{output}");
    assert!(output.contains("legacy"), "{output}");
    assert!(output.contains("1970-01-01T00:00:01.234"), "{output}");
    assert!(output.contains("1970-01-01T00:00:01.334"), "{output}");
    assert!(output.contains("ACTIVE"), "{output}");
    assert!(output.contains("true"), "{output}");
    assert!(!output.contains("hidden"), "{output}");
    assert!(output.find("legacy").unwrap() < output.find("gone").unwrap());
}

#[tokio::test]
async fn recycle_bin_hides_purging_tables() {
    let manager = kv_catalog_manager();
    let metadata = manager.table_metadata_manager_ref().clone();
    drop_table(
        &metadata,
        DEFAULT_CATALOG_NAME,
        DEFAULT_SCHEMA_NAME,
        "visible",
        10,
        Some(1234),
    )
    .await;
    drop_table(
        &metadata,
        DEFAULT_CATALOG_NAME,
        DEFAULT_SCHEMA_NAME,
        "purging",
        11,
        Some(1234),
    )
    .await;
    metadata.mark_dropped_table_purging(11, None).await.unwrap();

    let manager: Arc<dyn CatalogManager> = manager;
    let output = scan(&information_table(&manager, RECYCLE_BIN).await, vec![]).await;
    assert!(output.contains("visible"), "{output}");
    assert!(!output.contains("purging"), "{output}");
}

#[tokio::test]
async fn recycle_bin_ignores_malformed_tombstones_from_other_catalogs() {
    let backend: KvBackendRef = Arc::new(MemoryKvBackend::default());
    let manager = kv_catalog_manager_with_backend(backend.clone());
    let metadata = manager.table_metadata_manager_ref().clone();
    drop_table(
        &metadata,
        DEFAULT_CATALOG_NAME,
        DEFAULT_SCHEMA_NAME,
        "visible",
        10,
        Some(1234),
    )
    .await;

    let other_catalog = format!("{DEFAULT_CATALOG_NAME}_extra");
    let table_name_key = TableNameKey::new(&other_catalog, DEFAULT_SCHEMA_NAME, "broken");
    let tombstone_key =
        TombstoneManager::new(backend.clone()).to_tombstone(&table_name_key.to_bytes());
    backend
        .put(
            PutRequest::new()
                .with_key(tombstone_key)
                .with_value("invalid table-name value"),
        )
        .await
        .unwrap();

    let manager: Arc<dyn CatalogManager> = manager;
    let output = scan(&information_table(&manager, RECYCLE_BIN).await, vec![]).await;
    assert!(output.contains("visible"), "{output}");
    assert!(!output.contains("broken"), "{output}");
}

#[tokio::test]
async fn recycle_bin_predicates_and_live_same_name_are_independent() {
    let manager = kv_catalog_manager();
    let metadata = manager.table_metadata_manager_ref().clone();
    drop_table(
        &metadata,
        DEFAULT_CATALOG_NAME,
        DEFAULT_SCHEMA_NAME,
        "same",
        10,
        Some(1234),
    )
    .await;
    metadata
        .create_table_metadata(
            table_info(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, "same", 11),
            TableRouteValue::physical(vec![]),
            RegionWalOptions::default(),
        )
        .await
        .unwrap();

    let manager: Arc<dyn CatalogManager> = manager;
    let recycle_bin = information_table(&manager, RECYCLE_BIN).await;
    let output = scan(
        &recycle_bin,
        vec![
            col("object_id").eq(lit(10_u64)),
            col("object_type").eq(lit("TABLE")),
            col("original_object_name").eq(lit("same")),
            col("original_catalog_name").eq(lit(DEFAULT_CATALOG_NAME)),
            col("original_schema_name").eq(lit(DEFAULT_SCHEMA_NAME)),
            col("purge_status").eq(lit("ACTIVE")),
            col("restorable").eq(lit(true)),
        ],
    )
    .await;
    assert!(output.contains("same"), "{output}");
    assert!(
        !scan(&recycle_bin, vec![col("object_id").eq(lit(11_u64))])
            .await
            .contains("same")
    );

    let tables = information_table(&manager, "tables").await;
    let tables_output = scan(&tables, vec![col("table_name").eq(lit("same"))]).await;
    assert!(!tables_output.contains("DROPPED TABLE"), "{tables_output}");
    assert!(!tables_output.contains("10"), "{tables_output}");
}

#[tokio::test]
async fn recycle_bin_unsupported_catalog_manager_uses_existing_fallback() {
    let manager: Arc<dyn CatalogManager> = new_memory_catalog_manager().unwrap();
    let table = information_table(&manager, RECYCLE_BIN).await;
    let stream = table.scan_to_stream(ScanRequest::default()).await.unwrap();
    let error = stream.try_collect::<Vec<_>>().await.unwrap_err();
    assert!(
        error.to_string().contains("catalog manager reference"),
        "{error}"
    );
}

#[tokio::test]
async fn recycle_bin_propagates_backend_errors_from_scan() {
    let backend = MockKvBackendBuilder::default()
        .range_fn(Arc::new(|_| {
            common_meta::error::UnsupportedSnafu {
                operation: "list dropped tables",
            }
            .fail()
        }))
        .max_txn_ops(128)
        .build()
        .unwrap();
    let manager: Arc<dyn CatalogManager> = kv_catalog_manager_with_backend(Arc::new(backend));
    let table = information_table(&manager, RECYCLE_BIN).await;
    let stream = table.scan_to_stream(ScanRequest::default()).await.unwrap();

    let error = stream.try_collect::<Vec<_>>().await.unwrap_err();
    assert!(
        error.to_string().contains("Table metadata manager error"),
        "{error}"
    );
}
