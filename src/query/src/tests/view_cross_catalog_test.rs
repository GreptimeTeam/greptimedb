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

//! Tests for validation of nested (scalar-subquery) cross-catalog references
//! stored inside persisted views.

use std::collections::HashMap;
use std::sync::Arc;

use cache::{build_fundamental_cache_registry, with_default_composite_cache_registry};
use catalog::CatalogManagerRef;
use catalog::information_schema::NoopInformationExtension;
use catalog::kvbackend::KvBackendCatalogManagerBuilder;
use common_meta::cache::{CacheRegistryBuilder, LayeredCacheRegistryBuilder};
use common_meta::key::TableMetadataManager;
use common_meta::key::table_route::TableRouteValue;
use common_meta::kv_backend::KvBackend;
use common_meta::kv_backend::memory::MemoryKvBackend;
use datafusion::logical_expr::LogicalPlan;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, SchemaBuilder};
use session::context::{QueryContext, QueryContextRef};
use store_api::storage::TableId;
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};
use table::metadata::{TableInfo, TableInfoBuilder, TableMetaBuilder, TableType};

use crate::QueryEngineRef;
use crate::options::QueryOptions;
use crate::parser::QueryLanguageParser;
use crate::plan::extract_and_rewrite_full_table_names;
use crate::query_engine::{DefaultSerializer, QueryEngineFactory};

const FOREIGN_CATALOG: &str = "foreign_catalog";
const FOREIGN_SCHEMA: &str = "public";
const FOREIGN_TABLE: &str = "foreign_tbl";
const VIEW_NAME: &str = "v1";

/// Builds a base-table `TableInfo` with columns `(col1, ts, col2)`, mirroring
/// `common_meta::key::test_utils::new_test_table_info` without requiring the
/// `common-meta/testing` feature.
fn test_table_info(table_id: TableId, name: &str) -> TableInfo {
    let column_schemas = vec![
        ColumnSchema::new("col1", ConcreteDataType::int32_datatype(), true),
        ColumnSchema::new(
            "ts",
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        )
        .with_time_index(true),
        ColumnSchema::new("col2", ConcreteDataType::int32_datatype(), true),
    ];
    let schema = SchemaBuilder::try_from(column_schemas)
        .unwrap()
        .version(123)
        .build()
        .unwrap();

    let meta = TableMetaBuilder::empty()
        .schema(Arc::new(schema))
        .primary_key_indices(vec![0])
        .engine("engine")
        .next_column_id(3)
        .build()
        .unwrap();
    TableInfoBuilder::default()
        .table_id(table_id)
        .table_version(5)
        .name(name)
        .meta(meta)
        .build()
        .unwrap()
}

/// Builds a KV-backed catalog manager over a brand-new in-memory backend,
/// together with its table metadata manager and the backend handle.
fn build_catalog_manager() -> (
    CatalogManagerRef,
    Arc<TableMetadataManager>,
    Arc<MemoryKvBackend<common_meta::error::Error>>,
) {
    let backend = Arc::new(MemoryKvBackend::default());
    let catalog_manager = build_catalog_manager_with_backend(backend.clone());
    let table_metadata_manager = Arc::new(TableMetadataManager::new(backend.clone()));
    (catalog_manager, table_metadata_manager, backend)
}

/// Builds a KV-backed catalog manager over the given backend with fresh caches,
/// mirroring a freshly-started frontend that reads the same persisted metadata.
fn build_catalog_manager_with_backend(
    backend: Arc<MemoryKvBackend<common_meta::error::Error>>,
) -> CatalogManagerRef {
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

/// Builds a query engine over the given catalog manager.
fn build_engine(catalog_manager: CatalogManagerRef) -> QueryEngineRef {
    QueryEngineFactory::new(
        catalog_manager,
        None,
        None,
        None,
        None,
        false,
        QueryOptions::default(),
    )
    .query_engine()
}

/// Plans a SQL statement through the query engine's planner.
async fn plan_sql(
    engine: QueryEngineRef,
    query_ctx: &QueryContextRef,
    sql: &str,
) -> crate::error::Result<LogicalPlan> {
    let stmt = QueryLanguageParser::parse_sql(sql, query_ctx).unwrap();
    engine.planner().plan(&stmt, query_ctx.clone()).await
}

/// Persists a view named `view_name` over `sql` into `greptime.public`,
/// mirroring the view-creation flow.
async fn persist_view_named(
    table_metadata_manager: &Arc<TableMetadataManager>,
    engine: QueryEngineRef,
    query_ctx: &QueryContextRef,
    view_name: &str,
    view_id: u32,
    sql: &str,
) {
    let logical_plan = plan_sql(engine, query_ctx, sql).await.unwrap();
    let (table_names, plan) =
        extract_and_rewrite_full_table_names(logical_plan, query_ctx.clone()).unwrap();
    let encoded = DFLogicalSubstraitConvertor
        .encode(&plan, DefaultSerializer)
        .unwrap();
    let plan_columns: Vec<String> = plan
        .schema()
        .columns()
        .into_iter()
        .map(|c| c.name)
        .collect();

    let mut view_info = test_table_info(view_id, view_name);
    view_info.catalog_name = "greptime".to_string();
    view_info.schema_name = "public".to_string();
    view_info.table_type = TableType::View;
    table_metadata_manager
        .create_view_metadata(
            view_info,
            encoded.to_vec(),
            table_names,
            vec![],
            plan_columns,
            sql.to_string(),
        )
        .await
        .unwrap();
}

/// Persists a view over `sql` into `greptime.public`, exactly mirroring the
/// view-creation flow (`extract_and_rewrite_full_table_names` + substrait
/// encode + view metadata).
async fn persist_view(
    table_metadata_manager: &Arc<TableMetadataManager>,
    engine: QueryEngineRef,
    query_ctx: &QueryContextRef,
    sql: &str,
) {
    persist_view_named(table_metadata_manager, engine, query_ctx, VIEW_NAME, 2, sql).await;
}

/// Persists an additional view with the given name.
async fn persist_flat_view(
    table_metadata_manager: &Arc<TableMetadataManager>,
    engine: QueryEngineRef,
    query_ctx: &QueryContextRef,
    sql: &str,
    view_name: &str,
) {
    persist_view_named(table_metadata_manager, engine, query_ctx, view_name, 3, sql).await;
}

/// Collects the full error source chain as a single string.
fn error_chain(err: &dyn std::error::Error) -> String {
    let mut parts = vec![err.to_string()];
    let mut source = err.source();
    while let Some(e) = source {
        parts.push(e.to_string());
        source = e.source();
    }
    parts.join(" | ")
}

/// A view stored in catalog `greptime` references a table in another catalog
/// (`foreign_catalog`) inside a scalar subquery. After the foreign table is
/// dropped, querying the view must fail: the nested cross-catalog reference
/// has to be validated (existence) when the persisted view is decoded.
#[tokio::test]
async fn persisted_view_cross_catalog_nested_reference_validation() {
    let (catalog_manager, table_metadata_manager, backend) = build_catalog_manager();

    // Register a base table in the foreign catalog.
    let mut foreign_info = test_table_info(1, FOREIGN_TABLE);
    foreign_info.catalog_name = FOREIGN_CATALOG.to_string();
    foreign_info.schema_name = FOREIGN_SCHEMA.to_string();
    table_metadata_manager
        .create_table_metadata(
            foreign_info,
            TableRouteValue::physical(vec![]),
            HashMap::new(),
        )
        .await
        .unwrap();

    // Persist a view whose plan contains a scalar subquery scanning the
    // foreign table via a fully-qualified name.
    let query_ctx = Arc::new(QueryContext::with("greptime", "public"));
    let engine = build_engine(catalog_manager.clone());
    let view_sql = format!(
        "SELECT (SELECT col1 FROM {FOREIGN_CATALOG}.{FOREIGN_SCHEMA}.{FOREIGN_TABLE}) AS col1"
    );
    persist_view(
        &table_metadata_manager,
        engine.clone(),
        &query_ctx,
        &view_sql,
    )
    .await;

    // While the foreign table exists, the view resolves fine.
    plan_sql(engine.clone(), &query_ctx, "SELECT * FROM v1")
        .await
        .unwrap();

    // Drop the foreign table by removing its metadata from the KV backend.
    use common_meta::key::MetadataKey;
    use common_meta::key::table_info::TableInfoKey;
    use common_meta::key::table_name::TableNameKey;
    backend
        .delete(
            TableNameKey::new(FOREIGN_CATALOG, FOREIGN_SCHEMA, FOREIGN_TABLE)
                .to_bytes()
                .as_slice(),
            true,
        )
        .await
        .unwrap();
    backend
        .delete(TableInfoKey::new(1).to_bytes().as_slice(), true)
        .await
        .unwrap();

    // A fresh engine over fresh caches (same persisted metadata) must now fail
    // to plan a query against the view: the nested foreign reference no longer
    // exists and must be validated during view plan decoding.
    let fresh_catalog_manager = build_catalog_manager_with_backend(backend);
    let fresh_engine = build_engine(fresh_catalog_manager);
    let result = plan_sql(fresh_engine, &query_ctx, "SELECT * FROM v1").await;

    let chain = error_chain(&result.err().unwrap());
    assert!(
        chain.contains(FOREIGN_TABLE) || chain.contains(FOREIGN_CATALOG),
        "expected the failure to mention the stale nested foreign reference, got: {chain}"
    );
}

/// A view stored in catalog `greptime` references a table in another catalog
/// inside a scalar subquery. After the foreign table's referenced column type
/// is changed, querying the view must fail (schema compatibility validation).
#[tokio::test]
async fn persisted_view_cross_catalog_nested_reference_schema_change_validation() {
    let (catalog_manager, table_metadata_manager, backend) = build_catalog_manager();

    // Register a base table in the foreign catalog.
    let mut foreign_info = test_table_info(1, FOREIGN_TABLE);
    foreign_info.catalog_name = FOREIGN_CATALOG.to_string();
    foreign_info.schema_name = FOREIGN_SCHEMA.to_string();
    table_metadata_manager
        .create_table_metadata(
            foreign_info.clone(),
            TableRouteValue::physical(vec![]),
            HashMap::new(),
        )
        .await
        .unwrap();

    // Persist a view whose plan contains a scalar subquery scanning the
    // foreign table via a fully-qualified name.
    let query_ctx = Arc::new(QueryContext::with("greptime", "public"));
    let engine = build_engine(catalog_manager.clone());
    let view_sql = format!(
        "SELECT (SELECT col1 FROM {FOREIGN_CATALOG}.{FOREIGN_SCHEMA}.{FOREIGN_TABLE}) AS col1"
    );
    persist_view(
        &table_metadata_manager,
        engine.clone(),
        &query_ctx,
        &view_sql,
    )
    .await;

    // While the foreign table schema matches, the view resolves fine.
    plan_sql(engine.clone(), &query_ctx, "SELECT * FROM v1")
        .await
        .unwrap();

    // Change the foreign table's `col1` type from Int32 to Utf8 by rewriting
    // its table-info metadata in the KV backend (simulating ALTER TABLE).
    use common_meta::key::table_info::{TableInfoKey, TableInfoValue};
    use common_meta::key::{MetadataKey, MetadataValue};
    use common_meta::rpc::store::PutRequest;
    let mut changed_info = foreign_info;
    let column_schemas = vec![
        ColumnSchema::new("col1", ConcreteDataType::string_datatype(), true),
        ColumnSchema::new(
            "ts",
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        )
        .with_time_index(true),
        ColumnSchema::new("col2", ConcreteDataType::int32_datatype(), true),
    ];
    let schema = SchemaBuilder::try_from(column_schemas)
        .unwrap()
        .version(124)
        .build()
        .unwrap();
    changed_info.meta.schema = Arc::new(schema);
    backend
        .put(
            PutRequest::new()
                .with_key(TableInfoKey::new(1).to_bytes())
                .with_value(
                    TableInfoValue::new(changed_info)
                        .try_as_raw_value()
                        .unwrap(),
                ),
        )
        .await
        .unwrap();

    // A fresh engine over fresh caches (same persisted metadata) must now fail
    // to plan a query against the view: the nested foreign reference's schema
    // is no longer compatible and must be validated during view plan decoding.
    let fresh_catalog_manager = build_catalog_manager_with_backend(backend);
    let fresh_engine = build_engine(fresh_catalog_manager);
    let result = plan_sql(fresh_engine, &query_ctx, "SELECT * FROM v1").await;

    let chain = error_chain(&result.err().unwrap());
    assert!(
        chain.contains("col1") || chain.contains(FOREIGN_TABLE) || chain.contains(FOREIGN_CATALOG),
        "expected the failure to mention the incompatible nested foreign reference, got: {chain}"
    );
}

/// When cross-catalog queries are disallowed, ordinary SQL with a nested
/// foreign reference is rejected at plan time. A persisted view must not be
/// able to bypass that restriction: querying a view whose plan contains a
/// nested fully-qualified foreign scan must fail too.
#[tokio::test]
async fn persisted_view_cross_catalog_nested_reference_disallowed_validation() {
    let (catalog_manager, table_metadata_manager, backend) = build_catalog_manager();

    // Register a base table in the foreign catalog.
    let mut foreign_info = test_table_info(1, FOREIGN_TABLE);
    foreign_info.catalog_name = FOREIGN_CATALOG.to_string();
    foreign_info.schema_name = FOREIGN_SCHEMA.to_string();
    table_metadata_manager
        .create_table_metadata(
            foreign_info,
            TableRouteValue::physical(vec![]),
            HashMap::new(),
        )
        .await
        .unwrap();

    // Persist the view while cross-catalog queries are still allowed.
    let query_ctx = Arc::new(QueryContext::with("greptime", "public"));
    let engine = build_engine(catalog_manager.clone());
    let view_sql = format!(
        "SELECT (SELECT col1 FROM {FOREIGN_CATALOG}.{FOREIGN_SCHEMA}.{FOREIGN_TABLE}) AS col1"
    );
    persist_view(
        &table_metadata_manager,
        engine.clone(),
        &query_ctx,
        &view_sql,
    )
    .await;

    // Also persist a view with a flat (top-level) foreign scan, to make sure
    // the restriction covers both flat and nested references in views.
    let flat_sql = format!("SELECT col1 FROM {FOREIGN_CATALOG}.{FOREIGN_SCHEMA}.{FOREIGN_TABLE}");
    persist_flat_view(
        &table_metadata_manager,
        engine,
        &query_ctx,
        &flat_sql,
        "v_flat",
    )
    .await;

    // Build a fresh engine with cross-catalog queries disallowed.
    let restricted_catalog_manager = build_catalog_manager_with_backend(backend);
    let plugins = common_base::Plugins::new();
    plugins.insert(crate::query_engine::options::QueryOptions {
        disallow_cross_catalog_query: true,
    });
    let restricted_engine = QueryEngineFactory::new_with_plugins(
        restricted_catalog_manager,
        None,
        None,
        None,
        None,
        None,
        false,
        plugins,
        QueryOptions::default(),
    )
    .query_engine();

    // Control: ordinary SQL with a nested foreign reference must be rejected.
    let control = plan_sql(
        restricted_engine.clone(),
        &query_ctx,
        &format!(
            "SELECT (SELECT col1 FROM {FOREIGN_CATALOG}.{FOREIGN_SCHEMA}.{FOREIGN_TABLE}) AS col1"
        ),
    )
    .await;
    assert!(
        control.is_err(),
        "ordinary SQL with a nested foreign reference must be rejected when cross-catalog queries are disallowed"
    );

    // The persisted view must not bypass the restriction.
    let result = plan_sql(restricted_engine.clone(), &query_ctx, "SELECT * FROM v1").await;
    assert!(
        result.is_err(),
        "a persisted view with a nested foreign reference must not bypass the cross-catalog restriction"
    );

    // Neither may a persisted view with a flat (top-level) foreign scan.
    let flat_result = plan_sql(restricted_engine, &query_ctx, "SELECT * FROM v_flat").await;
    assert!(
        flat_result.is_err(),
        "a persisted view with a flat foreign reference must not bypass the cross-catalog restriction"
    );
}
