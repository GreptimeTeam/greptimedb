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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use api::v1::SemanticType;
use catalog::memory::MemoryCatalogManager;
use common_error::ext::WhateverResult;
use common_meta::key::SchemaMetadataManager;
use common_meta::kv_backend::memory::MemoryKvBackend;
use common_query::Output;
use common_query::request::QueryRequest;
use datanode::config::StorageConfig;
use datanode::event_listener::NoopRegionServerEventListener;
use datanode::partition_expr_fetcher::MetaPartitionExprFetcher;
use datanode::region_server::RegionServer;
use datanode::store;
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema};
use datatypes::types::{StructField, StructType};
use log_store::noop::log_store::NoopLogStore;
use mito2::engine::MitoEngineBuilder;
use mito2::sst::file_ref::FileReferenceManager;
use object_store::manager::ObjectStoreManager;
use query::QueryEngineFactory;
use query::dummy_catalog::{DummyCatalogManager, DummyTableProviderFactory};
use query::parser::QueryStatement;
use session::context::QueryContext;
use sql::dialect::GreptimeDbDialect;
use sql::parser::ParserContext;
use store_api::metadata::ColumnMetadata;
use store_api::region_request::{PathType, RegionCreateRequest, RegionRequest};
use store_api::storage::RegionId;
use table::metadata::{TableInfoBuilder, TableMetaBuilder};
use table::test_util::EmptyTable;

#[tokio::test]
async fn test_column_field_access_pushdown() -> WhateverResult<()> {
    common_telemetry::init_default_ut_logging();

    let column_schemas =
        vec![
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new(
                "a",
                ConcreteDataType::struct_datatype(StructType::new(Arc::new(vec![
                    StructField::new("x", ConcreteDataType::string_datatype(), true),
                ]))),
                true,
            ),
            ColumnSchema::new("b", ConcreteDataType::int32_datatype(), true),
        ];

    let plan = QueryEngineFactory::new(
        MemoryCatalogManager::new_with_table(EmptyTable::from_table_info(
            &TableInfoBuilder::new(
                "foo",
                TableMetaBuilder::empty()
                    .schema(Arc::new(Schema::new(column_schemas.clone())))
                    .primary_key_indices(vec![])
                    .next_column_id(3)
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap(),
        )),
        None,
        None,
        None,
        None,
        true,
        Default::default(),
    )
    .query_engine()
    .planner()
    .plan(
        &QueryStatement::Sql(
            ParserContext::create_with_dialect(
                "select a['x'] from foo where b > 0 order by ts limit 1",
                &GreptimeDbDialect {},
                Default::default(),
            )?
            .remove(0),
        ),
        QueryContext::arc(),
    )
    .await?;

    let data_home = "/tmp";
    let config = StorageConfig {
        data_home: data_home.to_string(),
        ..Default::default()
    };
    let kv_backend = Arc::new(MemoryKvBackend::new());
    let registry = cache::build_datanode_cache_registry(kv_backend.clone());
    let mito_engine = Arc::new(
        MitoEngineBuilder::new(
            data_home,
            Default::default(),
            Arc::new(NoopLogStore),
            Arc::new(ObjectStoreManager::new(
                "test",
                store::new_object_store(config.store.clone(), data_home).await?,
            )),
            Arc::new(SchemaMetadataManager::new(
                registry.get().unwrap(),
                registry.get().unwrap(),
            )),
            Arc::new(FileReferenceManager::new(None)),
            Arc::new(MetaPartitionExprFetcher::new(kv_backend)),
            Default::default(),
            0,
        )
        .try_build()
        .await?,
    );

    let mut server = RegionServer::with_table_provider(
        QueryEngineFactory::new(
            DummyCatalogManager::arc(),
            None,
            None,
            None,
            None,
            false,
            Default::default(),
        )
        .query_engine(),
        common_runtime::global_runtime(),
        Box::new(NoopRegionServerEventListener),
        Arc::new(DummyTableProviderFactory),
        0,
        Duration::from_secs(0),
        Default::default(),
    );
    server.register_engine(mito_engine);

    let region_id = RegionId::new(1024, 0);
    server
        .handle_request(
            region_id,
            RegionRequest::Create(RegionCreateRequest {
                engine: "mito".to_string(),
                column_metadatas: vec![
                    ColumnMetadata {
                        column_schema: column_schemas[0].clone(),
                        semantic_type: SemanticType::Timestamp,
                        column_id: 0,
                    },
                    ColumnMetadata {
                        column_schema: column_schemas[1].clone(),
                        semantic_type: SemanticType::Field,
                        column_id: 1,
                    },
                    ColumnMetadata {
                        column_schema: column_schemas[2].clone(),
                        semantic_type: SemanticType::Field,
                        column_id: 2,
                    },
                ],
                primary_key: vec![],
                options: HashMap::new(),
                table_dir: data_home.to_string(),
                path_type: PathType::Bare,
                partition_expr_json: None,
            }),
        )
        .await?;

    let request = QueryRequest {
        header: None,
        region_id,
        plan,
    };
    let result = server.handle_read(request).await?;

    let output = Output::new_with_stream(result).data.pretty_print().await;
    common_telemetry::debug!("{}", output);
    Ok(())
}
