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

use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

use api::v1::CreateTableExpr;
use catalog::error::{
    self as catalog_err, InternalSnafu, InvalidCatalogValueSnafu, InvalidSystemTableDefSnafu,
    Result as CatalogResult, TableMetadataManagerSnafu, UnimplementedSnafu,
};
use catalog::information_schema::InformationSchemaProvider;
use catalog::remote::KvCacheInvalidatorRef;
use catalog::{
    CatalogManager, DeregisterSchemaRequest, DeregisterTableRequest, RegisterSchemaRequest,
    RegisterSystemTableRequest, RegisterTableRequest, RenameTableRequest,
};
use client::client_manager::DatanodeClients;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, INFORMATION_SCHEMA_NAME};
use common_error::ext::BoxedError;
use common_meta::helper::{build_catalog_prefix, build_schema_prefix, CatalogKey, SchemaKey};
use common_meta::key::table_info::TableInfoKey;
use common_meta::key::table_name::TableNameKey;
use common_meta::key::table_region::TableRegionKey;
use common_meta::key::{TableMetaKey, TableMetadataManagerRef};
use common_meta::kv_backend::KvBackendRef;
use common_meta::rpc::store::RangeRequest;
use common_meta::rpc::KeyValue;
use common_meta::table_name::TableName;
use common_telemetry::{debug, warn};
use partition::manager::PartitionRuleManagerRef;
use snafu::prelude::*;
use table::metadata::TableId;
use table::table::numbers::NumbersTable;
use table::TableRef;

use crate::expr_factory;
use crate::instance::distributed::DistInstance;
use crate::table::DistTable;

#[derive(Clone)]
pub struct FrontendCatalogManager {
    backend: KvBackendRef,
    backend_cache_invalidator: KvCacheInvalidatorRef,
    partition_manager: PartitionRuleManagerRef,
    datanode_clients: Arc<DatanodeClients>,
    table_metadata_manager: TableMetadataManagerRef,

    // TODO(LFC): Remove this field.
    // DistInstance in FrontendCatalogManager is only used for creating distributed script table now.
    // Once we have some standalone distributed table creator (like create distributed table procedure),
    // we should use that.
    dist_instance: Option<Arc<DistInstance>>,
}

impl FrontendCatalogManager {
    pub fn new(
        backend: KvBackendRef,
        backend_cache_invalidator: KvCacheInvalidatorRef,
        partition_manager: PartitionRuleManagerRef,
        datanode_clients: Arc<DatanodeClients>,
        table_metadata_manager: TableMetadataManagerRef,
    ) -> Self {
        Self {
            backend,
            backend_cache_invalidator,
            partition_manager,
            datanode_clients,
            table_metadata_manager,
            dist_instance: None,
        }
    }

    pub fn set_dist_instance(&mut self, dist_instance: Arc<DistInstance>) {
        self.dist_instance = Some(dist_instance)
    }

    pub fn backend(&self) -> KvBackendRef {
        self.backend.clone()
    }

    pub fn partition_manager(&self) -> PartitionRuleManagerRef {
        self.partition_manager.clone()
    }

    pub fn datanode_clients(&self) -> Arc<DatanodeClients> {
        self.datanode_clients.clone()
    }

    pub async fn invalidate_schema(&self, catalog: &str, schema: &str) {
        let schema_key = SchemaKey {
            catalog_name: catalog.into(),
            schema_name: schema.into(),
        }
        .to_string();

        let key = schema_key.as_bytes();

        self.backend_cache_invalidator.invalidate_key(key).await;
    }

    pub async fn invalidate_table(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        table_id: TableId,
    ) {
        let key = TableNameKey::new(catalog, schema, table);
        self.backend_cache_invalidator
            .invalidate_key(&key.as_raw_key())
            .await;
        debug!(
            "invalidated cache key: {}",
            String::from_utf8_lossy(&key.as_raw_key())
        );

        let key = TableInfoKey::new(table_id);
        self.backend_cache_invalidator
            .invalidate_key(&key.as_raw_key())
            .await;
        debug!(
            "invalidated cache key: {}",
            String::from_utf8_lossy(&key.as_raw_key())
        );

        let key = TableRegionKey::new(table_id);
        self.backend_cache_invalidator
            .invalidate_key(&key.as_raw_key())
            .await;
        debug!(
            "invalidated cache key: {}",
            String::from_utf8_lossy(&key.as_raw_key())
        );

        self.partition_manager
            .table_routes()
            .invalidate_table_route(table_id)
            .await;
    }
}

// FIXME(hl): Frontend only needs a CatalogList, should replace with trait upcasting
// as soon as it's stable: https://github.com/rust-lang/rust/issues/65991
#[async_trait::async_trait]
impl CatalogManager for FrontendCatalogManager {
    async fn start(&self) -> catalog::error::Result<()> {
        Ok(())
    }

    async fn register_catalog(&self, _name: String) -> CatalogResult<bool> {
        unimplemented!("FrontendCatalogManager does not support registering catalog")
    }

    // TODO(LFC): Handle the table caching in (de)register_table.
    async fn register_table(&self, _request: RegisterTableRequest) -> CatalogResult<bool> {
        Ok(true)
    }

    async fn deregister_table(&self, _request: DeregisterTableRequest) -> CatalogResult<()> {
        Ok(())
    }

    async fn register_schema(
        &self,
        _request: RegisterSchemaRequest,
    ) -> catalog::error::Result<bool> {
        unimplemented!("FrontendCatalogManager does not support registering schema")
    }

    async fn deregister_schema(
        &self,
        _request: DeregisterSchemaRequest,
    ) -> catalog_err::Result<bool> {
        unimplemented!("FrontendCatalogManager does not support deregistering schema")
    }

    async fn rename_table(&self, _request: RenameTableRequest) -> catalog_err::Result<bool> {
        unimplemented!()
    }

    async fn register_system_table(
        &self,
        request: RegisterSystemTableRequest,
    ) -> catalog::error::Result<()> {
        if let Some(dist_instance) = &self.dist_instance {
            let open_hook = request.open_hook;
            let request = request.create_table_request;

            if let Some(table) = self
                .table(
                    &request.catalog_name,
                    &request.schema_name,
                    &request.table_name,
                )
                .await?
            {
                if let Some(hook) = open_hook {
                    (hook)(table)?;
                }
                return Ok(());
            }

            let time_index = request
                .schema
                .column_schemas
                .iter()
                .find_map(|x| {
                    if x.is_time_index() {
                        Some(x.name.clone())
                    } else {
                        None
                    }
                })
                .context(InvalidSystemTableDefSnafu {
                    err_msg: "Time index is not defined.",
                })?;

            let primary_keys = request
                .schema
                .column_schemas
                .iter()
                .enumerate()
                .filter_map(|(i, x)| {
                    if request.primary_key_indices.contains(&i) {
                        Some(x.name.clone())
                    } else {
                        None
                    }
                })
                .collect();

            let column_defs = expr_factory::column_schemas_to_defs(request.schema.column_schemas)
                .map_err(|e| {
                InvalidSystemTableDefSnafu {
                    err_msg: e.to_string(),
                }
                .build()
            })?;

            let mut create_table = CreateTableExpr {
                catalog_name: request.catalog_name,
                schema_name: request.schema_name,
                table_name: request.table_name,
                desc: request.desc.unwrap_or("".to_string()),
                column_defs,
                time_index,
                primary_keys,
                create_if_not_exists: request.create_if_not_exists,
                table_options: (&request.table_options).into(),
                table_id: None, // Should and will be assigned by Meta.
                region_numbers: vec![0],
                engine: request.engine,
            };

            let table = dist_instance
                .create_table(&mut create_table, None)
                .await
                .map_err(BoxedError::new)
                .context(InternalSnafu)?;

            if let Some(hook) = open_hook {
                (hook)(table)?;
            }
            Ok(())
        } else {
            UnimplementedSnafu {
                operation: "register system table",
            }
            .fail()
        }
    }

    async fn catalog_names(&self) -> CatalogResult<Vec<String>> {
        let key = build_catalog_prefix();
        let req = RangeRequest::new().with_prefix(key.as_bytes());

        let kvs = self
            .backend
            .range(req)
            .await
            .context(TableMetadataManagerSnafu)?
            .kvs;

        let mut res = HashSet::new();
        for KeyValue { key: k, value: _ } in kvs {
            let catalog_key = String::from_utf8_lossy(&k);
            if let Ok(key) = CatalogKey::parse(catalog_key.as_ref()) {
                let _ = res.insert(key.catalog_name);
            } else {
                warn!("invalid catalog key: {:?}", catalog_key);
            }
        }
        Ok(res.into_iter().collect())
    }

    async fn schema_names(&self, catalog: &str) -> CatalogResult<Vec<String>> {
        let key = build_schema_prefix(catalog);
        let req = RangeRequest::new().with_prefix(key.as_bytes());

        let kvs = self
            .backend
            .range(req)
            .await
            .context(TableMetadataManagerSnafu)?
            .kvs;

        let mut res = HashSet::new();
        for KeyValue { key: k, value: _ } in kvs {
            let key =
                SchemaKey::parse(String::from_utf8_lossy(&k)).context(InvalidCatalogValueSnafu)?;
            let _ = res.insert(key.schema_name);
        }
        Ok(res.into_iter().collect())
    }

    async fn table_names(&self, catalog: &str, schema: &str) -> CatalogResult<Vec<String>> {
        let mut tables = self
            .table_metadata_manager
            .table_name_manager()
            .tables(catalog, schema)
            .await
            .context(TableMetadataManagerSnafu)?
            .into_iter()
            .map(|(k, _)| k)
            .collect::<Vec<String>>();
        if catalog == DEFAULT_CATALOG_NAME && schema == DEFAULT_SCHEMA_NAME {
            tables.push("numbers".to_string());
        }

        Ok(tables)
    }

    async fn catalog_exist(&self, catalog: &str) -> CatalogResult<bool> {
        let key = CatalogKey {
            catalog_name: catalog.to_string(),
        }
        .to_string();
        self.backend
            .get(key.as_bytes())
            .await
            .context(TableMetadataManagerSnafu)
            .map(|x| x.is_some())
    }

    async fn schema_exist(&self, catalog: &str, schema: &str) -> CatalogResult<bool> {
        let schema_key = SchemaKey {
            catalog_name: catalog.to_string(),
            schema_name: schema.to_string(),
        }
        .to_string();
        Ok(self
            .backend()
            .get(schema_key.as_bytes())
            .await
            .context(TableMetadataManagerSnafu)?
            .is_some())
    }

    async fn table_exist(&self, catalog: &str, schema: &str, table: &str) -> CatalogResult<bool> {
        let key = TableNameKey::new(catalog, schema, table);
        self.table_metadata_manager
            .table_name_manager()
            .get(key)
            .await
            .context(TableMetadataManagerSnafu)
            .map(|x| x.is_some())
    }

    async fn table(
        &self,
        catalog: &str,
        schema: &str,
        table_name: &str,
    ) -> CatalogResult<Option<TableRef>> {
        if catalog == DEFAULT_CATALOG_NAME
            && schema == DEFAULT_SCHEMA_NAME
            && table_name == "numbers"
        {
            return Ok(Some(Arc::new(NumbersTable::default())));
        }

        if schema == INFORMATION_SCHEMA_NAME {
            // hack: use existing cyclin reference to get Arc<Self>.
            // This can be remove by refactoring the struct into something like Arc<Inner>
            let manager = if let Some(instance) = self.dist_instance.as_ref() {
                instance.catalog_manager() as _
            } else {
                return Ok(None);
            };

            let provider =
                InformationSchemaProvider::new(catalog.to_string(), Arc::downgrade(&manager));
            return provider.table(table_name);
        }

        let key = TableNameKey::new(catalog, schema, table_name);
        let Some(table_name_value) = self
            .table_metadata_manager
            .table_name_manager()
            .get(key)
            .await
            .context(TableMetadataManagerSnafu)?
        else {
            return Ok(None);
        };
        let table_id = table_name_value.table_id();

        let Some(table_info_value) = self
            .table_metadata_manager
            .table_info_manager()
            .get(table_id)
            .await
            .context(TableMetadataManagerSnafu)?
        else {
            return Ok(None);
        };
        let table_info = Arc::new(
            table_info_value
                .table_info
                .try_into()
                .context(catalog_err::InvalidTableInfoInCatalogSnafu)?,
        );
        let table = Arc::new(DistTable::new(
            TableName::new(catalog, schema, table_name),
            table_info,
            Arc::new(self.clone()),
        ));
        Ok(Some(table))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
