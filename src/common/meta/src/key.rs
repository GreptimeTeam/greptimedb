// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! This mod defines all the keys used in the metadata store (Metasrv).
//! Specifically, there are these kinds of keys:
//!
//! 1. Datanode table key: `__dn_table/{datanode_id}/{table_id}`
//!     - The value is a [DatanodeTableValue] struct; it contains `table_id` and the regions that
//!       belong to this Datanode.
//!     - This key is primary used in the startup of Datanode, to let Datanode know which tables
//!       and regions it should open.
//!
//! 2. Table info key: `__table_info/{table_id}`
//!     - The value is a [TableInfoValue] struct; it contains the whole table info (like column
//!       schemas).
//!     - This key is mainly used in constructing the table in Datanode and Frontend.
//!
//! 3. Catalog name key: `__catalog_name/{catalog_name}`
//!     - Indices all catalog names
//!
//! 4. Schema name key: `__schema_name/{catalog_name}/{schema_name}`
//!     - Indices all schema names belong to the {catalog_name}
//!
//! 5. Table name key: `__table_name/{catalog_name}/{schema_name}/{table_name}`
//!     - The value is a [TableNameValue] struct; it contains the table id.
//!     - Used in the table name to table id lookup.
//!
//! 6. Table region key: `__table_region/{table_id}`
//!     - The value is a [TableRegionValue] struct; it contains the region distribution of the
//!       table in the Datanodes.
//!
//! All keys have related managers. The managers take care of the serialization and deserialization
//! of keys and values, and the interaction with the underlying KV store backend.
//!
//! To simplify the managers used in struct fields and function parameters, we define a "unify"
//! table metadata manager: [TableMetadataManager]. It contains all the managers defined above.
//! It's recommended to just use this manager only.

pub mod catalog_name;
pub mod datanode_table;
pub mod schema_name;
pub mod table_info;
pub mod table_name;
pub mod table_region;
// TODO(weny): removes it.
#[allow(unused)]
pub mod table_route;

use std::sync::Arc;

use datanode_table::{DatanodeTableKey, DatanodeTableManager, DatanodeTableValue};
use lazy_static::lazy_static;
use regex::Regex;
use snafu::{ensure, OptionExt, ResultExt};
use table::metadata::{RawTableInfo, TableId};
use table_info::{TableInfoKey, TableInfoManager, TableInfoValue};
use table_name::{TableNameKey, TableNameManager, TableNameValue};
use table_region::{TableRegionKey, TableRegionManager, TableRegionValue};

use self::catalog_name::{CatalogManager, CatalogNameValue};
use self::schema_name::{SchemaManager, SchemaNameValue};
use self::table_route::{TableRouteManager, TableRouteValue};
use crate::error::{self, InvalidTableMetadataSnafu, Result, SerdeJsonSnafu};
pub use crate::key::table_route::{TableRouteKey, TABLE_ROUTE_PREFIX};
use crate::kv_backend::txn::TxnRequest;
use crate::kv_backend::KvBackendRef;
use crate::rpc::router::{region_distribution, RegionRoute};
use crate::rpc::store::BatchGetRequest;

pub const REMOVED_PREFIX: &str = "__removed";

const NAME_PATTERN: &str = "[a-zA-Z_:-][a-zA-Z0-9_:-]*";

const DATANODE_TABLE_KEY_PREFIX: &str = "__dn_table";
const TABLE_INFO_KEY_PREFIX: &str = "__table_info";
const TABLE_NAME_KEY_PREFIX: &str = "__table_name";
const TABLE_REGION_KEY_PREFIX: &str = "__table_region";
const CATALOG_NAME_KEY_PREFIX: &str = "__catalog_name";
const SCHEMA_NAME_KEY_PREFIX: &str = "__schema_name";

lazy_static! {
    static ref DATANODE_TABLE_KEY_PATTERN: Regex =
        Regex::new(&format!("^{DATANODE_TABLE_KEY_PREFIX}/([0-9])/([0-9])$")).unwrap();
}

lazy_static! {
    static ref TABLE_NAME_KEY_PATTERN: Regex = Regex::new(&format!(
        "^{TABLE_NAME_KEY_PREFIX}/({NAME_PATTERN})/({NAME_PATTERN})/({NAME_PATTERN})$"
    ))
    .unwrap();
}

lazy_static! {
    /// CATALOG_NAME_KEY: {CATALOG_NAME_KEY_PREFIX}/{catalog_name}
    static ref CATALOG_NAME_KEY_PATTERN: Regex = Regex::new(&format!(
        "^{CATALOG_NAME_KEY_PREFIX}/({NAME_PATTERN})$"
    ))
    .unwrap();
}

lazy_static! {
    /// SCHEMA_NAME_KEY: {SCHEMA_NAME_KEY_PREFIX}/{catalog_name}/{schema_name}
    static ref SCHEMA_NAME_KEY_PATTERN:Regex=Regex::new(&format!(
        "^{SCHEMA_NAME_KEY_PREFIX}/({NAME_PATTERN})/({NAME_PATTERN})$"
    ))
    .unwrap();
}

pub fn to_removed_key(key: &str) -> String {
    format!("{REMOVED_PREFIX}-{key}")
}

pub trait TableMetaKey {
    fn as_raw_key(&self) -> Vec<u8>;
}

pub type TableMetadataManagerRef = Arc<TableMetadataManager>;

pub struct TableMetadataManager {
    table_name_manager: TableNameManager,
    table_info_manager: TableInfoManager,
    table_region_manager: TableRegionManager,
    datanode_table_manager: DatanodeTableManager,
    catalog_manager: CatalogManager,
    schema_manager: SchemaManager,
    table_route_manager: TableRouteManager,
    kv_backend: KvBackendRef,
}

macro_rules! ensure_values {
    ($got:expr, $expected_value:expr, $name:expr) => {
        ensure!(
            $got == $expected_value,
            error::UnexpectedSnafu {
                err_msg: format!(
                    "Reads the different table info: {:?} during {}, expected: {:?}",
                    $got, $name, $expected_value
                )
            }
        );
    };
}

impl TableMetadataManager {
    pub fn new(kv_backend: KvBackendRef) -> Self {
        TableMetadataManager {
            table_name_manager: TableNameManager::new(kv_backend.clone()),
            table_info_manager: TableInfoManager::new(kv_backend.clone()),
            table_region_manager: TableRegionManager::new(kv_backend.clone()),
            datanode_table_manager: DatanodeTableManager::new(kv_backend.clone()),
            catalog_manager: CatalogManager::new(kv_backend.clone()),
            schema_manager: SchemaManager::new(kv_backend.clone()),
            table_route_manager: TableRouteManager::new(kv_backend.clone()),
            kv_backend,
        }
    }

    pub fn table_name_manager(&self) -> &TableNameManager {
        &self.table_name_manager
    }

    pub fn table_info_manager(&self) -> &TableInfoManager {
        &self.table_info_manager
    }

    pub fn table_region_manager(&self) -> &TableRegionManager {
        &self.table_region_manager
    }

    pub fn datanode_table_manager(&self) -> &DatanodeTableManager {
        &self.datanode_table_manager
    }

    pub fn catalog_manager(&self) -> &CatalogManager {
        &self.catalog_manager
    }

    pub fn schema_manager(&self) -> &SchemaManager {
        &self.schema_manager
    }

    pub fn table_route_manager(&self) -> &TableRouteManager {
        &self.table_route_manager
    }

    /// Creates metadata for table and returns an error if different metadata exists.
    /// The caller MUST ensure it has the exclusive access to `TableNameKey`.
    pub async fn create_table_metadata(
        &self,
        table_info: RawTableInfo,
        region_routes: Vec<RegionRoute>,
    ) -> Result<()> {
        let mut txn = TxnRequest::default();
        let table_id = table_info.ident.table_id;

        // Creates table name.
        let table_name = TableNameKey::new(
            &table_info.catalog_name,
            &table_info.schema_name,
            &table_info.name,
        );
        self.table_name_manager()
            .build_create_txn(&mut txn, &table_name, table_id)?;

        // Creates table info.
        let table_info_value = TableInfoValue::new(table_info);
        self.table_info_manager()
            .build_create_txn(&mut txn, table_id, &table_info_value)?;

        // Creates datanode table key value pairs.
        let distribution = region_distribution(&region_routes)?;
        self.datanode_table_manager()
            .build_create_txn(&mut txn, table_id, distribution)?;

        // Creates table route.
        let table_route_value = TableRouteValue::new(region_routes);
        self.table_route_manager()
            .build_create_txn(&mut txn, table_id, &table_route_value)?;

        let r = self.kv_backend.txn(txn.into()).await?;

        // Checks whether metadata was already created.
        if !r.succeeded {
            let mut batch = BatchGetRequest::default();

            let decode_table_info_fn = self
                .table_info_manager()
                .build_batch_get(&mut batch, table_id);

            let decode_table_route_fn = self
                .table_route_manager()
                .build_batch_get(&mut batch, table_id);

            let r = self.kv_backend.batch_get(batch).await?;

            let remote_table_info =
                decode_table_info_fn(&r.kvs)?.context(error::UnexpectedSnafu {
                    err_msg: "Reads the empty table info during the create table metadata",
                })?;

            let remote_table_route =
                decode_table_route_fn(&r.kvs)?.context(error::UnexpectedSnafu {
                    err_msg: "Reads the empty table route during the create table metadata",
                })?;

            let op_name = "the creating table metadata";
            ensure_values!(remote_table_info, table_info_value, op_name);
            ensure_values!(remote_table_route, table_route_value, op_name);
        }

        Ok(())
    }

    /// Deletes metadata for table and returns an error if different metadata exists.
    /// The caller MUST ensure it has the exclusive access to `TableNameKey`.
    pub async fn delete_table_metadata(
        &self,
        table_info_value: TableInfoValue,
        region_routes: Vec<RegionRoute>,
    ) -> Result<()> {
        let mut txn = TxnRequest::default();
        let table_info = &table_info_value.table_info;
        let table_id = table_info.ident.table_id;

        // Deletes table name.
        let table_name = TableNameKey::new(
            &table_info.catalog_name,
            &table_info.schema_name,
            &table_info.name,
        );
        self.table_name_manager()
            .build_delete_txn(&mut txn, &table_name, table_id)?;

        // Deletes table info.
        self.table_info_manager()
            .build_delete_txn(&mut txn, table_id, &table_info_value)?;

        // Deletes datanode table key value pairs.
        let distribution = region_distribution(&region_routes)?;
        self.datanode_table_manager()
            .build_delete_txn(&mut txn, table_id, distribution)?;

        // Deletes table route.
        let table_route_value = TableRouteValue::new(region_routes);
        self.table_route_manager()
            .build_delete_txn(&mut txn, table_id, &table_route_value)?;

        let r = self.kv_backend.txn(txn.into()).await?;

        // Checks whether metadata was already deleted.
        if !r.succeeded {
            let mut batch = BatchGetRequest::default();

            let decode_table_info_fn = self
                .table_info_manager()
                .build_batch_get(&mut batch, table_id);

            let decode_table_route_fn = self
                .table_route_manager()
                .build_batch_get(&mut batch, table_id);

            let r = self.kv_backend.batch_get(batch).await?;

            let remote_table_info = decode_table_info_fn(&r.kvs)?;
            let remote_table_route = decode_table_route_fn(&r.kvs)?;

            // Already deleted
            if remote_table_info.is_none() && remote_table_route.is_none() {
                return Ok(());
            }

            return match (remote_table_info, remote_table_route) {
                (None, None) => Ok(()),
                (Some(remote_table_info), None) => {
                    error::UnexpectedSnafu {
                        err_msg: format!("Reads the different table info: {:?} during the deleting table metadata, expected: {:?}",
                           remote_table_info.table_info, table_info_value.table_info
                        )
                    }.fail()
                }
                (None, Some(remote_table_route)) => {
                    error::UnexpectedSnafu {
                        err_msg: format!("Reads the different table route: {:?} during the deleting table metadata, expected: {:?}",
                            remote_table_route.region_routes, table_route_value.region_routes
                        )
                    }.fail()
                }
                (Some(remote_table_info), Some(remote_table_route)) => {
                    error::UnexpectedSnafu {
                        err_msg: format!("Reads the both different table info and table route: {:?},{:?} during the deleting table metadata, expected: {:?},{:?}",
                            remote_table_info.table_info, remote_table_route.region_routes,
                            table_info_value.table_info, table_route_value.region_routes
                        )
                    }.fail()
                }
            };
        }

        Ok(())
    }

    /// Renames the table name and returns an error if different metadata exists.
    /// The caller MUST ensure it has the exclusive access to old and new `TableNameKey`s,
    /// and the new `TableNameKey` MUST be empty.
    pub async fn rename_table(
        &self,
        current_table_info_value: TableInfoValue,
        new_table_name: String,
    ) -> Result<()> {
        let current_table_info = &current_table_info_value.table_info;
        let table_id = current_table_info.ident.table_id;

        let mut txn = TxnRequest::default();

        let table_name_key = TableNameKey::new(
            &current_table_info.catalog_name,
            &current_table_info.schema_name,
            &current_table_info.name,
        );

        let new_table_name_key = TableNameKey::new(
            &current_table_info.catalog_name,
            &current_table_info.schema_name,
            &new_table_name,
        );

        // Updates table name.
        self.table_name_manager().build_update_txn(
            &mut txn,
            &table_name_key,
            &new_table_name_key,
            table_id,
        )?;

        let mut new_table_info_value = current_table_info_value.clone();
        new_table_info_value.table_info.name = new_table_name;

        // Updates table info.
        self.table_info_manager().build_update_txn(
            &mut txn,
            table_id,
            &current_table_info_value,
            &new_table_info_value,
        )?;

        let r = self.kv_backend.txn(txn.into()).await?;

        // Checks whether metadata was already updated.
        if !r.succeeded {
            let mut batch = BatchGetRequest::default();
            let decode_table_info_fn = self
                .table_info_manager()
                .build_batch_get(&mut batch, table_id);
            let r = self.kv_backend.batch_get(batch).await?;
            let remote_table_info =
                decode_table_info_fn(&r.kvs)?.context(error::UnexpectedSnafu {
                    err_msg: "Reads the empty table info during the rename table metadata",
                })?;

            let op_name = "the renaming table metadata";
            ensure_values!(remote_table_info, new_table_info_value, op_name);
        }

        Ok(())
    }

    /// Updates table info and returns an error if different metadata exists.
    pub async fn update_table_info(
        &self,
        current_table_info_value: TableInfoValue,
        new_table_info: RawTableInfo,
    ) -> Result<()> {
        let mut txn = TxnRequest::default();
        let table_id = current_table_info_value.table_info.ident.table_id;

        let new_table_info_value = current_table_info_value.update(new_table_info);

        // Updates table info.
        self.table_info_manager().build_update_txn(
            &mut txn,
            table_id,
            &current_table_info_value,
            &new_table_info_value,
        )?;
        let r = self.kv_backend.txn(txn.into()).await?;

        // Checks whether metadata was already updated.
        if !r.succeeded {
            let mut batch = BatchGetRequest::default();
            let decode_table_info_fn = self
                .table_info_manager()
                .build_batch_get(&mut batch, table_id);
            let r = self.kv_backend.batch_get(batch).await?;
            let remote_table_info =
                decode_table_info_fn(&r.kvs)?.context(error::UnexpectedSnafu {
                    err_msg: "Reads the empty table info during the updating table info",
                })?;

            let op_name = "the updating table info";
            ensure_values!(remote_table_info, new_table_info_value, op_name);
        }
        Ok(())
    }

    pub async fn update_table_route(
        &self,
        table_id: TableId,
        current_table_route_value: TableRouteValue,
        new_region_routes: Vec<RegionRoute>,
    ) -> Result<()> {
        let mut txn = TxnRequest::default();

        // Updates the datanode table key value pairs.
        let current_region_distribution =
            region_distribution(&current_table_route_value.region_routes)?;
        let new_region_distribution = region_distribution(&new_region_routes)?;

        self.datanode_table_manager().build_update_txn(
            &mut txn,
            table_id,
            current_region_distribution,
            new_region_distribution,
        )?;

        // Updates the table_route.
        let new_table_route_value = current_table_route_value.update(new_region_routes);

        self.table_route_manager().build_update_txn(
            &mut txn,
            table_id,
            &current_table_route_value,
            &new_table_route_value,
        )?;

        let r = self.kv_backend.txn(txn.into()).await?;

        // Checks whether metadata was already updated.
        if !r.succeeded {
            let mut batch = BatchGetRequest::default();

            let decode_table_route_fn = self
                .table_route_manager()
                .build_batch_get(&mut batch, table_id);

            let r = self.kv_backend.batch_get(batch).await?;

            let remote_table_route =
                decode_table_route_fn(&r.kvs)?.context(error::UnexpectedSnafu {
                    err_msg: "Reads the empty table route during the updating table route",
                })?;

            let op_name = "the updating table route";
            ensure_values!(remote_table_route, new_table_route_value, op_name);
        }

        Ok(())
    }
}

macro_rules! impl_table_meta_key {
    ($($val_ty: ty), *) => {
        $(
            impl std::fmt::Display for $val_ty {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(f, "{}", String::from_utf8_lossy(&self.as_raw_key()))
                }
            }
        )*
    }
}

impl_table_meta_key!(
    TableNameKey<'_>,
    TableInfoKey,
    TableRegionKey,
    DatanodeTableKey
);

macro_rules! impl_table_meta_value {
    ($($val_ty: ty), *) => {
        $(
            impl $val_ty {
                pub fn try_from_raw_value_ref(raw_value: &[u8]) -> Result<Self> {
                    let raw_value = std::str::from_utf8(raw_value).map_err(|e| {
                        InvalidTableMetadataSnafu { err_msg: e.to_string() }.build()
                    })?;
                    serde_json::from_str(raw_value).context(SerdeJsonSnafu)
                }

                pub fn try_from_raw_value(raw_value: Vec<u8>) -> Result<Self> {
                    let raw_value = String::from_utf8(raw_value).map_err(|e| {
                        InvalidTableMetadataSnafu { err_msg: e.to_string() }.build()
                    })?;
                    serde_json::from_str(&raw_value).context(SerdeJsonSnafu)
                }

                pub fn try_as_raw_value(&self) -> Result<Vec<u8>> {
                    serde_json::to_vec(self).context(SerdeJsonSnafu)
                }
            }
        )*
    }
}

impl_table_meta_value! {
    CatalogNameValue,
    SchemaNameValue,
    TableNameValue,
    TableInfoValue,
    TableRegionValue,
    DatanodeTableValue,
    TableRouteValue
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, SchemaBuilder};
    use table::metadata::{RawTableInfo, TableInfo, TableInfoBuilder, TableMetaBuilder};

    use crate::key::table_info::TableInfoValue;
    use crate::key::table_route::TableRouteValue;
    use crate::key::{to_removed_key, TableMetadataManager};
    use crate::kv_backend::memory::MemoryKvBackend;
    use crate::peer::Peer;
    use crate::rpc::router::{Region, RegionRoute};

    #[test]
    fn test_to_removed_key() {
        let key = "test_key";
        let removed = "__removed-test_key";
        assert_eq!(removed, to_removed_key(key));
    }

    fn new_test_table_info() -> TableInfo {
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

        let meta = TableMetaBuilder::default()
            .schema(Arc::new(schema))
            .primary_key_indices(vec![0])
            .engine("engine")
            .next_column_id(3)
            .build()
            .unwrap();
        TableInfoBuilder::default()
            .table_id(10)
            .table_version(5)
            .name("mytable")
            .meta(meta)
            .build()
            .unwrap()
    }

    fn new_test_region_route() -> RegionRoute {
        RegionRoute {
            region: Region {
                id: 1.into(),
                name: "r1".to_string(),
                partition: None,
                attrs: BTreeMap::new(),
            },
            leader_peer: Some(Peer::new(2, "a2")),
            follower_peers: vec![Peer::new(1, "a1"), Peer::new(3, "a3")],
        }
    }

    #[tokio::test]
    async fn test_create_table_metadata() {
        let mem_kv = Arc::new(MemoryKvBackend::default());

        let table_metadata_manager = TableMetadataManager::new(mem_kv);

        let table_info: RawTableInfo = new_test_table_info().into();

        let region_route = new_test_region_route();
        let region_routes = vec![region_route.clone()];

        // creates metadata.
        table_metadata_manager
            .create_table_metadata(table_info.clone(), region_routes.clone())
            .await
            .unwrap();

        // if metadata was already created, it should be ok.
        table_metadata_manager
            .create_table_metadata(table_info.clone(), region_routes.clone())
            .await
            .unwrap();

        let mut modified_region_routes = region_routes.clone();
        modified_region_routes.push(region_route);

        // if remote metadata was exists, it should return an error.
        assert!(table_metadata_manager
            .create_table_metadata(table_info, modified_region_routes)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_delete_table_metadata() {
        let mem_kv = Arc::new(MemoryKvBackend::default());

        let table_metadata_manager = TableMetadataManager::new(mem_kv);

        let table_info: RawTableInfo = new_test_table_info().into();

        let region_route = new_test_region_route();
        let region_routes = vec![region_route.clone()];

        // creates metadata.
        table_metadata_manager
            .create_table_metadata(table_info.clone(), region_routes.clone())
            .await
            .unwrap();
        let table_info_value = TableInfoValue::new(table_info);

        // deletes modified metadata, it should return an error.
        let mut modified_region_routes = region_routes.clone();
        modified_region_routes.push(region_route);
        assert!(table_metadata_manager
            .delete_table_metadata(table_info_value.clone(), modified_region_routes)
            .await
            .is_err());

        // deletes metadata.
        table_metadata_manager
            .delete_table_metadata(table_info_value.clone(), region_routes.clone())
            .await
            .unwrap();

        // if metadata was already deleted, it should be ok.
        table_metadata_manager
            .delete_table_metadata(table_info_value.clone(), region_routes.clone())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_rename_table() {
        let mem_kv = Arc::new(MemoryKvBackend::default());

        let table_metadata_manager = TableMetadataManager::new(mem_kv);

        let table_info: RawTableInfo = new_test_table_info().into();

        let region_route = new_test_region_route();
        let region_routes = vec![region_route.clone()];

        // creates metadata.
        table_metadata_manager
            .create_table_metadata(table_info.clone(), region_routes.clone())
            .await
            .unwrap();
        let new_table_name = "another_name".to_string();

        let table_info_value = TableInfoValue::new(table_info.clone());

        table_metadata_manager
            .rename_table(table_info_value.clone(), new_table_name.clone())
            .await
            .unwrap();

        // if remote metadata was updated, it should be ok.
        table_metadata_manager
            .rename_table(table_info_value.clone(), new_table_name.clone())
            .await
            .unwrap();

        let mut modified_table_info = table_info;
        modified_table_info.name = "hi".to_string();
        let modified_table_info_value = table_info_value.update(modified_table_info);

        // if the table_info_value is wrong, it should return an error.
        // The ABA problem.
        assert!(table_metadata_manager
            .rename_table(modified_table_info_value.clone(), new_table_name.clone())
            .await
            .is_err())
    }

    #[tokio::test]
    async fn test_update_table_info() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let table_metadata_manager = TableMetadataManager::new(mem_kv);
        let table_info: RawTableInfo = new_test_table_info().into();
        let region_route = new_test_region_route();
        let region_routes = vec![region_route.clone()];

        // creates metadata.
        table_metadata_manager
            .create_table_metadata(table_info.clone(), region_routes.clone())
            .await
            .unwrap();
        let mut new_table_info = table_info.clone();
        new_table_info.name = "hi".to_string();

        let current_table_info_value = TableInfoValue::new(table_info.clone());

        // should be ok.
        table_metadata_manager
            .update_table_info(current_table_info_value.clone(), new_table_info.clone())
            .await
            .unwrap();

        // if table info was updated, it should be ok.
        table_metadata_manager
            .update_table_info(current_table_info_value.clone(), new_table_info.clone())
            .await
            .unwrap();

        let mut wrong_table_info = table_info.clone();
        wrong_table_info.name = "wrong".to_string();
        let wrong_table_info_value = current_table_info_value.update(wrong_table_info);

        // if the current_table_info_value is wrong, it should return an error.
        // The ABA problem.
        assert!(table_metadata_manager
            .update_table_info(wrong_table_info_value, new_table_info)
            .await
            .is_err())
    }

    #[tokio::test]
    async fn test_update_table_route() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let table_metadata_manager = TableMetadataManager::new(mem_kv);
        let table_info: RawTableInfo = new_test_table_info().into();
        let region_route = new_test_region_route();
        let region_routes = vec![region_route.clone()];
        let table_id = table_info.ident.table_id;
        let current_table_route_value = TableRouteValue::new(region_routes.clone());

        // creates metadata.
        table_metadata_manager
            .create_table_metadata(table_info.clone(), region_routes.clone())
            .await
            .unwrap();

        let new_region_routes = vec![region_route.clone(), region_route.clone()];

        // it should be ok.
        table_metadata_manager
            .update_table_route(
                table_id,
                current_table_route_value.clone(),
                new_region_routes.clone(),
            )
            .await
            .unwrap();

        // if the table route was updated. it should be ok.
        table_metadata_manager
            .update_table_route(
                table_id,
                current_table_route_value.clone(),
                new_region_routes.clone(),
            )
            .await
            .unwrap();

        // if the current_table_route_value is wrong, it should return an error.
        // The ABA problem.
        let wrong_table_route_value = current_table_route_value.update(vec![
            region_route.clone(),
            region_route.clone(),
            region_route.clone(),
        ]);

        assert!(table_metadata_manager
            .update_table_route(table_id, wrong_table_route_value, new_region_routes)
            .await
            .is_err());
    }
}
