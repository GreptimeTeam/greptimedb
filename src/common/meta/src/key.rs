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
// TODO(weny): removes it.
#[allow(deprecated)]
pub mod table_region;
// TODO(weny): removes it.
#[allow(deprecated)]
pub mod table_route;
#[cfg(any(test, feature = "testing"))]
pub mod test_utils;

use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::Arc;

use bytes::Bytes;
use common_catalog::consts::{
    DEFAULT_CATALOG_NAME, DEFAULT_PRIVATE_SCHEMA_NAME, DEFAULT_SCHEMA_NAME, INFORMATION_SCHEMA_NAME,
};
use common_telemetry::warn;
use datanode_table::{DatanodeTableKey, DatanodeTableManager, DatanodeTableValue};
use lazy_static::lazy_static;
use regex::Regex;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};
use store_api::storage::RegionNumber;
use table::metadata::{RawTableInfo, TableId};
use table_info::{TableInfoKey, TableInfoManager, TableInfoValue};
use table_name::{TableNameKey, TableNameManager, TableNameValue};

use self::catalog_name::{CatalogManager, CatalogNameKey, CatalogNameValue};
use self::datanode_table::RegionInfo;
use self::schema_name::{SchemaManager, SchemaNameKey, SchemaNameValue};
use self::table_route::{TableRouteManager, TableRouteValue};
use crate::ddl::utils::region_storage_path;
use crate::error::{self, Result, SerdeJsonSnafu};
use crate::kv_backend::txn::Txn;
use crate::kv_backend::KvBackendRef;
use crate::rpc::router::{region_distribution, RegionRoute, RegionStatus};
use crate::DatanodeId;

pub const REMOVED_PREFIX: &str = "__removed";

pub const NAME_PATTERN: &str = r"[a-zA-Z_:-][a-zA-Z0-9_:\-\.]*";

const DATANODE_TABLE_KEY_PREFIX: &str = "__dn_table";
const TABLE_REGION_KEY_PREFIX: &str = "__table_region";

pub const TABLE_INFO_KEY_PREFIX: &str = "__table_info";
pub const TABLE_NAME_KEY_PREFIX: &str = "__table_name";
pub const CATALOG_NAME_KEY_PREFIX: &str = "__catalog_name";
pub const SCHEMA_NAME_KEY_PREFIX: &str = "__schema_name";
pub const TABLE_ROUTE_PREFIX: &str = "__table_route";

pub const CACHE_KEY_PREFIXES: [&str; 4] = [
    TABLE_NAME_KEY_PREFIX,
    CATALOG_NAME_KEY_PREFIX,
    SCHEMA_NAME_KEY_PREFIX,
    TABLE_ROUTE_PREFIX,
];

pub type RegionDistribution = BTreeMap<DatanodeId, Vec<RegionNumber>>;

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

pub trait TableMetaValue {
    fn try_from_raw_value(raw_value: &[u8]) -> Result<Self>
    where
        Self: Sized;

    fn try_as_raw_value(&self) -> Result<Vec<u8>>;
}

pub type TableMetadataManagerRef = Arc<TableMetadataManager>;

pub struct TableMetadataManager {
    table_name_manager: TableNameManager,
    table_info_manager: TableInfoManager,
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
                    "Reads the different value: {:?} during {}, expected: {:?}",
                    $got, $name, $expected_value
                )
            }
        );
    };
}

/// A struct containing a deserialized value(`inner`) and an original bytes.
///
/// - Serialize behaviors:
///
/// The `inner` field will be ignored.
///
/// - Deserialize behaviors:
///
/// The `inner` field will be deserialized from the `bytes` field.
pub struct DeserializedValueWithBytes<T: DeserializeOwned + Serialize> {
    // The original bytes of the inner.
    bytes: Bytes,
    // The value was deserialized from the original bytes.
    inner: T,
}

impl<T: DeserializeOwned + Serialize> Deref for DeserializedValueWithBytes<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T: DeserializeOwned + Serialize + Debug> Debug for DeserializedValueWithBytes<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DeserializedValueWithBytes(inner: {:?}, bytes: {:?})",
            self.inner, self.bytes
        )
    }
}

impl<T: DeserializeOwned + Serialize> Serialize for DeserializedValueWithBytes<T> {
    /// - Serialize behaviors:
    ///
    /// The `inner` field will be ignored.
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // Safety: The original bytes are always JSON encoded.
        // It's more efficiently than `serialize_bytes`.
        serializer.serialize_str(&String::from_utf8_lossy(&self.bytes))
    }
}

impl<'de, T: DeserializeOwned + Serialize + TableMetaValue> Deserialize<'de>
    for DeserializedValueWithBytes<T>
{
    /// - Deserialize behaviors:
    ///
    /// The `inner` field will be deserialized from the `bytes` field.
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let buf = String::deserialize(deserializer)?;
        let bytes = Bytes::from(buf);

        let value = DeserializedValueWithBytes::from_inner_bytes(bytes)
            .map_err(|err| serde::de::Error::custom(err.to_string()))?;

        Ok(value)
    }
}

impl<T: Serialize + DeserializeOwned + Clone> Clone for DeserializedValueWithBytes<T> {
    fn clone(&self) -> Self {
        Self {
            bytes: self.bytes.clone(),
            inner: self.inner.clone(),
        }
    }
}

impl<T: Serialize + DeserializeOwned + TableMetaValue> DeserializedValueWithBytes<T> {
    /// Returns a struct containing a deserialized value and an original `bytes`.
    /// It accepts original bytes of inner.
    pub fn from_inner_bytes(bytes: Bytes) -> Result<Self> {
        let inner = T::try_from_raw_value(&bytes)?;
        Ok(Self { bytes, inner })
    }

    /// Returns a struct containing a deserialized value and an original `bytes`.
    /// It accepts original bytes of inner.
    pub fn from_inner_slice(bytes: &[u8]) -> Result<Self> {
        Self::from_inner_bytes(Bytes::copy_from_slice(bytes))
    }

    pub fn into_inner(self) -> T {
        self.inner
    }

    /// Returns original `bytes`
    pub fn into_bytes(&self) -> Vec<u8> {
        self.bytes.to_vec()
    }

    /// Notes: used for test purpose.
    pub fn from_inner(inner: T) -> Self {
        let bytes = serde_json::to_vec(&inner).unwrap();

        Self {
            bytes: Bytes::from(bytes),
            inner,
        }
    }
}

impl TableMetadataManager {
    pub fn new(kv_backend: KvBackendRef) -> Self {
        TableMetadataManager {
            table_name_manager: TableNameManager::new(kv_backend.clone()),
            table_info_manager: TableInfoManager::new(kv_backend.clone()),
            datanode_table_manager: DatanodeTableManager::new(kv_backend.clone()),
            catalog_manager: CatalogManager::new(kv_backend.clone()),
            schema_manager: SchemaManager::new(kv_backend.clone()),
            table_route_manager: TableRouteManager::new(kv_backend.clone()),
            kv_backend,
        }
    }

    pub async fn init(&self) -> Result<()> {
        let catalog_name = CatalogNameKey::new(DEFAULT_CATALOG_NAME);

        self.catalog_manager().create(catalog_name, true).await?;

        let internal_schemas = [
            DEFAULT_SCHEMA_NAME,
            INFORMATION_SCHEMA_NAME,
            DEFAULT_PRIVATE_SCHEMA_NAME,
        ];

        for schema_name in internal_schemas {
            let schema_key = SchemaNameKey::new(DEFAULT_CATALOG_NAME, schema_name);

            self.schema_manager().create(schema_key, None, true).await?;
        }

        Ok(())
    }

    pub fn table_name_manager(&self) -> &TableNameManager {
        &self.table_name_manager
    }

    pub fn table_info_manager(&self) -> &TableInfoManager {
        &self.table_info_manager
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

    #[cfg(feature = "testing")]
    pub fn kv_backend(&self) -> &KvBackendRef {
        &self.kv_backend
    }

    pub async fn get_full_table_info(
        &self,
        table_id: TableId,
    ) -> Result<(
        Option<DeserializedValueWithBytes<TableInfoValue>>,
        Option<DeserializedValueWithBytes<TableRouteValue>>,
    )> {
        let (get_table_route_txn, table_route_decoder) =
            self.table_route_manager.build_get_txn(table_id);

        let (get_table_info_txn, table_info_decoder) =
            self.table_info_manager.build_get_txn(table_id);

        let txn = Txn::merge_all(vec![get_table_route_txn, get_table_info_txn]);

        let r = self.kv_backend.txn(txn).await?;

        let table_info_value = table_info_decoder(&r.responses)?;

        let table_route_value = table_route_decoder(&r.responses)?;

        Ok((table_info_value, table_route_value))
    }

    /// Creates metadata for table and returns an error if different metadata exists.
    /// The caller MUST ensure it has the exclusive access to `TableNameKey`.
    pub async fn create_table_metadata(
        &self,
        mut table_info: RawTableInfo,
        table_route_value: TableRouteValue,
        region_wal_options: HashMap<RegionNumber, String>,
    ) -> Result<()> {
        let region_numbers = table_route_value.region_numbers();
        table_info.meta.region_numbers = region_numbers;
        let table_id = table_info.ident.table_id;
        let engine = table_info.meta.engine.clone();
        let region_storage_path =
            region_storage_path(&table_info.catalog_name, &table_info.schema_name);

        // Creates table name.
        let table_name = TableNameKey::new(
            &table_info.catalog_name,
            &table_info.schema_name,
            &table_info.name,
        );
        let create_table_name_txn = self
            .table_name_manager()
            .build_create_txn(&table_name, table_id)?;

        let region_options = (&table_info.meta.options).into();
        // Creates table info.
        let table_info_value = TableInfoValue::new(table_info);
        let (create_table_info_txn, on_create_table_info_failure) = self
            .table_info_manager()
            .build_create_txn(table_id, &table_info_value)?;

        let (create_table_route_txn, on_create_table_route_failure) = self
            .table_route_manager()
            .build_create_txn(table_id, &table_route_value)?;

        let mut txn = Txn::merge_all(vec![
            create_table_name_txn,
            create_table_info_txn,
            create_table_route_txn,
        ]);

        if let TableRouteValue::Physical(x) = &table_route_value {
            let create_datanode_table_txn = self.datanode_table_manager().build_create_txn(
                table_id,
                &engine,
                &region_storage_path,
                region_options,
                region_wal_options,
                region_distribution(&x.region_routes),
            )?;
            txn = txn.merge(create_datanode_table_txn);
        }

        let r = self.kv_backend.txn(txn).await?;

        // Checks whether metadata was already created.
        if !r.succeeded {
            let remote_table_info = on_create_table_info_failure(&r.responses)?
                .context(error::UnexpectedSnafu {
                    err_msg: "Reads the empty table info during the create table metadata",
                })?
                .into_inner();

            let remote_table_route = on_create_table_route_failure(&r.responses)?
                .context(error::UnexpectedSnafu {
                    err_msg: "Reads the empty table route during the create table metadata",
                })?
                .into_inner();

            let op_name = "the creating table metadata";
            ensure_values!(remote_table_info, table_info_value, op_name);
            ensure_values!(remote_table_route, table_route_value, op_name);
        }

        Ok(())
    }

    /// Deletes metadata for table.
    /// The caller MUST ensure it has the exclusive access to `TableNameKey`.
    pub async fn delete_table_metadata(
        &self,
        table_info_value: &DeserializedValueWithBytes<TableInfoValue>,
        table_route_value: &DeserializedValueWithBytes<TableRouteValue>,
    ) -> Result<()> {
        let table_info = &table_info_value.table_info;
        let table_id = table_info.ident.table_id;

        // Deletes table name.
        let table_name = TableNameKey::new(
            &table_info.catalog_name,
            &table_info.schema_name,
            &table_info.name,
        );

        let delete_table_name_txn = self
            .table_name_manager()
            .build_delete_txn(&table_name, table_id)?;

        // Deletes table info.
        let delete_table_info_txn = self
            .table_info_manager()
            .build_delete_txn(table_id, table_info_value)?;

        // Deletes datanode table key value pairs.
        let distribution = region_distribution(table_route_value.region_routes()?);
        let delete_datanode_txn = self
            .datanode_table_manager()
            .build_delete_txn(table_id, distribution)?;

        // Deletes table route.
        let delete_table_route_txn = self
            .table_route_manager()
            .build_delete_txn(table_id, table_route_value)?;

        let txn = Txn::merge_all(vec![
            delete_table_name_txn,
            delete_table_info_txn,
            delete_datanode_txn,
            delete_table_route_txn,
        ]);

        // It's always successes.
        let _ = self.kv_backend.txn(txn).await?;

        Ok(())
    }

    /// Renames the table name and returns an error if different metadata exists.
    /// The caller MUST ensure it has the exclusive access to old and new `TableNameKey`s,
    /// and the new `TableNameKey` MUST be empty.
    pub async fn rename_table(
        &self,
        current_table_info_value: DeserializedValueWithBytes<TableInfoValue>,
        new_table_name: String,
    ) -> Result<()> {
        let current_table_info = &current_table_info_value.table_info;
        let table_id = current_table_info.ident.table_id;

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
        let update_table_name_txn = self.table_name_manager().build_update_txn(
            &table_name_key,
            &new_table_name_key,
            table_id,
        )?;

        let new_table_info_value = current_table_info_value
            .inner
            .with_update(move |table_info| {
                table_info.name = new_table_name;
            });

        // Updates table info.
        let (update_table_info_txn, on_update_table_info_failure) = self
            .table_info_manager()
            .build_update_txn(table_id, &current_table_info_value, &new_table_info_value)?;

        let txn = Txn::merge_all(vec![update_table_name_txn, update_table_info_txn]);

        let r = self.kv_backend.txn(txn).await?;

        // Checks whether metadata was already updated.
        if !r.succeeded {
            let remote_table_info = on_update_table_info_failure(&r.responses)?
                .context(error::UnexpectedSnafu {
                    err_msg: "Reads the empty table info during the rename table metadata",
                })?
                .into_inner();

            let op_name = "the renaming table metadata";
            ensure_values!(remote_table_info, new_table_info_value, op_name);
        }

        Ok(())
    }

    /// Updates table info and returns an error if different metadata exists.
    pub async fn update_table_info(
        &self,
        current_table_info_value: DeserializedValueWithBytes<TableInfoValue>,
        new_table_info: RawTableInfo,
    ) -> Result<()> {
        let table_id = current_table_info_value.table_info.ident.table_id;

        let new_table_info_value = current_table_info_value.update(new_table_info);

        // Updates table info.
        let (update_table_info_txn, on_update_table_info_failure) = self
            .table_info_manager()
            .build_update_txn(table_id, &current_table_info_value, &new_table_info_value)?;

        let r = self.kv_backend.txn(update_table_info_txn).await?;

        // Checks whether metadata was already updated.
        if !r.succeeded {
            let remote_table_info = on_update_table_info_failure(&r.responses)?
                .context(error::UnexpectedSnafu {
                    err_msg: "Reads the empty table info during the updating table info",
                })?
                .into_inner();

            let op_name = "the updating table info";
            ensure_values!(remote_table_info, new_table_info_value, op_name);
        }
        Ok(())
    }

    pub async fn update_table_route(
        &self,
        table_id: TableId,
        region_info: RegionInfo,
        current_table_route_value: &DeserializedValueWithBytes<TableRouteValue>,
        new_region_routes: Vec<RegionRoute>,
        new_region_options: &HashMap<String, String>,
        new_region_wal_options: &HashMap<RegionNumber, String>,
    ) -> Result<()> {
        // Updates the datanode table key value pairs.
        let current_region_distribution =
            region_distribution(current_table_route_value.region_routes()?);
        let new_region_distribution = region_distribution(&new_region_routes);

        let update_datanode_table_txn = self.datanode_table_manager().build_update_txn(
            table_id,
            region_info,
            current_region_distribution,
            new_region_distribution,
            new_region_options,
            new_region_wal_options,
        )?;

        // Updates the table_route.
        let new_table_route_value = current_table_route_value.update(new_region_routes)?;

        let (update_table_route_txn, on_update_table_route_failure) = self
            .table_route_manager()
            .build_update_txn(table_id, current_table_route_value, &new_table_route_value)?;

        let txn = Txn::merge_all(vec![update_datanode_table_txn, update_table_route_txn]);

        let r = self.kv_backend.txn(txn).await?;

        // Checks whether metadata was already updated.
        if !r.succeeded {
            let remote_table_route = on_update_table_route_failure(&r.responses)?
                .context(error::UnexpectedSnafu {
                    err_msg: "Reads the empty table route during the updating table route",
                })?
                .into_inner();

            let op_name = "the updating table route";
            ensure_values!(remote_table_route, new_table_route_value, op_name);
        }

        Ok(())
    }

    /// Updates the leader status of the [RegionRoute].
    pub async fn update_leader_region_status<F>(
        &self,
        table_id: TableId,
        current_table_route_value: &DeserializedValueWithBytes<TableRouteValue>,
        next_region_route_status: F,
    ) -> Result<()>
    where
        F: Fn(&RegionRoute) -> Option<Option<RegionStatus>>,
    {
        let mut new_region_routes = current_table_route_value.region_routes()?.clone();

        let mut updated = 0;
        for route in &mut new_region_routes {
            if let Some(status) = next_region_route_status(route) {
                if route.set_leader_status(status) {
                    updated += 1;
                }
            }
        }

        if updated == 0 {
            warn!("No leader status updated");
            return Ok(());
        }

        // Updates the table_route.
        let new_table_route_value = current_table_route_value.update(new_region_routes)?;

        let (update_table_route_txn, on_update_table_route_failure) = self
            .table_route_manager()
            .build_update_txn(table_id, current_table_route_value, &new_table_route_value)?;

        let r = self.kv_backend.txn(update_table_route_txn).await?;

        // Checks whether metadata was already updated.
        if !r.succeeded {
            let remote_table_route = on_update_table_route_failure(&r.responses)?
                .context(error::UnexpectedSnafu {
                    err_msg: "Reads the empty table route during the updating leader region status",
                })?
                .into_inner();

            let op_name = "the updating leader region status";
            ensure_values!(remote_table_route, new_table_route_value, op_name);
        }

        Ok(())
    }
}

#[macro_export]
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

impl_table_meta_key!(TableNameKey<'_>, TableInfoKey, DatanodeTableKey);

#[macro_export]
macro_rules! impl_table_meta_value {
    ($($val_ty: ty), *) => {
        $(
            impl $crate::key::TableMetaValue for $val_ty {
                fn try_from_raw_value(raw_value: &[u8]) -> Result<Self> {
                    serde_json::from_slice(raw_value).context(SerdeJsonSnafu)
                }

                fn try_as_raw_value(&self) -> Result<Vec<u8>> {
                    serde_json::to_vec(self).context(SerdeJsonSnafu)
                }
            }
        )*
    }
}

#[macro_export]
macro_rules! impl_optional_meta_value {
    ($($val_ty: ty), *) => {
        $(
            impl $val_ty {
                pub fn try_from_raw_value(raw_value: &[u8]) -> Result<Option<Self>> {
                    serde_json::from_slice(raw_value).context(SerdeJsonSnafu)
                }

                pub fn try_as_raw_value(&self) -> Result<Vec<u8>> {
                    serde_json::to_vec(self).context(SerdeJsonSnafu)
                }
            }
        )*
    }
}

impl_table_meta_value! {
    TableNameValue,
    TableInfoValue,
    DatanodeTableValue
}

impl_optional_meta_value! {
    CatalogNameValue,
    SchemaNameValue
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Arc;

    use bytes::Bytes;
    use futures::TryStreamExt;
    use table::metadata::{RawTableInfo, TableInfo};

    use super::datanode_table::DatanodeTableKey;
    use super::test_utils;
    use crate::ddl::utils::region_storage_path;
    use crate::error::Result;
    use crate::key::datanode_table::RegionInfo;
    use crate::key::table_info::TableInfoValue;
    use crate::key::table_name::TableNameKey;
    use crate::key::table_route::TableRouteValue;
    use crate::key::{to_removed_key, DeserializedValueWithBytes, TableMetadataManager};
    use crate::kv_backend::memory::MemoryKvBackend;
    use crate::peer::Peer;
    use crate::rpc::router::{region_distribution, Region, RegionRoute, RegionStatus};

    #[test]
    fn test_deserialized_value_with_bytes() {
        let region_route = new_test_region_route();
        let region_routes = vec![region_route.clone()];

        let expected_region_routes =
            TableRouteValue::physical(vec![region_route.clone(), region_route.clone()]);
        let expected = serde_json::to_vec(&expected_region_routes).unwrap();

        // Serialize behaviors:
        // The inner field will be ignored.
        let value = DeserializedValueWithBytes {
            // ignored
            inner: TableRouteValue::physical(region_routes.clone()),
            bytes: Bytes::from(expected.clone()),
        };

        let encoded = serde_json::to_vec(&value).unwrap();

        // Deserialize behaviors:
        // The inner field will be deserialized from the bytes field.
        let decoded: DeserializedValueWithBytes<TableRouteValue> =
            serde_json::from_slice(&encoded).unwrap();

        assert_eq!(decoded.inner, expected_region_routes);
        assert_eq!(decoded.bytes, expected);
    }

    #[test]
    fn test_to_removed_key() {
        let key = "test_key";
        let removed = "__removed-test_key";
        assert_eq!(removed, to_removed_key(key));
    }

    fn new_test_region_route() -> RegionRoute {
        new_region_route(1, 2)
    }

    fn new_region_route(region_id: u64, datanode: u64) -> RegionRoute {
        RegionRoute {
            region: Region {
                id: region_id.into(),
                name: "r1".to_string(),
                partition: None,
                attrs: BTreeMap::new(),
            },
            leader_peer: Some(Peer::new(datanode, "a2")),
            follower_peers: vec![],
            leader_status: None,
        }
    }

    fn new_test_table_info(region_numbers: impl Iterator<Item = u32>) -> TableInfo {
        test_utils::new_test_table_info(10, region_numbers)
    }

    async fn create_physical_table_metadata(
        table_metadata_manager: &TableMetadataManager,
        table_info: RawTableInfo,
        region_routes: Vec<RegionRoute>,
    ) -> Result<()> {
        table_metadata_manager
            .create_table_metadata(
                table_info,
                TableRouteValue::physical(region_routes),
                HashMap::default(),
            )
            .await
    }

    #[tokio::test]
    async fn test_create_table_metadata() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let table_metadata_manager = TableMetadataManager::new(mem_kv);
        let region_route = new_test_region_route();
        let region_routes = &vec![region_route.clone()];
        let table_info: RawTableInfo =
            new_test_table_info(region_routes.iter().map(|r| r.region.id.region_number())).into();
        // creates metadata.
        create_physical_table_metadata(
            &table_metadata_manager,
            table_info.clone(),
            region_routes.clone(),
        )
        .await
        .unwrap();

        // if metadata was already created, it should be ok.
        assert!(create_physical_table_metadata(
            &table_metadata_manager,
            table_info.clone(),
            region_routes.clone(),
        )
        .await
        .is_ok());

        let mut modified_region_routes = region_routes.clone();
        modified_region_routes.push(region_route.clone());
        // if remote metadata was exists, it should return an error.
        assert!(create_physical_table_metadata(
            &table_metadata_manager,
            table_info.clone(),
            modified_region_routes
        )
        .await
        .is_err());

        let (remote_table_info, remote_table_route) = table_metadata_manager
            .get_full_table_info(10)
            .await
            .unwrap();

        assert_eq!(
            remote_table_info.unwrap().into_inner().table_info,
            table_info
        );
        assert_eq!(
            remote_table_route
                .unwrap()
                .into_inner()
                .region_routes()
                .unwrap(),
            region_routes
        );
    }

    #[tokio::test]
    async fn test_delete_table_metadata() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let table_metadata_manager = TableMetadataManager::new(mem_kv);
        let region_route = new_test_region_route();
        let region_routes = &vec![region_route.clone()];
        let table_info: RawTableInfo =
            new_test_table_info(region_routes.iter().map(|r| r.region.id.region_number())).into();
        let table_id = table_info.ident.table_id;
        let datanode_id = 2;
        let table_route_value = DeserializedValueWithBytes::from_inner(TableRouteValue::physical(
            region_routes.clone(),
        ));

        // creates metadata.
        create_physical_table_metadata(
            &table_metadata_manager,
            table_info.clone(),
            region_routes.clone(),
        )
        .await
        .unwrap();

        let table_info_value =
            DeserializedValueWithBytes::from_inner(TableInfoValue::new(table_info.clone()));

        // deletes metadata.
        table_metadata_manager
            .delete_table_metadata(&table_info_value, &table_route_value)
            .await
            .unwrap();

        // if metadata was already deleted, it should be ok.
        table_metadata_manager
            .delete_table_metadata(&table_info_value, &table_route_value)
            .await
            .unwrap();

        assert!(table_metadata_manager
            .table_info_manager()
            .get(table_id)
            .await
            .unwrap()
            .is_none());

        assert!(table_metadata_manager
            .table_route_manager()
            .get(table_id)
            .await
            .unwrap()
            .is_none());

        assert!(table_metadata_manager
            .datanode_table_manager()
            .tables(datanode_id)
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty());
        // Checks removed values
        let removed_table_info = table_metadata_manager
            .table_info_manager()
            .get_removed(table_id)
            .await
            .unwrap()
            .unwrap()
            .into_inner();
        assert_eq!(removed_table_info.table_info, table_info);

        let removed_table_route = table_metadata_manager
            .table_route_manager()
            .get_removed(table_id)
            .await
            .unwrap()
            .unwrap()
            .into_inner();
        assert_eq!(removed_table_route.region_routes().unwrap(), region_routes);
    }

    #[tokio::test]
    async fn test_rename_table() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let table_metadata_manager = TableMetadataManager::new(mem_kv);
        let region_route = new_test_region_route();
        let region_routes = vec![region_route.clone()];
        let table_info: RawTableInfo =
            new_test_table_info(region_routes.iter().map(|r| r.region.id.region_number())).into();
        let table_id = table_info.ident.table_id;
        // creates metadata.
        create_physical_table_metadata(
            &table_metadata_manager,
            table_info.clone(),
            region_routes.clone(),
        )
        .await
        .unwrap();

        let new_table_name = "another_name".to_string();
        let table_info_value =
            DeserializedValueWithBytes::from_inner(TableInfoValue::new(table_info.clone()));

        table_metadata_manager
            .rename_table(table_info_value.clone(), new_table_name.clone())
            .await
            .unwrap();
        // if remote metadata was updated, it should be ok.
        table_metadata_manager
            .rename_table(table_info_value.clone(), new_table_name.clone())
            .await
            .unwrap();
        let mut modified_table_info = table_info.clone();
        modified_table_info.name = "hi".to_string();
        let modified_table_info_value =
            DeserializedValueWithBytes::from_inner(table_info_value.update(modified_table_info));
        // if the table_info_value is wrong, it should return an error.
        // The ABA problem.
        assert!(table_metadata_manager
            .rename_table(modified_table_info_value.clone(), new_table_name.clone())
            .await
            .is_err());

        let old_table_name = TableNameKey::new(
            &table_info.catalog_name,
            &table_info.schema_name,
            &table_info.name,
        );
        let new_table_name = TableNameKey::new(
            &table_info.catalog_name,
            &table_info.schema_name,
            &new_table_name,
        );

        assert!(table_metadata_manager
            .table_name_manager()
            .get(old_table_name)
            .await
            .unwrap()
            .is_none());

        assert_eq!(
            table_metadata_manager
                .table_name_manager()
                .get(new_table_name)
                .await
                .unwrap()
                .unwrap()
                .table_id(),
            table_id
        );
    }

    #[tokio::test]
    async fn test_update_table_info() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let table_metadata_manager = TableMetadataManager::new(mem_kv);
        let region_route = new_test_region_route();
        let region_routes = vec![region_route.clone()];
        let table_info: RawTableInfo =
            new_test_table_info(region_routes.iter().map(|r| r.region.id.region_number())).into();
        let table_id = table_info.ident.table_id;
        // creates metadata.
        create_physical_table_metadata(
            &table_metadata_manager,
            table_info.clone(),
            region_routes.clone(),
        )
        .await
        .unwrap();

        let mut new_table_info = table_info.clone();
        new_table_info.name = "hi".to_string();
        let current_table_info_value =
            DeserializedValueWithBytes::from_inner(TableInfoValue::new(table_info.clone()));
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

        // updated table_info should equal the `new_table_info`
        let updated_table_info = table_metadata_manager
            .table_info_manager()
            .get(table_id)
            .await
            .unwrap()
            .unwrap()
            .into_inner();
        assert_eq!(updated_table_info.table_info, new_table_info);

        let mut wrong_table_info = table_info.clone();
        wrong_table_info.name = "wrong".to_string();
        let wrong_table_info_value = DeserializedValueWithBytes::from_inner(
            current_table_info_value.update(wrong_table_info),
        );
        // if the current_table_info_value is wrong, it should return an error.
        // The ABA problem.
        assert!(table_metadata_manager
            .update_table_info(wrong_table_info_value, new_table_info)
            .await
            .is_err())
    }

    #[tokio::test]
    async fn test_update_table_leader_region_status() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let table_metadata_manager = TableMetadataManager::new(mem_kv);
        let datanode = 1;
        let region_routes = vec![
            RegionRoute {
                region: Region {
                    id: 1.into(),
                    name: "r1".to_string(),
                    partition: None,
                    attrs: BTreeMap::new(),
                },
                leader_peer: Some(Peer::new(datanode, "a2")),
                leader_status: Some(RegionStatus::Downgraded),
                follower_peers: vec![],
            },
            RegionRoute {
                region: Region {
                    id: 2.into(),
                    name: "r2".to_string(),
                    partition: None,
                    attrs: BTreeMap::new(),
                },
                leader_peer: Some(Peer::new(datanode, "a1")),
                leader_status: None,
                follower_peers: vec![],
            },
        ];
        let table_info: RawTableInfo =
            new_test_table_info(region_routes.iter().map(|r| r.region.id.region_number())).into();
        let table_id = table_info.ident.table_id;
        let current_table_route_value = DeserializedValueWithBytes::from_inner(
            TableRouteValue::physical(region_routes.clone()),
        );

        // creates metadata.
        create_physical_table_metadata(
            &table_metadata_manager,
            table_info.clone(),
            region_routes.clone(),
        )
        .await
        .unwrap();

        table_metadata_manager
            .update_leader_region_status(table_id, &current_table_route_value, |region_route| {
                if region_route.leader_status.is_some() {
                    None
                } else {
                    Some(Some(RegionStatus::Downgraded))
                }
            })
            .await
            .unwrap();

        let updated_route_value = table_metadata_manager
            .table_route_manager()
            .get(table_id)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            updated_route_value.region_routes().unwrap()[0].leader_status,
            Some(RegionStatus::Downgraded)
        );
        assert_eq!(
            updated_route_value.region_routes().unwrap()[1].leader_status,
            Some(RegionStatus::Downgraded)
        );
    }

    async fn assert_datanode_table(
        table_metadata_manager: &TableMetadataManager,
        table_id: u32,
        region_routes: &[RegionRoute],
    ) {
        let region_distribution = region_distribution(region_routes);
        for (datanode, regions) in region_distribution {
            let got = table_metadata_manager
                .datanode_table_manager()
                .get(&DatanodeTableKey::new(datanode, table_id))
                .await
                .unwrap()
                .unwrap();

            assert_eq!(got.regions, regions)
        }
    }

    #[tokio::test]
    async fn test_update_table_route() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let table_metadata_manager = TableMetadataManager::new(mem_kv);
        let region_route = new_test_region_route();
        let region_routes = vec![region_route.clone()];
        let table_info: RawTableInfo =
            new_test_table_info(region_routes.iter().map(|r| r.region.id.region_number())).into();
        let table_id = table_info.ident.table_id;
        let engine = table_info.meta.engine.as_str();
        let region_storage_path =
            region_storage_path(&table_info.catalog_name, &table_info.schema_name);
        let current_table_route_value = DeserializedValueWithBytes::from_inner(
            TableRouteValue::physical(region_routes.clone()),
        );

        // creates metadata.
        create_physical_table_metadata(
            &table_metadata_manager,
            table_info.clone(),
            region_routes.clone(),
        )
        .await
        .unwrap();

        assert_datanode_table(&table_metadata_manager, table_id, &region_routes).await;
        let new_region_routes = vec![
            new_region_route(1, 1),
            new_region_route(2, 2),
            new_region_route(3, 3),
        ];
        // it should be ok.
        table_metadata_manager
            .update_table_route(
                table_id,
                RegionInfo {
                    engine: engine.to_string(),
                    region_storage_path: region_storage_path.to_string(),
                    region_options: HashMap::new(),
                    region_wal_options: HashMap::new(),
                },
                &current_table_route_value,
                new_region_routes.clone(),
                &HashMap::new(),
                &HashMap::new(),
            )
            .await
            .unwrap();
        assert_datanode_table(&table_metadata_manager, table_id, &new_region_routes).await;

        // if the table route was updated. it should be ok.
        table_metadata_manager
            .update_table_route(
                table_id,
                RegionInfo {
                    engine: engine.to_string(),
                    region_storage_path: region_storage_path.to_string(),
                    region_options: HashMap::new(),
                    region_wal_options: HashMap::new(),
                },
                &current_table_route_value,
                new_region_routes.clone(),
                &HashMap::new(),
                &HashMap::new(),
            )
            .await
            .unwrap();

        let current_table_route_value = DeserializedValueWithBytes::from_inner(
            current_table_route_value
                .inner
                .update(new_region_routes.clone())
                .unwrap(),
        );
        let new_region_routes = vec![new_region_route(2, 4), new_region_route(5, 5)];
        // it should be ok.
        table_metadata_manager
            .update_table_route(
                table_id,
                RegionInfo {
                    engine: engine.to_string(),
                    region_storage_path: region_storage_path.to_string(),
                    region_options: HashMap::new(),
                    region_wal_options: HashMap::new(),
                },
                &current_table_route_value,
                new_region_routes.clone(),
                &HashMap::new(),
                &HashMap::new(),
            )
            .await
            .unwrap();
        assert_datanode_table(&table_metadata_manager, table_id, &new_region_routes).await;

        // if the current_table_route_value is wrong, it should return an error.
        // The ABA problem.
        let wrong_table_route_value = DeserializedValueWithBytes::from_inner(
            current_table_route_value
                .update(vec![
                    new_region_route(1, 1),
                    new_region_route(2, 2),
                    new_region_route(3, 3),
                    new_region_route(4, 4),
                ])
                .unwrap(),
        );
        assert!(table_metadata_manager
            .update_table_route(
                table_id,
                RegionInfo {
                    engine: engine.to_string(),
                    region_storage_path: region_storage_path.to_string(),
                    region_options: HashMap::new(),
                    region_wal_options: HashMap::new(),
                },
                &wrong_table_route_value,
                new_region_routes,
                &HashMap::new(),
                &HashMap::new(),
            )
            .await
            .is_err());
    }
}
