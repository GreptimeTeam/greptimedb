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
//! 6. Flow info key: `__flow/info/{flow_id}`
//!     - Stores metadata of the flow.
//!
//! 7. Flow route key: `__flow/route/{flow_id}/{partition_id}`
//!     - Stores route of the flow.
//!
//! 8. Flow name key: `__flow/name/{catalog}/{flow_name}`
//!     - Mapping {catalog}/{flow_name} to {flow_id}
//!
//! 9. Flownode flow key: `__flow/flownode/{flownode_id}/{flow_id}/{partition_id}`
//!     - Mapping {flownode_id} to {flow_id}
//!
//! 10. Table flow key: `__flow/source_table/{table_id}/{flownode_id}/{flow_id}/{partition_id}`
//!     - Mapping source table's {table_id} to {flownode_id}
//!     - Used in `Flownode` booting.
//!
//! 11. View info key: `__view_info/{view_id}`
//!     - The value is a [ViewInfoValue] struct; it contains the encoded logical plan.
//!     - This key is mainly used in constructing the view in Datanode and Frontend.
//!
//! 12. Kafka topic key: `__topic_name/kafka/{topic_name}`
//!     - The key is used to track existing topics in Kafka.
//!     - The value is a [TopicNameValue](crate::key::topic_name::TopicNameValue) struct; it contains the `pruned_entry_id` which represents
//!       the highest entry id that has been pruned from the remote WAL.
//!     - When a region uses this topic, it should start replaying entries from `pruned_entry_id + 1` (minimum available entry id).
//!
//! 13. Topic name to region map key `__topic_region/{topic_name}/{region_id}`
//!     - Mapping {topic_name} to {region_id}
//!
//! All keys have related managers. The managers take care of the serialization and deserialization
//! of keys and values, and the interaction with the underlying KV store backend.
//!
//! To simplify the managers used in struct fields and function parameters, we define "unify"
//! table metadata manager: [TableMetadataManager]
//! and flow metadata manager: [FlowMetadataManager](crate::key::flow::FlowMetadataManager).
//! It contains all the managers defined above. It's recommended to just use this manager only.
//!
//! The whole picture of flow keys will be like this:
//!
//! __flow/
//!   info/
//!     {flow_id}
//!   route/
//!     {flow_id}/
//!      {partition_id}
//!
//!    name/
//!      {catalog_name}
//!        {flow_name}
//!
//!    flownode/
//!      {flownode_id}/
//!        {flow_id}/
//!          {partition_id}
//!
//!    source_table/
//!      {table_id}/
//!        {flownode_id}/
//!          {flow_id}/
//!            {partition_id}

pub mod catalog_name;
pub mod datanode_table;
pub mod flow;
pub mod maintenance;
pub mod node_address;
mod schema_metadata_manager;
pub mod schema_name;
pub mod table_info;
pub mod table_name;
pub mod table_route;
#[cfg(any(test, feature = "testing"))]
pub mod test_utils;
mod tombstone;
pub mod topic_name;
pub mod topic_region;
pub mod txn_helper;
pub mod view_info;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Debug;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use bytes::Bytes;
use common_catalog::consts::{
    DEFAULT_CATALOG_NAME, DEFAULT_PRIVATE_SCHEMA_NAME, DEFAULT_SCHEMA_NAME, INFORMATION_SCHEMA_NAME,
};
use common_telemetry::warn;
use common_wal::options::WalOptions;
use datanode_table::{DatanodeTableKey, DatanodeTableManager, DatanodeTableValue};
use flow::flow_route::FlowRouteValue;
use flow::table_flow::TableFlowValue;
use lazy_static::lazy_static;
use regex::Regex;
pub use schema_metadata_manager::{SchemaMetadataManager, SchemaMetadataManagerRef};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};
use store_api::storage::RegionNumber;
use table::metadata::{RawTableInfo, TableId};
use table::table_name::TableName;
use table_info::{TableInfoKey, TableInfoManager, TableInfoValue};
use table_name::{TableNameKey, TableNameManager, TableNameValue};
use topic_name::TopicNameManager;
use topic_region::{TopicRegionKey, TopicRegionManager};
use view_info::{ViewInfoKey, ViewInfoManager, ViewInfoValue};

use self::catalog_name::{CatalogManager, CatalogNameKey, CatalogNameValue};
use self::datanode_table::RegionInfo;
use self::flow::flow_info::FlowInfoValue;
use self::flow::flow_name::FlowNameValue;
use self::schema_name::{SchemaManager, SchemaNameKey, SchemaNameValue};
use self::table_route::{TableRouteManager, TableRouteValue};
use self::tombstone::TombstoneManager;
use crate::error::{self, Result, SerdeJsonSnafu};
use crate::key::flow::flow_state::FlowStateValue;
use crate::key::node_address::NodeAddressValue;
use crate::key::table_route::TableRouteKey;
use crate::key::txn_helper::TxnOpGetResponseSet;
use crate::kv_backend::txn::{Txn, TxnOp};
use crate::kv_backend::KvBackendRef;
use crate::rpc::router::{region_distribution, LeaderState, RegionRoute};
use crate::rpc::store::BatchDeleteRequest;
use crate::state_store::PoisonValue;
use crate::DatanodeId;

pub const NAME_PATTERN: &str = r"[a-zA-Z_:-][a-zA-Z0-9_:\-\.@#]*";
pub const MAINTENANCE_KEY: &str = "__maintenance";

pub const DATANODE_TABLE_KEY_PREFIX: &str = "__dn_table";
pub const TABLE_INFO_KEY_PREFIX: &str = "__table_info";
pub const VIEW_INFO_KEY_PREFIX: &str = "__view_info";
pub const TABLE_NAME_KEY_PREFIX: &str = "__table_name";
pub const CATALOG_NAME_KEY_PREFIX: &str = "__catalog_name";
pub const SCHEMA_NAME_KEY_PREFIX: &str = "__schema_name";
pub const TABLE_ROUTE_PREFIX: &str = "__table_route";
pub const NODE_ADDRESS_PREFIX: &str = "__node_address";
pub const KAFKA_TOPIC_KEY_PREFIX: &str = "__topic_name/kafka";
// The legacy topic key prefix is used to store the topic name in previous versions.
pub const LEGACY_TOPIC_KEY_PREFIX: &str = "__created_wal_topics/kafka";
pub const TOPIC_REGION_PREFIX: &str = "__topic_region";

/// The keys with these prefixes will be loaded into the cache when the leader starts.
pub const CACHE_KEY_PREFIXES: [&str; 5] = [
    TABLE_NAME_KEY_PREFIX,
    CATALOG_NAME_KEY_PREFIX,
    SCHEMA_NAME_KEY_PREFIX,
    TABLE_ROUTE_PREFIX,
    NODE_ADDRESS_PREFIX,
];

pub type RegionDistribution = BTreeMap<DatanodeId, Vec<RegionNumber>>;

/// The id of flow.
pub type FlowId = u32;
/// The partition of flow.
pub type FlowPartitionId = u32;

lazy_static! {
    pub static ref NAME_PATTERN_REGEX: Regex = Regex::new(NAME_PATTERN).unwrap();
}

lazy_static! {
    static ref TABLE_INFO_KEY_PATTERN: Regex =
        Regex::new(&format!("^{TABLE_INFO_KEY_PREFIX}/([0-9]+)$")).unwrap();
}

lazy_static! {
    static ref VIEW_INFO_KEY_PATTERN: Regex =
        Regex::new(&format!("^{VIEW_INFO_KEY_PREFIX}/([0-9]+)$")).unwrap();
}

lazy_static! {
    static ref TABLE_ROUTE_KEY_PATTERN: Regex =
        Regex::new(&format!("^{TABLE_ROUTE_PREFIX}/([0-9]+)$")).unwrap();
}

lazy_static! {
    static ref DATANODE_TABLE_KEY_PATTERN: Regex =
        Regex::new(&format!("^{DATANODE_TABLE_KEY_PREFIX}/([0-9]+)/([0-9]+)$")).unwrap();
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

lazy_static! {
    static ref NODE_ADDRESS_PATTERN: Regex =
        Regex::new(&format!("^{NODE_ADDRESS_PREFIX}/([0-9]+)/([0-9]+)$")).unwrap();
}

lazy_static! {
    pub static ref KAFKA_TOPIC_KEY_PATTERN: Regex =
        Regex::new(&format!("^{KAFKA_TOPIC_KEY_PREFIX}/(.*)$")).unwrap();
}

lazy_static! {
    pub static ref TOPIC_REGION_PATTERN: Regex = Regex::new(&format!(
        "^{TOPIC_REGION_PREFIX}/({NAME_PATTERN})/([0-9]+)$"
    ))
    .unwrap();
}

/// The key of metadata.
pub trait MetadataKey<'a, T> {
    fn to_bytes(&self) -> Vec<u8>;

    fn from_bytes(bytes: &'a [u8]) -> Result<T>;
}

#[derive(Debug, Clone, PartialEq)]
pub struct BytesAdapter(Vec<u8>);

impl From<Vec<u8>> for BytesAdapter {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}

impl<'a> MetadataKey<'a, BytesAdapter> for BytesAdapter {
    fn to_bytes(&self) -> Vec<u8> {
        self.0.clone()
    }

    fn from_bytes(bytes: &'a [u8]) -> Result<BytesAdapter> {
        Ok(BytesAdapter(bytes.to_vec()))
    }
}

pub(crate) trait MetadataKeyGetTxnOp {
    fn build_get_op(
        &self,
    ) -> (
        TxnOp,
        impl for<'a> FnMut(&'a mut TxnOpGetResponseSet) -> Option<Vec<u8>>,
    );
}

pub trait MetadataValue {
    fn try_from_raw_value(raw_value: &[u8]) -> Result<Self>
    where
        Self: Sized;

    fn try_as_raw_value(&self) -> Result<Vec<u8>>;
}

pub type TableMetadataManagerRef = Arc<TableMetadataManager>;

pub struct TableMetadataManager {
    table_name_manager: TableNameManager,
    table_info_manager: TableInfoManager,
    view_info_manager: ViewInfoManager,
    datanode_table_manager: DatanodeTableManager,
    catalog_manager: CatalogManager,
    schema_manager: SchemaManager,
    table_route_manager: TableRouteManager,
    tombstone_manager: TombstoneManager,
    topic_name_manager: TopicNameManager,
    topic_region_manager: TopicRegionManager,
    kv_backend: KvBackendRef,
}

#[macro_export]
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

impl<T: DeserializeOwned + Serialize> DerefMut for DeserializedValueWithBytes<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
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

impl<'de, T: DeserializeOwned + Serialize + MetadataValue> Deserialize<'de>
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

impl<T: Serialize + DeserializeOwned + MetadataValue> DeserializedValueWithBytes<T> {
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

    pub fn get_inner_ref(&self) -> &T {
        &self.inner
    }

    /// Returns original `bytes`
    pub fn get_raw_bytes(&self) -> Vec<u8> {
        self.bytes.to_vec()
    }

    #[cfg(any(test, feature = "testing"))]
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
            view_info_manager: ViewInfoManager::new(kv_backend.clone()),
            datanode_table_manager: DatanodeTableManager::new(kv_backend.clone()),
            catalog_manager: CatalogManager::new(kv_backend.clone()),
            schema_manager: SchemaManager::new(kv_backend.clone()),
            table_route_manager: TableRouteManager::new(kv_backend.clone()),
            tombstone_manager: TombstoneManager::new(kv_backend.clone()),
            topic_name_manager: TopicNameManager::new(kv_backend.clone()),
            topic_region_manager: TopicRegionManager::new(kv_backend.clone()),
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

    pub fn view_info_manager(&self) -> &ViewInfoManager {
        &self.view_info_manager
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

    pub fn topic_name_manager(&self) -> &TopicNameManager {
        &self.topic_name_manager
    }

    pub fn topic_region_manager(&self) -> &TopicRegionManager {
        &self.topic_region_manager
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
        let table_info_key = TableInfoKey::new(table_id);
        let table_route_key = TableRouteKey::new(table_id);
        let (table_info_txn, table_info_filter) = table_info_key.build_get_op();
        let (table_route_txn, table_route_filter) = table_route_key.build_get_op();

        let txn = Txn::new().and_then(vec![table_info_txn, table_route_txn]);
        let mut res = self.kv_backend.txn(txn).await?;
        let mut set = TxnOpGetResponseSet::from(&mut res.responses);
        let table_info_value = TxnOpGetResponseSet::decode_with(table_info_filter)(&mut set)?;
        let table_route_value = TxnOpGetResponseSet::decode_with(table_route_filter)(&mut set)?;
        Ok((table_info_value, table_route_value))
    }

    /// Creates metadata for view and returns an error if different metadata exists.
    /// The caller MUST ensure it has the exclusive access to `TableNameKey`.
    /// Parameters include:
    /// - `view_info`: the encoded logical plan
    /// - `table_names`: the resolved fully table names in logical plan
    /// - `columns`: the view columns
    /// - `plan_columns`: the original plan columns
    /// - `definition`: The SQL to create the view
    ///
    pub async fn create_view_metadata(
        &self,
        view_info: RawTableInfo,
        raw_logical_plan: Vec<u8>,
        table_names: HashSet<TableName>,
        columns: Vec<String>,
        plan_columns: Vec<String>,
        definition: String,
    ) -> Result<()> {
        let view_id = view_info.ident.table_id;

        // Creates view name.
        let view_name = TableNameKey::new(
            &view_info.catalog_name,
            &view_info.schema_name,
            &view_info.name,
        );
        let create_table_name_txn = self
            .table_name_manager()
            .build_create_txn(&view_name, view_id)?;

        // Creates table info.
        let table_info_value = TableInfoValue::new(view_info);

        let (create_table_info_txn, on_create_table_info_failure) = self
            .table_info_manager()
            .build_create_txn(view_id, &table_info_value)?;

        // Creates view info
        let view_info_value = ViewInfoValue::new(
            raw_logical_plan,
            table_names,
            columns,
            plan_columns,
            definition,
        );
        let (create_view_info_txn, on_create_view_info_failure) = self
            .view_info_manager()
            .build_create_txn(view_id, &view_info_value)?;

        let txn = Txn::merge_all(vec![
            create_table_name_txn,
            create_table_info_txn,
            create_view_info_txn,
        ]);

        let mut r = self.kv_backend.txn(txn).await?;

        // Checks whether metadata was already created.
        if !r.succeeded {
            let mut set = TxnOpGetResponseSet::from(&mut r.responses);
            let remote_table_info = on_create_table_info_failure(&mut set)?
                .context(error::UnexpectedSnafu {
                    err_msg: "Reads the empty table info in comparing operation of creating table metadata",
                })?
                .into_inner();

            let remote_view_info = on_create_view_info_failure(&mut set)?
                .context(error::UnexpectedSnafu {
                    err_msg: "Reads the empty view info in comparing operation of creating view metadata",
                })?
                .into_inner();

            let op_name = "the creating view metadata";
            ensure_values!(remote_table_info, table_info_value, op_name);
            ensure_values!(remote_view_info, view_info_value, op_name);
        }

        Ok(())
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

        // Creates table name.
        let table_name = TableNameKey::new(
            &table_info.catalog_name,
            &table_info.schema_name,
            &table_info.name,
        );
        let create_table_name_txn = self
            .table_name_manager()
            .build_create_txn(&table_name, table_id)?;

        let region_options = table_info.to_region_options();
        // Creates table info.
        let table_info_value = TableInfoValue::new(table_info);
        let (create_table_info_txn, on_create_table_info_failure) = self
            .table_info_manager()
            .build_create_txn(table_id, &table_info_value)?;

        let (create_table_route_txn, on_create_table_route_failure) = self
            .table_route_manager()
            .table_route_storage()
            .build_create_txn(table_id, &table_route_value)?;

        let create_topic_region_txn = self
            .topic_region_manager
            .build_create_txn(table_id, &region_wal_options)?;

        let mut txn = Txn::merge_all(vec![
            create_table_name_txn,
            create_table_info_txn,
            create_table_route_txn,
            create_topic_region_txn,
        ]);

        if let TableRouteValue::Physical(x) = &table_route_value {
            let region_storage_path = table_info_value.region_storage_path();
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

        let mut r = self.kv_backend.txn(txn).await?;

        // Checks whether metadata was already created.
        if !r.succeeded {
            let mut set = TxnOpGetResponseSet::from(&mut r.responses);
            let remote_table_info = on_create_table_info_failure(&mut set)?
                .context(error::UnexpectedSnafu {
                    err_msg: "Reads the empty table info in comparing operation of creating table metadata",
                })?
                .into_inner();

            let remote_table_route = on_create_table_route_failure(&mut set)?
                .context(error::UnexpectedSnafu {
                    err_msg: "Reads the empty table route in comparing operation of creating table metadata",
                })?
                .into_inner();

            let op_name = "the creating table metadata";
            ensure_values!(remote_table_info, table_info_value, op_name);
            ensure_values!(remote_table_route, table_route_value, op_name);
        }

        Ok(())
    }

    pub fn create_logical_tables_metadata_chunk_size(&self) -> usize {
        // The batch size is max_txn_size / 3 because the size of the `tables_data`
        // is 3 times the size of the `tables_data`.
        self.kv_backend.max_txn_ops() / 3
    }

    /// Creates metadata for multiple logical tables and return an error if different metadata exists.
    pub async fn create_logical_tables_metadata(
        &self,
        tables_data: Vec<(RawTableInfo, TableRouteValue)>,
    ) -> Result<()> {
        let len = tables_data.len();
        let mut txns = Vec::with_capacity(3 * len);
        struct OnFailure<F1, R1, F2, R2>
        where
            F1: FnOnce(&mut TxnOpGetResponseSet) -> R1,
            F2: FnOnce(&mut TxnOpGetResponseSet) -> R2,
        {
            table_info_value: TableInfoValue,
            on_create_table_info_failure: F1,
            table_route_value: TableRouteValue,
            on_create_table_route_failure: F2,
        }
        let mut on_failures = Vec::with_capacity(len);
        for (mut table_info, table_route_value) in tables_data {
            table_info.meta.region_numbers = table_route_value.region_numbers();
            let table_id = table_info.ident.table_id;

            // Creates table name.
            let table_name = TableNameKey::new(
                &table_info.catalog_name,
                &table_info.schema_name,
                &table_info.name,
            );
            let create_table_name_txn = self
                .table_name_manager()
                .build_create_txn(&table_name, table_id)?;
            txns.push(create_table_name_txn);

            // Creates table info.
            let table_info_value = TableInfoValue::new(table_info);
            let (create_table_info_txn, on_create_table_info_failure) =
                self.table_info_manager()
                    .build_create_txn(table_id, &table_info_value)?;
            txns.push(create_table_info_txn);

            let (create_table_route_txn, on_create_table_route_failure) = self
                .table_route_manager()
                .table_route_storage()
                .build_create_txn(table_id, &table_route_value)?;
            txns.push(create_table_route_txn);

            on_failures.push(OnFailure {
                table_info_value,
                on_create_table_info_failure,
                table_route_value,
                on_create_table_route_failure,
            });
        }

        let txn = Txn::merge_all(txns);
        let mut r = self.kv_backend.txn(txn).await?;

        // Checks whether metadata was already created.
        if !r.succeeded {
            let mut set = TxnOpGetResponseSet::from(&mut r.responses);
            for on_failure in on_failures {
                let remote_table_info = (on_failure.on_create_table_info_failure)(&mut set)?
                    .context(error::UnexpectedSnafu {
                        err_msg: "Reads the empty table info in comparing operation of creating table metadata",
                    })?
                    .into_inner();

                let remote_table_route = (on_failure.on_create_table_route_failure)(&mut set)?
                    .context(error::UnexpectedSnafu {
                        err_msg: "Reads the empty table route in comparing operation of creating table metadata",
                    })?
                    .into_inner();

                let op_name = "the creating logical tables metadata";
                ensure_values!(remote_table_info, on_failure.table_info_value, op_name);
                ensure_values!(remote_table_route, on_failure.table_route_value, op_name);
            }
        }

        Ok(())
    }

    fn table_metadata_keys(
        &self,
        table_id: TableId,
        table_name: &TableName,
        table_route_value: &TableRouteValue,
        region_wal_options: &HashMap<RegionNumber, WalOptions>,
    ) -> Result<Vec<Vec<u8>>> {
        // Builds keys
        let datanode_ids = if table_route_value.is_physical() {
            region_distribution(table_route_value.region_routes()?)
                .into_keys()
                .collect()
        } else {
            vec![]
        };
        let mut keys = Vec::with_capacity(3 + datanode_ids.len());
        let table_name = TableNameKey::new(
            &table_name.catalog_name,
            &table_name.schema_name,
            &table_name.table_name,
        );
        let table_info_key = TableInfoKey::new(table_id);
        let table_route_key = TableRouteKey::new(table_id);
        let datanode_table_keys = datanode_ids
            .into_iter()
            .map(|datanode_id| DatanodeTableKey::new(datanode_id, table_id))
            .collect::<HashSet<_>>();
        let topic_region_map = self
            .topic_region_manager
            .get_topic_region_mapping(table_id, region_wal_options);
        let topic_region_keys = topic_region_map
            .iter()
            .map(|(region_id, topic)| TopicRegionKey::new(*region_id, topic))
            .collect::<Vec<_>>();
        keys.push(table_name.to_bytes());
        keys.push(table_info_key.to_bytes());
        keys.push(table_route_key.to_bytes());
        for key in &datanode_table_keys {
            keys.push(key.to_bytes());
        }
        for key in topic_region_keys {
            keys.push(key.to_bytes());
        }
        Ok(keys)
    }

    /// Deletes metadata for table **logically**.
    /// The caller MUST ensure it has the exclusive access to `TableNameKey`.
    pub async fn delete_table_metadata(
        &self,
        table_id: TableId,
        table_name: &TableName,
        table_route_value: &TableRouteValue,
        region_wal_options: &HashMap<RegionNumber, WalOptions>,
    ) -> Result<()> {
        let keys =
            self.table_metadata_keys(table_id, table_name, table_route_value, region_wal_options)?;
        self.tombstone_manager.create(keys).await
    }

    /// Deletes metadata tombstone for table **permanently**.
    /// The caller MUST ensure it has the exclusive access to `TableNameKey`.
    pub async fn delete_table_metadata_tombstone(
        &self,
        table_id: TableId,
        table_name: &TableName,
        table_route_value: &TableRouteValue,
        region_wal_options: &HashMap<RegionNumber, WalOptions>,
    ) -> Result<()> {
        let table_metadata_keys =
            self.table_metadata_keys(table_id, table_name, table_route_value, region_wal_options)?;
        self.tombstone_manager.delete(table_metadata_keys).await
    }

    /// Restores metadata for table.
    /// The caller MUST ensure it has the exclusive access to `TableNameKey`.
    pub async fn restore_table_metadata(
        &self,
        table_id: TableId,
        table_name: &TableName,
        table_route_value: &TableRouteValue,
        region_wal_options: &HashMap<RegionNumber, WalOptions>,
    ) -> Result<()> {
        let keys =
            self.table_metadata_keys(table_id, table_name, table_route_value, region_wal_options)?;
        self.tombstone_manager.restore(keys).await
    }

    /// Deletes metadata for table **permanently**.
    /// The caller MUST ensure it has the exclusive access to `TableNameKey`.
    pub async fn destroy_table_metadata(
        &self,
        table_id: TableId,
        table_name: &TableName,
        table_route_value: &TableRouteValue,
        region_wal_options: &HashMap<RegionNumber, WalOptions>,
    ) -> Result<()> {
        let keys =
            self.table_metadata_keys(table_id, table_name, table_route_value, region_wal_options)?;
        let _ = self
            .kv_backend
            .batch_delete(BatchDeleteRequest::new().with_keys(keys))
            .await?;
        Ok(())
    }

    fn view_info_keys(&self, view_id: TableId, view_name: &TableName) -> Result<Vec<Vec<u8>>> {
        let mut keys = Vec::with_capacity(3);
        let view_name = TableNameKey::new(
            &view_name.catalog_name,
            &view_name.schema_name,
            &view_name.table_name,
        );
        let table_info_key = TableInfoKey::new(view_id);
        let view_info_key = ViewInfoKey::new(view_id);
        keys.push(view_name.to_bytes());
        keys.push(table_info_key.to_bytes());
        keys.push(view_info_key.to_bytes());

        Ok(keys)
    }

    /// Deletes metadata for view **permanently**.
    /// The caller MUST ensure it has the exclusive access to `ViewNameKey`.
    pub async fn destroy_view_info(&self, view_id: TableId, view_name: &TableName) -> Result<()> {
        let keys = self.view_info_keys(view_id, view_name)?;
        let _ = self
            .kv_backend
            .batch_delete(BatchDeleteRequest::new().with_keys(keys))
            .await?;
        Ok(())
    }

    /// Renames the table name and returns an error if different metadata exists.
    /// The caller MUST ensure it has the exclusive access to old and new `TableNameKey`s,
    /// and the new `TableNameKey` MUST be empty.
    pub async fn rename_table(
        &self,
        current_table_info_value: &DeserializedValueWithBytes<TableInfoValue>,
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
            .build_update_txn(table_id, current_table_info_value, &new_table_info_value)?;

        let txn = Txn::merge_all(vec![update_table_name_txn, update_table_info_txn]);

        let mut r = self.kv_backend.txn(txn).await?;

        // Checks whether metadata was already updated.
        if !r.succeeded {
            let mut set = TxnOpGetResponseSet::from(&mut r.responses);
            let remote_table_info = on_update_table_info_failure(&mut set)?
                .context(error::UnexpectedSnafu {
                    err_msg: "Reads the empty table info in comparing operation of the rename table metadata",
                })?
                .into_inner();

            let op_name = "the renaming table metadata";
            ensure_values!(remote_table_info, new_table_info_value, op_name);
        }

        Ok(())
    }

    /// Updates table info and returns an error if different metadata exists.
    /// And cascade-ly update all redundant table options for each region
    /// if region_distribution is present.
    pub async fn update_table_info(
        &self,
        current_table_info_value: &DeserializedValueWithBytes<TableInfoValue>,
        region_distribution: Option<RegionDistribution>,
        new_table_info: RawTableInfo,
    ) -> Result<()> {
        let table_id = current_table_info_value.table_info.ident.table_id;
        let new_table_info_value = current_table_info_value.update(new_table_info);

        // Updates table info.
        let (update_table_info_txn, on_update_table_info_failure) = self
            .table_info_manager()
            .build_update_txn(table_id, current_table_info_value, &new_table_info_value)?;

        let txn = if let Some(region_distribution) = region_distribution {
            // region options induced from table info.
            let new_region_options = new_table_info_value.table_info.to_region_options();
            let update_datanode_table_options_txn = self
                .datanode_table_manager
                .build_update_table_options_txn(table_id, region_distribution, new_region_options)
                .await?;
            Txn::merge_all([update_table_info_txn, update_datanode_table_options_txn])
        } else {
            update_table_info_txn
        };

        let mut r = self.kv_backend.txn(txn).await?;
        // Checks whether metadata was already updated.
        if !r.succeeded {
            let mut set = TxnOpGetResponseSet::from(&mut r.responses);
            let remote_table_info = on_update_table_info_failure(&mut set)?
                .context(error::UnexpectedSnafu {
                    err_msg: "Reads the empty table info in comparing operation of the updating table info",
                })?
                .into_inner();

            let op_name = "the updating table info";
            ensure_values!(remote_table_info, new_table_info_value, op_name);
        }
        Ok(())
    }

    /// Updates view info and returns an error if different metadata exists.
    /// Parameters include:
    /// - `view_id`: the view id
    /// - `current_view_info_value`: the current view info for CAS checking
    /// - `new_view_info`: the encoded logical plan
    /// - `table_names`: the resolved fully table names in logical plan
    /// - `columns`: the view columns
    /// - `plan_columns`: the original plan columns
    /// - `definition`: The SQL to create the view
    ///
    #[allow(clippy::too_many_arguments)]
    pub async fn update_view_info(
        &self,
        view_id: TableId,
        current_view_info_value: &DeserializedValueWithBytes<ViewInfoValue>,
        new_view_info: Vec<u8>,
        table_names: HashSet<TableName>,
        columns: Vec<String>,
        plan_columns: Vec<String>,
        definition: String,
    ) -> Result<()> {
        let new_view_info_value = current_view_info_value.update(
            new_view_info,
            table_names,
            columns,
            plan_columns,
            definition,
        );

        // Updates view info.
        let (update_view_info_txn, on_update_view_info_failure) = self
            .view_info_manager()
            .build_update_txn(view_id, current_view_info_value, &new_view_info_value)?;

        let mut r = self.kv_backend.txn(update_view_info_txn).await?;

        // Checks whether metadata was already updated.
        if !r.succeeded {
            let mut set = TxnOpGetResponseSet::from(&mut r.responses);
            let remote_view_info = on_update_view_info_failure(&mut set)?
                .context(error::UnexpectedSnafu {
                    err_msg: "Reads the empty view info in comparing operation of the updating view info",
                })?
                .into_inner();

            let op_name = "the updating view info";
            ensure_values!(remote_view_info, new_view_info_value, op_name);
        }
        Ok(())
    }

    pub fn batch_update_table_info_value_chunk_size(&self) -> usize {
        self.kv_backend.max_txn_ops()
    }

    pub async fn batch_update_table_info_values(
        &self,
        table_info_value_pairs: Vec<(DeserializedValueWithBytes<TableInfoValue>, RawTableInfo)>,
    ) -> Result<()> {
        let len = table_info_value_pairs.len();
        let mut txns = Vec::with_capacity(len);
        struct OnFailure<F, R>
        where
            F: FnOnce(&mut TxnOpGetResponseSet) -> R,
        {
            table_info_value: TableInfoValue,
            on_update_table_info_failure: F,
        }
        let mut on_failures = Vec::with_capacity(len);

        for (table_info_value, new_table_info) in table_info_value_pairs {
            let table_id = table_info_value.table_info.ident.table_id;

            let new_table_info_value = table_info_value.update(new_table_info);

            let (update_table_info_txn, on_update_table_info_failure) =
                self.table_info_manager().build_update_txn(
                    table_id,
                    &table_info_value,
                    &new_table_info_value,
                )?;

            txns.push(update_table_info_txn);

            on_failures.push(OnFailure {
                table_info_value: new_table_info_value,
                on_update_table_info_failure,
            });
        }

        let txn = Txn::merge_all(txns);
        let mut r = self.kv_backend.txn(txn).await?;

        if !r.succeeded {
            let mut set = TxnOpGetResponseSet::from(&mut r.responses);
            for on_failure in on_failures {
                let remote_table_info = (on_failure.on_update_table_info_failure)(&mut set)?
                    .context(error::UnexpectedSnafu {
                        err_msg: "Reads the empty table info in comparing operation of the updating table info",
                    })?
                    .into_inner();

                let op_name = "the batch updating table info";
                ensure_values!(remote_table_info, on_failure.table_info_value, op_name);
            }
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
            .table_route_storage()
            .build_update_txn(table_id, current_table_route_value, &new_table_route_value)?;

        let txn = Txn::merge_all(vec![update_datanode_table_txn, update_table_route_txn]);

        let mut r = self.kv_backend.txn(txn).await?;

        // Checks whether metadata was already updated.
        if !r.succeeded {
            let mut set = TxnOpGetResponseSet::from(&mut r.responses);
            let remote_table_route = on_update_table_route_failure(&mut set)?
                .context(error::UnexpectedSnafu {
                    err_msg: "Reads the empty table route in comparing operation of the updating table route",
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
        F: Fn(&RegionRoute) -> Option<Option<LeaderState>>,
    {
        let mut new_region_routes = current_table_route_value.region_routes()?.clone();

        let mut updated = 0;
        for route in &mut new_region_routes {
            if let Some(state) = next_region_route_status(route) {
                if route.set_leader_state(state) {
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
            .table_route_storage()
            .build_update_txn(table_id, current_table_route_value, &new_table_route_value)?;

        let mut r = self.kv_backend.txn(update_table_route_txn).await?;

        // Checks whether metadata was already updated.
        if !r.succeeded {
            let mut set = TxnOpGetResponseSet::from(&mut r.responses);
            let remote_table_route = on_update_table_route_failure(&mut set)?
                .context(error::UnexpectedSnafu {
                    err_msg: "Reads the empty table route in comparing operation of the updating leader region status",
                })?
                .into_inner();

            let op_name = "the updating leader region status";
            ensure_values!(remote_table_route, new_table_route_value, op_name);
        }

        Ok(())
    }
}

#[macro_export]
macro_rules! impl_metadata_value {
    ($($val_ty: ty), *) => {
        $(
            impl $crate::key::MetadataValue for $val_ty {
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

macro_rules! impl_metadata_key_get_txn_op {
    ($($key: ty), *) => {
        $(
            impl $crate::key::MetadataKeyGetTxnOp for $key {
                /// Returns a [TxnOp] to retrieve the corresponding value
                /// and a filter to retrieve the value from the [TxnOpGetResponseSet]
                fn build_get_op(
                    &self,
                ) -> (
                    TxnOp,
                    impl for<'a> FnMut(
                        &'a mut TxnOpGetResponseSet,
                    ) -> Option<Vec<u8>>,
                ) {
                    let raw_key = self.to_bytes();
                    (
                        TxnOp::Get(raw_key.clone()),
                        TxnOpGetResponseSet::filter(raw_key),
                    )
                }
            }
        )*
    }
}

impl_metadata_key_get_txn_op! {
    TableNameKey<'_>,
    TableInfoKey,
    ViewInfoKey,
    TableRouteKey,
    DatanodeTableKey
}

#[macro_export]
macro_rules! impl_optional_metadata_value {
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

impl_metadata_value! {
    TableNameValue,
    TableInfoValue,
    ViewInfoValue,
    DatanodeTableValue,
    FlowInfoValue,
    FlowNameValue,
    FlowRouteValue,
    TableFlowValue,
    NodeAddressValue,
    SchemaNameValue,
    FlowStateValue,
    PoisonValue
}

impl_optional_metadata_value! {
    CatalogNameValue,
    SchemaNameValue
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap, HashSet};
    use std::sync::Arc;

    use bytes::Bytes;
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use common_time::util::current_time_millis;
    use common_wal::options::{KafkaWalOptions, WalOptions};
    use futures::TryStreamExt;
    use store_api::storage::{RegionId, RegionNumber};
    use table::metadata::{RawTableInfo, TableInfo};
    use table::table_name::TableName;

    use super::datanode_table::DatanodeTableKey;
    use super::test_utils;
    use crate::ddl::test_util::create_table::test_create_table_task;
    use crate::ddl::utils::region_storage_path;
    use crate::error::Result;
    use crate::key::datanode_table::RegionInfo;
    use crate::key::table_info::TableInfoValue;
    use crate::key::table_name::TableNameKey;
    use crate::key::table_route::TableRouteValue;
    use crate::key::{
        DeserializedValueWithBytes, TableMetadataManager, ViewInfoValue, TOPIC_REGION_PREFIX,
    };
    use crate::kv_backend::memory::MemoryKvBackend;
    use crate::kv_backend::KvBackend;
    use crate::peer::Peer;
    use crate::rpc::router::{region_distribution, LeaderState, Region, RegionRoute};
    use crate::rpc::store::RangeRequest;
    use crate::wal_options_allocator::{allocate_region_wal_options, WalOptionsAllocator};

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
            leader_state: None,
            leader_down_since: None,
        }
    }

    fn new_test_table_info(region_numbers: impl Iterator<Item = u32>) -> TableInfo {
        test_utils::new_test_table_info(10, region_numbers)
    }

    fn new_test_table_names() -> HashSet<TableName> {
        let mut set = HashSet::new();
        set.insert(TableName {
            catalog_name: "greptime".to_string(),
            schema_name: "public".to_string(),
            table_name: "a_table".to_string(),
        });
        set.insert(TableName {
            catalog_name: "greptime".to_string(),
            schema_name: "public".to_string(),
            table_name: "b_table".to_string(),
        });
        set
    }

    async fn create_physical_table_metadata(
        table_metadata_manager: &TableMetadataManager,
        table_info: RawTableInfo,
        region_routes: Vec<RegionRoute>,
        region_wal_options: HashMap<RegionNumber, String>,
    ) -> Result<()> {
        table_metadata_manager
            .create_table_metadata(
                table_info,
                TableRouteValue::physical(region_routes),
                region_wal_options,
            )
            .await
    }

    fn create_mock_region_wal_options() -> HashMap<RegionNumber, WalOptions> {
        let topics = (0..2)
            .map(|i| format!("greptimedb_topic{}", i))
            .collect::<Vec<_>>();
        let wal_options = topics
            .iter()
            .map(|topic| {
                WalOptions::Kafka(KafkaWalOptions {
                    topic: topic.clone(),
                })
            })
            .collect::<Vec<_>>();

        (0..16)
            .enumerate()
            .map(|(i, region_number)| (region_number, wal_options[i % wal_options.len()].clone()))
            .collect()
    }

    #[tokio::test]
    async fn test_raft_engine_topic_region_map() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let table_metadata_manager = TableMetadataManager::new(mem_kv.clone());
        let region_route = new_test_region_route();
        let region_routes = &vec![region_route.clone()];
        let table_info: RawTableInfo =
            new_test_table_info(region_routes.iter().map(|r| r.region.id.region_number())).into();
        let wal_allocator = WalOptionsAllocator::RaftEngine;
        let regions = (0..16).collect();
        let region_wal_options =
            allocate_region_wal_options(regions, &wal_allocator, false).unwrap();
        create_physical_table_metadata(
            &table_metadata_manager,
            table_info.clone(),
            region_routes.clone(),
            region_wal_options.clone(),
        )
        .await
        .unwrap();

        let topic_region_key = TOPIC_REGION_PREFIX.to_string();
        let range_req = RangeRequest::new().with_prefix(topic_region_key);
        let resp = mem_kv.range(range_req).await.unwrap();
        // Should be empty because the topic region map is empty for raft engine.
        assert!(resp.kvs.is_empty());
    }

    #[tokio::test]
    async fn test_create_table_metadata() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let table_metadata_manager = TableMetadataManager::new(mem_kv);
        let region_route = new_test_region_route();
        let region_routes = &vec![region_route.clone()];
        let table_info: RawTableInfo =
            new_test_table_info(region_routes.iter().map(|r| r.region.id.region_number())).into();
        let region_wal_options = create_mock_region_wal_options()
            .into_iter()
            .map(|(k, v)| (k, serde_json::to_string(&v).unwrap()))
            .collect::<HashMap<_, _>>();

        // creates metadata.
        create_physical_table_metadata(
            &table_metadata_manager,
            table_info.clone(),
            region_routes.clone(),
            region_wal_options.clone(),
        )
        .await
        .unwrap();

        // if metadata was already created, it should be ok.
        assert!(create_physical_table_metadata(
            &table_metadata_manager,
            table_info.clone(),
            region_routes.clone(),
            region_wal_options.clone(),
        )
        .await
        .is_ok());

        let mut modified_region_routes = region_routes.clone();
        modified_region_routes.push(region_route.clone());
        // if remote metadata was exists, it should return an error.
        assert!(create_physical_table_metadata(
            &table_metadata_manager,
            table_info.clone(),
            modified_region_routes,
            region_wal_options.clone(),
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

        for i in 0..2 {
            let region_number = i as u32;
            let region_id = RegionId::new(table_info.ident.table_id, region_number);
            let topic = format!("greptimedb_topic{}", i);
            let regions = table_metadata_manager
                .topic_region_manager
                .regions(&topic)
                .await
                .unwrap();
            assert_eq!(regions.len(), 8);
            assert_eq!(regions[0], region_id);
        }
    }

    #[tokio::test]
    async fn test_create_logic_tables_metadata() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let table_metadata_manager = TableMetadataManager::new(mem_kv);
        let region_route = new_test_region_route();
        let region_routes = vec![region_route.clone()];
        let table_info: RawTableInfo =
            new_test_table_info(region_routes.iter().map(|r| r.region.id.region_number())).into();
        let table_id = table_info.ident.table_id;
        let table_route_value = TableRouteValue::physical(region_routes.clone());

        let tables_data = vec![(table_info.clone(), table_route_value.clone())];
        // creates metadata.
        table_metadata_manager
            .create_logical_tables_metadata(tables_data.clone())
            .await
            .unwrap();

        // if metadata was already created, it should be ok.
        assert!(table_metadata_manager
            .create_logical_tables_metadata(tables_data)
            .await
            .is_ok());

        let mut modified_region_routes = region_routes.clone();
        modified_region_routes.push(new_region_route(2, 3));
        let modified_table_route_value = TableRouteValue::physical(modified_region_routes.clone());
        let modified_tables_data = vec![(table_info.clone(), modified_table_route_value)];
        // if remote metadata was exists, it should return an error.
        assert!(table_metadata_manager
            .create_logical_tables_metadata(modified_tables_data)
            .await
            .is_err());

        let (remote_table_info, remote_table_route) = table_metadata_manager
            .get_full_table_info(table_id)
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
            &region_routes
        );
    }

    #[tokio::test]
    async fn test_create_many_logical_tables_metadata() {
        let kv_backend = Arc::new(MemoryKvBackend::default());
        let table_metadata_manager = TableMetadataManager::new(kv_backend);

        let mut tables_data = vec![];
        for i in 0..128 {
            let table_id = i + 1;
            let regin_number = table_id * 3;
            let region_id = RegionId::new(table_id, regin_number);
            let region_route = new_region_route(region_id.as_u64(), 2);
            let region_routes = vec![region_route.clone()];
            let table_info: RawTableInfo = test_utils::new_test_table_info_with_name(
                table_id,
                &format!("my_table_{}", table_id),
                region_routes.iter().map(|r| r.region.id.region_number()),
            )
            .into();
            let table_route_value = TableRouteValue::physical(region_routes.clone());

            tables_data.push((table_info, table_route_value));
        }

        // creates metadata.
        table_metadata_manager
            .create_logical_tables_metadata(tables_data)
            .await
            .unwrap();
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
        let region_wal_options = create_mock_region_wal_options();
        let serialized_region_wal_options = region_wal_options
            .iter()
            .map(|(k, v)| (*k, serde_json::to_string(v).unwrap()))
            .collect::<HashMap<_, _>>();

        // creates metadata.
        create_physical_table_metadata(
            &table_metadata_manager,
            table_info.clone(),
            region_routes.clone(),
            serialized_region_wal_options,
        )
        .await
        .unwrap();

        let table_name = TableName::new(
            table_info.catalog_name,
            table_info.schema_name,
            table_info.name,
        );
        let table_route_value = &TableRouteValue::physical(region_routes.clone());
        // deletes metadata.
        table_metadata_manager
            .delete_table_metadata(
                table_id,
                &table_name,
                table_route_value,
                &region_wal_options,
            )
            .await
            .unwrap();
        // Should be ignored.
        table_metadata_manager
            .delete_table_metadata(
                table_id,
                &table_name,
                table_route_value,
                &region_wal_options,
            )
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
            .table_route_storage()
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
        let table_info = table_metadata_manager
            .table_info_manager()
            .get(table_id)
            .await
            .unwrap();
        assert!(table_info.is_none());
        let table_route = table_metadata_manager
            .table_route_manager()
            .table_route_storage()
            .get(table_id)
            .await
            .unwrap();
        assert!(table_route.is_none());
        // Logical delete removes the topic region mapping as well.
        let regions = table_metadata_manager
            .topic_region_manager
            .regions("greptimedb_topic0")
            .await
            .unwrap();
        assert_eq!(regions.len(), 0);
        let regions = table_metadata_manager
            .topic_region_manager
            .regions("greptimedb_topic1")
            .await
            .unwrap();
        assert_eq!(regions.len(), 0);
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
            HashMap::new(),
        )
        .await
        .unwrap();

        let new_table_name = "another_name".to_string();
        let table_info_value =
            DeserializedValueWithBytes::from_inner(TableInfoValue::new(table_info.clone()));

        table_metadata_manager
            .rename_table(&table_info_value, new_table_name.clone())
            .await
            .unwrap();
        // if remote metadata was updated, it should be ok.
        table_metadata_manager
            .rename_table(&table_info_value, new_table_name.clone())
            .await
            .unwrap();
        let mut modified_table_info = table_info.clone();
        modified_table_info.name = "hi".to_string();
        let modified_table_info_value =
            DeserializedValueWithBytes::from_inner(table_info_value.update(modified_table_info));
        // if the table_info_value is wrong, it should return an error.
        // The ABA problem.
        assert!(table_metadata_manager
            .rename_table(&modified_table_info_value, new_table_name.clone())
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
            HashMap::new(),
        )
        .await
        .unwrap();

        let mut new_table_info = table_info.clone();
        new_table_info.name = "hi".to_string();
        let current_table_info_value =
            DeserializedValueWithBytes::from_inner(TableInfoValue::new(table_info.clone()));
        // should be ok.
        table_metadata_manager
            .update_table_info(&current_table_info_value, None, new_table_info.clone())
            .await
            .unwrap();
        // if table info was updated, it should be ok.
        table_metadata_manager
            .update_table_info(&current_table_info_value, None, new_table_info.clone())
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
            .update_table_info(&wrong_table_info_value, None, new_table_info)
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
                leader_state: Some(LeaderState::Downgrading),
                follower_peers: vec![],
                leader_down_since: Some(current_time_millis()),
            },
            RegionRoute {
                region: Region {
                    id: 2.into(),
                    name: "r2".to_string(),
                    partition: None,
                    attrs: BTreeMap::new(),
                },
                leader_peer: Some(Peer::new(datanode, "a1")),
                leader_state: None,
                follower_peers: vec![],
                leader_down_since: None,
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
            HashMap::new(),
        )
        .await
        .unwrap();

        table_metadata_manager
            .update_leader_region_status(table_id, &current_table_route_value, |region_route| {
                if region_route.leader_state.is_some() {
                    None
                } else {
                    Some(Some(LeaderState::Downgrading))
                }
            })
            .await
            .unwrap();

        let updated_route_value = table_metadata_manager
            .table_route_manager()
            .table_route_storage()
            .get(table_id)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            updated_route_value.region_routes().unwrap()[0].leader_state,
            Some(LeaderState::Downgrading)
        );

        assert!(updated_route_value.region_routes().unwrap()[0]
            .leader_down_since
            .is_some());

        assert_eq!(
            updated_route_value.region_routes().unwrap()[1].leader_state,
            Some(LeaderState::Downgrading)
        );
        assert!(updated_route_value.region_routes().unwrap()[1]
            .leader_down_since
            .is_some());
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
            HashMap::new(),
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

    #[tokio::test]
    async fn test_destroy_table_metadata() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let table_metadata_manager = TableMetadataManager::new(mem_kv.clone());
        let table_id = 1025;
        let table_name = "foo";
        let task = test_create_table_task(table_name, table_id);
        let options = create_mock_region_wal_options();
        let serialized_options = options
            .iter()
            .map(|(k, v)| (*k, serde_json::to_string(v).unwrap()))
            .collect::<HashMap<_, _>>();
        table_metadata_manager
            .create_table_metadata(
                task.table_info,
                TableRouteValue::physical(vec![
                    RegionRoute {
                        region: Region::new_test(RegionId::new(table_id, 1)),
                        leader_peer: Some(Peer::empty(1)),
                        follower_peers: vec![Peer::empty(5)],
                        leader_state: None,
                        leader_down_since: None,
                    },
                    RegionRoute {
                        region: Region::new_test(RegionId::new(table_id, 2)),
                        leader_peer: Some(Peer::empty(2)),
                        follower_peers: vec![Peer::empty(4)],
                        leader_state: None,
                        leader_down_since: None,
                    },
                    RegionRoute {
                        region: Region::new_test(RegionId::new(table_id, 3)),
                        leader_peer: Some(Peer::empty(3)),
                        follower_peers: vec![],
                        leader_state: None,
                        leader_down_since: None,
                    },
                ]),
                serialized_options,
            )
            .await
            .unwrap();
        let table_name = TableName::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, table_name);
        let table_route_value = table_metadata_manager
            .table_route_manager
            .table_route_storage()
            .get_with_raw_bytes(table_id)
            .await
            .unwrap()
            .unwrap();
        table_metadata_manager
            .destroy_table_metadata(table_id, &table_name, &table_route_value, &options)
            .await
            .unwrap();
        assert!(mem_kv.is_empty());
    }

    #[tokio::test]
    async fn test_restore_table_metadata() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let table_metadata_manager = TableMetadataManager::new(mem_kv.clone());
        let table_id = 1025;
        let table_name = "foo";
        let task = test_create_table_task(table_name, table_id);
        let options = create_mock_region_wal_options();
        let serialized_options = options
            .iter()
            .map(|(k, v)| (*k, serde_json::to_string(v).unwrap()))
            .collect::<HashMap<_, _>>();
        table_metadata_manager
            .create_table_metadata(
                task.table_info,
                TableRouteValue::physical(vec![
                    RegionRoute {
                        region: Region::new_test(RegionId::new(table_id, 1)),
                        leader_peer: Some(Peer::empty(1)),
                        follower_peers: vec![Peer::empty(5)],
                        leader_state: None,
                        leader_down_since: None,
                    },
                    RegionRoute {
                        region: Region::new_test(RegionId::new(table_id, 2)),
                        leader_peer: Some(Peer::empty(2)),
                        follower_peers: vec![Peer::empty(4)],
                        leader_state: None,
                        leader_down_since: None,
                    },
                    RegionRoute {
                        region: Region::new_test(RegionId::new(table_id, 3)),
                        leader_peer: Some(Peer::empty(3)),
                        follower_peers: vec![],
                        leader_state: None,
                        leader_down_since: None,
                    },
                ]),
                serialized_options,
            )
            .await
            .unwrap();
        let expected_result = mem_kv.dump();
        let table_route_value = table_metadata_manager
            .table_route_manager
            .table_route_storage()
            .get_with_raw_bytes(table_id)
            .await
            .unwrap()
            .unwrap();
        let region_routes = table_route_value.region_routes().unwrap();
        let table_name = TableName::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, table_name);
        let table_route_value = TableRouteValue::physical(region_routes.clone());
        table_metadata_manager
            .delete_table_metadata(table_id, &table_name, &table_route_value, &options)
            .await
            .unwrap();
        table_metadata_manager
            .restore_table_metadata(table_id, &table_name, &table_route_value, &options)
            .await
            .unwrap();
        let kvs = mem_kv.dump();
        assert_eq!(kvs, expected_result);
        // Should be ignored.
        table_metadata_manager
            .restore_table_metadata(table_id, &table_name, &table_route_value, &options)
            .await
            .unwrap();
        let kvs = mem_kv.dump();
        assert_eq!(kvs, expected_result);
    }

    #[tokio::test]
    async fn test_create_update_view_info() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let table_metadata_manager = TableMetadataManager::new(mem_kv);

        let view_info: RawTableInfo = new_test_table_info(Vec::<u32>::new().into_iter()).into();

        let view_id = view_info.ident.table_id;

        let logical_plan: Vec<u8> = vec![1, 2, 3];
        let columns = vec!["a".to_string()];
        let plan_columns = vec!["number".to_string()];
        let table_names = new_test_table_names();
        let definition = "CREATE VIEW test AS SELECT * FROM numbers";

        // Create metadata
        table_metadata_manager
            .create_view_metadata(
                view_info.clone(),
                logical_plan.clone(),
                table_names.clone(),
                columns.clone(),
                plan_columns.clone(),
                definition.to_string(),
            )
            .await
            .unwrap();

        {
            // assert view info
            let current_view_info = table_metadata_manager
                .view_info_manager()
                .get(view_id)
                .await
                .unwrap()
                .unwrap()
                .into_inner();
            assert_eq!(current_view_info.view_info, logical_plan);
            assert_eq!(current_view_info.table_names, table_names);
            assert_eq!(current_view_info.definition, definition);
            assert_eq!(current_view_info.columns, columns);
            assert_eq!(current_view_info.plan_columns, plan_columns);
            // assert table info
            let current_table_info = table_metadata_manager
                .table_info_manager()
                .get(view_id)
                .await
                .unwrap()
                .unwrap()
                .into_inner();
            assert_eq!(current_table_info.table_info, view_info);
        }

        let new_logical_plan: Vec<u8> = vec![4, 5, 6];
        let new_table_names = {
            let mut set = HashSet::new();
            set.insert(TableName {
                catalog_name: "greptime".to_string(),
                schema_name: "public".to_string(),
                table_name: "b_table".to_string(),
            });
            set.insert(TableName {
                catalog_name: "greptime".to_string(),
                schema_name: "public".to_string(),
                table_name: "c_table".to_string(),
            });
            set
        };
        let new_columns = vec!["b".to_string()];
        let new_plan_columns = vec!["number2".to_string()];
        let new_definition = "CREATE VIEW test AS SELECT * FROM b_table join c_table";

        let current_view_info_value = DeserializedValueWithBytes::from_inner(ViewInfoValue::new(
            logical_plan.clone(),
            table_names,
            columns,
            plan_columns,
            definition.to_string(),
        ));
        // should be ok.
        table_metadata_manager
            .update_view_info(
                view_id,
                &current_view_info_value,
                new_logical_plan.clone(),
                new_table_names.clone(),
                new_columns.clone(),
                new_plan_columns.clone(),
                new_definition.to_string(),
            )
            .await
            .unwrap();
        // if table info was updated, it should be ok.
        table_metadata_manager
            .update_view_info(
                view_id,
                &current_view_info_value,
                new_logical_plan.clone(),
                new_table_names.clone(),
                new_columns.clone(),
                new_plan_columns.clone(),
                new_definition.to_string(),
            )
            .await
            .unwrap();

        // updated view_info should equal the `new_logical_plan`
        let updated_view_info = table_metadata_manager
            .view_info_manager()
            .get(view_id)
            .await
            .unwrap()
            .unwrap()
            .into_inner();
        assert_eq!(updated_view_info.view_info, new_logical_plan);
        assert_eq!(updated_view_info.table_names, new_table_names);
        assert_eq!(updated_view_info.definition, new_definition);
        assert_eq!(updated_view_info.columns, new_columns);
        assert_eq!(updated_view_info.plan_columns, new_plan_columns);

        let wrong_view_info = logical_plan.clone();
        let wrong_definition = "wrong_definition";
        let wrong_view_info_value =
            DeserializedValueWithBytes::from_inner(current_view_info_value.update(
                wrong_view_info,
                new_table_names.clone(),
                new_columns.clone(),
                new_plan_columns.clone(),
                wrong_definition.to_string(),
            ));
        // if the current_view_info_value is wrong, it should return an error.
        // The ABA problem.
        assert!(table_metadata_manager
            .update_view_info(
                view_id,
                &wrong_view_info_value,
                new_logical_plan.clone(),
                new_table_names.clone(),
                vec!["c".to_string()],
                vec!["number3".to_string()],
                wrong_definition.to_string(),
            )
            .await
            .is_err());

        // The view_info is not changed.
        let current_view_info = table_metadata_manager
            .view_info_manager()
            .get(view_id)
            .await
            .unwrap()
            .unwrap()
            .into_inner();
        assert_eq!(current_view_info.view_info, new_logical_plan);
        assert_eq!(current_view_info.table_names, new_table_names);
        assert_eq!(current_view_info.definition, new_definition);
        assert_eq!(current_view_info.columns, new_columns);
        assert_eq!(current_view_info.plan_columns, new_plan_columns);
    }
}
