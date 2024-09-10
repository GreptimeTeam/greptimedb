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
use std::fmt::Display;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};
use store_api::storage::{RegionId, RegionNumber};
use table::metadata::TableId;

use crate::error::{
    InvalidMetadataSnafu, MetadataCorruptionSnafu, Result, SerdeJsonSnafu, TableRouteNotFoundSnafu,
    UnexpectedLogicalRouteTableSnafu,
};
use crate::key::node_address::{NodeAddressKey, NodeAddressValue};
use crate::key::txn_helper::TxnOpGetResponseSet;
use crate::key::{
    DeserializedValueWithBytes, MetadataKey, MetadataValue, RegionDistribution,
    TABLE_ROUTE_KEY_PATTERN, TABLE_ROUTE_PREFIX,
};
use crate::kv_backend::txn::Txn;
use crate::kv_backend::KvBackendRef;
use crate::rpc::router::{region_distribution, RegionRoute};
use crate::rpc::store::BatchGetRequest;

/// The key stores table routes
///
/// The layout: `__table_route/{table_id}`.
#[derive(Debug, PartialEq)]
pub struct TableRouteKey {
    pub table_id: TableId,
}

impl TableRouteKey {
    pub fn new(table_id: TableId) -> Self {
        Self { table_id }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TableRouteValue {
    Physical(PhysicalTableRouteValue),
    Logical(LogicalTableRouteValue),
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, Default)]
pub struct PhysicalTableRouteValue {
    pub region_routes: Vec<RegionRoute>,
    version: u64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct LogicalTableRouteValue {
    physical_table_id: TableId,
    region_ids: Vec<RegionId>,
}

impl TableRouteValue {
    /// Returns a [TableRouteValue::Physical] if `table_id` equals `physical_table_id`.
    /// Otherwise returns a [TableRouteValue::Logical].
    pub(crate) fn new(
        table_id: TableId,
        physical_table_id: TableId,
        region_routes: Vec<RegionRoute>,
    ) -> Self {
        if table_id == physical_table_id {
            TableRouteValue::physical(region_routes)
        } else {
            let region_routes = region_routes
                .into_iter()
                .map(|region| {
                    debug_assert_eq!(region.region.id.table_id(), physical_table_id);
                    RegionId::new(table_id, region.region.id.region_number())
                })
                .collect();
            TableRouteValue::logical(physical_table_id, region_routes)
        }
    }

    pub fn physical(region_routes: Vec<RegionRoute>) -> Self {
        Self::Physical(PhysicalTableRouteValue::new(region_routes))
    }

    pub fn logical(physical_table_id: TableId, region_ids: Vec<RegionId>) -> Self {
        Self::Logical(LogicalTableRouteValue::new(physical_table_id, region_ids))
    }

    /// Returns a new version [TableRouteValue] with `region_routes`.
    pub fn update(&self, region_routes: Vec<RegionRoute>) -> Result<Self> {
        ensure!(
            self.is_physical(),
            UnexpectedLogicalRouteTableSnafu {
                err_msg: format!("{self:?} is a non-physical TableRouteValue."),
            }
        );
        let version = self.as_physical_table_route_ref().version;
        Ok(Self::Physical(PhysicalTableRouteValue {
            region_routes,
            version: version + 1,
        }))
    }

    /// Returns the version.
    ///
    /// For test purpose.
    #[cfg(any(test, feature = "testing"))]
    pub fn version(&self) -> Result<u64> {
        ensure!(
            self.is_physical(),
            UnexpectedLogicalRouteTableSnafu {
                err_msg: format!("{self:?} is a non-physical TableRouteValue."),
            }
        );
        Ok(self.as_physical_table_route_ref().version)
    }

    /// Returns the corresponding [RegionRoute], returns `None` if it's the specific region is not found.
    ///
    /// Note: It throws an error if it's a logical table
    pub fn region_route(&self, region_id: RegionId) -> Result<Option<RegionRoute>> {
        ensure!(
            self.is_physical(),
            UnexpectedLogicalRouteTableSnafu {
                err_msg: format!("{self:?} is a non-physical TableRouteValue."),
            }
        );
        Ok(self
            .as_physical_table_route_ref()
            .region_routes
            .iter()
            .find(|route| route.region.id == region_id)
            .cloned())
    }

    /// Returns true if it's [TableRouteValue::Physical].
    pub fn is_physical(&self) -> bool {
        matches!(self, TableRouteValue::Physical(_))
    }

    /// Gets the [RegionRoute]s of this [TableRouteValue::Physical].
    pub fn region_routes(&self) -> Result<&Vec<RegionRoute>> {
        ensure!(
            self.is_physical(),
            UnexpectedLogicalRouteTableSnafu {
                err_msg: format!("{self:?} is a non-physical TableRouteValue."),
            }
        );
        Ok(&self.as_physical_table_route_ref().region_routes)
    }

    /// Returns the reference of [`PhysicalTableRouteValue`].
    ///
    /// # Panic
    /// If it is not the [`PhysicalTableRouteValue`].
    fn as_physical_table_route_ref(&self) -> &PhysicalTableRouteValue {
        match self {
            TableRouteValue::Physical(x) => x,
            _ => unreachable!("Mistakenly been treated as a Physical TableRoute: {self:?}"),
        }
    }

    /// Converts to [`PhysicalTableRouteValue`].
    ///
    /// # Panic
    /// If it is not the [`PhysicalTableRouteValue`].
    pub fn into_physical_table_route(self) -> PhysicalTableRouteValue {
        match self {
            TableRouteValue::Physical(x) => x,
            _ => unreachable!("Mistakenly been treated as a Physical TableRoute: {self:?}"),
        }
    }

    pub fn region_numbers(&self) -> Vec<RegionNumber> {
        match self {
            TableRouteValue::Physical(x) => x
                .region_routes
                .iter()
                .map(|region_route| region_route.region.id.region_number())
                .collect(),
            TableRouteValue::Logical(x) => x
                .region_ids()
                .iter()
                .map(|region_id| region_id.region_number())
                .collect(),
        }
    }
}

impl MetadataValue for TableRouteValue {
    fn try_from_raw_value(raw_value: &[u8]) -> Result<Self> {
        let r = serde_json::from_slice::<TableRouteValue>(raw_value);
        match r {
            // Compatible with old TableRouteValue.
            Err(e) if e.is_data() => Ok(Self::Physical(
                serde_json::from_slice::<PhysicalTableRouteValue>(raw_value)
                    .context(SerdeJsonSnafu)?,
            )),
            Ok(x) => Ok(x),
            Err(e) => Err(e).context(SerdeJsonSnafu),
        }
    }

    fn try_as_raw_value(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).context(SerdeJsonSnafu)
    }
}

impl PhysicalTableRouteValue {
    pub fn new(region_routes: Vec<RegionRoute>) -> Self {
        Self {
            region_routes,
            version: 0,
        }
    }
}

impl LogicalTableRouteValue {
    pub fn new(physical_table_id: TableId, region_ids: Vec<RegionId>) -> Self {
        Self {
            physical_table_id,
            region_ids,
        }
    }

    pub fn physical_table_id(&self) -> TableId {
        self.physical_table_id
    }

    pub fn region_ids(&self) -> &Vec<RegionId> {
        &self.region_ids
    }
}

impl<'a> MetadataKey<'a, TableRouteKey> for TableRouteKey {
    fn to_bytes(&self) -> Vec<u8> {
        self.to_string().into_bytes()
    }

    fn from_bytes(bytes: &[u8]) -> Result<TableRouteKey> {
        let key = std::str::from_utf8(bytes).map_err(|e| {
            InvalidMetadataSnafu {
                err_msg: format!(
                    "TableRouteKey '{}' is not a valid UTF8 string: {e}",
                    String::from_utf8_lossy(bytes)
                ),
            }
            .build()
        })?;
        let captures = TABLE_ROUTE_KEY_PATTERN
            .captures(key)
            .context(InvalidMetadataSnafu {
                err_msg: format!("Invalid TableRouteKey '{key}'"),
            })?;
        // Safety: pass the regex check above
        let table_id = captures[1].parse::<TableId>().unwrap();
        Ok(TableRouteKey { table_id })
    }
}

impl Display for TableRouteKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", TABLE_ROUTE_PREFIX, self.table_id)
    }
}

pub type TableRouteManagerRef = Arc<TableRouteManager>;

pub struct TableRouteManager {
    storage: TableRouteStorage,
}

impl TableRouteManager {
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self {
            storage: TableRouteStorage::new(kv_backend),
        }
    }

    /// Returns the [`PhysicalTableRouteValue`] in the first level,
    /// It won't follow the [`LogicalTableRouteValue`] to find the next level [`PhysicalTableRouteValue`].
    ///
    /// Returns an error if the first level value is not a [`PhysicalTableRouteValue`].
    pub async fn try_get_physical_table_route(
        &self,
        table_id: TableId,
    ) -> Result<Option<PhysicalTableRouteValue>> {
        match self.storage.get(table_id).await? {
            Some(route) => {
                ensure!(
                    route.is_physical(),
                    UnexpectedLogicalRouteTableSnafu {
                        err_msg: format!("{route:?} is a non-physical TableRouteValue.")
                    }
                );
                Ok(Some(route.into_physical_table_route()))
            }
            None => Ok(None),
        }
    }

    /// Returns the [TableId] recursively.
    ///
    /// Returns a [TableRouteNotFound](crate::error::Error::TableRouteNotFound) Error if:
    /// - the table(`logical_or_physical_table_id`) does not exist.
    pub async fn get_physical_table_id(
        &self,
        logical_or_physical_table_id: TableId,
    ) -> Result<TableId> {
        let table_route = self
            .storage
            .get_inner(logical_or_physical_table_id)
            .await?
            .context(TableRouteNotFoundSnafu {
                table_id: logical_or_physical_table_id,
            })?;

        match table_route {
            TableRouteValue::Physical(_) => Ok(logical_or_physical_table_id),
            TableRouteValue::Logical(x) => Ok(x.physical_table_id()),
        }
    }

    /// Returns the [TableRouteValue::Physical] recursively.
    ///
    /// Returns a [TableRouteNotFound](error::Error::TableRouteNotFound) Error if:
    /// - the physical table(`logical_or_physical_table_id`) does not exist
    /// - the corresponding physical table of the logical table(`logical_or_physical_table_id`) does not exist.
    pub async fn get_physical_table_route(
        &self,
        logical_or_physical_table_id: TableId,
    ) -> Result<(TableId, PhysicalTableRouteValue)> {
        let table_route = self
            .storage
            .get(logical_or_physical_table_id)
            .await?
            .context(TableRouteNotFoundSnafu {
                table_id: logical_or_physical_table_id,
            })?;

        match table_route {
            TableRouteValue::Physical(x) => Ok((logical_or_physical_table_id, x)),
            TableRouteValue::Logical(x) => {
                let physical_table_id = x.physical_table_id();
                let physical_table_route = self.storage.get(physical_table_id).await?.context(
                    TableRouteNotFoundSnafu {
                        table_id: physical_table_id,
                    },
                )?;
                let physical_table_route = physical_table_route.into_physical_table_route();
                Ok((physical_table_id, physical_table_route))
            }
        }
    }

    /// Returns the [TableRouteValue::Physical] recursively.
    ///
    /// Returns a [TableRouteNotFound](crate::error::Error::TableRouteNotFound) Error if:
    /// - one of the logical tables corresponding to the physical table does not exist.
    ///
    /// **Notes**: it may return a subset of `logical_or_physical_table_ids`.
    pub async fn batch_get_physical_table_routes(
        &self,
        logical_or_physical_table_ids: &[TableId],
    ) -> Result<HashMap<TableId, PhysicalTableRouteValue>> {
        let table_routes = self
            .storage
            .batch_get(logical_or_physical_table_ids)
            .await?;
        // Returns a subset of `logical_or_physical_table_ids`.
        let table_routes = table_routes
            .into_iter()
            .zip(logical_or_physical_table_ids)
            .filter_map(|(route, id)| route.map(|route| (*id, route)))
            .collect::<HashMap<_, _>>();

        let mut physical_table_routes = HashMap::with_capacity(table_routes.len());
        let mut logical_table_ids = HashMap::with_capacity(table_routes.len());

        for (table_id, table_route) in table_routes {
            match table_route {
                TableRouteValue::Physical(x) => {
                    physical_table_routes.insert(table_id, x);
                }
                TableRouteValue::Logical(x) => {
                    logical_table_ids.insert(table_id, x.physical_table_id());
                }
            }
        }

        if logical_table_ids.is_empty() {
            return Ok(physical_table_routes);
        }

        // Finds the logical tables corresponding to the physical tables.
        let physical_table_ids = logical_table_ids
            .values()
            .cloned()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let table_routes = self
            .table_route_storage()
            .batch_get(&physical_table_ids)
            .await?;
        let table_routes = table_routes
            .into_iter()
            .zip(physical_table_ids)
            .filter_map(|(route, id)| route.map(|route| (id, route)))
            .collect::<HashMap<_, _>>();

        for (logical_table_id, physical_table_id) in logical_table_ids {
            let table_route =
                table_routes
                    .get(&physical_table_id)
                    .context(TableRouteNotFoundSnafu {
                        table_id: physical_table_id,
                    })?;
            match table_route {
                TableRouteValue::Physical(x) => {
                    physical_table_routes.insert(logical_table_id, x.clone());
                }
                TableRouteValue::Logical(x) => {
                    // Never get here, because we use a physical table id cannot obtain a logical table.
                    MetadataCorruptionSnafu {
                        err_msg: format!(
                            "logical table {} {:?} cannot be resolved to a physical table.",
                            logical_table_id, x
                        ),
                    }
                    .fail()?;
                }
            }
        }

        Ok(physical_table_routes)
    }

    /// Returns [`RegionDistribution`] of the table(`table_id`).
    pub async fn get_region_distribution(
        &self,
        table_id: TableId,
    ) -> Result<Option<RegionDistribution>> {
        self.storage
            .get(table_id)
            .await?
            .map(|table_route| Ok(region_distribution(table_route.region_routes()?)))
            .transpose()
    }

    /// Returns low-level APIs.
    pub fn table_route_storage(&self) -> &TableRouteStorage {
        &self.storage
    }
}

/// Low-level operations of [TableRouteValue].
pub struct TableRouteStorage {
    kv_backend: KvBackendRef,
}

impl TableRouteStorage {
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self { kv_backend }
    }

    /// Builds a create table route transaction,
    /// it expected the `__table_route/{table_id}` wasn't occupied.
    pub fn build_create_txn(
        &self,
        table_id: TableId,
        table_route_value: &TableRouteValue,
    ) -> Result<(
        Txn,
        impl FnOnce(
            &mut TxnOpGetResponseSet,
        ) -> Result<Option<DeserializedValueWithBytes<TableRouteValue>>>,
    )> {
        let key = TableRouteKey::new(table_id);
        let raw_key = key.to_bytes();

        let txn = Txn::put_if_not_exists(raw_key.clone(), table_route_value.try_as_raw_value()?);

        Ok((
            txn,
            TxnOpGetResponseSet::decode_with(TxnOpGetResponseSet::filter(raw_key)),
        ))
    }

    /// Builds a update table route transaction,
    /// it expected the remote value equals the `current_table_route_value`.
    /// It retrieves the latest value if the comparing failed.
    pub(crate) fn build_update_txn(
        &self,
        table_id: TableId,
        current_table_route_value: &DeserializedValueWithBytes<TableRouteValue>,
        new_table_route_value: &TableRouteValue,
    ) -> Result<(
        Txn,
        impl FnOnce(
            &mut TxnOpGetResponseSet,
        ) -> Result<Option<DeserializedValueWithBytes<TableRouteValue>>>,
    )> {
        let key = TableRouteKey::new(table_id);
        let raw_key = key.to_bytes();
        let raw_value = current_table_route_value.get_raw_bytes();
        let new_raw_value: Vec<u8> = new_table_route_value.try_as_raw_value()?;

        let txn = Txn::compare_and_put(raw_key.clone(), raw_value, new_raw_value);

        Ok((
            txn,
            TxnOpGetResponseSet::decode_with(TxnOpGetResponseSet::filter(raw_key)),
        ))
    }

    /// Returns the [`TableRouteValue`].
    pub async fn get(&self, table_id: TableId) -> Result<Option<TableRouteValue>> {
        let mut table_route = self.get_inner(table_id).await?;
        if let Some(table_route) = &mut table_route {
            self.remap_route_address(table_route).await?;
        };

        Ok(table_route)
    }

    async fn get_inner(&self, table_id: TableId) -> Result<Option<TableRouteValue>> {
        let key = TableRouteKey::new(table_id);
        self.kv_backend
            .get(&key.to_bytes())
            .await?
            .map(|kv| TableRouteValue::try_from_raw_value(&kv.value))
            .transpose()
    }

    /// Returns the [`TableRouteValue`] wrapped with [`DeserializedValueWithBytes`].
    pub async fn get_with_raw_bytes(
        &self,
        table_id: TableId,
    ) -> Result<Option<DeserializedValueWithBytes<TableRouteValue>>> {
        let mut table_route = self.get_with_raw_bytes_inner(table_id).await?;
        if let Some(table_route) = &mut table_route {
            self.remap_route_address(table_route).await?;
        };

        Ok(table_route)
    }

    async fn get_with_raw_bytes_inner(
        &self,
        table_id: TableId,
    ) -> Result<Option<DeserializedValueWithBytes<TableRouteValue>>> {
        let key = TableRouteKey::new(table_id);
        self.kv_backend
            .get(&key.to_bytes())
            .await?
            .map(|kv| DeserializedValueWithBytes::from_inner_slice(&kv.value))
            .transpose()
    }

    /// Returns the physical `DeserializedValueWithBytes<TableRouteValue>` recursively.
    ///
    /// Returns a [TableRouteNotFound](crate::error::Error::TableRouteNotFound) Error if:
    /// - the physical table(`logical_or_physical_table_id`) does not exist
    /// - the corresponding physical table of the logical table(`logical_or_physical_table_id`) does not exist.
    pub async fn get_physical_table_route_with_raw_bytes(
        &self,
        logical_or_physical_table_id: TableId,
    ) -> Result<(TableId, DeserializedValueWithBytes<TableRouteValue>)> {
        let table_route = self
            .get_with_raw_bytes(logical_or_physical_table_id)
            .await?
            .context(TableRouteNotFoundSnafu {
                table_id: logical_or_physical_table_id,
            })?;

        match table_route.get_inner_ref() {
            TableRouteValue::Physical(_) => Ok((logical_or_physical_table_id, table_route)),
            TableRouteValue::Logical(x) => {
                let physical_table_id = x.physical_table_id();
                let physical_table_route = self
                    .get_with_raw_bytes(physical_table_id)
                    .await?
                    .context(TableRouteNotFoundSnafu {
                        table_id: physical_table_id,
                    })?;
                Ok((physical_table_id, physical_table_route))
            }
        }
    }

    /// Returns batch of [`TableRouteValue`] that respects the order of `table_ids`.
    pub async fn batch_get(&self, table_ids: &[TableId]) -> Result<Vec<Option<TableRouteValue>>> {
        let mut table_routes = self.batch_get_inner(table_ids).await?;
        self.remap_routes_addresses(&mut table_routes).await?;

        Ok(table_routes)
    }

    async fn batch_get_inner(&self, table_ids: &[TableId]) -> Result<Vec<Option<TableRouteValue>>> {
        let keys = table_ids
            .iter()
            .map(|id| TableRouteKey::new(*id).to_bytes())
            .collect::<Vec<_>>();
        let resp = self
            .kv_backend
            .batch_get(BatchGetRequest { keys: keys.clone() })
            .await?;

        let kvs = resp
            .kvs
            .into_iter()
            .map(|kv| (kv.key, kv.value))
            .collect::<HashMap<_, _>>();
        keys.into_iter()
            .map(|key| {
                if let Some(value) = kvs.get(&key) {
                    Ok(Some(TableRouteValue::try_from_raw_value(value)?))
                } else {
                    Ok(None)
                }
            })
            .collect()
    }

    async fn remap_routes_addresses(
        &self,
        table_routes: &mut [Option<TableRouteValue>],
    ) -> Result<()> {
        let keys = table_routes
            .iter()
            .flat_map(|table_route| {
                table_route
                    .as_ref()
                    .map(extract_address_keys)
                    .unwrap_or_default()
            })
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let node_addrs = self.get_node_addresses(keys).await?;
        for table_route in table_routes.iter_mut().flatten() {
            set_addresses(&node_addrs, table_route)?;
        }

        Ok(())
    }

    async fn remap_route_address(&self, table_route: &mut TableRouteValue) -> Result<()> {
        let keys = extract_address_keys(table_route).into_iter().collect();
        let node_addrs = self.get_node_addresses(keys).await?;
        set_addresses(&node_addrs, table_route)?;

        Ok(())
    }

    async fn get_node_addresses(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<HashMap<u64, NodeAddressValue>> {
        if keys.is_empty() {
            return Ok(HashMap::default());
        }

        self.kv_backend
            .batch_get(BatchGetRequest { keys })
            .await?
            .kvs
            .into_iter()
            .map(|kv| {
                let node_id = NodeAddressKey::from_bytes(&kv.key)?.node_id;
                let node_addr = NodeAddressValue::try_from_raw_value(&kv.value)?;
                Ok((node_id, node_addr))
            })
            .collect()
    }
}

fn set_addresses(
    node_addrs: &HashMap<u64, NodeAddressValue>,
    table_route: &mut TableRouteValue,
) -> Result<()> {
    let TableRouteValue::Physical(physical_table_route) = table_route else {
        return Ok(());
    };

    for region_route in &mut physical_table_route.region_routes {
        if let Some(leader) = &mut region_route.leader_peer {
            if let Some(node_addr) = node_addrs.get(&leader.id) {
                leader.addr = node_addr.peer.addr.clone();
            }
        }
        for follower in &mut region_route.follower_peers {
            if let Some(node_addr) = node_addrs.get(&follower.id) {
                follower.addr = node_addr.peer.addr.clone();
            }
        }
    }

    Ok(())
}

fn extract_address_keys(table_route: &TableRouteValue) -> HashSet<Vec<u8>> {
    let TableRouteValue::Physical(physical_table_route) = table_route else {
        return HashSet::default();
    };

    physical_table_route
        .region_routes
        .iter()
        .flat_map(|region_route| {
            region_route
                .follower_peers
                .iter()
                .map(|peer| NodeAddressKey::with_datanode(peer.id).to_bytes())
                .chain(
                    region_route
                        .leader_peer
                        .as_ref()
                        .map(|leader| NodeAddressKey::with_datanode(leader.id).to_bytes()),
                )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::kv_backend::memory::MemoryKvBackend;
    use crate::kv_backend::{KvBackend, TxnService};
    use crate::peer::Peer;
    use crate::rpc::store::PutRequest;

    #[test]
    fn test_table_route_compatibility() {
        let old_raw_v = r#"{"region_routes":[{"region":{"id":1,"name":"r1","partition":null,"attrs":{}},"leader_peer":{"id":2,"addr":"a2"},"follower_peers":[]},{"region":{"id":1,"name":"r1","partition":null,"attrs":{}},"leader_peer":{"id":2,"addr":"a2"},"follower_peers":[]}],"version":0}"#;
        let v = TableRouteValue::try_from_raw_value(old_raw_v.as_bytes()).unwrap();

        let new_raw_v = format!("{:?}", v);
        assert_eq!(
            new_raw_v,
            r#"Physical(PhysicalTableRouteValue { region_routes: [RegionRoute { region: Region { id: 1(0, 1), name: "r1", partition: None, attrs: {} }, leader_peer: Some(Peer { id: 2, addr: "a2" }), follower_peers: [], leader_status: None, leader_down_since: None }, RegionRoute { region: Region { id: 1(0, 1), name: "r1", partition: None, attrs: {} }, leader_peer: Some(Peer { id: 2, addr: "a2" }), follower_peers: [], leader_status: None, leader_down_since: None }], version: 0 })"#
        );
    }

    #[test]
    fn test_key_serialization() {
        let key = TableRouteKey::new(42);
        let raw_key = key.to_bytes();
        assert_eq!(raw_key, b"__table_route/42");
    }

    #[test]
    fn test_key_deserialization() {
        let expected = TableRouteKey::new(42);
        let key = TableRouteKey::from_bytes(b"__table_route/42").unwrap();
        assert_eq!(key, expected);
    }

    #[tokio::test]
    async fn test_table_route_storage_get_with_raw_bytes_empty() {
        let kv = Arc::new(MemoryKvBackend::default());
        let table_route_storage = TableRouteStorage::new(kv);
        let table_route = table_route_storage.get_with_raw_bytes(1024).await.unwrap();
        assert!(table_route.is_none());
    }

    #[tokio::test]
    async fn test_table_route_storage_get_with_raw_bytes() {
        let kv = Arc::new(MemoryKvBackend::default());
        let table_route_storage = TableRouteStorage::new(kv.clone());
        let table_route = table_route_storage.get_with_raw_bytes(1024).await.unwrap();
        assert!(table_route.is_none());
        let table_route_manager = TableRouteManager::new(kv.clone());
        let table_route_value = TableRouteValue::Logical(LogicalTableRouteValue {
            physical_table_id: 1023,
            region_ids: vec![RegionId::new(1023, 1)],
        });
        let (txn, _) = table_route_manager
            .table_route_storage()
            .build_create_txn(1024, &table_route_value)
            .unwrap();
        let r = kv.txn(txn).await.unwrap();
        assert!(r.succeeded);
        let table_route = table_route_storage.get_with_raw_bytes(1024).await.unwrap();
        assert!(table_route.is_some());
        let got = table_route.unwrap().inner;
        assert_eq!(got, table_route_value);
    }

    #[tokio::test]
    async fn test_table_route_batch_get() {
        let kv = Arc::new(MemoryKvBackend::default());
        let table_route_storage = TableRouteStorage::new(kv.clone());
        let routes = table_route_storage
            .batch_get(&[1023, 1024, 1025])
            .await
            .unwrap();

        assert!(routes.iter().all(Option::is_none));
        let table_route_manager = TableRouteManager::new(kv.clone());
        let routes = [
            (
                1024,
                TableRouteValue::Logical(LogicalTableRouteValue {
                    physical_table_id: 1023,
                    region_ids: vec![RegionId::new(1023, 1)],
                }),
            ),
            (
                1025,
                TableRouteValue::Logical(LogicalTableRouteValue {
                    physical_table_id: 1023,
                    region_ids: vec![RegionId::new(1023, 2)],
                }),
            ),
        ];
        for (table_id, route) in &routes {
            let (txn, _) = table_route_manager
                .table_route_storage()
                .build_create_txn(*table_id, route)
                .unwrap();
            let r = kv.txn(txn).await.unwrap();
            assert!(r.succeeded);
        }

        let results = table_route_storage
            .batch_get(&[9999, 1025, 8888, 1024])
            .await
            .unwrap();
        assert!(results[0].is_none());
        assert_eq!(results[1].as_ref().unwrap(), &routes[1].1);
        assert!(results[2].is_none());
        assert_eq!(results[3].as_ref().unwrap(), &routes[0].1);
    }

    #[tokio::test]
    async fn remap_route_address_updates_addresses() {
        let kv = Arc::new(MemoryKvBackend::default());
        let table_route_storage = TableRouteStorage::new(kv.clone());
        let mut table_route = TableRouteValue::Physical(PhysicalTableRouteValue {
            region_routes: vec![RegionRoute {
                leader_peer: Some(Peer {
                    id: 1,
                    ..Default::default()
                }),
                follower_peers: vec![Peer {
                    id: 2,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            version: 0,
        });

        kv.put(PutRequest {
            key: NodeAddressKey::with_datanode(1).to_bytes(),
            value: NodeAddressValue {
                peer: Peer {
                    addr: "addr1".to_string(),
                    ..Default::default()
                },
            }
            .try_as_raw_value()
            .unwrap(),
            ..Default::default()
        })
        .await
        .unwrap();

        table_route_storage
            .remap_route_address(&mut table_route)
            .await
            .unwrap();

        if let TableRouteValue::Physical(physical_table_route) = table_route {
            assert_eq!(
                physical_table_route.region_routes[0]
                    .leader_peer
                    .as_ref()
                    .unwrap()
                    .addr,
                "addr1"
            );
            assert_eq!(
                physical_table_route.region_routes[0].follower_peers[0].addr,
                ""
            );
        } else {
            panic!("Expected PhysicalTableRouteValue");
        }
    }
}
