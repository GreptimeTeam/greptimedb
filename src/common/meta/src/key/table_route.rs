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
use std::fmt::Display;

use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};
use store_api::storage::{RegionId, RegionNumber};
use table::metadata::TableId;

use super::{DeserializedValueWithBytes, TableMetaValue};
use crate::error::{
    Result, SerdeJsonSnafu, TableRouteNotFoundSnafu, UnexpectedLogicalRouteTableSnafu,
};
use crate::key::{to_removed_key, RegionDistribution, TableMetaKey, TABLE_ROUTE_PREFIX};
use crate::kv_backend::txn::{Compare, CompareOp, Txn, TxnOp, TxnOpResponse};
use crate::kv_backend::KvBackendRef;
use crate::rpc::router::{region_distribution, RegionRoute};
use crate::rpc::store::BatchGetRequest;

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

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
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
    pub fn physical(region_routes: Vec<RegionRoute>) -> Self {
        Self::Physical(PhysicalTableRouteValue::new(region_routes))
    }

    /// Returns a new version [TableRouteValue] with `region_routes`.
    pub fn update(&self, region_routes: Vec<RegionRoute>) -> Result<Self> {
        ensure!(
            self.is_physical(),
            UnexpectedLogicalRouteTableSnafu {
                err_msg: format!("{self:?} is a non-physical TableRouteValue."),
            }
        );
        let version = self.physical_table_route().version;
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
        Ok(self.physical_table_route().version)
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
            .physical_table_route()
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
        Ok(&self.physical_table_route().region_routes)
    }

    fn physical_table_route(&self) -> &PhysicalTableRouteValue {
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
                .collect::<Vec<_>>(),
            TableRouteValue::Logical(x) => x
                .region_ids()
                .iter()
                .map(|region_id| region_id.region_number())
                .collect::<Vec<_>>(),
        }
    }
}

impl TableMetaValue for TableRouteValue {
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

impl TableMetaKey for TableRouteKey {
    fn as_raw_key(&self) -> Vec<u8> {
        self.to_string().into_bytes()
    }
}

impl Display for TableRouteKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", TABLE_ROUTE_PREFIX, self.table_id)
    }
}

pub struct TableRouteManager {
    kv_backend: KvBackendRef,
}

impl TableRouteManager {
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self { kv_backend }
    }

    pub(crate) fn build_get_txn(
        &self,
        table_id: TableId,
    ) -> (
        Txn,
        impl FnOnce(&Vec<TxnOpResponse>) -> Result<Option<DeserializedValueWithBytes<TableRouteValue>>>,
    ) {
        let key = TableRouteKey::new(table_id);
        let raw_key = key.as_raw_key();
        let txn = Txn::new().and_then(vec![TxnOp::Get(raw_key.clone())]);

        (txn, Self::build_decode_fn(raw_key))
    }

    /// Builds a create table route transaction. it expected the `__table_route/{table_id}` wasn't occupied.
    pub(crate) fn build_create_txn(
        &self,
        table_id: TableId,
        table_route_value: &TableRouteValue,
    ) -> Result<(
        Txn,
        impl FnOnce(&Vec<TxnOpResponse>) -> Result<Option<DeserializedValueWithBytes<TableRouteValue>>>,
    )> {
        let key = TableRouteKey::new(table_id);
        let raw_key = key.as_raw_key();

        let txn = Txn::new()
            .when(vec![Compare::with_not_exist_value(
                raw_key.clone(),
                CompareOp::Equal,
            )])
            .and_then(vec![TxnOp::Put(
                raw_key.clone(),
                table_route_value.try_as_raw_value()?,
            )])
            .or_else(vec![TxnOp::Get(raw_key.clone())]);

        Ok((txn, Self::build_decode_fn(raw_key)))
    }

    /// Builds a update table route transaction, it expected the remote value equals the `current_table_route_value`.
    /// It retrieves the latest value if the comparing failed.
    pub(crate) fn build_update_txn(
        &self,
        table_id: TableId,
        current_table_route_value: &DeserializedValueWithBytes<TableRouteValue>,
        new_table_route_value: &TableRouteValue,
    ) -> Result<(
        Txn,
        impl FnOnce(&Vec<TxnOpResponse>) -> Result<Option<DeserializedValueWithBytes<TableRouteValue>>>,
    )> {
        let key = TableRouteKey::new(table_id);
        let raw_key = key.as_raw_key();
        let raw_value = current_table_route_value.into_bytes();
        let new_raw_value: Vec<u8> = new_table_route_value.try_as_raw_value()?;

        let txn = Txn::new()
            .when(vec![Compare::with_value(
                raw_key.clone(),
                CompareOp::Equal,
                raw_value,
            )])
            .and_then(vec![TxnOp::Put(raw_key.clone(), new_raw_value)])
            .or_else(vec![TxnOp::Get(raw_key.clone())]);

        Ok((txn, Self::build_decode_fn(raw_key)))
    }

    /// Builds a delete table route transaction, it expected the remote value equals the `table_route_value`.
    pub(crate) fn build_delete_txn(
        &self,
        table_id: TableId,
        table_route_value: &DeserializedValueWithBytes<TableRouteValue>,
    ) -> Result<Txn> {
        let key = TableRouteKey::new(table_id);
        let raw_key = key.as_raw_key();
        let raw_value = table_route_value.into_bytes();
        let removed_key = to_removed_key(&String::from_utf8_lossy(&raw_key));

        let txn = Txn::new().and_then(vec![
            TxnOp::Delete(raw_key),
            TxnOp::Put(removed_key.into_bytes(), raw_value),
        ]);

        Ok(txn)
    }

    fn build_decode_fn(
        raw_key: Vec<u8>,
    ) -> impl FnOnce(&Vec<TxnOpResponse>) -> Result<Option<DeserializedValueWithBytes<TableRouteValue>>>
    {
        move |response: &Vec<TxnOpResponse>| {
            response
                .iter()
                .filter_map(|resp| {
                    if let TxnOpResponse::ResponseGet(r) = resp {
                        Some(r)
                    } else {
                        None
                    }
                })
                .flat_map(|r| &r.kvs)
                .find(|kv| kv.key == raw_key)
                .map(|kv| DeserializedValueWithBytes::from_inner_slice(&kv.value))
                .transpose()
        }
    }

    pub async fn get(
        &self,
        table_id: TableId,
    ) -> Result<Option<DeserializedValueWithBytes<TableRouteValue>>> {
        let key = TableRouteKey::new(table_id);
        self.kv_backend
            .get(&key.as_raw_key())
            .await?
            .map(|kv| DeserializedValueWithBytes::from_inner_slice(&kv.value))
            .transpose()
    }

    pub async fn get_physical_table_id(
        &self,
        logical_or_physical_table_id: TableId,
    ) -> Result<TableId> {
        let table_route = self
            .get(logical_or_physical_table_id)
            .await?
            .context(TableRouteNotFoundSnafu {
                table_id: logical_or_physical_table_id,
            })?
            .into_inner();

        match table_route {
            TableRouteValue::Physical(_) => Ok(logical_or_physical_table_id),
            TableRouteValue::Logical(x) => Ok(x.physical_table_id()),
        }
    }

    pub async fn get_physical_table_route(
        &self,
        logical_or_physical_table_id: TableId,
    ) -> Result<(TableId, PhysicalTableRouteValue)> {
        let table_route = self
            .get(logical_or_physical_table_id)
            .await?
            .context(TableRouteNotFoundSnafu {
                table_id: logical_or_physical_table_id,
            })?
            .into_inner();

        match table_route {
            TableRouteValue::Physical(x) => Ok((logical_or_physical_table_id, x)),
            TableRouteValue::Logical(x) => {
                let physical_table_id = x.physical_table_id();
                let physical_table_route =
                    self.get(physical_table_id)
                        .await?
                        .context(TableRouteNotFoundSnafu {
                            table_id: physical_table_id,
                        })?;
                Ok((
                    physical_table_id,
                    physical_table_route.physical_table_route().clone(),
                ))
            }
        }
    }

    /// It may return a subset of the `table_ids`.
    pub async fn batch_get(
        &self,
        table_ids: &[TableId],
    ) -> Result<HashMap<TableId, TableRouteValue>> {
        let lookup_table = table_ids
            .iter()
            .map(|id| (TableRouteKey::new(*id).as_raw_key(), id))
            .collect::<HashMap<_, _>>();

        let resp = self
            .kv_backend
            .batch_get(BatchGetRequest {
                keys: lookup_table.keys().cloned().collect::<Vec<_>>(),
            })
            .await?;

        let values = resp
            .kvs
            .iter()
            .map(|kv| {
                Ok((
                    // Safety: must exist.
                    **lookup_table.get(kv.key()).unwrap(),
                    TableRouteValue::try_from_raw_value(&kv.value)?,
                ))
            })
            .collect::<Result<HashMap<_, _>>>()?;

        Ok(values)
    }

    #[cfg(test)]
    pub async fn get_removed(
        &self,
        table_id: TableId,
    ) -> Result<Option<DeserializedValueWithBytes<TableRouteValue>>> {
        let key = TableRouteKey::new(table_id).to_string();
        let removed_key = to_removed_key(&key).into_bytes();
        self.kv_backend
            .get(&removed_key)
            .await?
            .map(|x| DeserializedValueWithBytes::from_inner_slice(&x.value))
            .transpose()
    }

    pub async fn get_region_distribution(
        &self,
        table_id: TableId,
    ) -> Result<Option<RegionDistribution>> {
        self.get(table_id)
            .await?
            .map(|table_route| Ok(region_distribution(table_route.region_routes()?)))
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_route_compatibility() {
        let old_raw_v = r#"{"region_routes":[{"region":{"id":1,"name":"r1","partition":null,"attrs":{}},"leader_peer":{"id":2,"addr":"a2"},"follower_peers":[]},{"region":{"id":1,"name":"r1","partition":null,"attrs":{}},"leader_peer":{"id":2,"addr":"a2"},"follower_peers":[]}],"version":0}"#;
        let v = TableRouteValue::try_from_raw_value(old_raw_v.as_bytes()).unwrap();

        let new_raw_v = format!("{:?}", v);
        assert_eq!(
            new_raw_v,
            r#"Physical(PhysicalTableRouteValue { region_routes: [RegionRoute { region: Region { id: 1(0, 1), name: "r1", partition: None, attrs: {} }, leader_peer: Some(Peer { id: 2, addr: "a2" }), follower_peers: [], leader_status: None }, RegionRoute { region: Region { id: 1(0, 1), name: "r1", partition: None, attrs: {} }, leader_peer: Some(Peer { id: 2, addr: "a2" }), follower_peers: [], leader_status: None }], version: 0 })"#
        );
    }
}
