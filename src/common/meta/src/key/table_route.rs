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

use std::fmt::Display;

use serde::{Deserialize, Serialize};
use table::metadata::TableId;

use super::DeserializedValueWithBytes;
use crate::error::Result;
use crate::key::{to_removed_key, RegionDistribution, TableMetaKey, TABLE_ROUTE_PREFIX};
use crate::kv_backend::txn::{Compare, CompareOp, Txn, TxnOp, TxnOpResponse};
use crate::kv_backend::KvBackendRef;
use crate::rpc::router::{region_distribution, RegionRoute};

pub struct TableRouteKey {
    pub table_id: TableId,
}

impl TableRouteKey {
    pub fn new(table_id: TableId) -> Self {
        Self { table_id }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct TableRouteValue {
    pub region_routes: Vec<RegionRoute>,
    version: u64,
}

impl TableRouteValue {
    pub fn new(region_routes: Vec<RegionRoute>) -> Self {
        Self {
            region_routes,
            version: 0,
        }
    }

    pub fn update(&self, region_routes: Vec<RegionRoute>) -> Self {
        Self {
            region_routes,
            version: self.version + 1,
        }
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
            .map(|table_route| region_distribution(&table_route.into_inner().region_routes))
            .transpose()
    }
}
