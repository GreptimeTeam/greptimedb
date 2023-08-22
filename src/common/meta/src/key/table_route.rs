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

use api::v1::meta::TableName;
use serde::{Deserialize, Serialize};
use table::metadata::TableId;

use crate::error::Result;
use crate::key::{to_removed_key, RegionDistribution, TableMetaKey};
use crate::kv_backend::txn::{Compare, CompareOp, Txn, TxnOp, TxnOpResponse};
use crate::kv_backend::KvBackendRef;
use crate::rpc::router::{region_distribution, RegionRoute};

pub const TABLE_ROUTE_PREFIX: &str = "__meta_table_route";

pub const NEXT_TABLE_ROUTE_PREFIX: &str = "__table_route";

// TODO(weny): Renames it to TableRouteKey.
pub struct NextTableRouteKey {
    pub table_id: TableId,
}

impl NextTableRouteKey {
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

impl TableMetaKey for NextTableRouteKey {
    fn as_raw_key(&self) -> Vec<u8> {
        self.to_string().into_bytes()
    }
}

impl Display for NextTableRouteKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", NEXT_TABLE_ROUTE_PREFIX, self.table_id)
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
        impl FnOnce(&Vec<TxnOpResponse>) -> Result<Option<TableRouteValue>>,
    ) {
        let key = NextTableRouteKey::new(table_id);
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
        impl FnOnce(&Vec<TxnOpResponse>) -> Result<Option<TableRouteValue>>,
    )> {
        let key = NextTableRouteKey::new(table_id);
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
        current_table_route_value: &TableRouteValue,
        new_table_route_value: &TableRouteValue,
    ) -> Result<(
        Txn,
        impl FnOnce(&Vec<TxnOpResponse>) -> Result<Option<TableRouteValue>>,
    )> {
        let key = NextTableRouteKey::new(table_id);
        let raw_key = key.as_raw_key();
        let raw_value = current_table_route_value.try_as_raw_value()?;
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
        table_route_value: &TableRouteValue,
    ) -> Result<Txn> {
        let key = NextTableRouteKey::new(table_id);
        let raw_key = key.as_raw_key();
        let raw_value = table_route_value.try_as_raw_value()?;
        let removed_key = to_removed_key(&String::from_utf8_lossy(&raw_key));

        let txn = Txn::new().and_then(vec![
            TxnOp::Delete(raw_key),
            TxnOp::Put(removed_key.into_bytes(), raw_value),
        ]);

        Ok(txn)
    }

    fn build_decode_fn(
        raw_key: Vec<u8>,
    ) -> impl FnOnce(&Vec<TxnOpResponse>) -> Result<Option<TableRouteValue>> {
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
                .map(|kv| TableRouteValue::try_from_raw_value(&kv.value))
                .transpose()
        }
    }

    pub async fn get(&self, table_id: TableId) -> Result<Option<TableRouteValue>> {
        let key = NextTableRouteKey::new(table_id);
        self.kv_backend
            .get(&key.as_raw_key())
            .await?
            .map(|kv| TableRouteValue::try_from_raw_value(&kv.value))
            .transpose()
    }

    #[cfg(test)]
    pub async fn get_removed(&self, table_id: TableId) -> Result<Option<TableRouteValue>> {
        let key = NextTableRouteKey::new(table_id).to_string();
        let removed_key = to_removed_key(&key).into_bytes();
        self.kv_backend
            .get(&removed_key)
            .await?
            .map(|x| TableRouteValue::try_from_raw_value(&x.value))
            .transpose()
    }

    pub async fn get_region_distribution(
        &self,
        table_id: TableId,
    ) -> Result<Option<RegionDistribution>> {
        self.get(table_id)
            .await?
            .map(|table_route| region_distribution(&table_route.region_routes))
            .transpose()
    }
}

#[deprecated(since = "0.4.0", note = "Please use the NextTableRouteKey instead")]
#[derive(Copy, Clone)]
pub struct TableRouteKey<'a> {
    pub table_id: TableId,
    pub catalog_name: &'a str,
    pub schema_name: &'a str,
    pub table_name: &'a str,
}

impl<'a> TableRouteKey<'a> {
    pub fn with_table_name(table_id: TableId, t: &'a TableName) -> Self {
        Self {
            table_id,
            catalog_name: &t.catalog_name,
            schema_name: &t.schema_name,
            table_name: &t.table_name,
        }
    }

    pub fn prefix(&self) -> String {
        format!(
            "{}-{}-{}-{}",
            TABLE_ROUTE_PREFIX, self.catalog_name, self.schema_name, self.table_name
        )
    }

    pub fn removed_key(&self) -> String {
        to_removed_key(&self.to_string())
    }
}

impl<'a> Display for TableRouteKey<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.prefix(), self.table_id)
    }
}

#[cfg(test)]
mod tests {

    use api::v1::meta::TableName as PbTableName;

    use super::TableRouteKey;

    #[test]
    fn test_table_route_key() {
        let key = TableRouteKey {
            table_id: 123,
            catalog_name: "greptime",
            schema_name: "public",
            table_name: "demo",
        };

        let prefix = key.prefix();
        assert_eq!("__meta_table_route-greptime-public-demo", prefix);

        let key_string = key.to_string();
        assert_eq!("__meta_table_route-greptime-public-demo-123", key_string);

        let removed = key.removed_key();
        assert_eq!(
            "__removed-__meta_table_route-greptime-public-demo-123",
            removed
        );
    }

    #[test]
    fn test_with_table_name() {
        let table_name = PbTableName {
            catalog_name: "greptime".to_string(),
            schema_name: "public".to_string(),
            table_name: "demo".to_string(),
        };

        let key = TableRouteKey::with_table_name(123, &table_name);

        assert_eq!(123, key.table_id);
        assert_eq!("greptime", key.catalog_name);
        assert_eq!("public", key.schema_name);
        assert_eq!("demo", key.table_name);
    }
}
