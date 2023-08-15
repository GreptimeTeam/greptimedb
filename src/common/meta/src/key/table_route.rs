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
use snafu::ensure;
use table::metadata::TableId;

use crate::error::{Result, UnexpectedSnafu};
use crate::key::{to_removed_key, TableMetaKey};
use crate::kv_backend::txn::{Compare, CompareOp, TxnOp, TxnRequest};
use crate::kv_backend::KvBackendRef;
use crate::rpc::router::{region_distribution, RegionRoute, Table, TableRoute};
use crate::rpc::store::{BatchGetRequest, CompareAndPutRequest, MoveValueRequest};
use crate::rpc::KeyValue;

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

    /// Builds a create table route transaction. it expected the `__table_route/{table_id}` wasn't occupied.
    pub(crate) fn build_create_txn(
        &self,
        txn: &mut TxnRequest,
        table_id: TableId,
        table_route_value: &TableRouteValue,
    ) -> Result<()> {
        let key = NextTableRouteKey::new(table_id);
        let raw_key = key.as_raw_key();

        txn.compare.push(Compare::with_not_exist_value(
            raw_key.clone(),
            CompareOp::Equal,
        ));

        txn.success
            .push(TxnOp::Put(raw_key, table_route_value.try_as_raw_value()?));

        Ok(())
    }

    /// Builds a delete table info transaction, it expected the remote value equals the `current_table_info_value`.
    pub(crate) fn build_update_txn(
        &self,
        txn: &mut TxnRequest,
        table_id: TableId,
        current_table_route_value: &TableRouteValue,
        new_table_route_value: &TableRouteValue,
    ) -> Result<()> {
        let key = NextTableRouteKey::new(table_id);
        let raw_key = key.as_raw_key();
        let raw_value = current_table_route_value.try_as_raw_value()?;

        txn.compare.push(Compare::with_value(
            raw_key.clone(),
            CompareOp::Equal,
            raw_value.clone(),
        ));

        let new_raw_value: Vec<u8> = new_table_route_value.try_as_raw_value()?;

        txn.success.push(TxnOp::Put(raw_key, new_raw_value));

        Ok(())
    }

    /// Builds a delete table info transaction, it expected the remote value equals the `table_info_value`.
    pub(crate) fn build_delete_txn(
        &self,
        txn: &mut TxnRequest,
        table_id: TableId,
        table_route_value: &TableRouteValue,
    ) -> Result<()> {
        let key = NextTableRouteKey::new(table_id);
        let raw_key = key.as_raw_key();
        let raw_value = table_route_value.try_as_raw_value()?;
        let removed_key = to_removed_key(&String::from_utf8_lossy(&raw_key));

        txn.success.push(TxnOp::Delete(raw_key));
        txn.success
            .push(TxnOp::Put(removed_key.into_bytes(), raw_value));

        Ok(())
    }

    pub(crate) fn build_batch_get(
        &self,
        batch: &mut BatchGetRequest,
        table_id: TableId,
    ) -> impl FnOnce(&Vec<KeyValue>) -> Result<Option<TableRouteValue>> {
        let key = NextTableRouteKey::new(table_id);
        let raw_key = key.as_raw_key();
        batch.keys.push(raw_key.clone());

        move |kvs: &Vec<KeyValue>| {
            kvs.iter()
                .find(|kv| kv.key == raw_key)
                .map(|kv| TableRouteValue::try_from_raw_value_ref(&kv.value))
                .transpose()
        }
    }

    pub async fn get(&self, table_id: TableId) -> Result<Option<TableRouteValue>> {
        let key = NextTableRouteKey::new(table_id);
        self.kv_backend
            .get(&key.as_raw_key())
            .await?
            .map(|kv| TableRouteValue::try_from_raw_value(kv.value))
            .transpose()
    }

    // Creates TableRoute key and value. If the key already exists, check whether the value is the same.
    pub async fn create(&self, table_id: TableId, region_routes: Vec<RegionRoute>) -> Result<()> {
        let key = NextTableRouteKey::new(table_id);
        let val = TableRouteValue::new(region_routes);
        let req = CompareAndPutRequest::new()
            .with_key(key.as_raw_key())
            .with_value(val.try_as_raw_value()?);

        self.kv_backend.compare_and_put(req).await?.handle(|resp| {
            if !resp.success {
                let Some(cur) = resp
                    .prev_kv
                    .map(|kv|TableRouteValue::try_from_raw_value(kv.value))
                    .transpose()?
                else {
                    return UnexpectedSnafu {
                        err_msg: format!("compare_and_put expect None but failed with current value None, key: {key}, val: {val:?}"),
                    }.fail();
                };

                ensure!(
                    cur==val,
                    UnexpectedSnafu {
                        err_msg: format!("current value '{cur:?}' already existed for key '{key}', {val:?} is not set"),
                    }
                );
            }
            Ok(())
        })
    }

    /// Compares and puts value of key. `expect` is the expected value, if backend's current value associated
    /// with key is the same as `expect`, the value will be updated to `val`.
    ///
    /// - If the compare-and-set operation successfully updated value, this method will return an `Ok(Ok())`
    /// - If associated value is not the same as `expect`, no value will be updated and an
    ///   `Ok(Err(Option<TableRoute>))` will be returned. The `Option<TableRoute>` indicates
    ///   the current associated value of key.
    /// - If any error happens during operation, an `Err(Error)` will be returned.
    pub async fn compare_and_put(
        &self,
        table_id: TableId,
        expect: Option<TableRouteValue>,
        region_routes: Vec<RegionRoute>,
    ) -> Result<std::result::Result<(), Option<TableRouteValue>>> {
        let key = NextTableRouteKey::new(table_id);
        let raw_key = key.as_raw_key();

        let (expect, version) = if let Some(x) = expect {
            (x.try_as_raw_value()?, x.version + 1)
        } else {
            (vec![], 0)
        };
        let value = TableRouteValue {
            region_routes,
            version,
        };
        let raw_value = value.try_as_raw_value()?;

        let req = CompareAndPutRequest::new()
            .with_key(raw_key)
            .with_expect(expect)
            .with_value(raw_value);

        self.kv_backend.compare_and_put(req).await?.handle(|resp| {
            Ok(if resp.success {
                Ok(())
            } else {
                Err(resp
                    .prev_kv
                    .map(|x| TableRouteValue::try_from_raw_value(x.value))
                    .transpose()?)
            })
        })
    }

    pub async fn remove(&self, table_id: TableId) -> Result<()> {
        let key = NextTableRouteKey::new(table_id).as_raw_key();
        let removed_key = to_removed_key(&String::from_utf8_lossy(&key));
        let req = MoveValueRequest::new(key, removed_key.as_bytes());
        self.kv_backend.move_value(req).await?;
        Ok(())
    }
}

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
    use std::sync::Arc;

    use api::v1::meta::TableName as PbTableName;

    use super::TableRouteKey;
    use crate::key::table_route::{TableRouteManager, TableRouteValue};
    use crate::kv_backend::memory::MemoryKvBackend;
    use crate::rpc::router::{RegionRoute, Table, TableRoute};
    use crate::table_name::TableName;

    #[tokio::test]
    async fn test_table_route_manager() {
        let mgr = TableRouteManager::new(Arc::new(MemoryKvBackend::default()));

        let table_id = 1024u32;
        let table = Table {
            id: table_id as u64,
            table_name: TableName::new("foo", "bar", "baz"),
            table_schema: b"mock schema".to_vec(),
        };
        let region_route = RegionRoute::default();
        let region_routes = vec![region_route];

        mgr.create(table_id, region_routes.clone()).await.unwrap();

        let got = mgr.get(1024).await.unwrap().unwrap();

        assert_eq!(got.region_routes, region_routes);

        let empty = mgr.get(1023).await.unwrap();
        assert!(empty.is_none());

        let expect = TableRouteValue::new(region_routes);

        let mut updated = expect.clone();
        updated.region_routes.push(RegionRoute::default());

        mgr.compare_and_put(1024, Some(expect.clone()), updated.region_routes.clone())
            .await
            .unwrap();

        mgr.compare_and_put(1024, Some(expect.clone()), updated.region_routes)
            .await
            .unwrap();
    }

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
