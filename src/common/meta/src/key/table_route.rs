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
use snafu::ensure;
use table::metadata::TableId;

use crate::error::{Result, UnexpectedSnafu};
use crate::key::{to_removed_key, TableMetaKey};
use crate::kv_backend::KvBackendRef;
use crate::rpc::router::{RegionRoute, Table, TableRoute};
use crate::rpc::store::{CompareAndPutRequest, MoveValueRequest};

pub const TABLE_ROUTE_PREFIX: &str = "__meta_table_route";

pub const NEXT_TABLE_ROUTE_PREFIX: &str = "__table_route";

// TODO(weny): Renames it to TableRouteKey.
pub struct NextTableRouteKey {
    table_id: TableId,
}

impl NextTableRouteKey {
    pub fn new(table_id: TableId) -> Self {
        Self { table_id }
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

    pub async fn get(&self, key: &NextTableRouteKey) -> Result<Option<TableRoute>> {
        self.kv_backend
            .get(&key.as_raw_key())
            .await?
            .map(|kv| TableRoute::try_from_raw_value(kv.value))
            .transpose()
    }

    // Creates TableRoute key and value. If the key already exists, check whether the value is the same.
    pub async fn create(&self, table: Table, region_routes: Vec<RegionRoute>) -> Result<()> {
        let key = NextTableRouteKey::new(table.id as u32);
        let val = TableRoute::new(table, region_routes);
        let req = CompareAndPutRequest::new()
            .with_key(key.as_raw_key())
            .with_value(val.try_as_raw_value()?);

        self.kv_backend.compare_and_put(req).await?.handle(|resp| {
            if !resp.success {
                let Some(cur) = resp
                    .prev_kv
                    .map(|kv|TableRoute::try_from_raw_value(kv.value))
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
        expect: Option<TableRoute>,
        table_route: TableRoute,
    ) -> Result<std::result::Result<(), Option<TableRoute>>> {
        let key = NextTableRouteKey::new(table_id);
        let raw_key = key.as_raw_key();

        let expect = if let Some(x) = expect {
            x.try_as_raw_value()?
        } else {
            vec![]
        };
        let raw_value = table_route.try_as_raw_value()?;

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
                    .map(|x| TableRoute::try_from_raw_value(x.value))
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
    use api::v1::meta::TableName;

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
        let table_name = TableName {
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
