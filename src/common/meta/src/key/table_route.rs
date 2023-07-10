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
use table::metadata::TableId;

use crate::key::to_removed_key;

pub const TABLE_ROUTE_PREFIX: &str = "__meta_table_route";

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
