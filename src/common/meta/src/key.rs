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
//! 1. Table info key: `__table_info/{table_id}`
//!     - The value is a [TableInfoValue] struct; it contains the whole table info (like column
//!       schemas).
//!     - This key is mainly used in constructing the table in Datanode and Frontend.
//!
//! 2. Table region key: `__table_region/{table_id}`
//!     - The value is a [TableRegionValue] struct; it contains the region distribution of the
//!       table in the Datanodes.
//!
//! All keys have related managers. The managers take care of the serialization and deserialization
//! of keys and values, and the interaction with the underlying KV store backend.
//!
//! To simplify the managers used in struct fields and function parameters, we define a "unify"
//! table metadata manager: [TableMetadataManager]. It contains all the managers defined above.
//! It's recommended to just use this manager only.

pub mod table_info;
pub mod table_region;
mod table_route;

use std::sync::Arc;

use snafu::ResultExt;
use table_info::{TableInfoManager, TableInfoValue};
use table_region::{TableRegionManager, TableRegionValue};

use crate::error::{InvalidTableMetadataSnafu, Result, SerdeJsonSnafu};
pub use crate::key::table_route::{TableRouteKey, TABLE_ROUTE_PREFIX};
use crate::kv_backend::KvBackendRef;

pub const REMOVED_PREFIX: &str = "__removed";

const TABLE_INFO_KEY_PREFIX: &str = "__table_info";
const TABLE_REGION_KEY_PREFIX: &str = "__table_region";

pub fn to_removed_key(key: &str) -> String {
    format!("{REMOVED_PREFIX}-{key}")
}

pub trait TableMetaKey {
    fn as_raw_key(&self) -> Vec<u8>;
}

pub type TableMetadataManagerRef = Arc<TableMetadataManager>;

pub struct TableMetadataManager {
    table_info_manager: TableInfoManager,
    table_region_manager: TableRegionManager,
}

impl TableMetadataManager {
    pub fn new(kv_backend: KvBackendRef) -> Self {
        TableMetadataManager {
            table_info_manager: TableInfoManager::new(kv_backend.clone()),
            table_region_manager: TableRegionManager::new(kv_backend),
        }
    }

    pub fn table_info_manager(&self) -> &TableInfoManager {
        &self.table_info_manager
    }

    pub fn table_region_manager(&self) -> &TableRegionManager {
        &self.table_region_manager
    }
}

macro_rules! impl_table_meta_value {
    ( $($val_ty: ty), *) => {
        $(
            impl $val_ty {
                pub fn try_from_raw_value(raw_value: Vec<u8>) -> Result<Self> {
                    let raw_value = String::from_utf8(raw_value).map_err(|e| {
                        InvalidTableMetadataSnafu { err_msg: e.to_string() }.build()
                    })?;
                    serde_json::from_str(&raw_value).context(SerdeJsonSnafu)
                }

                pub fn try_as_raw_value(&self) -> Result<Vec<u8>> {
                    serde_json::to_string(self)
                        .map(|x| x.into_bytes())
                        .context(SerdeJsonSnafu)
                }
            }
        )*
    }
}

impl_table_meta_value! {
    TableInfoValue,
    TableRegionValue
}

#[cfg(test)]
mod tests {
    use crate::key::to_removed_key;

    #[test]
    fn test_to_removed_key() {
        let key = "test_key";
        let removed = "__removed-test_key";
        assert_eq!(removed, to_removed_key(key));
    }
}
