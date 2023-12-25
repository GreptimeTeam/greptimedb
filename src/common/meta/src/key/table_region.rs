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

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::storage::RegionNumber;
use table::metadata::TableId;

use super::TABLE_REGION_KEY_PREFIX;
use crate::error::{Result, SerdeJsonSnafu};
use crate::key::TableMetaKey;
use crate::{impl_table_meta_key, impl_table_meta_value, DatanodeId};

pub type RegionDistribution = BTreeMap<DatanodeId, Vec<RegionNumber>>;

#[deprecated(
    since = "0.4.0",
    note = "Please use the TableRouteManager's get_region_distribution method instead"
)]
pub struct TableRegionKey {
    table_id: TableId,
}

impl TableRegionKey {
    pub fn new(table_id: TableId) -> Self {
        Self { table_id }
    }
}

impl TableMetaKey for TableRegionKey {
    fn as_raw_key(&self) -> Vec<u8> {
        format!("{}/{}", TABLE_REGION_KEY_PREFIX, self.table_id).into_bytes()
    }
}

impl_table_meta_key! {TableRegionKey}

#[deprecated(
    since = "0.4.0",
    note = "Please use the TableRouteManager's get_region_distribution method instead"
)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableRegionValue {
    pub region_distribution: RegionDistribution,
    version: u64,
}

impl TableRegionValue {
    pub fn new(region_distribution: RegionDistribution) -> Self {
        Self {
            region_distribution,
            version: 0,
        }
    }
}

impl_table_meta_value! {TableRegionValue}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key::TableMetaValue;

    #[test]
    fn test_serde() {
        let key = TableRegionKey::new(1);
        let raw_key = key.as_raw_key();
        assert_eq!(raw_key, b"__table_region/1");

        let value = TableRegionValue {
            region_distribution: RegionDistribution::from([(1, vec![1, 2, 3]), (2, vec![4, 5, 6])]),
            version: 0,
        };
        let literal = br#"{"region_distribution":{"1":[1,2,3],"2":[4,5,6]},"version":0}"#;

        assert_eq!(value.try_as_raw_value().unwrap(), literal);
        assert_eq!(
            TableRegionValue::try_from_raw_value(literal).unwrap(),
            value,
        );
    }
}
