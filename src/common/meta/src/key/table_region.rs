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
use std::fmt::Display;

use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use store_api::storage::RegionNumber;
use table::metadata::TableId;

use super::{MetaKey, TABLE_REGION_KEY_PREFIX};
use crate::error::{InvalidTableMetadataSnafu, Result, SerdeJsonSnafu};
use crate::{impl_table_meta_value, DatanodeId};

pub type RegionDistribution = BTreeMap<DatanodeId, Vec<RegionNumber>>;

#[deprecated(
    since = "0.4.0",
    note = "Please use the TableRouteManager's get_region_distribution method instead"
)]
pub struct TableRegionKey {
    table_id: TableId,
}

lazy_static! {
    static ref TABLE_REGION_KEY_PATTERN: Regex =
        Regex::new(&format!("^{TABLE_REGION_KEY_PREFIX}/([0-9]+)$")).unwrap();
}

impl TableRegionKey {
    pub fn new(table_id: TableId) -> Self {
        Self { table_id }
    }
}

impl Display for TableRegionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", TABLE_REGION_KEY_PREFIX, self.table_id)
    }
}

impl<'a> MetaKey<'a, TableRegionKey> for TableRegionKey {
    fn to_bytes(&self) -> Vec<u8> {
        self.to_string().into_bytes()
    }

    fn from_bytes(bytes: &'a [u8]) -> Result<TableRegionKey> {
        let key = std::str::from_utf8(bytes).map_err(|e| {
            InvalidTableMetadataSnafu {
                err_msg: format!(
                    "TableRegionKey '{}' is not a valid UTF8 string: {e}",
                    String::from_utf8_lossy(bytes)
                ),
            }
            .build()
        })?;
        let captures =
            TABLE_REGION_KEY_PATTERN
                .captures(key)
                .context(InvalidTableMetadataSnafu {
                    err_msg: format!("Invalid TableRegionKey '{key}'"),
                })?;
        // Safety: pass the regex check above
        let table_id = captures[1].parse::<TableId>().unwrap();
        Ok(TableRegionKey { table_id })
    }
}

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
        let raw_key = key.to_bytes();
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
