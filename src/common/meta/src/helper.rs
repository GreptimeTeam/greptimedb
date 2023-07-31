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
use std::fmt::{Display, Formatter};

use common_catalog::error::{
    DeserializeCatalogEntryValueSnafu, Error, InvalidCatalogSnafu, SerializeCatalogEntryValueSnafu,
};
use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};
use table::metadata::{RawTableInfo, TableId};

pub const CATALOG_KEY_PREFIX: &str = "__c";
pub const SCHEMA_KEY_PREFIX: &str = "__s";

/// The pattern of a valid catalog, schema or table name.
const NAME_PATTERN: &str = "[a-zA-Z_:][a-zA-Z0-9_:]*";

lazy_static! {
    static ref CATALOG_KEY_PATTERN: Regex =
        Regex::new(&format!("^{CATALOG_KEY_PREFIX}-({NAME_PATTERN})$")).unwrap();
}

lazy_static! {
    static ref SCHEMA_KEY_PATTERN: Regex = Regex::new(&format!(
        "^{SCHEMA_KEY_PREFIX}-({NAME_PATTERN})-({NAME_PATTERN})$"
    ))
    .unwrap();
}

pub fn build_catalog_prefix() -> String {
    format!("{CATALOG_KEY_PREFIX}-")
}

pub fn build_schema_prefix(catalog_name: impl AsRef<str>) -> String {
    format!("{SCHEMA_KEY_PREFIX}-{}-", catalog_name.as_ref())
}

/// Table global info contains necessary info for a datanode to create table regions, including
/// table id, table meta(schema...), region id allocation across datanodes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableGlobalValue {
    /// Id of datanode that created the global table info kv. only for debugging.
    pub node_id: u64,
    /// Allocation of region ids across all datanodes.
    pub regions_id_map: HashMap<u64, Vec<u32>>,
    pub table_info: RawTableInfo,
}

impl TableGlobalValue {
    pub fn table_id(&self) -> TableId {
        self.table_info.ident.table_id
    }
}

pub struct CatalogKey {
    pub catalog_name: String,
}

impl Display for CatalogKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(CATALOG_KEY_PREFIX)?;
        f.write_str("-")?;
        f.write_str(&self.catalog_name)
    }
}

impl CatalogKey {
    pub fn parse(s: impl AsRef<str>) -> Result<Self, Error> {
        let key = s.as_ref();
        let captures = CATALOG_KEY_PATTERN
            .captures(key)
            .context(InvalidCatalogSnafu { key })?;
        ensure!(captures.len() == 2, InvalidCatalogSnafu { key });
        Ok(Self {
            catalog_name: captures[1].to_string(),
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CatalogValue;

pub struct SchemaKey {
    pub catalog_name: String,
    pub schema_name: String,
}

impl Display for SchemaKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(SCHEMA_KEY_PREFIX)?;
        f.write_str("-")?;
        f.write_str(&self.catalog_name)?;
        f.write_str("-")?;
        f.write_str(&self.schema_name)
    }
}

impl SchemaKey {
    pub fn parse(s: impl AsRef<str>) -> Result<Self, Error> {
        let key = s.as_ref();
        let captures = SCHEMA_KEY_PATTERN
            .captures(key)
            .context(InvalidCatalogSnafu { key })?;
        ensure!(captures.len() == 3, InvalidCatalogSnafu { key });
        Ok(Self {
            catalog_name: captures[1].to_string(),
            schema_name: captures[2].to_string(),
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SchemaValue;

macro_rules! define_catalog_value {
    ( $($val_ty: ty), *) => {
            $(
                impl $val_ty {
                    pub fn parse(s: impl AsRef<str>) -> Result<Self, Error> {
                        serde_json::from_str(s.as_ref())
                            .context(DeserializeCatalogEntryValueSnafu { raw: s.as_ref() })
                    }

                    pub fn from_bytes(bytes: impl AsRef<[u8]>) -> Result<Self, Error> {
                         Self::parse(&String::from_utf8_lossy(bytes.as_ref()))
                    }

                    pub fn as_bytes(&self) -> Result<Vec<u8>, Error> {
                        Ok(serde_json::to_string(self)
                            .context(SerializeCatalogEntryValueSnafu)?
                            .into_bytes())
                    }
                }
            )*
        }
}

define_catalog_value!(TableGlobalValue, CatalogValue, SchemaValue);

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_parse_catalog_key() {
        let key = "__c-C";
        let catalog_key = CatalogKey::parse(key).unwrap();
        assert_eq!("C", catalog_key.catalog_name);
        assert_eq!(key, catalog_key.to_string());
    }

    #[test]
    fn test_parse_schema_key() {
        let key = "__s-C-S";
        let schema_key = SchemaKey::parse(key).unwrap();
        assert_eq!("C", schema_key.catalog_name);
        assert_eq!("S", schema_key.schema_name);
        assert_eq!(key, schema_key.to_string());
    }
}
