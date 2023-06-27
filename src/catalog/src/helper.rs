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
use serde::{Deserialize, Serialize, Serializer};
use snafu::{ensure, OptionExt, ResultExt};
use table::metadata::{RawTableInfo, TableId, TableVersion};

pub const CATALOG_KEY_PREFIX: &str = "__c";
pub const SCHEMA_KEY_PREFIX: &str = "__s";
pub const TABLE_GLOBAL_KEY_PREFIX: &str = "__tg";
pub const TABLE_REGIONAL_KEY_PREFIX: &str = "__tr";

const ALPHANUMERICS_NAME_PATTERN: &str = "[a-zA-Z_][a-zA-Z0-9_]*";

lazy_static! {
    static ref CATALOG_KEY_PATTERN: Regex = Regex::new(&format!(
        "^{CATALOG_KEY_PREFIX}-({ALPHANUMERICS_NAME_PATTERN})$"
    ))
    .unwrap();
}

lazy_static! {
    static ref SCHEMA_KEY_PATTERN: Regex = Regex::new(&format!(
        "^{SCHEMA_KEY_PREFIX}-({ALPHANUMERICS_NAME_PATTERN})-({ALPHANUMERICS_NAME_PATTERN})$"
    ))
    .unwrap();
}

lazy_static! {
    static ref TABLE_GLOBAL_KEY_PATTERN: Regex = Regex::new(&format!(
        "^{TABLE_GLOBAL_KEY_PREFIX}-({ALPHANUMERICS_NAME_PATTERN})-({ALPHANUMERICS_NAME_PATTERN})-({ALPHANUMERICS_NAME_PATTERN})$"
    ))
    .unwrap();
}

lazy_static! {
    static ref TABLE_REGIONAL_KEY_PATTERN: Regex = Regex::new(&format!(
        "^{TABLE_REGIONAL_KEY_PREFIX}-({ALPHANUMERICS_NAME_PATTERN})-({ALPHANUMERICS_NAME_PATTERN})-({ALPHANUMERICS_NAME_PATTERN})-([0-9]+)$"
    ))
    .unwrap();
}

pub fn build_catalog_prefix() -> String {
    format!("{CATALOG_KEY_PREFIX}-")
}

pub fn build_schema_prefix(catalog_name: impl AsRef<str>) -> String {
    format!("{SCHEMA_KEY_PREFIX}-{}-", catalog_name.as_ref())
}

/// Global table info has only one key across all datanodes so it does not have `node_id` field.
pub fn build_table_global_prefix(
    catalog_name: impl AsRef<str>,
    schema_name: impl AsRef<str>,
) -> String {
    format!(
        "{TABLE_GLOBAL_KEY_PREFIX}-{}-{}-",
        catalog_name.as_ref(),
        schema_name.as_ref()
    )
}

/// Regional table info varies between datanode, so it contains a `node_id` field.
pub fn build_table_regional_prefix(
    catalog_name: impl AsRef<str>,
    schema_name: impl AsRef<str>,
) -> String {
    format!(
        "{}-{}-{}-",
        TABLE_REGIONAL_KEY_PREFIX,
        catalog_name.as_ref(),
        schema_name.as_ref()
    )
}

/// Table global info has only one key across all datanodes so it does not have `node_id` field.
#[derive(Clone, Hash, Eq, PartialEq)]
pub struct TableGlobalKey {
    pub catalog_name: String,
    pub schema_name: String,
    pub table_name: String,
}

impl Display for TableGlobalKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(TABLE_GLOBAL_KEY_PREFIX)?;
        f.write_str("-")?;
        f.write_str(&self.catalog_name)?;
        f.write_str("-")?;
        f.write_str(&self.schema_name)?;
        f.write_str("-")?;
        f.write_str(&self.table_name)
    }
}

impl TableGlobalKey {
    pub fn parse<S: AsRef<str>>(s: S) -> Result<Self, Error> {
        let key = s.as_ref();
        let captures = TABLE_GLOBAL_KEY_PATTERN
            .captures(key)
            .context(InvalidCatalogSnafu { key })?;
        ensure!(captures.len() == 4, InvalidCatalogSnafu { key });

        Ok(Self {
            catalog_name: captures[1].to_string(),
            schema_name: captures[2].to_string(),
            table_name: captures[3].to_string(),
        })
    }

    pub fn to_raw_key(&self) -> Vec<u8> {
        self.to_string().into_bytes()
    }

    pub fn try_from_raw_key(key: &[u8]) -> Result<Self, Error> {
        Self::parse(String::from_utf8_lossy(key))
    }
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

    pub fn engine(&self) -> &str {
        &self.table_info.meta.engine
    }
}

/// Table regional info that varies between datanode, so it contains a `node_id` field.
pub struct TableRegionalKey {
    pub catalog_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub node_id: u64,
}

impl Display for TableRegionalKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(TABLE_REGIONAL_KEY_PREFIX)?;
        f.write_str("-")?;
        f.write_str(&self.catalog_name)?;
        f.write_str("-")?;
        f.write_str(&self.schema_name)?;
        f.write_str("-")?;
        f.write_str(&self.table_name)?;
        f.write_str("-")?;
        f.serialize_u64(self.node_id)
    }
}

impl TableRegionalKey {
    pub fn parse<S: AsRef<str>>(s: S) -> Result<Self, Error> {
        let key = s.as_ref();
        let captures = TABLE_REGIONAL_KEY_PATTERN
            .captures(key)
            .context(InvalidCatalogSnafu { key })?;
        ensure!(captures.len() == 5, InvalidCatalogSnafu { key });
        let node_id = captures[4]
            .to_string()
            .parse()
            .map_err(|_| InvalidCatalogSnafu { key }.build())?;
        Ok(Self {
            catalog_name: captures[1].to_string(),
            schema_name: captures[2].to_string(),
            table_name: captures[3].to_string(),
            node_id,
        })
    }
}

/// Regional table info of specific datanode, including table version on that datanode and
/// region ids allocated by metasrv.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TableRegionalValue {
    // We can remove the `Option` from the table id once all regional values
    // stored in meta have table ids.
    pub table_id: Option<TableId>,
    pub version: TableVersion,
    pub regions_ids: Vec<u32>,
    pub engine_name: Option<String>,
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

define_catalog_value!(
    TableRegionalValue,
    TableGlobalValue,
    CatalogValue,
    SchemaValue
);

#[cfg(test)]
mod tests {
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, RawSchema, Schema};
    use table::metadata::{RawTableMeta, TableIdent, TableType};

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

    #[test]
    fn test_parse_table_key() {
        let key = "__tg-C-S-T";
        let entry = TableGlobalKey::parse(key).unwrap();
        assert_eq!("C", entry.catalog_name);
        assert_eq!("S", entry.schema_name);
        assert_eq!("T", entry.table_name);
        assert_eq!(key, &entry.to_string());
    }

    #[test]
    fn test_build_prefix() {
        assert_eq!("__c-", build_catalog_prefix());
        assert_eq!("__s-CATALOG-", build_schema_prefix("CATALOG"));
        assert_eq!(
            "__tg-CATALOG-SCHEMA-",
            build_table_global_prefix("CATALOG", "SCHEMA")
        );
    }

    #[test]
    fn test_serialize_schema() {
        let schema = Schema::new(vec![ColumnSchema::new(
            "name",
            ConcreteDataType::string_datatype(),
            true,
        )]);

        let meta = RawTableMeta {
            schema: RawSchema::from(&schema),
            engine: "mito".to_string(),
            created_on: chrono::DateTime::default(),
            primary_key_indices: vec![0, 1],
            next_column_id: 3,
            engine_options: Default::default(),
            value_indices: vec![2, 3],
            options: Default::default(),
            region_numbers: vec![1],
        };

        let table_info = RawTableInfo {
            ident: TableIdent {
                table_id: 42,
                version: 1,
            },
            name: "table_1".to_string(),
            desc: Some("blah".to_string()),
            catalog_name: "catalog_1".to_string(),
            schema_name: "schema_1".to_string(),
            meta,
            table_type: TableType::Base,
        };

        let value = TableGlobalValue {
            node_id: 0,
            regions_id_map: HashMap::from([(0, vec![1, 2, 3])]),
            table_info,
        };
        let serialized = serde_json::to_string(&value).unwrap();
        let deserialized = TableGlobalValue::parse(serialized).unwrap();
        assert_eq!(value, deserialized);
    }

    #[test]
    fn test_table_global_value_compatibility() {
        let s = r#"{"node_id":1,"regions_id_map":{"1":[0]},"table_info":{"ident":{"table_id":1098,"version":1},"name":"container_cpu_limit","desc":"Created on insertion","catalog_name":"greptime","schema_name":"dd","meta":{"schema":{"column_schemas":[{"name":"container_id","data_type":{"String":null},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}},{"name":"container_name","data_type":{"String":null},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}},{"name":"docker_image","data_type":{"String":null},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}},{"name":"host","data_type":{"String":null},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}},{"name":"image_name","data_type":{"String":null},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}},{"name":"image_tag","data_type":{"String":null},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}},{"name":"interval","data_type":{"String":null},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}},{"name":"runtime","data_type":{"String":null},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}},{"name":"short_image","data_type":{"String":null},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}},{"name":"type","data_type":{"String":null},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}},{"name":"dd_value","data_type":{"Float64":{}},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}},{"name":"ts","data_type":{"Timestamp":{"Millisecond":null}},"is_nullable":false,"is_time_index":true,"default_constraint":null,"metadata":{"greptime:time_index":"true"}},{"name":"git.repository_url","data_type":{"String":null},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}}],"timestamp_index":11,"version":1},"primary_key_indices":[0,1,2,3,4,5,6,7,8,9,12],"value_indices":[10,11],"engine":"mito","next_column_id":12,"region_numbers":[],"engine_options":{},"options":{},"created_on":"1970-01-01T00:00:00Z"},"table_type":"Base"}}"#;
        TableGlobalValue::parse(s).unwrap();
    }
}
