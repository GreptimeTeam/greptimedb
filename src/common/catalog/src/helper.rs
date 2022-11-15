use std::collections::HashMap;
use std::fmt::{Display, Formatter};

use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Serialize, Serializer};
use snafu::{ensure, OptionExt, ResultExt};
use table::metadata::{RawTableMeta, TableId, TableVersion};

use crate::consts::{
    CATALOG_KEY_PREFIX, SCHEMA_KEY_PREFIX, TABLE_GLOBAL_KEY_PREFIX, TABLE_REGIONAL_KEY_PREFIX,
};
use crate::error::{
    DeserializeCatalogEntryValueSnafu, Error, InvalidCatalogSnafu, SerializeCatalogEntryValueSnafu,
};

lazy_static! {
    static ref CATALOG_KEY_PATTERN: Regex =
        Regex::new(&format!("^{}-([a-zA-Z_]+)$", CATALOG_KEY_PREFIX)).unwrap();
}

lazy_static! {
    static ref SCHEMA_KEY_PATTERN: Regex = Regex::new(&format!(
        "^{}-([a-zA-Z_]+)-([a-zA-Z_]+)$",
        SCHEMA_KEY_PREFIX
    ))
    .unwrap();
}

lazy_static! {
    static ref TABLE_GLOBAL_KEY_PATTERN: Regex = Regex::new(&format!(
        "^{}-([a-zA-Z_]+)-([a-zA-Z_]+)-([a-zA-Z0-9_]+)$",
        TABLE_GLOBAL_KEY_PREFIX
    ))
    .unwrap();
}

lazy_static! {
    static ref TABLE_REGIONAL_KEY_PATTERN: Regex = Regex::new(&format!(
        "^{}-([a-zA-Z_]+)-([a-zA-Z_]+)-([a-zA-Z0-9_]+)-([0-9]+)$",
        TABLE_REGIONAL_KEY_PREFIX
    ))
    .unwrap();
}

pub fn build_catalog_prefix() -> String {
    format!("{}-", CATALOG_KEY_PREFIX)
}

pub fn build_schema_prefix(catalog_name: impl AsRef<str>) -> String {
    format!("{}-{}-", SCHEMA_KEY_PREFIX, catalog_name.as_ref())
}

pub fn build_table_global_prefix(
    catalog_name: impl AsRef<str>,
    schema_name: impl AsRef<str>,
) -> String {
    format!(
        "{}-{}-{}-",
        TABLE_GLOBAL_KEY_PREFIX,
        catalog_name.as_ref(),
        schema_name.as_ref()
    )
}

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
}

/// Table global info contains necessary info for a datanode to create table regions, including
/// table id, table meta(schema...), region id allocation across datanodes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableGlobalValue {
    /// Table id is the same across all datanodes.
    pub id: TableId,
    /// Id of datanode that created the global table info kv. only for debugging.
    pub node_id: u64,
    // TODO(LFC): Maybe remove it?
    /// Allocation of region ids across all datanodes.
    pub regions_id_map: HashMap<u64, Vec<u32>>,
    // TODO(LFC): Too much for assembling the table schema that DistTable needs, find another way.
    pub meta: RawTableMeta,
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
    pub version: TableVersion,
    pub regions_ids: Vec<u32>,
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

        let value = TableGlobalValue {
            id: 42,
            node_id: 0,
            regions_id_map: HashMap::from([(0, vec![1, 2, 3])]),
            meta,
        };
        let serialized = serde_json::to_string(&value).unwrap();
        let deserialized = TableGlobalValue::parse(&serialized).unwrap();
        assert_eq!(value, deserialized);
    }
}
