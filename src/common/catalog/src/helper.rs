use std::fmt::{Display, Formatter};
use std::str::FromStr;

use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Serialize, Serializer};
use snafu::{ensure, OptionExt, ResultExt};
use table::metadata::{TableId, TableMeta, TableVersion};

use crate::consts::{CATALOG_KEY_PREFIX, SCHEMA_KEY_PREFIX, TABLE_KEY_PREFIX};
use crate::error::{
    DeserializeCatalogEntryValueSnafu, Error, InvalidCatalogSnafu, SerializeCatalogEntryValueSnafu,
};

lazy_static! {
    static ref CATALOG_KEY_PATTERN: Regex = Regex::new(&format!(
        "^{}-([a-zA-Z_]+)-([a-zA-Z_]+)$",
        CATALOG_KEY_PREFIX
    ))
    .unwrap();
}

lazy_static! {
    static ref SCHEMA_KEY_PATTERN: Regex = Regex::new(&format!(
        "^{}-([a-zA-Z_]+)-([a-zA-Z_]+)-([a-zA-Z_]+)$",
        SCHEMA_KEY_PREFIX
    ))
    .unwrap();
}

lazy_static! {
    static ref TABLE_KEY_PATTERN: Regex = Regex::new(&format!(
        "^{}-([a-zA-Z_]+)-([a-zA-Z_]+)-([a-zA-Z_]+)-([0-9]+)-([a-zA-Z_]+)$",
        TABLE_KEY_PREFIX
    ))
    .unwrap();
}

pub fn build_catalog_prefix() -> String {
    format!("{}-", CATALOG_KEY_PREFIX)
}

pub fn build_schema_prefix(catalog_name: impl AsRef<str>) -> String {
    format!("{}-{}-", SCHEMA_KEY_PREFIX, catalog_name.as_ref())
}

pub fn build_table_prefix(catalog_name: impl AsRef<str>, schema_name: impl AsRef<str>) -> String {
    format!(
        "{}-{}-{}-",
        TABLE_KEY_PREFIX,
        catalog_name.as_ref(),
        schema_name.as_ref()
    )
}

pub struct TableKey {
    pub catalog_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub version: TableVersion,
    pub node_id: String,
}

impl Display for TableKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(TABLE_KEY_PREFIX)?;
        f.write_str("-")?;
        f.write_str(&self.catalog_name)?;
        f.write_str("-")?;
        f.write_str(&self.schema_name)?;
        f.write_str("-")?;
        f.write_str(&self.table_name)?;
        f.write_str("-")?;
        f.serialize_u64(self.version)?;
        f.write_str("-")?;
        f.write_str(&self.node_id)
    }
}

impl TableKey {
    pub fn parse<S: AsRef<str>>(s: S) -> Result<Self, Error> {
        let key = s.as_ref();
        let captures = TABLE_KEY_PATTERN
            .captures(key)
            .context(InvalidCatalogSnafu { key })?;
        ensure!(captures.len() == 6, InvalidCatalogSnafu { key });

        let version =
            u64::from_str(&captures[4]).map_err(|_| InvalidCatalogSnafu { key }.build())?;
        Ok(Self {
            catalog_name: captures[1].to_string(),
            schema_name: captures[2].to_string(),
            table_name: captures[3].to_string(),
            version,
            node_id: captures[5].to_string(),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableValue {
    pub id: TableId,
    pub node_id: String,
    pub meta: TableMeta,
}

impl TableValue {
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

pub struct CatalogKey {
    pub catalog_name: String,
    pub node_id: String,
}

impl Display for CatalogKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(CATALOG_KEY_PREFIX)?;
        f.write_str("-")?;
        f.write_str(&self.catalog_name)?;
        f.write_str("-")?;
        f.write_str(&self.node_id)
    }
}

impl CatalogKey {
    pub fn parse(s: impl AsRef<str>) -> Result<Self, Error> {
        let key = s.as_ref();
        let captures = CATALOG_KEY_PATTERN
            .captures(key)
            .context(InvalidCatalogSnafu { key })?;
        ensure!(captures.len() == 3, InvalidCatalogSnafu { key });
        Ok(Self {
            catalog_name: captures[1].to_string(),
            node_id: captures[2].to_string(),
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CatalogValue;

impl CatalogValue {
    pub fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        Ok(serde_json::to_string(self)
            .context(SerializeCatalogEntryValueSnafu)?
            .into_bytes())
    }
}

pub struct SchemaKey {
    pub catalog_name: String,
    pub schema_name: String,
    pub node_id: String,
}

impl Display for SchemaKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(SCHEMA_KEY_PREFIX)?;
        f.write_str("-")?;
        f.write_str(&self.catalog_name)?;
        f.write_str("-")?;
        f.write_str(&self.schema_name)?;
        f.write_str("-")?;
        f.write_str(&self.node_id)
    }
}

impl SchemaKey {
    pub fn parse(s: impl AsRef<str>) -> Result<Self, Error> {
        let key = s.as_ref();
        let captures = SCHEMA_KEY_PATTERN
            .captures(key)
            .context(InvalidCatalogSnafu { key })?;
        ensure!(captures.len() == 4, InvalidCatalogSnafu { key });

        Ok(Self {
            catalog_name: captures[1].to_string(),
            schema_name: captures[2].to_string(),
            node_id: captures[3].to_string(),
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SchemaValue;

impl SchemaValue {
    pub fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        Ok(serde_json::to_string(self)
            .context(SerializeCatalogEntryValueSnafu)?
            .into_bytes())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};

    use super::*;

    #[test]
    fn test_parse_catalog_key() {
        let key = "__c-C-N";
        let catalog_key = CatalogKey::parse(key).unwrap();
        assert_eq!("C", catalog_key.catalog_name);
        assert_eq!("N", catalog_key.node_id);
        assert_eq!(key, catalog_key.to_string());
    }

    #[test]
    fn test_parse_schema_key() {
        let key = "__s-C-S-N";
        let schema_key = SchemaKey::parse(key).unwrap();
        assert_eq!("C", schema_key.catalog_name);
        assert_eq!("S", schema_key.schema_name);
        assert_eq!("N", schema_key.node_id);
        assert_eq!(key, schema_key.to_string());
    }

    #[test]
    fn test_parse_table_key() {
        let key = "__t-C-S-T-42-N";
        let entry = TableKey::parse(key).unwrap();
        assert_eq!("C", entry.catalog_name);
        assert_eq!("S", entry.schema_name);
        assert_eq!("T", entry.table_name);
        assert_eq!("N", entry.node_id);
        assert_eq!(42, entry.version);
        assert_eq!(key, &entry.to_string());
    }

    #[test]
    fn test_build_prefix() {
        assert_eq!("__c-", build_catalog_prefix());
        assert_eq!("__s-CATALOG-", build_schema_prefix("CATALOG"));
        assert_eq!(
            "__t-CATALOG-SCHEMA-",
            build_table_prefix("CATALOG", "SCHEMA")
        );
    }

    #[test]
    fn test_serialize_schema() {
        let schema_ref = Arc::new(Schema::new(vec![ColumnSchema::new(
            "name",
            ConcreteDataType::string_datatype(),
            true,
        )]));

        let meta = TableMeta {
            schema: schema_ref,
            engine: "mito".to_string(),
            created_on: chrono::DateTime::default(),
            primary_key_indices: vec![0, 1],
            next_column_id: 3,
            engine_options: Default::default(),
            value_indices: vec![2, 3],
            options: Default::default(),
        };

        let value = TableValue {
            id: 42,
            node_id: "localhost".to_string(),
            meta,
        };
        let serialized = serde_json::to_string(&value).unwrap();
        let deserialized = TableValue::parse(&serialized).unwrap();
        assert_eq!(value, deserialized);
    }
}
