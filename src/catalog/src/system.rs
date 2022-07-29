use std::sync::Arc;

use common_recordbatch::SendableRecordBatchStream;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use table::engine::{EngineContext, TableEngineRef};
use table::requests::{CreateTableRequest, OpenTableRequest};
use table::TableRef;

use crate::catalog::consts::{
    INFORMATION_SCHEMA_NAME, SYSTEM_CATALOG_NAME, SYSTEM_CATALOG_TABLE_ID,
    SYSTEM_CATALOG_TABLE_NAME,
};
use crate::catalog::error::Result;
use crate::catalog::error::{
    CreateSystemCatalogSnafu, EmptyValueSnafu, Error, InvalidKeySnafu, OpenSystemCatalogSnafu,
    ValueDeserializeSnafu,
};

#[allow(dead_code)]
pub struct SystemCatalogTable {
    schema: SchemaRef,
    table: TableRef,
}

#[allow(dead_code)]
impl SystemCatalogTable {
    pub async fn new(engine: TableEngineRef) -> Result<Self> {
        let request = OpenTableRequest {
            engine: engine.name().to_string(),
            catalog_name: SYSTEM_CATALOG_NAME.to_string(),
            schema_name: INFORMATION_SCHEMA_NAME.to_string(),
            table_name: SYSTEM_CATALOG_TABLE_NAME.to_string(),
            table_id: SYSTEM_CATALOG_TABLE_ID,
        };
        let schema = Arc::new(build_system_catalog_schema());
        let ctx = EngineContext::default(); // init engine context
        if let Some(table) = engine
            .open_table(&ctx, request)
            .await
            .context(OpenSystemCatalogSnafu)?
        {
            Ok(Self { table, schema })
        } else {
            // system catalog table is not yet created, try to create
            let request = CreateTableRequest {
                name: SYSTEM_CATALOG_TABLE_NAME.to_string(),
                desc: Some("System catalog inner KV".to_string()),
                schema: schema.clone(),
            };

            let table = engine
                .create_table(&ctx, request)
                .await
                .context(CreateSystemCatalogSnafu)?;
            Ok(Self { table, schema })
        }
    }

    /// Create a stream of all entries inside system catalog table
    pub async fn records(&self) -> Result<SendableRecordBatchStream> {
        let empty_filters = vec![];
        let full_projection = None;
        let stream = self
            .table
            .scan(&full_projection, &empty_filters, None)
            .await
            .unwrap();
        Ok(stream)
    }
}

/// Build system catalog table schema
fn build_system_catalog_schema() -> Schema {
    let mut cols = Vec::with_capacity(6);
    cols.push(ColumnSchema::new(
        "key".to_string(),
        ConcreteDataType::binary_datatype(),
        false,
    ));
    cols.push(ColumnSchema::new(
        "value".to_string(),
        ConcreteDataType::binary_datatype(),
        false,
    ));
    Schema::new(cols)
}

pub fn decode_system_catalog(key: Option<&[u8]>, value: Option<&[u8]>) -> Result<Entry> {
    let key = key.context(InvalidKeySnafu { key: None })?;
    let value = value.context(EmptyValueSnafu)?;

    match EntryType::try_from(*key.get(0).context(InvalidKeySnafu { key: None })?)? {
        EntryType::Catalog => {
            let entry: CatalogEntry =
                serde_json::from_slice(value).context(ValueDeserializeSnafu)?;
            Ok(Entry::Catalog(entry))
        }
        EntryType::Schema => {
            let entry: SchemaEntry =
                serde_json::from_slice(value).context(ValueDeserializeSnafu)?;
            Ok(Entry::Schema(entry))
        }
        EntryType::Table => {
            let entry: TableEntry = serde_json::from_slice(value).context(ValueDeserializeSnafu)?;
            Ok(Entry::Table(entry))
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub enum EntryType {
    Catalog = 1,
    Schema = 2,
    Table = 3,
}

impl TryFrom<u8> for EntryType {
    type Error = Error;

    fn try_from(value: u8) -> std::result::Result<Self, Self::Error> {
        match value {
            b if b == Self::Catalog as u8 => Ok(Self::Catalog),
            b if b == Self::Schema as u8 => Ok(Self::Schema),
            b if b == Self::Table as u8 => Ok(Self::Table),
            b => Err(InvalidKeySnafu { key: Some(b) }.build()),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum Entry {
    Catalog(CatalogEntry),
    Schema(SchemaEntry),
    Table(TableEntry),
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct CatalogEntry {
    pub catalog_name: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct SchemaEntry {
    pub catalog_name: String,
    pub schema_name: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct TableEntry {
    pub catalog_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub table_id: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_serialize() {
        let entry = CatalogEntry {
            catalog_name: "test_catalog".to_string(),
        };
        let serialized = serde_json::to_string(&entry).unwrap();
        let result: CatalogEntry = serde_json::from_str(&serialized).unwrap();
        assert_eq!(entry, result);

        let entry = SchemaEntry {
            catalog_name: "test_catalog".to_string(),
            schema_name: "test_schema".to_string(),
        };
        let string = serde_json::to_string(&entry).unwrap();
        let result: SchemaEntry = serde_json::from_str(&string).unwrap();
        assert_eq!(entry, result);

        let entry = TableEntry {
            catalog_name: "test_catalog".to_string(),
            schema_name: "test_schema".to_string(),
            table_name: "test_table".to_string(),
            table_id: 42,
        };
        let string = serde_json::to_string(&entry).unwrap();
        let result: TableEntry = serde_json::from_str(&string).unwrap();
        assert_eq!(entry, result);
    }
}
