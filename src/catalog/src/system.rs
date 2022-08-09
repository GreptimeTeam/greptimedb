use std::any::Any;
use std::sync::Arc;

use common_query::logical_plan::Expr;
use common_recordbatch::SendableRecordBatchStream;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};
use table::engine::{EngineContext, TableEngineRef};
use table::requests::{CreateTableRequest, OpenTableRequest};
use table::{Table, TableRef};

use crate::consts::{
    INFORMATION_SCHEMA_NAME, SYSTEM_CATALOG_NAME, SYSTEM_CATALOG_TABLE_ID,
    SYSTEM_CATALOG_TABLE_NAME,
};
use crate::error::{
    CreateSystemCatalogSnafu, EmptyValueSnafu, Error, InvalidEntryTypeSnafu, InvalidKeySnafu,
    OpenSystemCatalogSnafu, Result, SystemCatalogSchemaSnafu, ValueDeserializeSnafu,
};

#[allow(dead_code)]
pub struct SystemCatalogTable {
    schema: SchemaRef,
    pub table: TableRef,
}

#[async_trait::async_trait]
impl Table for SystemCatalogTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    async fn scan(
        &self,
        _projection: &Option<Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> table::Result<SendableRecordBatchStream> {
        panic!("System catalog table does not support scan!")
    }
}

#[allow(dead_code)]
impl SystemCatalogTable {
    pub async fn new(engine: TableEngineRef) -> Result<Self> {
        let request = OpenTableRequest {
            catalog_name: SYSTEM_CATALOG_NAME.to_string(),
            schema_name: INFORMATION_SCHEMA_NAME.to_string(),
            table_name: SYSTEM_CATALOG_TABLE_NAME.to_string(),
            table_id: SYSTEM_CATALOG_TABLE_ID,
        };
        let schema = Arc::new(build_system_catalog_schema()?);
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
                desc: Some("System catalog table".to_string()),
                schema: schema.clone(),
                primary_key_indices: vec![0, 1, 2],
                create_if_not_exists: true,
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
fn build_system_catalog_schema() -> Result<Schema> {
    let mut cols = Vec::with_capacity(6);
    cols.push(ColumnSchema::new(
        "entry_type".to_string(),
        ConcreteDataType::uint8_datatype(),
        false,
    ));
    cols.push(ColumnSchema::new(
        "key".to_string(),
        ConcreteDataType::binary_datatype(),
        false,
    ));
    cols.push(ColumnSchema::new(
        "timestamp".to_string(),
        ConcreteDataType::int64_datatype(),
        false,
    ));
    cols.push(ColumnSchema::new(
        "value".to_string(),
        ConcreteDataType::binary_datatype(),
        false,
    ));
    Schema::with_timestamp_index(cols, 2).context(SystemCatalogSchemaSnafu)
}

pub fn decode_system_catalog(
    entry_type: Option<u8>,
    key: Option<&[u8]>,
    value: Option<&[u8]>,
) -> Result<Entry> {
    let entry_type = entry_type.context(InvalidKeySnafu { key: None })?;
    let key = String::from_utf8_lossy(key.context(InvalidKeySnafu { key: None })?);
    let value = value.context(EmptyValueSnafu)?;

    match EntryType::try_from(entry_type)? {
        EntryType::Catalog => {
            // As for catalog entry, the key is a string with format: `<catalog_name>`
            // and the value is current not used.
            let catalog_name = key.to_string();
            Ok(Entry::Catalog(CatalogEntry { catalog_name }))
        }
        EntryType::Schema => {
            // As for schema entry, the key is a string with format: `<catalog_name>.<schema_name>`
            // and the value is current not used.
            let schema_parts = key.split('.').collect::<Vec<_>>();
            ensure!(
                schema_parts.len() == 2,
                InvalidKeySnafu {
                    key: Some(key.to_string())
                }
            );
            Ok(Entry::Schema(SchemaEntry {
                catalog_name: schema_parts[0].to_string(),
                schema_name: schema_parts[1].to_string(),
            }))
        }

        EntryType::Table => {
            // As for table entry, the key is a string with format: `<catalog_name>.<schema_name>.<table_name>`
            // and the value is a JSON string with format: `{"table_id": <table_id>}`
            let table_parts = key.split('.').collect::<Vec<_>>();
            ensure!(
                table_parts.len() >= 3,
                InvalidKeySnafu {
                    key: Some(key.to_string())
                }
            );
            let table_meta: TableEntryValue =
                serde_json::from_slice(value).context(ValueDeserializeSnafu)?;
            Ok(Entry::Table(TableEntry {
                catalog_name: table_parts[0].to_string(),
                schema_name: table_parts[1].to_string(),
                table_name: table_parts[2].to_string(),
                table_id: table_meta.table_id,
            }))
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
            b => Err(InvalidEntryTypeSnafu {
                entry_type: Some(b),
            }
            .build()),
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

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct TableEntryValue {
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
