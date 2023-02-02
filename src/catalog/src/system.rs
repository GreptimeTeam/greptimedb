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

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use common_catalog::consts::{
    DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, INFORMATION_SCHEMA_NAME, SYSTEM_CATALOG_NAME,
    SYSTEM_CATALOG_TABLE_ID, SYSTEM_CATALOG_TABLE_NAME,
};
use common_query::logical_plan::Expr;
use common_query::physical_plan::{PhysicalPlanRef, SessionContext};
use common_recordbatch::SendableRecordBatchStream;
use common_telemetry::debug;
use common_time::util;
use datatypes::prelude::{ConcreteDataType, ScalarVector, VectorRef};
use datatypes::schema::{ColumnSchema, Schema, SchemaBuilder, SchemaRef};
use datatypes::vectors::{BinaryVector, TimestampMillisecondVector, UInt8Vector};
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};
use table::engine::{EngineContext, TableEngineRef};
use table::metadata::{TableId, TableInfoRef};
use table::requests::{CreateTableRequest, DeleteRequest, InsertRequest, OpenTableRequest};
use table::{Table, TableRef};

use crate::error::{
    self, CreateSystemCatalogSnafu, EmptyValueSnafu, Error, InvalidEntryTypeSnafu, InvalidKeySnafu,
    OpenSystemCatalogSnafu, Result, ValueDeserializeSnafu,
};
use crate::DeregisterTableRequest;

pub const ENTRY_TYPE_INDEX: usize = 0;
pub const KEY_INDEX: usize = 1;
pub const VALUE_INDEX: usize = 3;

pub struct SystemCatalogTable(TableRef);

#[async_trait::async_trait]
impl Table for SystemCatalogTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.0.schema()
    }

    async fn scan(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> table::Result<PhysicalPlanRef> {
        self.0.scan(projection, filters, limit).await
    }

    /// Insert values into table.
    async fn insert(&self, request: InsertRequest) -> table::error::Result<usize> {
        self.0.insert(request).await
    }

    fn table_info(&self) -> TableInfoRef {
        self.0.table_info()
    }

    async fn delete(&self, request: DeleteRequest) -> table::Result<usize> {
        self.0.delete(request).await
    }
}

impl SystemCatalogTable {
    pub async fn new(engine: TableEngineRef) -> Result<Self> {
        let request = OpenTableRequest {
            catalog_name: SYSTEM_CATALOG_NAME.to_string(),
            schema_name: INFORMATION_SCHEMA_NAME.to_string(),
            table_name: SYSTEM_CATALOG_TABLE_NAME.to_string(),
            table_id: SYSTEM_CATALOG_TABLE_ID,
            region_numbers: vec![0],
        };
        let schema = Arc::new(build_system_catalog_schema());
        let ctx = EngineContext::default();

        if let Some(table) = engine
            .open_table(&ctx, request)
            .await
            .context(OpenSystemCatalogSnafu)?
        {
            Ok(Self(table))
        } else {
            // system catalog table is not yet created, try to create
            let request = CreateTableRequest {
                id: SYSTEM_CATALOG_TABLE_ID,
                catalog_name: SYSTEM_CATALOG_NAME.to_string(),
                schema_name: INFORMATION_SCHEMA_NAME.to_string(),
                table_name: SYSTEM_CATALOG_TABLE_NAME.to_string(),
                desc: Some("System catalog table".to_string()),
                schema: schema.clone(),
                region_numbers: vec![0],
                primary_key_indices: vec![ENTRY_TYPE_INDEX, KEY_INDEX],
                create_if_not_exists: true,
                table_options: HashMap::new(),
            };

            let table = engine
                .create_table(&ctx, request)
                .await
                .context(CreateSystemCatalogSnafu)?;
            Ok(Self(table))
        }
    }

    /// Create a stream of all entries inside system catalog table
    pub async fn records(&self) -> Result<SendableRecordBatchStream> {
        let full_projection = None;
        let ctx = SessionContext::new();
        let scan = self
            .scan(full_projection, &[], None)
            .await
            .context(error::SystemCatalogTableScanSnafu)?;
        let stream = scan
            .execute(0, ctx.task_ctx())
            .context(error::SystemCatalogTableScanExecSnafu)?;
        Ok(stream)
    }
}

/// Build system catalog table schema.
/// A system catalog table consists of 6 columns, namely
/// - entry_type: type of entry in current row, can be any variant of [EntryType].
/// - key: a binary encoded key of entry, differs according to different entry type.
/// - timestamp: currently not used.
/// - value: JSON-encoded value of entry's metadata.
/// - gmt_created: create time of this metadata.
/// - gmt_modified: last updated time of this metadata.
fn build_system_catalog_schema() -> Schema {
    let cols = vec![
        ColumnSchema::new(
            "entry_type".to_string(),
            ConcreteDataType::uint8_datatype(),
            false,
        ),
        ColumnSchema::new(
            "key".to_string(),
            ConcreteDataType::binary_datatype(),
            false,
        ),
        ColumnSchema::new(
            "timestamp".to_string(),
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        )
        .with_time_index(true),
        ColumnSchema::new(
            "value".to_string(),
            ConcreteDataType::binary_datatype(),
            false,
        ),
        ColumnSchema::new(
            "gmt_created".to_string(),
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        ),
        ColumnSchema::new(
            "gmt_modified".to_string(),
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        ),
    ];

    // The schema of this table must be valid.
    SchemaBuilder::try_from(cols).unwrap().build().unwrap()
}

/// Formats key string for table entry in system catalog
#[inline]
pub fn format_table_entry_key(catalog: &str, schema: &str, table_id: TableId) -> String {
    format!("{catalog}.{schema}.{table_id}")
}

pub fn build_table_insert_request(
    catalog: String,
    schema: String,
    table_name: String,
    table_id: TableId,
) -> InsertRequest {
    let entry_key = format_table_entry_key(&catalog, &schema, table_id);
    build_insert_request(
        EntryType::Table,
        entry_key.as_bytes(),
        serde_json::to_string(&TableEntryValue { table_name })
            .unwrap()
            .as_bytes(),
    )
}

pub(crate) fn build_table_deletion_request(
    request: &DeregisterTableRequest,
    table_id: TableId,
) -> DeleteRequest {
    let table_key = format_table_entry_key(&request.catalog, &request.schema, table_id);
    DeleteRequest {
        key_column_values: build_primary_key_columns(EntryType::Table, table_key.as_bytes()),
    }
}

fn build_primary_key_columns(entry_type: EntryType, key: &[u8]) -> HashMap<String, VectorRef> {
    let mut m = HashMap::with_capacity(3);
    m.insert(
        "entry_type".to_string(),
        Arc::new(UInt8Vector::from_slice(&[entry_type as u8])) as _,
    );
    m.insert(
        "key".to_string(),
        Arc::new(BinaryVector::from_slice(&[key])) as _,
    );
    // Timestamp in key part is intentionally left to 0
    m.insert(
        "timestamp".to_string(),
        Arc::new(TimestampMillisecondVector::from_slice(&[0])) as _,
    );
    m
}

pub fn build_schema_insert_request(catalog_name: String, schema_name: String) -> InsertRequest {
    let full_schema_name = format!("{catalog_name}.{schema_name}");
    build_insert_request(
        EntryType::Schema,
        full_schema_name.as_bytes(),
        serde_json::to_string(&SchemaEntryValue {})
            .unwrap()
            .as_bytes(),
    )
}

pub fn build_insert_request(entry_type: EntryType, key: &[u8], value: &[u8]) -> InsertRequest {
    let primary_key_columns = build_primary_key_columns(entry_type, key);

    let mut columns_values = HashMap::with_capacity(6);
    columns_values.extend(primary_key_columns.into_iter());

    columns_values.insert(
        "value".to_string(),
        Arc::new(BinaryVector::from_slice(&[value])) as _,
    );

    let now = util::current_time_millis();
    columns_values.insert(
        "gmt_created".to_string(),
        Arc::new(TimestampMillisecondVector::from_slice(&[now])) as _,
    );

    columns_values.insert(
        "gmt_modified".to_string(),
        Arc::new(TimestampMillisecondVector::from_slice(&[now])) as _,
    );

    InsertRequest {
        catalog_name: DEFAULT_CATALOG_NAME.to_string(),
        schema_name: DEFAULT_SCHEMA_NAME.to_string(),
        table_name: SYSTEM_CATALOG_TABLE_NAME.to_string(),
        columns_values,
    }
}

pub fn decode_system_catalog(
    entry_type: Option<u8>,
    key: Option<&[u8]>,
    value: Option<&[u8]>,
) -> Result<Entry> {
    debug!(
        "Decode system catalog entry: {:?}, {:?}, {:?}",
        entry_type, key, value
    );
    let entry_type = entry_type.context(InvalidKeySnafu { key: None })?;
    let key = String::from_utf8_lossy(key.context(InvalidKeySnafu { key: None })?);

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
            // As for table entry, the key is a string with format: `<catalog_name>.<schema_name>.<table_id>`
            // and the value is a JSON string with format: `{"table_name": <table_name>}`
            let table_parts = key.split('.').collect::<Vec<_>>();
            ensure!(
                table_parts.len() >= 3,
                InvalidKeySnafu {
                    key: Some(key.to_string())
                }
            );
            let value = value.context(EmptyValueSnafu)?;
            debug!("Table meta value: {}", String::from_utf8_lossy(value));
            let table_meta: TableEntryValue =
                serde_json::from_slice(value).context(ValueDeserializeSnafu)?;
            let table_id = table_parts[2].parse::<TableId>().unwrap();
            Ok(Entry::Table(TableEntry {
                catalog_name: table_parts[0].to_string(),
                schema_name: table_parts[1].to_string(),
                table_name: table_meta.table_name,
                table_id,
            }))
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
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
            b => InvalidEntryTypeSnafu {
                entry_type: Some(b),
            }
            .fail(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Ord, PartialOrd)]
pub enum Entry {
    Catalog(CatalogEntry),
    Schema(SchemaEntry),
    Table(TableEntry),
}

#[derive(Debug, PartialEq, Eq, Ord, PartialOrd)]
pub struct CatalogEntry {
    pub catalog_name: String,
}

#[derive(Debug, PartialEq, Eq, Ord, PartialOrd)]
pub struct SchemaEntry {
    pub catalog_name: String,
    pub schema_name: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaEntryValue;

#[derive(Debug, PartialEq, Eq, Ord, PartialOrd)]
pub struct TableEntry {
    pub catalog_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub table_id: TableId,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableEntryValue {
    pub table_name: String,
}

#[cfg(test)]
mod tests {
    use common_recordbatch::RecordBatches;
    use datatypes::value::Value;
    use log_store::NoopLogStore;
    use mito::config::EngineConfig;
    use mito::engine::MitoEngine;
    use object_store::ObjectStore;
    use storage::config::EngineConfig as StorageEngineConfig;
    use storage::EngineImpl;
    use table::metadata::TableType;
    use table::metadata::TableType::Base;
    use tempdir::TempDir;

    use super::*;

    #[test]
    pub fn test_decode_catalog_entry() {
        let entry = decode_system_catalog(
            Some(EntryType::Catalog as u8),
            Some("some_catalog".as_bytes()),
            None,
        )
        .unwrap();
        if let Entry::Catalog(e) = entry {
            assert_eq!("some_catalog", e.catalog_name);
        } else {
            panic!("Unexpected type: {entry:?}");
        }
    }

    #[test]
    pub fn test_decode_schema_entry() {
        let entry = decode_system_catalog(
            Some(EntryType::Schema as u8),
            Some("some_catalog.some_schema".as_bytes()),
            None,
        )
        .unwrap();

        if let Entry::Schema(e) = entry {
            assert_eq!("some_catalog", e.catalog_name);
            assert_eq!("some_schema", e.schema_name);
        } else {
            panic!("Unexpected type: {entry:?}");
        }
    }

    #[test]
    pub fn test_decode_table() {
        let entry = decode_system_catalog(
            Some(EntryType::Table as u8),
            Some("some_catalog.some_schema.42".as_bytes()),
            Some("{\"table_name\":\"some_table\"}".as_bytes()),
        )
        .unwrap();

        if let Entry::Table(e) = entry {
            assert_eq!("some_catalog", e.catalog_name);
            assert_eq!("some_schema", e.schema_name);
            assert_eq!("some_table", e.table_name);
            assert_eq!(42, e.table_id);
        } else {
            panic!("Unexpected type: {entry:?}");
        }
    }

    #[test]
    #[should_panic]
    pub fn test_decode_mismatch() {
        decode_system_catalog(
            Some(EntryType::Table as u8),
            Some("some_catalog.some_schema.42".as_bytes()),
            None,
        )
        .unwrap();
    }

    #[test]
    pub fn test_entry_type() {
        assert_eq!(EntryType::Catalog, EntryType::try_from(1).unwrap());
        assert_eq!(EntryType::Schema, EntryType::try_from(2).unwrap());
        assert_eq!(EntryType::Table, EntryType::try_from(3).unwrap());
        assert!(EntryType::try_from(4).is_err());
    }

    pub async fn prepare_table_engine() -> (TempDir, TableEngineRef) {
        let dir = TempDir::new("system-table-test").unwrap();
        let store_dir = dir.path().to_string_lossy();
        let accessor = object_store::backend::fs::Builder::default()
            .root(&store_dir)
            .build()
            .unwrap();
        let object_store = ObjectStore::new(accessor);
        let table_engine = Arc::new(MitoEngine::new(
            EngineConfig::default(),
            EngineImpl::new(
                StorageEngineConfig::default(),
                Arc::new(NoopLogStore::default()),
                object_store.clone(),
            ),
            object_store,
        ));
        (dir, table_engine)
    }

    #[tokio::test]
    async fn test_system_table_type() {
        let (_dir, table_engine) = prepare_table_engine().await;
        let system_table = SystemCatalogTable::new(table_engine).await.unwrap();
        assert_eq!(Base, system_table.table_type());
    }

    #[tokio::test]
    async fn test_system_table_info() {
        let (_dir, table_engine) = prepare_table_engine().await;
        let system_table = SystemCatalogTable::new(table_engine).await.unwrap();
        let info = system_table.table_info();
        assert_eq!(TableType::Base, info.table_type);
        assert_eq!(SYSTEM_CATALOG_TABLE_NAME, info.name);
        assert_eq!(SYSTEM_CATALOG_TABLE_ID, info.ident.table_id);
        assert_eq!(SYSTEM_CATALOG_NAME, info.catalog_name);
        assert_eq!(INFORMATION_SCHEMA_NAME, info.schema_name);
    }

    #[tokio::test]
    async fn test_system_catalog_table_records() {
        let (_, table_engine) = prepare_table_engine().await;
        let catalog_table = SystemCatalogTable::new(table_engine).await.unwrap();

        let table_insertion = build_table_insert_request(
            DEFAULT_CATALOG_NAME.to_string(),
            DEFAULT_SCHEMA_NAME.to_string(),
            "my_table".to_string(),
            1,
        );
        let result = catalog_table.insert(table_insertion).await.unwrap();
        assert_eq!(result, 1);

        let records = catalog_table.records().await.unwrap();
        let mut batches = RecordBatches::try_collect(records).await.unwrap().take();
        assert_eq!(batches.len(), 1);
        let batch = batches.remove(0);
        assert_eq!(batch.num_rows(), 1);

        let row = batch.rows().next().unwrap();
        let Value::UInt8(entry_type) = row[0] else { unreachable!() };
        let Value::Binary(key) = row[1].clone() else { unreachable!() };
        let Value::Binary(value) = row[3].clone() else { unreachable!() };
        let entry = decode_system_catalog(Some(entry_type), Some(&*key), Some(&*value)).unwrap();
        let expected = Entry::Table(TableEntry {
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
            schema_name: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: "my_table".to_string(),
            table_id: 1,
        });
        assert_eq!(entry, expected);

        let table_deletion = build_table_deletion_request(
            &DeregisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: "my_table".to_string(),
            },
            1,
        );
        let result = catalog_table.delete(table_deletion).await.unwrap();
        assert_eq!(result, 1);

        let records = catalog_table.records().await.unwrap();
        let batches = RecordBatches::try_collect(records).await.unwrap().take();
        assert_eq!(batches.len(), 0);
    }
}
