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

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use std::sync::Arc;

use async_stream::stream;
use catalog::error::Error;
use catalog::helper::{CatalogKey, CatalogValue, SchemaKey, SchemaValue};
use catalog::remote::{Kv, KvBackend, ValueIter};
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_recordbatch::RecordBatch;
use common_telemetry::logging::info;
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema};
use datatypes::vectors::StringVector;
use serde::Serializer;
use table::engine::{EngineContext, TableEngine, TableReference};
use table::metadata::TableId;
use table::requests::{AlterTableRequest, CreateTableRequest, DropTableRequest, OpenTableRequest};
use table::test_util::MemTable;
use table::TableRef;
use tokio::sync::RwLock;

pub struct MockKvBackend {
    map: RwLock<BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl Default for MockKvBackend {
    fn default() -> Self {
        let mut map = BTreeMap::default();
        let catalog_value = CatalogValue {}.as_bytes().unwrap();
        let schema_value = SchemaValue {}.as_bytes().unwrap();

        let default_catalog_key = CatalogKey {
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
        }
        .to_string();

        let default_schema_key = SchemaKey {
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
            schema_name: DEFAULT_SCHEMA_NAME.to_string(),
        }
        .to_string();

        // create default catalog and schema
        map.insert(default_catalog_key.into(), catalog_value);
        map.insert(default_schema_key.into(), schema_value);

        let map = RwLock::new(map);
        Self { map }
    }
}

impl Display for MockKvBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        futures::executor::block_on(async {
            let map = self.map.read().await;
            for (k, v) in map.iter() {
                f.serialize_str(&String::from_utf8_lossy(k))?;
                f.serialize_str(" -> ")?;
                f.serialize_str(&String::from_utf8_lossy(v))?;
                f.serialize_str("\n")?;
            }
            Ok(())
        })
    }
}

#[async_trait::async_trait]
impl KvBackend for MockKvBackend {
    fn range<'a, 'b>(&'a self, key: &[u8]) -> ValueIter<'b, Error>
    where
        'a: 'b,
    {
        let prefix = key.to_vec();
        let prefix_string = String::from_utf8_lossy(&prefix).to_string();
        Box::pin(stream!({
            let maps = self.map.read().await.clone();
            for (k, v) in maps.range(prefix.clone()..) {
                let key_string = String::from_utf8_lossy(k).to_string();
                let matches = key_string.starts_with(&prefix_string);
                if matches {
                    yield Ok(Kv(k.clone(), v.clone()))
                } else {
                    info!("Stream finished");
                    return;
                }
            }
        }))
    }

    async fn set(&self, key: &[u8], val: &[u8]) -> Result<(), Error> {
        let mut map = self.map.write().await;
        map.insert(key.to_vec(), val.to_vec());
        Ok(())
    }

    async fn compare_and_set(
        &self,
        key: &[u8],
        expect: &[u8],
        val: &[u8],
    ) -> Result<Result<(), Option<Vec<u8>>>, Error> {
        let mut map = self.map.write().await;
        let existing = map.entry(key.to_vec());
        match existing {
            Entry::Vacant(e) => {
                if expect.is_empty() {
                    e.insert(val.to_vec());
                    Ok(Ok(()))
                } else {
                    Ok(Err(None))
                }
            }
            Entry::Occupied(mut existing) => {
                if existing.get() == expect {
                    existing.insert(val.to_vec());
                    Ok(Ok(()))
                } else {
                    Ok(Err(Some(existing.get().clone())))
                }
            }
        }
    }

    async fn delete_range(&self, key: &[u8], end: &[u8]) -> Result<(), Error> {
        let mut map = self.map.write().await;
        if end.is_empty() {
            let _ = map.remove(key);
        } else {
            let start = key.to_vec();
            let end = end.to_vec();
            let range = start..end;

            map.retain(|k, _| !range.contains(k));
        }
        Ok(())
    }
}

#[derive(Default)]
pub struct MockTableEngine {
    tables: RwLock<HashMap<String, TableRef>>,
}

#[async_trait::async_trait]
impl TableEngine for MockTableEngine {
    fn name(&self) -> &str {
        "MockTableEngine"
    }

    /// Create a table with only one column
    async fn create_table(
        &self,
        _ctx: &EngineContext,
        request: CreateTableRequest,
    ) -> table::Result<TableRef> {
        let table_name = request.table_name.clone();
        let catalog_name = request.catalog_name.clone();
        let schema_name = request.schema_name.clone();

        let default_table_id = "0".to_owned();
        let table_id = TableId::from_str(
            request
                .table_options
                .extra_options
                .get("table_id")
                .unwrap_or(&default_table_id),
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "name",
            ConcreteDataType::string_datatype(),
            true,
        )]));

        let data = vec![Arc::new(StringVector::from(vec!["a", "b", "c"])) as _];
        let record_batch = RecordBatch::new(schema, data).unwrap();
        let table: TableRef = Arc::new(MemTable::new_with_catalog(
            &table_name,
            record_batch,
            table_id,
            catalog_name,
            schema_name,
            vec![0],
        )) as Arc<_>;

        let mut tables = self.tables.write().await;
        tables.insert(table_name, table.clone() as TableRef);
        Ok(table)
    }

    async fn open_table(
        &self,
        _ctx: &EngineContext,
        request: OpenTableRequest,
    ) -> table::Result<Option<TableRef>> {
        Ok(self.tables.read().await.get(&request.table_name).cloned())
    }

    async fn alter_table(
        &self,
        _ctx: &EngineContext,
        _request: AlterTableRequest,
    ) -> table::Result<TableRef> {
        unimplemented!()
    }

    fn get_table(
        &self,
        _ctx: &EngineContext,
        table_ref: &TableReference,
    ) -> table::Result<Option<TableRef>> {
        futures::executor::block_on(async {
            Ok(self
                .tables
                .read()
                .await
                .get(&table_ref.to_string())
                .cloned())
        })
    }

    fn table_exists(&self, _ctx: &EngineContext, table_ref: &TableReference) -> bool {
        futures::executor::block_on(async {
            self.tables
                .read()
                .await
                .contains_key(&table_ref.to_string())
        })
    }

    async fn drop_table(
        &self,
        _ctx: &EngineContext,
        _request: DropTableRequest,
    ) -> table::Result<bool> {
        unimplemented!()
    }

    async fn close(&self) -> table::Result<()> {
        Ok(())
    }
}
