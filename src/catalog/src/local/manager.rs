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

use std::sync::Arc;

use common_catalog::consts::{
    DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, INFORMATION_SCHEMA_NAME, MITO_ENGINE,
    NUMBERS_TABLE_ID, SYSTEM_CATALOG_NAME, SYSTEM_CATALOG_TABLE_ID, SYSTEM_CATALOG_TABLE_NAME,
};
use common_recordbatch::{RecordBatch, SendableRecordBatchStream};
use common_telemetry::info;
use datatypes::prelude::ScalarVector;
use datatypes::vectors::{BinaryVector, UInt8Vector};
use futures_util::lock::Mutex;
use snafu::{ensure, OptionExt, ResultExt};
use table::engine::manager::TableEngineManagerRef;
use table::engine::EngineContext;
use table::requests::OpenTableRequest;
use table::table::numbers::{NumbersTable, NUMBERS_TABLE_NAME};

use crate::error::{
    CatalogNotFoundSnafu, OpenTableSnafu, ReadSystemCatalogSnafu, Result, SchemaNotFoundSnafu,
    SystemCatalogSnafu, SystemCatalogTypeMismatchSnafu, TableEngineNotFoundSnafu,
    TableNotFoundSnafu,
};
use crate::system::{
    decode_system_catalog, Entry, SystemCatalogTable, TableEntry, ENTRY_TYPE_INDEX, KEY_INDEX,
    VALUE_INDEX,
};
use crate::tables::SystemCatalog;
use crate::{
    handle_system_table_request, CatalogManagerRef, RegisterSchemaRequest,
    RegisterSystemTableRequest, RegisterTableRequest,
};

pub struct SystemTableInitializer {
    system: Arc<SystemCatalog>,
    catalog_manager: CatalogManagerRef,
    engine_manager: TableEngineManagerRef,
    system_table_requests: Mutex<Vec<RegisterSystemTableRequest>>,
}

impl SystemTableInitializer {
    pub async fn try_new(
        engine_manager: TableEngineManagerRef,
        catalog_manager: CatalogManagerRef,
    ) -> Result<Self> {
        let engine = engine_manager
            .engine(MITO_ENGINE)
            .context(TableEngineNotFoundSnafu {
                engine_name: MITO_ENGINE,
            })?;
        let table = SystemCatalogTable::new(engine.clone()).await?;
        let system_catalog = Arc::new(SystemCatalog::new(table));
        Ok(Self {
            system: system_catalog,
            catalog_manager,
            engine_manager,
            system_table_requests: Mutex::new(Vec::default()),
        })
    }

    /// Scan all entries from system catalog table
    pub async fn init(&self) -> Result<()> {
        self.init_system_catalog().await?;
        let system_records = self.system.information_schema.system.records().await?;
        let entries = self.collect_system_catalog_entries(system_records).await?;
        self.handle_system_catalog_entries(entries).await?;

        // Processing system table hooks
        let mut sys_table_requests = self.system_table_requests.lock().await;
        let engine = self
            .engine_manager
            .engine(MITO_ENGINE)
            .context(TableEngineNotFoundSnafu {
                engine_name: MITO_ENGINE,
            })?;

        handle_system_table_request(
            self.catalog_manager.as_ref(),
            engine,
            &mut sys_table_requests,
        )
        .await?;
        Ok(())
    }

    async fn init_system_catalog(&self) -> Result<()> {
        let catalog_manager = &self.catalog_manager;
        catalog_manager.register_catalog(SYSTEM_CATALOG_NAME)?;

        catalog_manager.register_schema(RegisterSchemaRequest {
            catalog: SYSTEM_CATALOG_NAME.to_string(),
            schema: INFORMATION_SCHEMA_NAME.to_string(),
        })?;

        let register_table_req = RegisterTableRequest {
            catalog: SYSTEM_CATALOG_NAME.to_string(),
            schema: INFORMATION_SCHEMA_NAME.to_string(),
            table_name: SYSTEM_CATALOG_TABLE_NAME.to_string(),
            table_id: SYSTEM_CATALOG_TABLE_ID,
            table: self.system.information_schema.system.as_table_ref(),
        };
        catalog_manager.register_table(register_table_req)?;

        // Add numbers table for test
        let register_number_table_req = RegisterTableRequest {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: NUMBERS_TABLE_NAME.to_string(),
            table_id: NUMBERS_TABLE_ID,
            table: NumbersTable::table(NUMBERS_TABLE_ID),
        };

        catalog_manager.register_table(register_number_table_req)?;

        Ok(())
    }

    /// Collect stream of system catalog entries to `Vec<Entry>`
    async fn collect_system_catalog_entries(
        &self,
        stream: SendableRecordBatchStream,
    ) -> Result<Vec<Entry>> {
        let record_batch = common_recordbatch::util::collect(stream)
            .await
            .context(ReadSystemCatalogSnafu)?;
        let rbs = record_batch
            .into_iter()
            .map(Self::record_batch_to_entry)
            .collect::<Result<Vec<_>>>()?;
        Ok(rbs.into_iter().flat_map(Vec::into_iter).collect::<_>())
    }

    /// Convert `RecordBatch` to a vector of `Entry`.
    fn record_batch_to_entry(rb: RecordBatch) -> Result<Vec<Entry>> {
        ensure!(
            rb.num_columns() >= 6,
            SystemCatalogSnafu {
                msg: format!("Length mismatch: {}", rb.num_columns())
            }
        );

        let entry_type = rb
            .column(ENTRY_TYPE_INDEX)
            .as_any()
            .downcast_ref::<UInt8Vector>()
            .with_context(|| SystemCatalogTypeMismatchSnafu {
                data_type: rb.column(ENTRY_TYPE_INDEX).data_type(),
            })?;

        let key = rb
            .column(KEY_INDEX)
            .as_any()
            .downcast_ref::<BinaryVector>()
            .with_context(|| SystemCatalogTypeMismatchSnafu {
                data_type: rb.column(KEY_INDEX).data_type(),
            })?;

        let value = rb
            .column(VALUE_INDEX)
            .as_any()
            .downcast_ref::<BinaryVector>()
            .with_context(|| SystemCatalogTypeMismatchSnafu {
                data_type: rb.column(VALUE_INDEX).data_type(),
            })?;

        let mut res = Vec::with_capacity(rb.num_rows());
        for ((t, k), v) in entry_type
            .iter_data()
            .zip(key.iter_data())
            .zip(value.iter_data())
        {
            let entry = decode_system_catalog(t, k, v)?;
            res.push(entry);
        }
        Ok(res)
    }

    /// Processes records from system catalog table.
    async fn handle_system_catalog_entries(&self, entries: Vec<Entry>) -> Result<()> {
        let entries = Self::sort_entries(entries);
        for entry in entries {
            match entry {
                Entry::Catalog(c) => {
                    self.catalog_manager.register_catalog(&c.catalog_name)?;
                    info!("Register catalog: {}", c.catalog_name);
                }
                Entry::Schema(s) => {
                    let req = RegisterSchemaRequest {
                        catalog: s.catalog_name.clone(),
                        schema: s.schema_name.clone(),
                    };
                    self.catalog_manager.register_schema(req)?;
                    info!("Registered schema: {:?}", s);
                }
                Entry::Table(t) => {
                    if t.is_deleted {
                        continue;
                    }
                    self.open_and_register_table(&t).await?;
                    info!("Registered table: {:?}", t);
                }
            }
        }
        Ok(())
    }

    /// Sort catalog entries to ensure catalog entries comes first, then schema entries,
    /// and table entries is the last.
    fn sort_entries(mut entries: Vec<Entry>) -> Vec<Entry> {
        entries.sort();
        entries
    }

    async fn open_and_register_table(&self, t: &TableEntry) -> Result<()> {
        self.check_catalog_schema_exist(&t.catalog_name, &t.schema_name)
            .await?;

        let context = EngineContext {};
        let open_request = OpenTableRequest {
            catalog_name: t.catalog_name.clone(),
            schema_name: t.schema_name.clone(),
            table_name: t.table_name.clone(),
            table_id: t.table_id,
            region_numbers: vec![0],
        };
        let engine = self
            .engine_manager
            .engine(&t.engine)
            .context(TableEngineNotFoundSnafu {
                engine_name: &t.engine,
            })?;

        let table_ref = engine
            .open_table(&context, open_request)
            .await
            .with_context(|_| OpenTableSnafu {
                table_info: format!(
                    "{}.{}.{}, id: {}",
                    &t.catalog_name, &t.schema_name, &t.table_name, t.table_id
                ),
            })?
            .with_context(|| TableNotFoundSnafu {
                table_info: format!(
                    "{}.{}.{}, id: {}",
                    &t.catalog_name, &t.schema_name, &t.table_name, t.table_id
                ),
            })?;

        let register_request = RegisterTableRequest {
            catalog: t.catalog_name.clone(),
            schema: t.schema_name.clone(),
            table_name: t.table_name.clone(),
            table_id: t.table_id,
            table: table_ref,
        };
        self.catalog_manager.register_table(register_request)?;
        Ok(())
    }

    async fn check_catalog_schema_exist(
        &self,
        catalog_name: &str,
        schema_name: &str,
    ) -> Result<()> {
        if !self.catalog_manager.catalog_exist(catalog_name).await? {
            return CatalogNotFoundSnafu { catalog_name }.fail()?;
        }
        if !self
            .catalog_manager
            .schema_exist(catalog_name, schema_name)
            .await?
        {
            return SchemaNotFoundSnafu {
                catalog: catalog_name,
                schema: schema_name,
            }
            .fail()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use mito::engine::MITO_ENGINE;

    use super::*;
    use crate::system::{CatalogEntry, SchemaEntry};

    #[test]
    fn test_sort_entry() {
        let vec = vec![
            Entry::Table(TableEntry {
                catalog_name: "C1".to_string(),
                schema_name: "S1".to_string(),
                table_name: "T1".to_string(),
                table_id: 1,
                engine: MITO_ENGINE.to_string(),
                is_deleted: false,
            }),
            Entry::Catalog(CatalogEntry {
                catalog_name: "C2".to_string(),
            }),
            Entry::Schema(SchemaEntry {
                catalog_name: "C1".to_string(),
                schema_name: "S1".to_string(),
            }),
            Entry::Schema(SchemaEntry {
                catalog_name: "C2".to_string(),
                schema_name: "S2".to_string(),
            }),
            Entry::Catalog(CatalogEntry {
                catalog_name: "".to_string(),
            }),
            Entry::Table(TableEntry {
                catalog_name: "C1".to_string(),
                schema_name: "S1".to_string(),
                table_name: "T2".to_string(),
                table_id: 2,
                engine: MITO_ENGINE.to_string(),
                is_deleted: false,
            }),
        ];
        let res = SystemTableInitializer::sort_entries(vec);
        assert_matches!(res[0], Entry::Catalog(..));
        assert_matches!(res[1], Entry::Catalog(..));
        assert_matches!(res[2], Entry::Schema(..));
        assert_matches!(res[3], Entry::Schema(..));
        assert_matches!(res[4], Entry::Table(..));
        assert_matches!(res[5], Entry::Table(..));
    }
}
