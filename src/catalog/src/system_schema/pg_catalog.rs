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
use std::sync::{Arc, Weak};

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, PG_CATALOG_NAME, PG_CATALOG_TABLE_ID_START};
use common_error::ext::BoxedError;
use common_recordbatch::SendableRecordBatchStream;
use common_recordbatch::adapter::RecordBatchStreamAdapter;
use common_telemetry::warn;
use datafusion::datasource::TableType;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter as DfRecordBatchStreamAdapter;
use datafusion_pg_catalog::pg_catalog::catalog_info::CatalogInfo;
use datafusion_pg_catalog::pg_catalog::context::EmptyContextProvider;
use datafusion_pg_catalog::pg_catalog::{
    PG_CATALOG_TABLES, PgCatalogSchemaProvider, PgCatalogStaticTables, PgCatalogTable,
};
use snafu::ResultExt;
use store_api::storage::ScanRequest;
use table::TableRef;
use table::metadata::TableId;

use crate::CatalogManager;
use crate::error::{InternalSnafu, ProjectSchemaSnafu, Result};
use crate::system_schema::{
    SystemSchemaProvider, SystemSchemaProviderInner, SystemTable, SystemTableRef,
};

/// [`PGCatalogProvider`] is the provider for a schema named `pg_catalog`, it is not a catalog.
pub struct PGCatalogProvider {
    catalog_name: String,
    inner: PgCatalogSchemaProvider<CatalogManagerWrapper, EmptyContextProvider>,
    tables: HashMap<String, TableRef>,
    table_ids: HashMap<&'static str, u32>,
}

impl SystemSchemaProvider for PGCatalogProvider {
    fn tables(&self) -> &HashMap<String, TableRef> {
        assert!(!self.tables.is_empty());

        &self.tables
    }
}

impl PGCatalogProvider {
    pub fn new(catalog_name: String, catalog_manager: Weak<dyn CatalogManager>) -> Self {
        // safe to expect/unwrap because it contains only schema read, this can
        // be ensured by sqlness tests
        let static_tables =
            PgCatalogStaticTables::try_new().expect("Failed to initialize static tables");
        let inner = PgCatalogSchemaProvider::try_new(
            CatalogManagerWrapper {
                catalog_name: catalog_name.clone(),
                catalog_manager,
            },
            Arc::new(static_tables),
            EmptyContextProvider,
        )
        .expect("Failed to initialize PgCatalogSchemaProvider");

        let mut table_ids = HashMap::new();
        let mut table_id = PG_CATALOG_TABLE_ID_START;
        for name in PG_CATALOG_TABLES {
            table_ids.insert(*name, table_id);
            table_id += 1;
        }

        let mut provider = Self {
            catalog_name,
            inner,
            tables: HashMap::new(),
            table_ids,
        };
        provider.build_tables();
        provider
    }

    fn build_tables(&mut self) {
        // SECURITY NOTE:
        // Must follow the same security rules as [`InformationSchemaProvider::build_tables`].
        let mut tables = HashMap::new();
        // It's safe to unwrap here because we are sure that the constants have been handle correctly inside system_table.
        for name in PG_CATALOG_TABLES {
            if let Some(table) = self.build_table(name) {
                tables.insert(name.to_string(), table);
            }
        }

        self.tables = tables;
    }
}

impl SystemSchemaProviderInner for PGCatalogProvider {
    fn schema_name() -> &'static str {
        PG_CATALOG_NAME
    }

    fn system_table(&self, name: &str) -> Option<SystemTableRef> {
        if let Some((table_name, table_id)) = self.table_ids.get_key_value(name) {
            let table = self.inner.build_table_by_name(name).expect(name);

            if let Some(table) = table {
                if let Ok(system_table) = DFTableProviderAsSystemTable::try_new(
                    *table_id,
                    table_name,
                    table::metadata::TableType::Temporary,
                    table,
                ) {
                    Some(Arc::new(system_table))
                } else {
                    warn!("failed to create pg_catalog system table {}", name);
                    None
                }
            } else {
                None
            }
        } else {
            None
        }
    }

    fn catalog_name(&self) -> &str {
        &self.catalog_name
    }
}

#[derive(Clone)]
pub struct CatalogManagerWrapper {
    catalog_name: String,
    catalog_manager: Weak<dyn CatalogManager>,
}

impl CatalogManagerWrapper {
    fn catalog_manager(&self) -> std::result::Result<Arc<dyn CatalogManager>, DataFusionError> {
        self.catalog_manager.upgrade().ok_or_else(|| {
            DataFusionError::Internal("Failed to access catalog manager".to_string())
        })
    }
}

impl std::fmt::Debug for CatalogManagerWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogManagerWrapper").finish()
    }
}

#[async_trait]
impl CatalogInfo for CatalogManagerWrapper {
    async fn catalog_names(&self) -> std::result::Result<Vec<String>, DataFusionError> {
        if self.catalog_name == DEFAULT_CATALOG_NAME {
            CatalogManager::catalog_names(self.catalog_manager()?.as_ref())
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))
        } else {
            Ok(vec![self.catalog_name.clone()])
        }
    }

    async fn schema_names(
        &self,
        catalog_name: &str,
    ) -> std::result::Result<Option<Vec<String>>, DataFusionError> {
        self.catalog_manager()?
            .schema_names(catalog_name, None)
            .await
            .map(Some)
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    async fn table_names(
        &self,
        catalog_name: &str,
        schema_name: &str,
    ) -> std::result::Result<Option<Vec<String>>, DataFusionError> {
        self.catalog_manager()?
            .table_names(catalog_name, schema_name, None)
            .await
            .map(Some)
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    async fn table_schema(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> std::result::Result<Option<SchemaRef>, DataFusionError> {
        let table = self
            .catalog_manager()?
            .table(catalog_name, schema_name, table_name, None)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(table.map(|t| t.schema().arrow_schema().clone()))
    }

    async fn table_type(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> std::result::Result<Option<TableType>, DataFusionError> {
        let table = self
            .catalog_manager()?
            .table(catalog_name, schema_name, table_name, None)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(table.map(|t| t.table_type().into()))
    }
}

struct DFTableProviderAsSystemTable {
    pub table_id: TableId,
    pub table_name: &'static str,
    pub table_type: table::metadata::TableType,
    pub schema: Arc<datatypes::schema::Schema>,
    pub table_provider: PgCatalogTable,
}

impl DFTableProviderAsSystemTable {
    pub fn try_new(
        table_id: TableId,
        table_name: &'static str,
        table_type: table::metadata::TableType,
        table_provider: PgCatalogTable,
    ) -> Result<Self> {
        let arrow_schema = table_provider.schema();
        let schema = Arc::new(arrow_schema.try_into().context(ProjectSchemaSnafu)?);
        Ok(Self {
            table_id,
            table_name,
            table_type,
            schema,
            table_provider,
        })
    }
}

impl SystemTable for DFTableProviderAsSystemTable {
    fn table_id(&self) -> TableId {
        self.table_id
    }

    fn table_name(&self) -> &'static str {
        self.table_name
    }

    fn schema(&self) -> Arc<datatypes::schema::Schema> {
        self.schema.clone()
    }

    fn table_type(&self) -> table::metadata::TableType {
        self.table_type
    }

    fn to_stream(&self, _request: ScanRequest) -> Result<SendableRecordBatchStream> {
        match &self.table_provider {
            PgCatalogTable::Static(table) => {
                let schema = self.schema.arrow_schema().clone();
                let data = table
                    .data()
                    .iter()
                    .map(|rb| Ok(rb.clone()))
                    .collect::<Vec<_>>();
                let stream = Box::pin(DfRecordBatchStreamAdapter::new(
                    schema,
                    futures::stream::iter(data),
                ));
                Ok(Box::pin(
                    RecordBatchStreamAdapter::try_new(stream)
                        .map_err(BoxedError::new)
                        .context(InternalSnafu)?,
                ))
            }

            PgCatalogTable::Dynamic(table) => {
                let stream = table.execute(Arc::new(TaskContext::default()));
                Ok(Box::pin(
                    RecordBatchStreamAdapter::try_new(stream)
                        .map_err(BoxedError::new)
                        .context(InternalSnafu)?,
                ))
            }

            PgCatalogTable::Empty(_) => {
                let schema = self.schema.arrow_schema().clone();
                let stream = Box::pin(DfRecordBatchStreamAdapter::new(
                    schema,
                    futures::stream::iter(vec![]),
                ));
                Ok(Box::pin(
                    RecordBatchStreamAdapter::try_new(stream)
                        .map_err(BoxedError::new)
                        .context(InternalSnafu)?,
                ))
            }
        }
    }
}
