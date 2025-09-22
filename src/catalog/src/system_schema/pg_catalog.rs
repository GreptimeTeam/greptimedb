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

mod pg_catalog_memory_table;
mod pg_class;
mod pg_database;
mod pg_namespace;
mod table_names;

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Weak};

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use common_catalog::consts::{self, DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, PG_CATALOG_NAME};
use common_error::ext::BoxedError;
use common_recordbatch::adapter::RecordBatchStreamAdapter;
use common_recordbatch::SendableRecordBatchStream;
use datafusion::datasource::TableType;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter as DfRecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion_postgres::pg_catalog::catalog_info::CatalogInfo;
use datafusion_postgres::pg_catalog::ArrowTable;
use datatypes::schema::ColumnSchema;
use lazy_static::lazy_static;
use paste::paste;
use pg_catalog_memory_table::get_schema_columns;
use pg_class::PGClass;
use pg_database::PGDatabase;
use pg_namespace::PGNamespace;
use session::context::{Channel, QueryContext};
use snafu::ResultExt;
use store_api::storage::ScanRequest;
use table::metadata::TableId;
use table::TableRef;
pub use table_names::*;

use self::pg_namespace::oid_map::{PGNamespaceOidMap, PGNamespaceOidMapRef};
use crate::error::{InternalSnafu, ProjectSchemaSnafu, Result};
use crate::system_schema::memory_table::MemoryTable;
use crate::system_schema::utils::tables::u32_column;
use crate::system_schema::{SystemSchemaProvider, SystemSchemaProviderInner, SystemTableRef};
use crate::CatalogManager;

use super::SystemTable;

lazy_static! {
    static ref MEMORY_TABLES: &'static [&'static str] = &[table_names::PG_TYPE];
}

/// The column name for the OID column.
/// The OID column is a unique identifier of type u32 for each object in the database.
const OID_COLUMN_NAME: &str = "oid";

fn oid_column() -> ColumnSchema {
    u32_column(OID_COLUMN_NAME)
}

/// [`PGCatalogProvider`] is the provider for a schema named `pg_catalog`, it is not a catalog.
pub struct PGCatalogProvider {
    catalog_name: String,
    catalog_manager: Weak<dyn CatalogManager>,
    tables: HashMap<String, TableRef>,

    // Workaround to store mapping of schema_name to a numeric id
    namespace_oid_map: PGNamespaceOidMapRef,
}

impl SystemSchemaProvider for PGCatalogProvider {
    fn tables(&self) -> &HashMap<String, TableRef> {
        assert!(!self.tables.is_empty());

        &self.tables
    }
}

// TODO(j0hn50n133): Not sure whether to avoid duplication with `information_schema` or not.
macro_rules! setup_memory_table {
    ($name: expr) => {
        paste! {
            {
                let (schema, columns) = get_schema_columns($name);
                Some(Arc::new(MemoryTable::new(
                    consts::[<PG_CATALOG_ $name  _TABLE_ID>],
                    $name,
                    schema,
                    columns
                )) as _)
            }
        }
    };
}

impl PGCatalogProvider {
    pub fn new(catalog_name: String, catalog_manager: Weak<dyn CatalogManager>) -> Self {
        let mut provider = Self {
            catalog_name,
            catalog_manager,
            tables: HashMap::new(),
            namespace_oid_map: Arc::new(PGNamespaceOidMap::new()),
        };
        provider.build_tables();
        provider
    }

    fn build_tables(&mut self) {
        // SECURITY NOTE:
        // Must follow the same security rules as [`InformationSchemaProvider::build_tables`].
        let mut tables = HashMap::new();
        // TODO(J0HN50N133): modeling the table_name as a enum type to get rid of expect/unwrap here
        // It's safe to unwrap here because we are sure that the constants have been handle correctly inside system_table.
        for name in MEMORY_TABLES.iter() {
            tables.insert(name.to_string(), self.build_table(name).expect(name));
        }
        tables.insert(
            PG_NAMESPACE.to_string(),
            self.build_table(PG_NAMESPACE).expect(PG_NAMESPACE),
        );
        tables.insert(
            PG_CLASS.to_string(),
            self.build_table(PG_CLASS).expect(PG_CLASS),
        );
        tables.insert(
            PG_DATABASE.to_string(),
            self.build_table(PG_DATABASE).expect(PG_DATABASE),
        );
        self.tables = tables;
    }
}

impl SystemSchemaProviderInner for PGCatalogProvider {
    fn schema_name() -> &'static str {
        PG_CATALOG_NAME
    }

    fn system_table(&self, name: &str) -> Option<SystemTableRef> {
        match name {
            table_names::PG_TYPE => setup_memory_table!(PG_TYPE),
            table_names::PG_NAMESPACE => Some(Arc::new(PGNamespace::new(
                self.catalog_name.clone(),
                self.catalog_manager.clone(),
                self.namespace_oid_map.clone(),
            ))),
            table_names::PG_CLASS => Some(Arc::new(PGClass::new(
                self.catalog_name.clone(),
                self.catalog_manager.clone(),
                self.namespace_oid_map.clone(),
            ))),
            table_names::PG_DATABASE => Some(Arc::new(PGDatabase::new(
                self.catalog_name.clone(),
                self.catalog_manager.clone(),
                self.namespace_oid_map.clone(),
            ))),
            _ => None,
        }
    }

    fn catalog_name(&self) -> &str {
        &self.catalog_name
    }
}

/// Provide query context to call the [`CatalogManager`]'s method.
static PG_QUERY_CTX: LazyLock<QueryContext> = LazyLock::new(|| {
    QueryContext::with_channel(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, Channel::Postgres)
});

fn query_ctx() -> Option<&'static QueryContext> {
    Some(&PG_QUERY_CTX)
}

#[derive(Clone)]
pub struct CatalogManagerWrapper(Arc<dyn CatalogManager>);

impl std::fmt::Debug for CatalogManagerWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogManagerWrapper").finish()
    }
}

// TODO(sunng87): address query_context
#[async_trait]
impl CatalogInfo for CatalogManagerWrapper {
    async fn catalog_names(&self) -> std::result::Result<Vec<String>, DataFusionError> {
        CatalogManager::catalog_names(self.0.as_ref())
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    async fn schema_names(
        &self,
        catalog_name: &str,
    ) -> std::result::Result<Option<Vec<String>>, DataFusionError> {
        self.0
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
        self.0
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
            .0
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
            .0
            .table(catalog_name, schema_name, table_name, None)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(table.map(|t| t.table_type().into()))
    }
}

enum PgCatalogTable {
    Static(Arc<ArrowTable>),
    Dynamic(Arc<dyn PartitionStream>),
}

impl PgCatalogTable {
    fn schema(&self) -> SchemaRef {
        match self {
            Self::Static(table) => table.schema().clone(),
            Self::Dynamic(table) => table.schema().clone(),
        }
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
        let schema = Arc::new(
            table_provider
                .schema()
                .try_into()
                .context(ProjectSchemaSnafu)?,
        );
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
                    .into_iter()
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
        }
    }
}
