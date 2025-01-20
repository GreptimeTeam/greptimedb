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

use std::sync::{Arc, Weak};

use arrow_schema::SchemaRef as ArrowSchemaRef;
use common_catalog::consts::PG_CATALOG_PG_DATABASE_TABLE_ID;
use common_error::ext::BoxedError;
use common_recordbatch::adapter::RecordBatchStreamAdapter;
use common_recordbatch::{DfSendableRecordBatchStream, RecordBatch};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter as DfRecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream as DfPartitionStream;
use datatypes::scalars::ScalarVectorBuilder;
use datatypes::schema::{Schema, SchemaRef};
use datatypes::value::Value;
use datatypes::vectors::{StringVectorBuilder, UInt32VectorBuilder, VectorRef};
use snafu::{OptionExt, ResultExt};
use store_api::storage::ScanRequest;

use super::pg_namespace::oid_map::PGNamespaceOidMapRef;
use super::{query_ctx, OID_COLUMN_NAME, PG_DATABASE};
use crate::error::{
    CreateRecordBatchSnafu, InternalSnafu, Result, UpgradeWeakCatalogManagerRefSnafu,
};
use crate::information_schema::Predicates;
use crate::system_schema::utils::tables::{string_column, u32_column};
use crate::system_schema::SystemTable;
use crate::CatalogManager;

// === column name ===
pub const DATNAME: &str = "datname";

/// The initial capacity of the vector builders.
const INIT_CAPACITY: usize = 42;

/// The `pg_catalog.database` table implementation.
pub(super) struct PGDatabase {
    schema: SchemaRef,
    catalog_name: String,
    catalog_manager: Weak<dyn CatalogManager>,

    // Workaround to convert schema_name to a numeric id
    namespace_oid_map: PGNamespaceOidMapRef,
}

impl std::fmt::Debug for PGDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PGDatabase")
            .field("schema", &self.schema)
            .field("catalog_name", &self.catalog_name)
            .finish()
    }
}

impl PGDatabase {
    pub(super) fn new(
        catalog_name: String,
        catalog_manager: Weak<dyn CatalogManager>,
        namespace_oid_map: PGNamespaceOidMapRef,
    ) -> Self {
        Self {
            schema: Self::schema(),
            catalog_name,
            catalog_manager,
            namespace_oid_map,
        }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            u32_column(OID_COLUMN_NAME),
            string_column(DATNAME),
        ]))
    }

    fn builder(&self) -> PGCDatabaseBuilder {
        PGCDatabaseBuilder::new(
            self.schema.clone(),
            self.catalog_name.clone(),
            self.catalog_manager.clone(),
            self.namespace_oid_map.clone(),
        )
    }
}

impl DfPartitionStream for PGDatabase {
    fn schema(&self) -> &ArrowSchemaRef {
        self.schema.arrow_schema()
    }

    fn execute(&self, _: Arc<TaskContext>) -> DfSendableRecordBatchStream {
        let schema = self.schema.arrow_schema().clone();
        let mut builder = self.builder();
        Box::pin(DfRecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move {
                builder
                    .make_database(None)
                    .await
                    .map(|x| x.into_df_record_batch())
                    .map_err(Into::into)
            }),
        ))
    }
}

impl SystemTable for PGDatabase {
    fn table_id(&self) -> table::metadata::TableId {
        PG_CATALOG_PG_DATABASE_TABLE_ID
    }

    fn table_name(&self) -> &'static str {
        PG_DATABASE
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn to_stream(
        &self,
        request: ScanRequest,
    ) -> Result<common_recordbatch::SendableRecordBatchStream> {
        let schema = self.schema.arrow_schema().clone();
        let mut builder = self.builder();
        let stream = Box::pin(DfRecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move {
                builder
                    .make_database(Some(request))
                    .await
                    .map(|x| x.into_df_record_batch())
                    .map_err(Into::into)
            }),
        ));
        Ok(Box::pin(
            RecordBatchStreamAdapter::try_new(stream)
                .map_err(BoxedError::new)
                .context(InternalSnafu)?,
        ))
    }
}

/// Builds the `pg_catalog.pg_database` table row by row
/// `oid` use schema name as a workaround since we don't have numeric schema id.
/// `nspname` is the schema name.
struct PGCDatabaseBuilder {
    schema: SchemaRef,
    catalog_name: String,
    catalog_manager: Weak<dyn CatalogManager>,
    namespace_oid_map: PGNamespaceOidMapRef,

    oid: UInt32VectorBuilder,
    datname: StringVectorBuilder,
}

impl PGCDatabaseBuilder {
    fn new(
        schema: SchemaRef,
        catalog_name: String,
        catalog_manager: Weak<dyn CatalogManager>,
        namespace_oid_map: PGNamespaceOidMapRef,
    ) -> Self {
        Self {
            schema,
            catalog_name,
            catalog_manager,
            namespace_oid_map,

            oid: UInt32VectorBuilder::with_capacity(INIT_CAPACITY),
            datname: StringVectorBuilder::with_capacity(INIT_CAPACITY),
        }
    }

    async fn make_database(&mut self, request: Option<ScanRequest>) -> Result<RecordBatch> {
        let catalog_name = self.catalog_name.clone();
        let catalog_manager = self
            .catalog_manager
            .upgrade()
            .context(UpgradeWeakCatalogManagerRefSnafu)?;
        let predicates = Predicates::from_scan_request(&request);
        for schema_name in catalog_manager
            .schema_names(&catalog_name, query_ctx())
            .await?
        {
            self.add_database(&predicates, &schema_name);
        }
        self.finish()
    }

    fn add_database(&mut self, predicates: &Predicates, schema_name: &str) {
        let oid = self.namespace_oid_map.get_oid(schema_name);
        let row: [(&str, &Value); 2] = [
            (OID_COLUMN_NAME, &Value::from(oid)),
            (DATNAME, &Value::from(schema_name)),
        ];

        if !predicates.eval(&row) {
            return;
        }

        self.oid.push(Some(oid));
        self.datname.push(Some(schema_name));
    }

    fn finish(&mut self) -> Result<RecordBatch> {
        let columns: Vec<VectorRef> =
            vec![Arc::new(self.oid.finish()), Arc::new(self.datname.finish())];
        RecordBatch::new(self.schema.clone(), columns).context(CreateRecordBatchSnafu)
    }
}
