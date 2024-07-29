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
use common_catalog::consts::PG_CATALOG_PG_CLASS_TABLE_ID;
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
use futures::TryStreamExt;
use snafu::{OptionExt, ResultExt};
use store_api::storage::ScanRequest;
use table::metadata::TableType;

use super::{OID_COLUMN_NAME, PG_CLASS};
use crate::error::{
    CreateRecordBatchSnafu, InternalSnafu, Result, UpgradeWeakCatalogManagerRefSnafu,
};
use crate::information_schema::Predicates;
use crate::system_schema::utils::tables::{string_column, u32_column};
use crate::system_schema::SystemTable;
use crate::CatalogManager;

// === column name ===
pub const RELNAME: &str = "relname";
pub const RELNAMESPACE: &str = "relnamespace";
pub const RELKIND: &str = "relkind";
pub const RELOWNER: &str = "relowner";

// === enum value of relkind ===
pub const RELKIND_TABLE: &str = "r";
pub const RELKIND_VIEW: &str = "v";

/// The initial capacity of the vector builders.
const INIT_CAPACITY: usize = 42;
/// The dummy owner id for the namespace.
const DUMMY_OWNER_ID: u32 = 0;

/// The `pg_catalog.pg_class` table implementation.
pub(super) struct PGClass {
    schema: SchemaRef,
    catalog_name: String,
    catalog_manager: Weak<dyn CatalogManager>,
}

impl PGClass {
    pub(super) fn new(catalog_name: String, catalog_manager: Weak<dyn CatalogManager>) -> Self {
        Self {
            schema: Self::schema(),
            catalog_name,
            catalog_manager,
        }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            u32_column(OID_COLUMN_NAME),
            string_column(RELNAME),
            string_column(RELNAMESPACE),
            string_column(RELKIND),
            u32_column(RELOWNER),
        ]))
    }

    fn builder(&self) -> PGClassBuilder {
        PGClassBuilder::new(
            self.schema.clone(),
            self.catalog_name.clone(),
            self.catalog_manager.clone(),
        )
    }
}

impl SystemTable for PGClass {
    fn table_id(&self) -> table::metadata::TableId {
        PG_CATALOG_PG_CLASS_TABLE_ID
    }

    fn table_name(&self) -> &'static str {
        PG_CLASS
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
                    .make_class(Some(request))
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

impl DfPartitionStream for PGClass {
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
                    .make_class(None)
                    .await
                    .map(|x| x.into_df_record_batch())
                    .map_err(Into::into)
            }),
        ))
    }
}

/// Builds the `pg_catalog.pg_class` table row by row
/// TODO(J0HN50N133): `relowner` is always the [`DUMMY_OWNER_ID`] cuz we don't have user.
/// Once we have user system, make it the actual owner of the table.
struct PGClassBuilder {
    schema: SchemaRef,
    catalog_name: String,
    catalog_manager: Weak<dyn CatalogManager>,

    oid: UInt32VectorBuilder,
    relname: StringVectorBuilder,
    relnamespace: StringVectorBuilder,
    relkind: StringVectorBuilder,
    relowner: UInt32VectorBuilder,
}

impl PGClassBuilder {
    fn new(
        schema: SchemaRef,
        catalog_name: String,
        catalog_manager: Weak<dyn CatalogManager>,
    ) -> Self {
        Self {
            schema,
            catalog_name,
            catalog_manager,

            oid: UInt32VectorBuilder::with_capacity(INIT_CAPACITY),
            relname: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            relnamespace: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            relkind: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            relowner: UInt32VectorBuilder::with_capacity(INIT_CAPACITY),
        }
    }

    async fn make_class(&mut self, request: Option<ScanRequest>) -> Result<RecordBatch> {
        let catalog_name = self.catalog_name.clone();
        let catalog_manager = self
            .catalog_manager
            .upgrade()
            .context(UpgradeWeakCatalogManagerRefSnafu)?;
        let predicates = Predicates::from_scan_request(&request);
        for schema_name in catalog_manager.schema_names(&catalog_name).await? {
            let mut stream = catalog_manager.tables(&catalog_name, &schema_name);
            while let Some(table) = stream.try_next().await? {
                let table_info = table.table_info();
                self.add_class(
                    &predicates,
                    table_info.table_id(),
                    &schema_name,
                    &table_info.name,
                    if table_info.table_type == TableType::View {
                        RELKIND_VIEW
                    } else {
                        RELKIND_TABLE
                    },
                );
            }
        }
        self.finish()
    }

    fn add_class(
        &mut self,
        predicates: &Predicates,
        oid: u32,
        schema: &str,
        table: &str,
        kind: &str,
    ) {
        let row = [
            (OID_COLUMN_NAME, &Value::from(oid)),
            (RELNAMESPACE, &Value::from(schema)),
            (RELNAME, &Value::from(table)),
            (RELKIND, &Value::from(kind)),
            (RELOWNER, &Value::from(DUMMY_OWNER_ID)),
        ];

        if !predicates.eval(&row) {
            return;
        }

        self.oid.push(Some(oid));
        self.relnamespace.push(Some(schema));
        self.relname.push(Some(table));
        self.relkind.push(Some(kind));
        self.relowner.push(Some(DUMMY_OWNER_ID));
    }

    fn finish(&mut self) -> Result<RecordBatch> {
        let columns: Vec<VectorRef> = vec![
            Arc::new(self.oid.finish()),
            Arc::new(self.relname.finish()),
            Arc::new(self.relnamespace.finish()),
            Arc::new(self.relkind.finish()),
            Arc::new(self.relowner.finish()),
        ];
        RecordBatch::new(self.schema.clone(), columns).context(CreateRecordBatchSnafu)
    }
}
