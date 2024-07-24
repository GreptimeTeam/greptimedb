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
use common_catalog::consts::PG_CATALOG_PG_NAMESPACE_TABLE_ID;
use common_error::ext::BoxedError;
use common_recordbatch::adapter::RecordBatchStreamAdapter;
use common_recordbatch::{DfSendableRecordBatchStream, RecordBatch, SendableRecordBatchStream};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter as DfRecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream as DfPartitionStream;
use datatypes::scalars::ScalarVectorBuilder;
use datatypes::schema::{Schema, SchemaRef};
use datatypes::value::Value;
use datatypes::vectors::{StringVectorBuilder, VectorRef};
use snafu::{OptionExt, ResultExt};
use store_api::storage::ScanRequest;

use super::{OID_COLUMN_NAME, PG_NAMESPACE};
use crate::error::{
    CreateRecordBatchSnafu, InternalSnafu, Result, UpgradeWeakCatalogManagerRefSnafu,
};
use crate::information_schema::Predicates;
use crate::system_schema::utils::tables::string_column;
use crate::system_schema::SystemTable;
use crate::CatalogManager;

/// The `pg_catalog.pg_namespace` table implementation.
/// namespace is a schema in greptime

const NSPNAME: &str = "nspname";
const INIT_CAPACITY: usize = 42;

pub(super) struct PGNamespace {
    schema: SchemaRef,
    catalog_name: String,
    catalog_manager: Weak<dyn CatalogManager>,
}

impl PGNamespace {
    pub(super) fn new(catalog_name: String, catalog_manager: Weak<dyn CatalogManager>) -> Self {
        Self {
            schema: Self::schema(),
            catalog_name,
            catalog_manager,
        }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            // TODO(J0HN50N133): we do not have a numeric schema id, use schema name as a workaround. Use a proper schema id once we have it.
            string_column(OID_COLUMN_NAME),
            string_column(NSPNAME),
        ]))
    }

    fn builder(&self) -> PGNamespaceBuilder {
        PGNamespaceBuilder::new(
            self.schema.clone(),
            self.catalog_name.clone(),
            self.catalog_manager.clone(),
        )
    }
}

impl SystemTable for PGNamespace {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_id(&self) -> table::metadata::TableId {
        PG_CATALOG_PG_NAMESPACE_TABLE_ID
    }

    fn table_name(&self) -> &'static str {
        PG_NAMESPACE
    }

    fn to_stream(&self, request: ScanRequest) -> Result<SendableRecordBatchStream> {
        let schema = self.schema.arrow_schema().clone();
        let mut builder = self.builder();
        let stream = Box::pin(DfRecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move {
                builder
                    .make_namespace(Some(request))
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

impl DfPartitionStream for PGNamespace {
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
                    .make_namespace(None)
                    .await
                    .map(|x| x.into_df_record_batch())
                    .map_err(Into::into)
            }),
        ))
    }
}

/// Builds the `pg_catalog.pg_namespace` table row by row
/// `oid` use schema name as a workaround since we don't have numeric schema id.
/// `nspname` is the schema name.
struct PGNamespaceBuilder {
    schema: SchemaRef,
    catalog_name: String,
    catalog_manager: Weak<dyn CatalogManager>,

    oid: StringVectorBuilder,
    nspname: StringVectorBuilder,
}

impl PGNamespaceBuilder {
    fn new(
        schema: SchemaRef,
        catalog_name: String,
        catalog_manager: Weak<dyn CatalogManager>,
    ) -> Self {
        Self {
            schema,
            catalog_name,
            catalog_manager,
            oid: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            nspname: StringVectorBuilder::with_capacity(INIT_CAPACITY),
        }
    }

    /// Construct the `pg_catalog.pg_namespace` virtual table
    async fn make_namespace(&mut self, request: Option<ScanRequest>) -> Result<RecordBatch> {
        let catalog_name = self.catalog_name.clone();
        let catalog_manager = self
            .catalog_manager
            .upgrade()
            .context(UpgradeWeakCatalogManagerRefSnafu)?;
        let predicates = Predicates::from_scan_request(&request);
        for schema_name in catalog_manager.schema_names(&catalog_name).await? {
            self.add_namespace(&predicates, &schema_name);
        }
        self.finish()
    }
    fn finish(&mut self) -> Result<RecordBatch> {
        let columns: Vec<VectorRef> =
            vec![Arc::new(self.oid.finish()), Arc::new(self.nspname.finish())];
        RecordBatch::new(self.schema.clone(), columns).context(CreateRecordBatchSnafu)
    }

    fn add_namespace(&mut self, predicates: &Predicates, schema_name: &str) {
        let row = [
            (OID_COLUMN_NAME, &Value::from(schema_name)),
            (NSPNAME, &Value::from(schema_name)),
        ];
        if !predicates.eval(&row) {
            return;
        }
        self.oid.push(Some(schema_name));
        self.nspname.push(Some(schema_name));
    }
}
