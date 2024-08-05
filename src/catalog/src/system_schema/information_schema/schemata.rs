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
use common_catalog::consts::INFORMATION_SCHEMA_SCHEMATA_TABLE_ID;
use common_error::ext::BoxedError;
use common_meta::key::schema_name::SchemaNameKey;
use common_recordbatch::adapter::RecordBatchStreamAdapter;
use common_recordbatch::{RecordBatch, SendableRecordBatchStream};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter as DfRecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream as DfPartitionStream;
use datafusion::physical_plan::SendableRecordBatchStream as DfSendableRecordBatchStream;
use datatypes::prelude::{ConcreteDataType, ScalarVectorBuilder, VectorRef};
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use datatypes::value::Value;
use datatypes::vectors::StringVectorBuilder;
use snafu::{OptionExt, ResultExt};
use store_api::storage::{ScanRequest, TableId};

use super::SCHEMATA;
use crate::error::{
    CreateRecordBatchSnafu, InternalSnafu, Result, TableMetadataManagerSnafu,
    UpgradeWeakCatalogManagerRefSnafu,
};
use crate::system_schema::information_schema::{InformationTable, Predicates};
use crate::system_schema::utils;
use crate::CatalogManager;

pub const CATALOG_NAME: &str = "catalog_name";
pub const SCHEMA_NAME: &str = "schema_name";
const DEFAULT_CHARACTER_SET_NAME: &str = "default_character_set_name";
const DEFAULT_COLLATION_NAME: &str = "default_collation_name";
/// The database options
pub const SCHEMA_OPTS: &str = "options";
const INIT_CAPACITY: usize = 42;

/// The `information_schema.schemata` table implementation.
pub(super) struct InformationSchemaSchemata {
    schema: SchemaRef,
    catalog_name: String,
    catalog_manager: Weak<dyn CatalogManager>,
}

impl InformationSchemaSchemata {
    pub(super) fn new(catalog_name: String, catalog_manager: Weak<dyn CatalogManager>) -> Self {
        Self {
            schema: Self::schema(),
            catalog_name,
            catalog_manager,
        }
    }

    pub(crate) fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            ColumnSchema::new(CATALOG_NAME, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(SCHEMA_NAME, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(
                DEFAULT_CHARACTER_SET_NAME,
                ConcreteDataType::string_datatype(),
                false,
            ),
            ColumnSchema::new(
                DEFAULT_COLLATION_NAME,
                ConcreteDataType::string_datatype(),
                false,
            ),
            ColumnSchema::new("sql_path", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new(SCHEMA_OPTS, ConcreteDataType::string_datatype(), true),
        ]))
    }

    fn builder(&self) -> InformationSchemaSchemataBuilder {
        InformationSchemaSchemataBuilder::new(
            self.schema.clone(),
            self.catalog_name.clone(),
            self.catalog_manager.clone(),
        )
    }
}

impl InformationTable for InformationSchemaSchemata {
    fn table_id(&self) -> TableId {
        INFORMATION_SCHEMA_SCHEMATA_TABLE_ID
    }

    fn table_name(&self) -> &'static str {
        SCHEMATA
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn to_stream(&self, request: ScanRequest) -> Result<SendableRecordBatchStream> {
        let schema = self.schema.arrow_schema().clone();
        let mut builder = self.builder();
        let stream = Box::pin(DfRecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move {
                builder
                    .make_schemata(Some(request))
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

/// Builds the `information_schema.schemata` table row by row
///
/// Columns are based on <https://docs.pingcap.com/tidb/stable/information-schema-schemata>
struct InformationSchemaSchemataBuilder {
    schema: SchemaRef,
    catalog_name: String,
    catalog_manager: Weak<dyn CatalogManager>,

    catalog_names: StringVectorBuilder,
    schema_names: StringVectorBuilder,
    charset_names: StringVectorBuilder,
    collation_names: StringVectorBuilder,
    sql_paths: StringVectorBuilder,
    schema_options: StringVectorBuilder,
}

impl InformationSchemaSchemataBuilder {
    fn new(
        schema: SchemaRef,
        catalog_name: String,
        catalog_manager: Weak<dyn CatalogManager>,
    ) -> Self {
        Self {
            schema,
            catalog_name,
            catalog_manager,
            catalog_names: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            schema_names: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            charset_names: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            collation_names: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            sql_paths: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            schema_options: StringVectorBuilder::with_capacity(INIT_CAPACITY),
        }
    }

    /// Construct the `information_schema.schemata` virtual table
    async fn make_schemata(&mut self, request: Option<ScanRequest>) -> Result<RecordBatch> {
        let catalog_name = self.catalog_name.clone();
        let catalog_manager = self
            .catalog_manager
            .upgrade()
            .context(UpgradeWeakCatalogManagerRefSnafu)?;
        let table_metadata_manager = utils::table_meta_manager(&self.catalog_manager)?;
        let predicates = Predicates::from_scan_request(&request);

        for schema_name in catalog_manager.schema_names(&catalog_name).await? {
            let opts = if let Some(table_metadata_manager) = &table_metadata_manager {
                table_metadata_manager
                    .schema_manager()
                    .get(SchemaNameKey::new(&catalog_name, &schema_name))
                    .await
                    .context(TableMetadataManagerSnafu)?
                    // information_schema is not available from this
                    // table_metadata_manager and we return None
                    .map(|schema_opts| format!("{schema_opts}"))
            } else {
                None
            };

            self.add_schema(
                &predicates,
                &catalog_name,
                &schema_name,
                opts.as_deref().unwrap_or(""),
            );
        }

        self.finish()
    }

    fn add_schema(
        &mut self,
        predicates: &Predicates,
        catalog_name: &str,
        schema_name: &str,
        schema_options: &str,
    ) {
        let row = [
            (CATALOG_NAME, &Value::from(catalog_name)),
            (SCHEMA_NAME, &Value::from(schema_name)),
            (DEFAULT_CHARACTER_SET_NAME, &Value::from("utf8")),
            (DEFAULT_COLLATION_NAME, &Value::from("utf8_bin")),
            (SCHEMA_OPTS, &Value::from(schema_options)),
        ];

        if !predicates.eval(&row) {
            return;
        }

        self.catalog_names.push(Some(catalog_name));
        self.schema_names.push(Some(schema_name));
        self.charset_names.push(Some("utf8"));
        self.collation_names.push(Some("utf8_bin"));
        self.sql_paths.push(None);
        self.schema_options.push(Some(schema_options));
    }

    fn finish(&mut self) -> Result<RecordBatch> {
        let columns: Vec<VectorRef> = vec![
            Arc::new(self.catalog_names.finish()),
            Arc::new(self.schema_names.finish()),
            Arc::new(self.charset_names.finish()),
            Arc::new(self.collation_names.finish()),
            Arc::new(self.sql_paths.finish()),
            Arc::new(self.schema_options.finish()),
        ];
        RecordBatch::new(self.schema.clone(), columns).context(CreateRecordBatchSnafu)
    }
}

impl DfPartitionStream for InformationSchemaSchemata {
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
                    .make_schemata(None)
                    .await
                    .map(|x| x.into_df_record_batch())
                    .map_err(Into::into)
            }),
        ))
    }
}
