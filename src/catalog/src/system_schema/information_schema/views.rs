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
use common_catalog::consts::INFORMATION_SCHEMA_VIEW_TABLE_ID;
use common_error::ext::BoxedError;
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
use futures::TryStreamExt;
use snafu::{OptionExt, ResultExt};
use store_api::storage::{ScanRequest, TableId};
use table::metadata::TableType;

use super::VIEWS;
use crate::error::{
    CastManagerSnafu, CreateRecordBatchSnafu, GetViewCacheSnafu, InternalSnafu, Result,
    UpgradeWeakCatalogManagerRefSnafu, ViewInfoNotFoundSnafu,
};
use crate::kvbackend::KvBackendCatalogManager;
use crate::system_schema::information_schema::{InformationTable, Predicates};
use crate::CatalogManager;
const INIT_CAPACITY: usize = 42;

pub const TABLE_CATALOG: &str = "table_catalog";
pub const TABLE_SCHEMA: &str = "table_schema";
pub const TABLE_NAME: &str = "table_name";
pub const VIEW_DEFINITION: &str = "view_definition";
pub const CHECK_OPTION: &str = "check_option";
pub const IS_UPDATABLE: &str = "is_updatable";
pub const DEFINER: &str = "definer";
pub const SECURITY_TYPE: &str = "security_type";
pub const CHARACTER_SET_CLIENT: &str = "character_set_client";
pub const COLLATION_CONNECTION: &str = "collation_connection";

/// The `information_schema.views` to provides information about views in databases.
pub(super) struct InformationSchemaViews {
    schema: SchemaRef,
    catalog_name: String,
    catalog_manager: Weak<dyn CatalogManager>,
}

impl InformationSchemaViews {
    pub(super) fn new(catalog_name: String, catalog_manager: Weak<dyn CatalogManager>) -> Self {
        Self {
            schema: Self::schema(),
            catalog_name,
            catalog_manager,
        }
    }

    pub(crate) fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            ColumnSchema::new(TABLE_CATALOG, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(TABLE_SCHEMA, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(TABLE_NAME, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(VIEW_DEFINITION, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(CHECK_OPTION, ConcreteDataType::string_datatype(), true),
            ColumnSchema::new(IS_UPDATABLE, ConcreteDataType::string_datatype(), true),
            ColumnSchema::new(DEFINER, ConcreteDataType::string_datatype(), true),
            ColumnSchema::new(SECURITY_TYPE, ConcreteDataType::string_datatype(), true),
            ColumnSchema::new(
                CHARACTER_SET_CLIENT,
                ConcreteDataType::string_datatype(),
                true,
            ),
            ColumnSchema::new(
                COLLATION_CONNECTION,
                ConcreteDataType::string_datatype(),
                true,
            ),
        ]))
    }

    fn builder(&self) -> InformationSchemaViewsBuilder {
        InformationSchemaViewsBuilder::new(
            self.schema.clone(),
            self.catalog_name.clone(),
            self.catalog_manager.clone(),
        )
    }
}

impl InformationTable for InformationSchemaViews {
    fn table_id(&self) -> TableId {
        INFORMATION_SCHEMA_VIEW_TABLE_ID
    }

    fn table_name(&self) -> &'static str {
        VIEWS
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
                    .make_views(Some(request))
                    .await
                    .map(|x| x.into_df_record_batch())
                    .map_err(|err| datafusion::error::DataFusionError::External(Box::new(err)))
            }),
        ));
        Ok(Box::pin(
            RecordBatchStreamAdapter::try_new(stream)
                .map_err(BoxedError::new)
                .context(InternalSnafu)?,
        ))
    }
}

/// Builds the `information_schema.VIEWS` table row by row
///
/// Columns are based on <https://dev.mysql.com/doc/refman/8.4/en/information-schema-views-table.html>
struct InformationSchemaViewsBuilder {
    schema: SchemaRef,
    catalog_name: String,
    catalog_manager: Weak<dyn CatalogManager>,

    catalog_names: StringVectorBuilder,
    schema_names: StringVectorBuilder,
    table_names: StringVectorBuilder,
    view_definitions: StringVectorBuilder,
    check_options: StringVectorBuilder,
    is_updatable: StringVectorBuilder,
    definer: StringVectorBuilder,
    security_type: StringVectorBuilder,
    character_set_client: StringVectorBuilder,
    collation_connection: StringVectorBuilder,
}

impl InformationSchemaViewsBuilder {
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
            table_names: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            view_definitions: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            check_options: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            is_updatable: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            definer: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            security_type: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            character_set_client: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            collation_connection: StringVectorBuilder::with_capacity(INIT_CAPACITY),
        }
    }

    /// Construct the `information_schema.views` virtual table
    async fn make_views(&mut self, request: Option<ScanRequest>) -> Result<RecordBatch> {
        let catalog_name = self.catalog_name.clone();
        let catalog_manager = self
            .catalog_manager
            .upgrade()
            .context(UpgradeWeakCatalogManagerRefSnafu)?;
        let predicates = Predicates::from_scan_request(&request);
        let view_info_cache = catalog_manager
            .as_any()
            .downcast_ref::<KvBackendCatalogManager>()
            .context(CastManagerSnafu)?
            .view_info_cache()?;

        for schema_name in catalog_manager.schema_names(&catalog_name).await? {
            let mut stream = catalog_manager.tables(&catalog_name, &schema_name);

            while let Some(table) = stream.try_next().await? {
                let table_info = table.table_info();
                if table_info.table_type == TableType::View {
                    let view_info = view_info_cache
                        .get(table_info.ident.table_id)
                        .await
                        .context(GetViewCacheSnafu)?
                        .context(ViewInfoNotFoundSnafu {
                            name: &table_info.name,
                        })?;
                    self.add_view(
                        &predicates,
                        &catalog_name,
                        &schema_name,
                        &table_info.name,
                        &view_info.definition,
                    )
                }
            }
        }

        self.finish()
    }

    fn add_view(
        &mut self,
        predicates: &Predicates,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        definition: &str,
    ) {
        let row = [
            (TABLE_CATALOG, &Value::from(catalog_name)),
            (TABLE_SCHEMA, &Value::from(schema_name)),
            (TABLE_NAME, &Value::from(table_name)),
        ];

        if !predicates.eval(&row) {
            return;
        }
        self.catalog_names.push(Some(catalog_name));
        self.schema_names.push(Some(schema_name));
        self.table_names.push(Some(table_name));
        self.view_definitions.push(Some(definition));
        self.check_options.push(None);
        // View is not updatable, statements such UPDATE , DELETE , and INSERT are illegal and are rejected.
        self.is_updatable.push(Some("NO"));
        self.definer.push(None);
        self.security_type.push(None);
        self.character_set_client.push(Some("utf8"));
        self.collation_connection.push(Some("utf8_bin"));
    }

    fn finish(&mut self) -> Result<RecordBatch> {
        let columns: Vec<VectorRef> = vec![
            Arc::new(self.catalog_names.finish()),
            Arc::new(self.schema_names.finish()),
            Arc::new(self.table_names.finish()),
            Arc::new(self.view_definitions.finish()),
            Arc::new(self.check_options.finish()),
            Arc::new(self.is_updatable.finish()),
            Arc::new(self.definer.finish()),
            Arc::new(self.security_type.finish()),
            Arc::new(self.character_set_client.finish()),
            Arc::new(self.collation_connection.finish()),
        ];
        RecordBatch::new(self.schema.clone(), columns).context(CreateRecordBatchSnafu)
    }
}

impl DfPartitionStream for InformationSchemaViews {
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
                    .make_views(None)
                    .await
                    .map(|x| x.into_df_record_batch())
                    .map_err(Into::into)
            }),
        ))
    }
}
