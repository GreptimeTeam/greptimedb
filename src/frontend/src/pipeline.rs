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
use std::result::Result as StdResult;
use std::sync::{Arc, RwLock};

use api::v1::ddl_request::Expr;
use api::v1::greptime_request::Request;
use api::v1::value::ValueData;
use api::v1::{
    ColumnDataType, ColumnDef, ColumnSchema as PbColumnSchema, CreateTableExpr, DdlRequest, Row,
    RowInsertRequest, RowInsertRequests, Rows, SemanticType,
};
use arc_swap::access::Access;
use arc_swap::ArcSwap;
use catalog::{CatalogManagerRef, RegisterSystemTableRequest};
use client::DEFAULT_SCHEMA_NAME;
use common_catalog::consts::default_engine;
use common_error::ext::{BoxedError, ErrorExt};
use common_meta::table_name::TableName;
use common_query::Output;
use common_telemetry::{error, info};
use common_time::util;
use pipeline::transform::GreptimeTransformer;
use pipeline::{parse, Content, Pipeline};
use servers::query_handler::grpc::GrpcQueryHandler;
use session::context::{QueryContext, QueryContextBuilder, QueryContextRef};
use snafu::{OptionExt, ResultExt};
use table::metadata::TableInfo;
use table::TableRef;

use crate::error::{CatalogSnafu, Error, InsertPipelineSnafu, Result, TableNotFoundSnafu};

type FrontendGrpcQueryHandlerRef = Arc<dyn GrpcQueryHandler<Error = Error> + Send + Sync>;
type PipelineTableRef = Arc<PipelineTable>;

pub const PIPELINE_TABLE_NAME: &str = "pipelines";

struct DummyHandler;

impl DummyHandler {
    pub fn arc() -> Arc<Self> {
        Arc::new(Self {})
    }
}

#[async_trait::async_trait]
impl GrpcQueryHandler for DummyHandler {
    type Error = Error;

    async fn do_query(
        &self,
        _query: Request,
        _ctx: QueryContextRef,
    ) -> std::result::Result<Output, Self::Error> {
        unreachable!();
    }
}

struct PipelineOperator {
    grpc_handler: ArcSwap<FrontendGrpcQueryHandlerRef>,
    catalog_manager: CatalogManagerRef,
    tables: RwLock<HashMap<String, PipelineTableRef>>,
    pipelines: RwLock<HashMap<String, Pipeline<GreptimeTransformer>>>,
}

struct PipelineTable {
    grpc_handler: FrontendGrpcQueryHandlerRef,
    table: TableRef,
}

impl PipelineTable {
    pub fn new(table: TableRef, grpc_handler: FrontendGrpcQueryHandlerRef) -> Self {
        let grpc_handler = DummyHandler::arc();
        Self {
            grpc_handler,
            table,
        }
    }
    //ArcSwap::new(Arc::new(grpc_handler.clone() as _))

    fn build_insert_column_schemas() -> Vec<PbColumnSchema> {
        vec![
            PbColumnSchema {
                column_name: "name".to_string(),
                datatype: ColumnDataType::String.into(),
                semantic_type: SemanticType::Tag.into(),
                ..Default::default()
            },
            PbColumnSchema {
                column_name: "pipeline".to_string(),
                datatype: ColumnDataType::String.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            },
            PbColumnSchema {
                column_name: "created_at".to_string(),
                datatype: ColumnDataType::TimestampMillisecond.into(),
                semantic_type: SemanticType::Timestamp.into(),
                ..Default::default()
            },
            PbColumnSchema {
                column_name: "updated_at".to_string(),
                datatype: ColumnDataType::TimestampMillisecond.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            },
        ]
    }

    fn query_ctx(table_info: &TableInfo) -> QueryContextRef {
        QueryContextBuilder::default()
            .current_catalog(table_info.catalog_name.to_string())
            .current_schema(table_info.schema_name.to_string())
            .build()
            .into()
    }

    pub async fn insert(&self, schema: &str, name: &str, pipeline: &str) -> Result<()> {
        let now = util::current_time_millis();

        let table_info = self.table.table_info();

        let insert = RowInsertRequest {
            table_name: PIPELINE_TABLE_NAME.to_string(),
            rows: Some(Rows {
                schema: Self::build_insert_column_schemas(),
                rows: vec![Row {
                    values: vec![
                        ValueData::StringValue(name.to_string()).into(),
                        ValueData::StringValue(pipeline.to_string()).into(),
                        ValueData::TimestampMillisecondValue(now).into(),
                        ValueData::TimestampMillisecondValue(now).into(),
                    ],
                }],
            }),
        };

        let requests = RowInsertRequests {
            inserts: vec![insert],
        };

        let output = self
            .grpc_handler
            .do_query(Request::RowInserts(requests), Self::query_ctx(&table_info))
            .await
            .map_err(BoxedError::new)
            .context(InsertPipelineSnafu { name })?;

        info!(
            "Inserted script: {} into scripts table: {}, output: {:?}.",
            name,
            table_info.full_table_name(),
            output
        );

        Ok(())
    }
}

impl PipelineOperator {
    pub fn get_pipeline_table(&self, name: &str) -> Option<PipelineTableRef> {
        self.tables.read().unwrap().get(name).cloned()
    }

    pub fn build_pipeline_schema() -> (String, Vec<String>, Vec<ColumnDef>) {
        let created_at = "created_at";
        let updated_at = "updated_at";
        let pipeline_content = "pipeline";
        let pipeline_name = "name";

        (
            created_at.to_string(),
            vec![],
            vec![
                ColumnDef {
                    name: created_at.to_string(),
                    data_type: ColumnDataType::TimestampMillisecond as i32,
                    is_nullable: false,
                    default_constraint: vec![],
                    semantic_type: SemanticType::Timestamp as i32,
                    comment: "".to_string(),
                    datatype_extension: None,
                },
                ColumnDef {
                    name: updated_at.to_string(),
                    data_type: ColumnDataType::TimestampMillisecond as i32,
                    is_nullable: false,
                    default_constraint: vec![],
                    semantic_type: SemanticType::Field as i32,
                    comment: "".to_string(),
                    datatype_extension: None,
                },
                ColumnDef {
                    name: pipeline_content.to_string(),
                    data_type: ColumnDataType::String as i32,
                    is_nullable: false,
                    default_constraint: vec![],
                    semantic_type: SemanticType::Tag as i32,
                    comment: "".to_string(),
                    datatype_extension: None,
                },
                ColumnDef {
                    name: pipeline_name.to_string(),
                    data_type: ColumnDataType::String as i32,
                    is_nullable: false,
                    default_constraint: vec![],
                    semantic_type: SemanticType::Field as i32,
                    comment: "".to_string(),
                    datatype_extension: None,
                },
            ],
        )
    }

    pub fn create_table_request(&self, catalog: &str) -> RegisterSystemTableRequest {
        let (time_index, primary_keys, column_defs) = Self::build_pipeline_schema();

        let create_table_expr = CreateTableExpr {
            catalog_name: catalog.to_string(),
            // TODO(dennis): put the scripts table into `system` schema?
            // We always put the scripts table into `public` schema right now.
            schema_name: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: PIPELINE_TABLE_NAME.to_string(),
            desc: "GreptimeDB scripts table for Python".to_string(),
            column_defs,
            time_index,
            primary_keys,
            create_if_not_exists: true,
            table_options: Default::default(),
            table_id: None, // Should and will be assigned by Meta.
            engine: default_engine().to_string(),
        };

        RegisterSystemTableRequest {
            create_table_expr,
            open_hook: None,
        }
    }

    fn add_pipeline_table_to_cache(&self, catalog: &str, table: TableRef) {
        let mut tables = self.tables.write().unwrap();
        if tables.contains_key(catalog) {
            return;
        }
        // FIXME (qtang): we should add field to store in PipelineTable
        tables.insert(
            catalog.to_string(),
            Arc::new(PipelineTable {
                grpc_handler: self.grpc_handler.load().as_ref().clone(),
                table,
            }),
        );
    }

    pub async fn create_pipeline_table_if_not_exists(&self, catalog: &str) -> Result<()> {
        if let Some(_) = self.get_pipeline_table(catalog) {
            return Ok(());
        }

        let RegisterSystemTableRequest {
            create_table_expr: expr,
            open_hook,
        } = self.create_table_request(catalog);

        if let Some(table) = self
            .catalog_manager
            .table(&expr.catalog_name, &expr.schema_name, &expr.table_name)
            .await
            .context(CatalogSnafu)?
        {
            if let Some(open_hook) = open_hook {
                (open_hook)(table.clone()).await.context(CatalogSnafu)?;
            }

            self.add_pipeline_table_to_cache(catalog, table);

            return Ok(());
        }

        let table_name = TableName::new(&expr.catalog_name, &expr.schema_name, &expr.table_name);

        let _ = self
            .grpc_handler
            .load()
            .do_query(
                Request::Ddl(DdlRequest {
                    expr: Some(Expr::CreateTable(expr)),
                }),
                QueryContext::arc(),
            )
            .await?;

        let table = self
            .catalog_manager
            .table(
                &table_name.catalog_name,
                &table_name.schema_name,
                &table_name.table_name,
            )
            .await
            .context(CatalogSnafu)?
            .with_context(|| TableNotFoundSnafu {
                table_name: table_name.to_string(),
            })?;

        if let Some(open_hook) = open_hook {
            (open_hook)(table.clone()).await.context(CatalogSnafu)?;
        }

        info!(
            "Created scripts table {}.",
            table.table_info().full_table_name()
        );

        self.add_pipeline_table_to_cache(catalog, table);

        Ok(())
    }

    pub async fn insert_and_compile(
        &self,
        catalog: &str,
        schema: &str,
        name: &str,
        pipeline: &str,
    ) -> Result<Arc<Pipeline<GreptimeTransformer>>> {
        let yaml_content = Content::Yaml(pipeline.into());
        let pipeline: StdResult<Pipeline<GreptimeTransformer>, String> = parse(&yaml_content);
        todo!()
    }

    pub async fn insert_pipeline(
        &self,
        query_ctx: QueryContextRef,
        name: &str,
        pipeline: &str,
    ) -> servers::error::Result<()> {
        self.create_pipeline_table_if_not_exists(query_ctx.current_catalog())
            .await
            .map_err(|e| {
                if e.status_code().should_log_error() {
                    error!(e; "Failed to create pipeline table");
                }

                servers::error::InternalSnafu {
                    err_msg: e.to_string(),
                }
                .build()
            })?;

        let _s = self
            .insert_and_compile(
                query_ctx.current_catalog(),
                query_ctx.current_schema(),
                name,
                pipeline,
            )
            .await
            .map_err(|e| {
                if e.status_code().should_log_error() {
                    error!(e; "Failed to insert script");
                }

                BoxedError::new(e)
            })
            .context(servers::error::InsertScriptSnafu { name })?;

        Ok(())
    }
}

impl PipelineOperator {
    pub fn new(catalog_manager: CatalogManagerRef) -> Self {
        let grpc_handler = ArcSwap::new(Arc::new(DummyHandler::arc() as _));
        Self {
            grpc_handler: grpc_handler,
            catalog_manager,
            tables: RwLock::new(HashMap::new()),
            pipelines: RwLock::new(HashMap::new()),
        }
    }

    // FIXME (qtang): we should impl this
    pub async fn execute_script(
        &self,
        query_ctx: QueryContextRef,
        name: &str,
        params: HashMap<String, String>,
    ) -> servers::error::Result<()> {
        todo!()
    }
}
