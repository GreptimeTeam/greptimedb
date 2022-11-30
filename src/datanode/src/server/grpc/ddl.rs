// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use api::result::AdminResultBuilder;
use api::v1::{AdminResult, AlterExpr, CreateExpr, DropTableExpr};
use common_error::prelude::{ErrorExt, StatusCode};
use common_grpc_expr::{alter_expr_to_request, create_expr_to_request};
use common_query::Output;
use common_telemetry::{error, info};
use futures::TryFutureExt;
use session::context::SessionContext;
use snafu::prelude::*;
use table::requests::DropTableRequest;

use crate::error::{AlterExprToRequestSnafu, BumpTableIdSnafu, CreateExprToRequestSnafu};
use crate::instance::Instance;
use crate::sql::SqlRequest;

impl Instance {
    /// Handle gRPC create table requests.
    pub(crate) async fn handle_create(&self, expr: CreateExpr) -> AdminResult {
        // Respect CreateExpr's table id and region ids if present, or allocate table id
        // from local table id provider and set region id to 0.
        let table_id = if let Some(table_id) = expr.table_id {
            info!(
                "Creating table {:?}.{:?}.{:?} with table id from frontend: {}",
                expr.catalog_name, expr.schema_name, expr.table_name, table_id
            );
            table_id
        } else {
            match self.table_id_provider.as_ref() {
                None => {
                    return AdminResultBuilder::default()
                        .status_code(StatusCode::Internal as u32)
                        .err_msg("Table id provider absent in standalone mode".to_string())
                        .build();
                }
                Some(table_id_provider) => {
                    match table_id_provider
                        .next_table_id()
                        .await
                        .context(BumpTableIdSnafu)
                    {
                        Ok(table_id) => {
                            info!(
                        "Creating table {:?}.{:?}.{:?} with table id from catalog manager: {}",
                        &expr.catalog_name, &expr.schema_name, expr.table_name, table_id
                    );
                            table_id
                        }
                        Err(e) => {
                            error!(e;"Failed to create table id when creating table: {:?}.{:?}.{:?}", &expr.catalog_name, &expr.schema_name, expr.table_name);
                            return AdminResultBuilder::default()
                                .status_code(e.status_code() as u32)
                                .err_msg(e.to_string())
                                .build();
                        }
                    }
                }
            }
        };

        let request = create_expr_to_request(table_id, expr).context(CreateExprToRequestSnafu);
        let result = futures::future::ready(request)
            .and_then(|request| {
                self.sql_handler().execute(
                    SqlRequest::CreateTable(request),
                    Arc::new(SessionContext::new()),
                )
            })
            .await;
        match result {
            Ok(Output::AffectedRows(rows)) => AdminResultBuilder::default()
                .status_code(StatusCode::Success as u32)
                .mutate_result(rows as u32, 0)
                .build(),
            // Unreachable because we are executing "CREATE TABLE"; otherwise it's an internal bug.
            Ok(Output::Stream(_)) | Ok(Output::RecordBatches(_)) => unreachable!(),
            Err(err) => AdminResultBuilder::default()
                .status_code(err.status_code() as u32)
                .err_msg(err.to_string())
                .build(),
        }
    }

    pub(crate) async fn handle_alter(&self, expr: AlterExpr) -> AdminResult {
        let request = match alter_expr_to_request(expr)
            .context(AlterExprToRequestSnafu)
            .transpose()
        {
            None => {
                return AdminResultBuilder::default()
                    .status_code(StatusCode::Success as u32)
                    .mutate_result(0, 0)
                    .build()
            }
            Some(req) => req,
        };

        let result = futures::future::ready(request)
            .and_then(|request| {
                self.sql_handler()
                    .execute(SqlRequest::Alter(request), Arc::new(SessionContext::new()))
            })
            .await;
        match result {
            Ok(Output::AffectedRows(rows)) => AdminResultBuilder::default()
                .status_code(StatusCode::Success as u32)
                .mutate_result(rows as u32, 0)
                .build(),
            Ok(Output::Stream(_)) | Ok(Output::RecordBatches(_)) => unreachable!(),
            Err(err) => AdminResultBuilder::default()
                .status_code(err.status_code() as u32)
                .err_msg(err.to_string())
                .build(),
        }
    }

    pub(crate) async fn handle_drop_table(&self, expr: DropTableExpr) -> AdminResult {
        let req = DropTableRequest {
            catalog_name: expr.catalog_name,
            schema_name: expr.schema_name,
            table_name: expr.table_name,
        };
        let result = self
            .sql_handler()
            .execute(SqlRequest::DropTable(req), Arc::new(SessionContext::new()))
            .await;
        match result {
            Ok(Output::AffectedRows(rows)) => AdminResultBuilder::default()
                .status_code(StatusCode::Success as u32)
                .mutate_result(rows as _, 0)
                .build(),
            Ok(Output::Stream(_)) | Ok(Output::RecordBatches(_)) => unreachable!(),
            Err(err) => AdminResultBuilder::default()
                .status_code(err.status_code() as u32)
                .err_msg(err.to_string())
                .build(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::{ColumnDataType, ColumnDef};
    use common_catalog::consts::MIN_USER_TABLE_ID;
    use common_grpc_expr::create_table_schema;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnDefaultConstraint, ColumnSchema, SchemaBuilder, SchemaRef};
    use datatypes::value::Value;

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_create_expr_to_request() {
        common_telemetry::init_default_ut_logging();
        let expr = testing_create_expr();
        let request = create_expr_to_request(1024, expr).unwrap();
        assert_eq!(request.id, MIN_USER_TABLE_ID);
        assert_eq!(request.catalog_name, "greptime".to_string());
        assert_eq!(request.schema_name, "public".to_string());
        assert_eq!(request.table_name, "my-metrics");
        assert_eq!(request.desc, Some("blabla".to_string()));
        assert_eq!(request.schema, expected_table_schema());
        assert_eq!(request.primary_key_indices, vec![1, 0]);
        assert!(request.create_if_not_exists);

        let mut expr = testing_create_expr();
        expr.primary_keys = vec!["host".to_string(), "not-exist-column".to_string()];
        let result = create_expr_to_request(1025, expr);
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Column `not-exist-column` not found in table `my-metrics`"),
            "{}",
            err_msg
        );
    }

    #[test]
    fn test_create_table_schema() {
        let mut expr = testing_create_expr();
        let schema = create_table_schema(&expr).unwrap();
        assert_eq!(schema, expected_table_schema());

        expr.time_index = "not-exist-column".to_string();
        let result = create_table_schema(&expr);
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Missing timestamp column"),
            "actual: {}",
            err_msg
        );
    }

    #[test]

    fn test_create_column_schema() {
        let column_def = ColumnDef {
            name: "a".to_string(),
            datatype: 1024,
            is_nullable: true,
            default_constraint: None,
        };
        let result = column_def.try_as_column_schema();
        assert!(matches!(
            result.unwrap_err(),
            api::error::Error::UnknownColumnDataType { .. }
        ));

        let column_def = ColumnDef {
            name: "a".to_string(),
            datatype: ColumnDataType::String as i32,
            is_nullable: true,
            default_constraint: None,
        };
        let column_schema = column_def.try_as_column_schema().unwrap();
        assert_eq!(column_schema.name, "a");
        assert_eq!(column_schema.data_type, ConcreteDataType::string_datatype());
        assert!(column_schema.is_nullable());

        let default_constraint = ColumnDefaultConstraint::Value(Value::from("default value"));
        let column_def = ColumnDef {
            name: "a".to_string(),
            datatype: ColumnDataType::String as i32,
            is_nullable: true,
            default_constraint: Some(default_constraint.clone().try_into().unwrap()),
        };
        let column_schema = column_def.try_as_column_schema().unwrap();
        assert_eq!(column_schema.name, "a");
        assert_eq!(column_schema.data_type, ConcreteDataType::string_datatype());
        assert!(column_schema.is_nullable());
        assert_eq!(
            default_constraint,
            *column_schema.default_constraint().unwrap()
        );
    }

    fn testing_create_expr() -> CreateExpr {
        let column_defs = vec![
            ColumnDef {
                name: "host".to_string(),
                datatype: ColumnDataType::String as i32,
                is_nullable: false,
                default_constraint: None,
            },
            ColumnDef {
                name: "ts".to_string(),
                datatype: ColumnDataType::Timestamp as i32,
                is_nullable: false,
                default_constraint: None,
            },
            ColumnDef {
                name: "cpu".to_string(),
                datatype: ColumnDataType::Float32 as i32,
                is_nullable: true,
                default_constraint: None,
            },
            ColumnDef {
                name: "memory".to_string(),
                datatype: ColumnDataType::Float64 as i32,
                is_nullable: true,
                default_constraint: None,
            },
        ];
        CreateExpr {
            catalog_name: None,
            schema_name: None,
            table_name: "my-metrics".to_string(),
            desc: Some("blabla".to_string()),
            column_defs,
            time_index: "ts".to_string(),
            primary_keys: vec!["ts".to_string(), "host".to_string()],
            create_if_not_exists: true,
            table_options: Default::default(),
            table_id: Some(MIN_USER_TABLE_ID),
            region_ids: vec![0],
        }
    }

    fn expected_table_schema() -> SchemaRef {
        let column_schemas = vec![
            ColumnSchema::new("host", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new("ts", ConcreteDataType::timestamp_millis_datatype(), false)
                .with_time_index(true),
            ColumnSchema::new("cpu", ConcreteDataType::float32_datatype(), true),
            ColumnSchema::new("memory", ConcreteDataType::float64_datatype(), true),
        ];
        Arc::new(
            SchemaBuilder::try_from(column_schemas)
                .unwrap()
                .build()
                .unwrap(),
        )
    }
}
