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

use api::v1::{AlterExpr, CreateTableExpr, DropTableExpr, FlushTableExpr};
use common_grpc_expr::{alter_expr_to_request, create_expr_to_request};
use common_query::Output;
use common_telemetry::info;
use session::context::QueryContext;
use snafu::prelude::*;
use table::requests::{DropTableRequest, FlushTableRequest};

use crate::error::{
    AlterExprToRequestSnafu, BumpTableIdSnafu, CreateExprToRequestSnafu,
    IncorrectInternalStateSnafu, Result,
};
use crate::instance::Instance;
use crate::sql::SqlRequest;

impl Instance {
    /// Handle gRPC create table requests.
    pub(crate) async fn handle_create(&self, expr: CreateTableExpr) -> Result<Output> {
        let table_name = format!(
            "{}.{}.{}",
            expr.catalog_name, expr.schema_name, expr.table_name
        );

        // TODO(LFC): Revisit table id related feature, add more tests.
        // Also merge this mod with mod instance::grpc.

        // Respect CreateExpr's table id and region ids if present, or allocate table id
        // from local table id provider and set region id to 0.
        let table_id = if let Some(table_id) = &expr.table_id {
            info!(
                "Creating table {table_name} with table id {} from Frontend",
                table_id.id
            );
            table_id.id
        } else {
            let provider =
                self.table_id_provider
                    .as_ref()
                    .context(IncorrectInternalStateSnafu {
                        state: "Table id provider absent in standalone mode",
                    })?;
            let table_id = provider.next_table_id().await.context(BumpTableIdSnafu)?;
            info!("Creating table {table_name} with table id {table_id} from TableIdProvider");
            table_id
        };

        let request = create_expr_to_request(table_id, expr).context(CreateExprToRequestSnafu)?;

        self.sql_handler()
            .execute(SqlRequest::CreateTable(request), QueryContext::arc())
            .await
    }

    pub(crate) async fn handle_alter(&self, expr: AlterExpr) -> Result<Output> {
        let request = alter_expr_to_request(expr).context(AlterExprToRequestSnafu)?;
        self.sql_handler()
            .execute(SqlRequest::Alter(request), QueryContext::arc())
            .await
    }

    pub(crate) async fn handle_drop_table(&self, expr: DropTableExpr) -> Result<Output> {
        let req = DropTableRequest {
            catalog_name: expr.catalog_name,
            schema_name: expr.schema_name,
            table_name: expr.table_name,
        };
        self.sql_handler()
            .execute(SqlRequest::DropTable(req), QueryContext::arc())
            .await
    }

    pub(crate) async fn handle_flush_table(&self, expr: FlushTableExpr) -> Result<Output> {
        let table_name = if expr.table_name.trim().is_empty() {
            None
        } else {
            Some(expr.table_name)
        };

        let req = FlushTableRequest {
            catalog_name: expr.catalog_name,
            schema_name: expr.schema_name,
            table_name,
            region_number: expr.region_id,
        };
        self.sql_handler()
            .execute(SqlRequest::FlushTable(req), QueryContext::arc())
            .await
    }
}

#[cfg(test)]
mod tests {
    use api::v1::{column_def, ColumnDataType, ColumnDef, TableId};
    use common_catalog::consts::MIN_USER_TABLE_ID;
    use common_grpc_expr::create_table_schema;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnDefaultConstraint, ColumnSchema, RawSchema};
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
        assert_eq!(request.desc, Some("blabla little magic fairy".to_string()));
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
            "actual: {err_msg}",
        );
    }

    #[test]
    fn test_create_column_schema() {
        let column_def = ColumnDef {
            name: "a".to_string(),
            datatype: 1024,
            is_nullable: true,
            default_constraint: vec![],
        };
        let result = column_def::try_as_column_schema(&column_def);
        assert!(matches!(
            result.unwrap_err(),
            api::error::Error::UnknownColumnDataType { .. }
        ));

        let column_def = ColumnDef {
            name: "a".to_string(),
            datatype: ColumnDataType::String as i32,
            is_nullable: true,
            default_constraint: vec![],
        };
        let column_schema = column_def::try_as_column_schema(&column_def).unwrap();
        assert_eq!(column_schema.name, "a");
        assert_eq!(column_schema.data_type, ConcreteDataType::string_datatype());
        assert!(column_schema.is_nullable());

        let default_constraint = ColumnDefaultConstraint::Value(Value::from("default value"));
        let column_def = ColumnDef {
            name: "a".to_string(),
            datatype: ColumnDataType::String as i32,
            is_nullable: true,
            default_constraint: default_constraint.clone().try_into().unwrap(),
        };
        let column_schema = column_def::try_as_column_schema(&column_def).unwrap();
        assert_eq!(column_schema.name, "a");
        assert_eq!(column_schema.data_type, ConcreteDataType::string_datatype());
        assert!(column_schema.is_nullable());
        assert_eq!(
            default_constraint,
            *column_schema.default_constraint().unwrap()
        );
    }

    fn testing_create_expr() -> CreateTableExpr {
        let column_defs = vec![
            ColumnDef {
                name: "host".to_string(),
                datatype: ColumnDataType::String as i32,
                is_nullable: false,
                default_constraint: vec![],
            },
            ColumnDef {
                name: "ts".to_string(),
                datatype: ColumnDataType::TimestampMillisecond as i32,
                is_nullable: false,
                default_constraint: vec![],
            },
            ColumnDef {
                name: "cpu".to_string(),
                datatype: ColumnDataType::Float32 as i32,
                is_nullable: true,
                default_constraint: vec![],
            },
            ColumnDef {
                name: "memory".to_string(),
                datatype: ColumnDataType::Float64 as i32,
                is_nullable: true,
                default_constraint: vec![],
            },
        ];
        CreateTableExpr {
            catalog_name: "".to_string(),
            schema_name: "".to_string(),
            table_name: "my-metrics".to_string(),
            desc: "blabla little magic fairy".to_string(),
            column_defs,
            time_index: "ts".to_string(),
            primary_keys: vec!["ts".to_string(), "host".to_string()],
            create_if_not_exists: true,
            table_options: Default::default(),
            table_id: Some(TableId {
                id: MIN_USER_TABLE_ID,
            }),
            region_ids: vec![0],
        }
    }

    fn expected_table_schema() -> RawSchema {
        let column_schemas = vec![
            ColumnSchema::new("host", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
                .with_time_index(true),
            ColumnSchema::new("cpu", ConcreteDataType::float32_datatype(), true),
            ColumnSchema::new("memory", ConcreteDataType::float64_datatype(), true),
        ];

        RawSchema::new(column_schemas)
    }
}
