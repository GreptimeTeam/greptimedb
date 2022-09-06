use std::sync::Arc;

use api::v1::{AdminResult, ColumnDataType, ColumnDef, CreateExpr};
use common_error::prelude::{ErrorExt, StatusCode};
use datatypes::prelude::*;
use datatypes::schema::{ColumnSchema, SchemaBuilder, SchemaRef};
use futures::TryFutureExt;
use query::Output;
use snafu::prelude::*;
use table::requests::CreateTableRequest;

use crate::error::{self, Result};
use crate::instance::Instance;
use crate::server::grpc::handler::AdminResultBuilder;
use crate::sql::SqlRequest;

impl Instance {
    pub(crate) async fn handle_create(&self, expr: CreateExpr) -> AdminResult {
        let request = self.create_expr_to_request(expr);
        let result = futures::future::ready(request)
            .and_then(|request| self.sql_handler().execute(SqlRequest::Create(request)))
            .await;
        match result {
            Ok(Output::AffectedRows(rows)) => AdminResultBuilder::default()
                .status_code(StatusCode::Success as u32)
                .mutate_result(rows as u32, 0)
                .build(),
            // Unreachable because we are executing "CREATE TABLE"; otherwise it's an internal bug.
            Ok(Output::RecordBatch(_)) => unreachable!(),
            Err(err) => AdminResultBuilder::default()
                .status_code(err.status_code() as u32)
                .err_msg(err.to_string())
                .build(),
        }
    }

    fn create_expr_to_request(&self, expr: CreateExpr) -> Result<CreateTableRequest> {
        let schema = create_table_schema(&expr)?;

        let primary_key_indices = expr
            .primary_keys
            .iter()
            .map(|key| {
                schema
                    .column_index_by_name(key)
                    .context(error::KeyColumnNotFoundSnafu { name: key })
            })
            .collect::<Result<Vec<usize>>>()?;

        let table_id = self.catalog_manager().next_table_id();

        Ok(CreateTableRequest {
            id: table_id,
            catalog_name: expr.catalog_name,
            schema_name: expr.schema_name,
            table_name: expr.table_name,
            desc: expr.desc,
            schema,
            primary_key_indices,
            create_if_not_exists: expr.create_if_not_exists,
            table_options: expr.table_options,
        })
    }
}

fn create_table_schema(expr: &CreateExpr) -> Result<SchemaRef> {
    let column_schemas = expr
        .column_defs
        .iter()
        .map(create_column_schema)
        .collect::<Result<Vec<ColumnSchema>>>()?;
    let ts_index = column_schemas
        .iter()
        .enumerate()
        .find_map(|(i, column)| {
            if column.name == expr.time_index {
                Some(i)
            } else {
                None
            }
        })
        .context(error::KeyColumnNotFoundSnafu {
            name: &expr.time_index,
        })?;
    Ok(Arc::new(
        SchemaBuilder::from(column_schemas)
            .timestamp_index(ts_index)
            .build()
            .context(error::CreateSchemaSnafu)?,
    ))
}

fn create_column_schema(column_def: &ColumnDef) -> Result<ColumnSchema> {
    let data_type =
        ColumnDataType::from_i32(column_def.data_type).context(error::InvalidColumnDefSnafu {
            msg: format!("unknown ColumnDataType {}", column_def.data_type),
        })?;
    let data_type = match data_type {
        ColumnDataType::Boolean => ConcreteDataType::boolean_datatype(),
        ColumnDataType::Int8 => ConcreteDataType::int8_datatype(),
        ColumnDataType::Int16 => ConcreteDataType::int16_datatype(),
        ColumnDataType::Int32 => ConcreteDataType::int32_datatype(),
        ColumnDataType::Int64 => ConcreteDataType::int64_datatype(),
        ColumnDataType::Uint8 => ConcreteDataType::uint8_datatype(),
        ColumnDataType::Uint16 => ConcreteDataType::uint16_datatype(),
        ColumnDataType::Uint32 => ConcreteDataType::uint32_datatype(),
        ColumnDataType::Uint64 => ConcreteDataType::uint64_datatype(),
        ColumnDataType::Float32 => ConcreteDataType::float32_datatype(),
        ColumnDataType::Float64 => ConcreteDataType::float64_datatype(),
        ColumnDataType::Binary => ConcreteDataType::binary_datatype(),
        ColumnDataType::String => ConcreteDataType::string_datatype(),
        ColumnDataType::Date => ConcreteDataType::date_datatype(),
        ColumnDataType::Datetime => ConcreteDataType::datetime_datatype(),
    };
    Ok(ColumnSchema {
        name: column_def.name.clone(),
        data_type,
        is_nullable: column_def.is_nullable,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::tests::test_util;

    #[tokio::test]
    async fn test_create_expr_to_request() {
        let (opts, _guard) = test_util::create_tmp_dir_and_datanode_opts();
        let instance = Instance::new(&opts).await.unwrap();
        instance.start().await.unwrap();

        let expr = testing_create_expr();
        let request = instance.create_expr_to_request(expr).unwrap();
        assert_eq!(request.id, 1);
        assert_eq!(request.catalog_name, None);
        assert_eq!(request.schema_name, None);
        assert_eq!(request.table_name, "my-metrics");
        assert_eq!(request.desc, Some("blabla".to_string()));
        assert_eq!(request.schema, expected_table_schema());
        assert_eq!(request.primary_key_indices, vec![1, 0]);
        assert!(request.create_if_not_exists);

        let mut expr = testing_create_expr();
        expr.primary_keys = vec!["host".to_string(), "not-exist-column".to_string()];
        let result = instance.create_expr_to_request(expr);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Specified timestamp key or primary key column not found: not-exist-column"));
    }

    #[test]
    fn test_create_table_schema() {
        let mut expr = testing_create_expr();
        let schema = create_table_schema(&expr).unwrap();
        assert_eq!(schema, expected_table_schema());

        expr.time_index = "not-exist-column".to_string();
        let result = create_table_schema(&expr);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Specified timestamp key or primary key column not found: not-exist-column"));
    }

    #[test]
    fn test_create_column_schema() {
        let column_def = ColumnDef {
            name: "a".to_string(),
            data_type: 1024,
            is_nullable: true,
        };
        let result = create_column_schema(&column_def);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid ColumnDef in protobuf msg: unknown ColumnDataType 1024"));

        let column_def = ColumnDef {
            name: "a".to_string(),
            data_type: 12, // string
            is_nullable: true,
        };
        let column_schema = create_column_schema(&column_def).unwrap();
        assert_eq!(column_schema.name, "a");
        assert_eq!(column_schema.data_type, ConcreteDataType::string_datatype());
        assert!(column_schema.is_nullable);
    }

    fn testing_create_expr() -> CreateExpr {
        let column_defs = vec![
            ColumnDef {
                name: "host".to_string(),
                data_type: 12, // string
                is_nullable: false,
            },
            ColumnDef {
                name: "ts".to_string(),
                data_type: 4, // int64
                is_nullable: false,
            },
            ColumnDef {
                name: "cpu".to_string(),
                data_type: 9, // float32
                is_nullable: true,
            },
            ColumnDef {
                name: "memory".to_string(),
                data_type: 10, // float64
                is_nullable: true,
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
            table_options: HashMap::new(),
        }
    }

    fn expected_table_schema() -> SchemaRef {
        let column_schemas = vec![
            ColumnSchema {
                name: "host".to_string(),
                data_type: ConcreteDataType::string_datatype(),
                is_nullable: false,
            },
            ColumnSchema {
                name: "ts".to_string(),
                data_type: ConcreteDataType::int64_datatype(),
                is_nullable: false,
            },
            ColumnSchema {
                name: "cpu".to_string(),
                data_type: ConcreteDataType::float32_datatype(),
                is_nullable: true,
            },
            ColumnSchema {
                name: "memory".to_string(),
                data_type: ConcreteDataType::float64_datatype(),
                is_nullable: true,
            },
        ];
        Arc::new(
            SchemaBuilder::from(column_schemas)
                .timestamp_index(1)
                .build()
                .unwrap(),
        )
    }
}
