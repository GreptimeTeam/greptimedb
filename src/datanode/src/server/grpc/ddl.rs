use std::sync::Arc;

use api::helper::ColumnDataTypeWrapper;
use api::result::AdminResultBuilder;
use api::v1::{alter_expr::Kind, AdminResult, AlterExpr, ColumnDef, CreateExpr};
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_error::prelude::{ErrorExt, StatusCode};
use common_query::Output;
use common_telemetry::info;
use datatypes::schema::ColumnDefaultConstraint;
use datatypes::schema::{ColumnSchema, SchemaBuilder, SchemaRef};
use futures::TryFutureExt;
use snafu::prelude::*;
use table::metadata::TableId;
use table::requests::{AddColumnRequest, AlterKind, AlterTableRequest, CreateTableRequest};

use crate::error::{
    self, BumpTableIdSnafu, ColumnDefaultConstraintSnafu, IllegalCreateRequestSnafu,
    MissingFieldSnafu, Result,
};
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
            let table_id = self
                .table_id_provider
                .as_ref()
                .context(IllegalCreateRequestSnafu)
                .unwrap()
                .next_table_id()
                .await
                .context(BumpTableIdSnafu)
                .unwrap();
            // info!(
            //     "Creating table {}.{}.{} with table id from catalog manager: {}",
            //     catalog_name, schema_name, expr.table_name, table_id
            // );
            table_id
        };

        let request = create_expr_to_request(table_id, expr).await;
        let result = futures::future::ready(request)
            .and_then(|request| self.sql_handler().execute(SqlRequest::CreateTable(request)))
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
        let request = match alter_expr_to_request(expr).transpose() {
            Some(req) => req,
            None => {
                return AdminResultBuilder::default()
                    .status_code(StatusCode::Success as u32)
                    .mutate_result(0, 0)
                    .build()
            }
        };

        let result = futures::future::ready(request)
            .and_then(|request| self.sql_handler().execute(SqlRequest::Alter(request)))
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
}

async fn create_expr_to_request(table_id: TableId, expr: CreateExpr) -> Result<CreateTableRequest> {
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

    let catalog_name = expr
        .catalog_name
        .unwrap_or_else(|| DEFAULT_CATALOG_NAME.to_string());
    let schema_name = expr
        .schema_name
        .unwrap_or_else(|| DEFAULT_SCHEMA_NAME.to_string());

    let region_ids = if expr.region_ids.is_empty() {
        vec![0]
    } else {
        expr.region_ids
    };

    Ok(CreateTableRequest {
        id: table_id,
        catalog_name,
        schema_name,
        table_name: expr.table_name,
        desc: expr.desc,
        schema,
        region_numbers: region_ids,
        primary_key_indices,
        create_if_not_exists: expr.create_if_not_exists,
        table_options: expr.table_options,
    })
}

fn alter_expr_to_request(expr: AlterExpr) -> Result<Option<AlterTableRequest>> {
    match expr.kind {
        Some(Kind::AddColumns(add_columns)) => {
            let mut add_column_requests = vec![];
            for add_column_expr in add_columns.add_columns {
                let column_def = add_column_expr.column_def.context(MissingFieldSnafu {
                    field: "column_def",
                })?;

                let schema = create_column_schema(&column_def)?;
                add_column_requests.push(AddColumnRequest {
                    column_schema: schema,
                    is_key: add_column_expr.is_key,
                })
            }

            let alter_kind = AlterKind::AddColumns {
                columns: add_column_requests,
            };

            let request = AlterTableRequest {
                catalog_name: expr.catalog_name,
                schema_name: expr.schema_name,
                table_name: expr.table_name,
                alter_kind,
            };
            Ok(Some(request))
        }
        None => Ok(None),
    }
}

fn create_table_schema(expr: &CreateExpr) -> Result<SchemaRef> {
    let column_schemas = expr
        .column_defs
        .iter()
        .map(create_column_schema)
        .collect::<Result<Vec<ColumnSchema>>>()?;

    ensure!(
        column_schemas
            .iter()
            .any(|column| column.name == expr.time_index),
        error::KeyColumnNotFoundSnafu {
            name: &expr.time_index,
        }
    );

    let column_schemas = column_schemas
        .into_iter()
        .map(|column_schema| {
            if column_schema.name == expr.time_index {
                column_schema.with_time_index(true)
            } else {
                column_schema
            }
        })
        .collect::<Vec<_>>();

    Ok(Arc::new(
        SchemaBuilder::try_from(column_schemas)
            .context(error::CreateSchemaSnafu)?
            .build()
            .context(error::CreateSchemaSnafu)?,
    ))
}

fn create_column_schema(column_def: &ColumnDef) -> Result<ColumnSchema> {
    let data_type =
        ColumnDataTypeWrapper::try_new(column_def.datatype).context(error::ColumnDataTypeSnafu)?;
    let default_constraint = match &column_def.default_constraint {
        None => None,
        Some(v) => {
            Some(ColumnDefaultConstraint::try_from(&v[..]).context(ColumnDefaultConstraintSnafu)?)
        }
    };
    ColumnSchema::new(
        column_def.name.clone(),
        data_type.into(),
        column_def.is_nullable,
    )
    .with_default_constraint(default_constraint)
    .context(ColumnDefaultConstraintSnafu)
}

#[cfg(test)]
mod tests {
    use common_catalog::consts::MIN_USER_TABLE_ID;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::value::Value;

    use super::*;
    use crate::tests::test_util;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_create_expr_to_request() {
        common_telemetry::init_default_ut_logging();
        let (opts, _guard) = test_util::create_tmp_dir_and_datanode_opts("create_expr_to_request");
        let instance = Instance::with_mock_meta_client(&opts).await.unwrap();
        instance.start().await.unwrap();

        let expr = testing_create_expr();
        let request = create_expr_to_request(1024, expr).await.unwrap();
        assert_eq!(request.id, common_catalog::consts::MIN_USER_TABLE_ID);
        assert_eq!(request.catalog_name, "greptime".to_string());
        assert_eq!(request.schema_name, "public".to_string());
        assert_eq!(request.table_name, "my-metrics");
        assert_eq!(request.desc, Some("blabla".to_string()));
        assert_eq!(request.schema, expected_table_schema());
        assert_eq!(request.primary_key_indices, vec![1, 0]);
        assert!(request.create_if_not_exists);

        let mut expr = testing_create_expr();
        expr.primary_keys = vec!["host".to_string(), "not-exist-column".to_string()];
        let result = create_expr_to_request(1025, expr).await;
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
            datatype: 1024,
            is_nullable: true,
            default_constraint: None,
        };
        let result = create_column_schema(&column_def);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Column datatype error, source: Unknown proto column datatype: 1024"
        );

        let column_def = ColumnDef {
            name: "a".to_string(),
            datatype: 12, // string
            is_nullable: true,
            default_constraint: None,
        };
        let column_schema = create_column_schema(&column_def).unwrap();
        assert_eq!(column_schema.name, "a");
        assert_eq!(column_schema.data_type, ConcreteDataType::string_datatype());
        assert!(column_schema.is_nullable());

        let default_constraint = ColumnDefaultConstraint::Value(Value::from("defaut value"));
        let column_def = ColumnDef {
            name: "a".to_string(),
            datatype: 12, // string
            is_nullable: true,
            default_constraint: Some(default_constraint.clone().try_into().unwrap()),
        };
        let column_schema = create_column_schema(&column_def).unwrap();
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
                datatype: 12, // string
                is_nullable: false,
                default_constraint: None,
            },
            ColumnDef {
                name: "ts".to_string(),
                datatype: 15, // timestamp
                is_nullable: false,
                default_constraint: None,
            },
            ColumnDef {
                name: "cpu".to_string(),
                datatype: 9, // float32
                is_nullable: true,
                default_constraint: None,
            },
            ColumnDef {
                name: "memory".to_string(),
                datatype: 10, // float64
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
