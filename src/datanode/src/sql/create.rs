use std::collections::HashMap;
use std::sync::Arc;

use catalog::RegisterTableRequest;
use common_telemetry::tracing::info;
use datatypes::schema::SchemaBuilder;
use query::query_engine::Output;
use snafu::{OptionExt, ResultExt};
use sql::ast::TableConstraint;
use sql::statements::create_table::CreateTable;
use store_api::storage::consts::TIME_INDEX_NAME;
use table::engine::EngineContext;
use table::metadata::TableId;
use table::requests::*;

use crate::error::{
    self, ConstraintNotSupportedSnafu, CreateSchemaSnafu, CreateTableSnafu,
    InsertSystemCatalogSnafu, KeyColumnNotFoundSnafu, Result,
};
use crate::sql::{column_def_to_schema, table_idents_to_full_name, SqlHandler};

impl SqlHandler {
    pub(crate) async fn create(&self, req: CreateTableRequest) -> Result<Output> {
        let ctx = EngineContext {};
        let catalog_name = req.catalog_name.clone();
        let schema_name = req.schema_name.clone();
        let table_name = req.table_name.clone();
        let table_id = req.id;

        let table = self
            .table_engine
            .create_table(&ctx, req)
            .await
            .with_context(|_| CreateTableSnafu {
                table_name: &table_name,
            })?;

        let register_req = RegisterTableRequest {
            catalog: catalog_name,
            schema: schema_name,
            table_name: table_name.clone(),
            table_id,
            table,
        };

        self.catalog_manager
            .register_table(register_req)
            .await
            .context(InsertSystemCatalogSnafu)?;
        info!("Successfully created table: {:?}", table_name);
        // TODO(hl): maybe support create multiple tables
        Ok(Output::AffectedRows(1))
    }

    /// Converts [CreateTable] to [SqlRequest::Create].
    pub(crate) fn create_to_request(
        &self,
        table_id: TableId,
        stmt: CreateTable,
    ) -> Result<CreateTableRequest> {
        let mut ts_index = usize::MAX;
        let mut primary_keys = vec![];

        let (catalog_name, schema_name, table_name) = table_idents_to_full_name(&stmt.name)?;

        let col_map = stmt
            .columns
            .iter()
            .map(|e| e.name.value.clone())
            .enumerate()
            .map(|(k, v)| (v, k))
            .collect::<HashMap<_, _>>();

        for c in stmt.constraints {
            match c {
                TableConstraint::Unique {
                    name,
                    columns,
                    is_primary,
                } => {
                    if let Some(name) = name {
                        if name.value == TIME_INDEX_NAME {
                            ts_index = *col_map.get(&columns[0].value).context(
                                KeyColumnNotFoundSnafu {
                                    name: columns[0].value.to_string(),
                                },
                            )?;
                        } else {
                            return error::InvalidSqlSnafu {
                                msg: format!("Cannot recognize named UNIQUE constraint: {}", name),
                            }
                            .fail();
                        }
                    } else if is_primary {
                        for col in columns {
                            primary_keys.push(*col_map.get(&col.value).context(
                                KeyColumnNotFoundSnafu {
                                    name: col.value.to_string(),
                                },
                            )?);
                        }
                    } else {
                        return error::InvalidSqlSnafu {
                            msg: format!(
                                "Unrecognized non-primary unnamed UNIQUE constraint: {:?}",
                                name
                            ),
                        }
                        .fail();
                    }
                }
                _ => {
                    return ConstraintNotSupportedSnafu {
                        constraint: format!("{:?}", c),
                    }
                    .fail()
                }
            }
        }

        if primary_keys.is_empty() {
            info!(
                "Creating table: {:?}.{:?}.{} but primary key not set, use time index column: {}",
                catalog_name, schema_name, table_name, ts_index
            );
            primary_keys.push(ts_index);
        }

        let columns_schemas: Vec<_> = stmt
            .columns
            .iter()
            .map(column_def_to_schema)
            .collect::<Result<Vec<_>>>()?;

        let schema = Arc::new(
            SchemaBuilder::from(columns_schemas)
                .timestamp_index(ts_index)
                .build()
                .context(CreateSchemaSnafu)?,
        );

        let request = CreateTableRequest {
            id: table_id,
            catalog_name,
            schema_name,
            table_name,
            desc: None,
            schema,
            primary_key_indices: primary_keys,
            create_if_not_exists: stmt.if_not_exists,
            table_options: HashMap::new(),
        };
        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use common_time::timestamp::TimeUnit;
    use datatypes::prelude::ConcreteDataType;
    use sql::ast::Ident;
    use sql::ast::{DataType as SqlDataType, ObjectName};
    use sql::dialect::GenericDialect;
    use sql::parser::ParserContext;
    use sql::statements::statement::Statement;

    use super::*;
    use crate::error::Error;
    use crate::sql::sql_data_type_to_concrete_data_type;
    use crate::tests::test_util::create_mock_sql_handler;

    fn sql_to_statement(sql: &str) -> CreateTable {
        let mut res = ParserContext::create_with_dialect(sql, &GenericDialect {}).unwrap();
        assert_eq!(1, res.len());
        match res.pop().unwrap() {
            Statement::Create(c) => c,
            _ => {
                panic!("Unexpected statement!")
            }
        }
    }

    #[tokio::test]
    pub async fn test_create_to_request() {
        let handler = create_mock_sql_handler().await;
        let parsed_stmt = sql_to_statement("create table demo_table( host string, ts bigint, cpu double default 0, memory double, TIME INDEX (ts), PRIMARY KEY(ts, host)) engine=mito with(regions=1);");
        let c = handler.create_to_request(42, parsed_stmt).unwrap();
        assert_eq!("demo_table", c.table_name);
        assert_eq!(42, c.id);
        assert!(c.schema_name.is_none());
        assert!(c.catalog_name.is_none());
        assert!(!c.create_if_not_exists);
        assert_eq!(vec![1, 0], c.primary_key_indices);
        assert_eq!(1, c.schema.timestamp_index().unwrap());
        assert_eq!(4, c.schema.column_schemas().len());
    }

    /// Time index not specified in sql
    #[tokio::test]
    pub async fn test_time_index_not_specified() {
        let handler = create_mock_sql_handler().await;
        let parsed_stmt = sql_to_statement("create table demo_table( host string, ts bigint, cpu double default 0, memory double, PRIMARY KEY(ts, host)) engine=mito with(regions=1);");
        let error = handler.create_to_request(42, parsed_stmt).unwrap_err();
        assert_matches!(error, Error::CreateSchema { .. });
    }

    /// If primary key is not specified, time index should be used as primary key.  
    #[tokio::test]
    pub async fn test_primary_key_not_specified() {
        let handler = create_mock_sql_handler().await;

        let parsed_stmt = sql_to_statement("create table demo_table( host string, ts bigint, cpu double default 0, memory double, TIME INDEX (ts)) engine=mito with(regions=1);");

        let c = handler.create_to_request(42, parsed_stmt).unwrap();
        assert_eq!(1, c.primary_key_indices.len());
        assert_eq!(
            c.schema.timestamp_index().unwrap(),
            c.primary_key_indices[0]
        );
    }

    /// Constraints specified, not column cannot be found.
    #[tokio::test]
    pub async fn test_key_not_found() {
        let handler = create_mock_sql_handler().await;

        let parsed_stmt = sql_to_statement(
            "create table demo_table( host string, TIME INDEX (ts)) engine=mito with(regions=1);",
        );

        let error = handler.create_to_request(42, parsed_stmt).unwrap_err();
        assert_matches!(error, Error::KeyColumnNotFound { .. });
    }

    #[tokio::test]
    pub async fn test_parse_create_sql() {
        let create_table = sql_to_statement(
            r"create table c.s.demo(
                             host string,
                             ts bigint,
                             cpu double default 0,
                             memory double,
                             TIME INDEX (ts),
                             PRIMARY KEY(ts, host)) engine=mito
                             with(regions=1);
         ",
        );

        let handler = create_mock_sql_handler().await;

        let request = handler.create_to_request(42, create_table).unwrap();

        assert_eq!(42, request.id);
        assert_eq!(Some("c".to_string()), request.catalog_name);
        assert_eq!(Some("s".to_string()), request.schema_name);
        assert_eq!("demo".to_string(), request.table_name);
        assert!(!request.create_if_not_exists);
        assert_eq!(4, request.schema.column_schemas().len());

        assert_eq!(vec![1, 0], request.primary_key_indices);
        assert_eq!(
            ConcreteDataType::string_datatype(),
            request
                .schema
                .column_schema_by_name("host")
                .unwrap()
                .data_type
        );
        assert_eq!(
            ConcreteDataType::int64_datatype(),
            request
                .schema
                .column_schema_by_name("ts")
                .unwrap()
                .data_type
        );
        assert_eq!(
            ConcreteDataType::float64_datatype(),
            request
                .schema
                .column_schema_by_name("cpu")
                .unwrap()
                .data_type
        );
        assert_eq!(
            ConcreteDataType::float64_datatype(),
            request
                .schema
                .column_schema_by_name("memory")
                .unwrap()
                .data_type
        );
    }

    fn check_type(sql_type: SqlDataType, data_type: ConcreteDataType) {
        assert_eq!(
            data_type,
            sql_data_type_to_concrete_data_type(&sql_type).unwrap()
        );
    }

    #[test]
    pub fn test_sql_data_type_to_concrete_data_type() {
        check_type(
            SqlDataType::BigInt(None),
            ConcreteDataType::int64_datatype(),
        );
        check_type(SqlDataType::Int(None), ConcreteDataType::int32_datatype());
        check_type(
            SqlDataType::SmallInt(None),
            ConcreteDataType::int16_datatype(),
        );
        check_type(SqlDataType::Char(None), ConcreteDataType::string_datatype());
        check_type(
            SqlDataType::Varchar(None),
            ConcreteDataType::string_datatype(),
        );
        check_type(SqlDataType::Text, ConcreteDataType::string_datatype());
        check_type(SqlDataType::String, ConcreteDataType::string_datatype());
        check_type(
            SqlDataType::Float(None),
            ConcreteDataType::float32_datatype(),
        );
        check_type(SqlDataType::Double, ConcreteDataType::float64_datatype());
        check_type(SqlDataType::Boolean, ConcreteDataType::boolean_datatype());
        check_type(SqlDataType::Date, ConcreteDataType::date_datatype());
        check_type(
            SqlDataType::Custom(ObjectName(vec![Ident::new("datetime")])),
            ConcreteDataType::datetime_datatype(),
        );
        check_type(
            SqlDataType::Timestamp,
            ConcreteDataType::timestamp_datatype(TimeUnit::Millisecond),
        );
    }
}
