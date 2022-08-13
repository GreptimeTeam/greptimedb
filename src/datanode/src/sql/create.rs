use std::collections::HashMap;
use std::sync::Arc;

use common_telemetry::tracing::info;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema};
use query::query_engine::Output;
use snafu::{OptionExt, ResultExt};
use sql::ast::{ColumnDef, ColumnOption, DataType as SqlDataType, ObjectName, TableConstraint};
use sql::statements::create_table::CreateTable;
use store_api::storage::consts::TIME_INDEX_NAME;
use table::engine::{EngineContext, TableEngine};
use table::metadata::TableId;
use table::requests::*;

use crate::error::{
    CreateSchemaSnafu, CreateTableSnafu, InvalidCreateTableSqlSnafu, KeyColumnNotFoundSnafu,
    Result, SqlTypeNotSupportedSnafu,
};
use crate::sql::{SqlHandler, SqlRequest};

impl<Engine: TableEngine> SqlHandler<Engine> {
    pub(crate) async fn create(&self, req: CreateTableRequest) -> Result<Output> {
        let ctx = EngineContext {};
        let table_name = req.table_name.clone();
        self.table_engine
            .create_table(&ctx, req)
            .await
            .with_context(|_| CreateTableSnafu {
                table_name: table_name.clone(),
            })?;
        info!("Successfully created table: {:?}", table_name);
        Ok(Output::AffectedRows(1)) // maybe support create multiple tables
    }

    /// Converts [CreateTable] to [SqlRequest::Create].
    pub(crate) fn create_to_request(
        &self,
        table_id: TableId,
        stmt: CreateTable,
    ) -> Result<SqlRequest> {
        let mut ts_index = usize::MAX;
        let mut primary_keys = vec![];
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
                            return InvalidCreateTableSqlSnafu {
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
                        return InvalidCreateTableSqlSnafu {
                            msg: format!(
                                "Unrecognized non-primary unnamed UNIQUE constraint: {:?}",
                                name
                            ),
                        }
                        .fail();
                    }
                }
                _ => {
                    unimplemented!()
                }
            }
        }

        let columns_schemas: Vec<_> = stmt
            .columns
            .iter()
            .map(column_def_to_schema)
            .collect::<Result<Vec<_>>>()?;

        let schema = Arc::new(
            Schema::with_timestamp_index(columns_schemas, ts_index).context(CreateSchemaSnafu)?,
        );

        let (catalog_name, schema_name, table_name) = table_idents_to_fqn(stmt.name)?;
        let request = CreateTableRequest {
            id: table_id,
            catalog_name,
            schema_name,
            table_name,
            desc: None,
            schema,
            primary_key_indices: primary_keys,
            create_if_not_exists: stmt.if_not_exists,
        };
        Ok(SqlRequest::Create(request))
    }
}

/// Converts maybe full-qualified table name (`<catalog>.<schema>.<table>` or `<table>` when catalog and schema are default)
/// to tuples  
fn table_idents_to_fqn(obj_name: ObjectName) -> Result<(Option<String>, Option<String>, String)> {
    if obj_name.0.len() == 1 {
        Ok((None, None, obj_name.0[0].value.clone()))
    } else if obj_name.0.len() == 3 {
        Ok((
            Some(obj_name.0[0].value.clone()),
            Some(obj_name.0[1].value.clone()),
            obj_name.0[2].value.clone(),
        ))
    } else {
        InvalidCreateTableSqlSnafu {
            msg: format!(
                "table name can only be <catalog>.<schema>.<table> or <table>, but found: {}",
                obj_name
            ),
        }
        .fail()
    }
}

fn column_def_to_schema(def: &ColumnDef) -> Result<ColumnSchema> {
    let mut nullable = true;
    for o in &def.options {
        match o.option {
            ColumnOption::Null => {
                nullable = true;
            }
            ColumnOption::NotNull => {
                nullable = false;
            }
            _ => {
                // other options currently not supported
            }
        }
    }

    Ok(ColumnSchema {
        name: def.name.value.clone(),
        data_type: sql_data_type_to_concrete_data_type(&def.data_type)?,
        is_nullable: nullable,
    })
}

fn sql_data_type_to_concrete_data_type(t: &SqlDataType) -> Result<ConcreteDataType> {
    match t {
        SqlDataType::BigInt(_) => Ok(ConcreteDataType::int64_datatype()),
        SqlDataType::Int(_) => Ok(ConcreteDataType::int32_datatype()),
        SqlDataType::SmallInt(_) => Ok(ConcreteDataType::int16_datatype()),
        SqlDataType::Char(_)
        | SqlDataType::Varchar(_)
        | SqlDataType::Text
        | SqlDataType::String => Ok(ConcreteDataType::string_datatype()),
        SqlDataType::Float(_) => Ok(ConcreteDataType::float32_datatype()),
        SqlDataType::Real => Ok(ConcreteDataType::float32_datatype()),
        SqlDataType::Double => Ok(ConcreteDataType::float64_datatype()),
        SqlDataType::Boolean => Ok(ConcreteDataType::boolean_datatype()),
        // TODO(hl): Date/DateTime/Timestamp not supported
        _ => SqlTypeNotSupportedSnafu { t: t.clone() }.fail(),
    }
}

#[cfg(test)]
mod tests {
    use sql::ast::{DataType, Ident};
    use store_api::storage::consts;
    use table_engine::config::EngineConfig;
    use table_engine::table::test_util::{new_test_object_store, MockEngine, MockMitoEngine};

    use super::*;

    #[tokio::test]
    pub async fn test_create_to_request() {
        let (_dir, object_store) = new_test_object_store("setup_mock_engine_and_table").await;
        let mock_engine =
            MockMitoEngine::new(EngineConfig::default(), MockEngine::default(), object_store);

        let handler = SqlHandler::new(mock_engine);

        let constraints = vec![
            TableConstraint::Unique {
                name: Some(Ident::new(consts::TIME_INDEX_NAME)),
                columns: vec![Ident::new("ts")],
                is_primary: false,
            },
            TableConstraint::Unique {
                name: None,
                columns: vec![Ident::new("host"), Ident::new("ts")],
                is_primary: true,
            },
        ];

        let parsed_stmt = CreateTable {
            if_not_exists: false,
            table_id: 0,
            name: ObjectName(vec![Ident::new("demo_table")]),
            columns: vec![
                ColumnDef {
                    name: Ident::new("host"),
                    data_type: DataType::String,
                    collation: None,
                    options: vec![],
                },
                ColumnDef {
                    name: Ident::new("ts"),
                    data_type: DataType::BigInt(None),
                    collation: None,
                    options: vec![],
                },
            ],
            engine: "".to_string(),
            constraints,
            options: vec![],
        };

        let request = handler.create_to_request(42, parsed_stmt).unwrap();
        match request {
            SqlRequest::Insert(_) => {
                panic!("Not supposed to be an insert statement")
            }
            SqlRequest::Create(c) => {
                assert_eq!("demo_table", c.table_name);
                assert_eq!(42, c.id);
                assert!(c.schema_name.is_none());
                assert!(c.catalog_name.is_none());
                assert!(!c.create_if_not_exists);
                assert_eq!(vec![0, 1], c.primary_key_indices);
                assert_eq!(1, c.schema.timestamp_index().unwrap());
                assert_eq!(2, c.schema.column_schemas().len());
            }
        }
    }
}
