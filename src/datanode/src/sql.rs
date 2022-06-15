//! sql handler
use std::str::FromStr;

use datatypes::prelude::ConcreteDataType;
use datatypes::prelude::VectorBuilder;
use datatypes::value::Value;
use query::catalog::schema::SchemaProviderRef;
use query::query_engine::Output;
use snafu::ensure;
use snafu::OptionExt;
use snafu::ResultExt;
use sql::ast::Value as SqlValue;
use sql::statements::{insert::Insert, statement::Statement};
use table::engine::{EngineContext, TableEngine};
use table::requests::*;

use crate::error::{
    ColumnNotFoundSnafu, ColumnTypeMismatchSnafu, ColumnValuesNumberMismatchSnafu, GetTableSnafu,
    InsertSnafu, ParseSqlValueSnafu, Result, TableNotFoundSnafu,
};

pub enum SqlRequest {
    Insert(InsertRequest),
}

// Handler to execute SQL except querying.
pub struct SqlHandler<Engine: TableEngine> {
    table_engine: Engine,
}

impl<Engine: TableEngine> SqlHandler<Engine>
where
    <Engine as TableEngine>::Error: 'static,
{
    pub fn new(table_engine: Engine) -> Self {
        Self { table_engine }
    }

    pub async fn execute(&self, request: SqlRequest) -> Result<Output> {
        match request {
            SqlRequest::Insert(req) => {
                let table_name = &req.table_name.to_string();
                let table = self
                    .table_engine
                    .get_table(&EngineContext::default(), &req.table_name)
                    .map_err(|e| Box::new(e) as _)
                    .context(GetTableSnafu { table_name })?
                    .context(TableNotFoundSnafu { table_name })?;

                let affected_rows = table
                    .insert(req)
                    .await
                    .context(InsertSnafu { table_name })?;

                Ok(Output::AffectedRows(affected_rows))
            }
        }
    }

    // Cast sql statement into sql request
    pub fn statement_to_request(
        &self,
        schema_provider: SchemaProviderRef,
        statement: Statement,
    ) -> Result<SqlRequest> {
        match statement {
            Statement::Insert(stmt) => self.insert_to_request(schema_provider, *stmt),
            _ => unimplemented!(),
        }
    }

    fn insert_to_request(
        &self,
        schema_provider: SchemaProviderRef,
        stmt: Insert,
    ) -> Result<SqlRequest> {
        let columns = stmt.columns();
        let values = stmt.values();
        //TODO(dennis): table name may be in the form of `catalog.schema.table`,
        //   but we don't process it right now.
        let table_name = stmt.table_name();

        let table = schema_provider
            .table(&table_name)
            .with_context(|| TableNotFoundSnafu {
                table_name: table_name.clone(),
            })?;
        let schema = table.schema();
        let columns_num = if columns.is_empty() {
            schema.column_schemas().len()
        } else {
            columns.len()
        };
        let rows_num = values.len();

        let mut columns_builders: Vec<(&String, &ConcreteDataType, VectorBuilder)> =
            Vec::with_capacity(columns_num);

        if columns.is_empty() {
            for column_schema in schema.column_schemas() {
                let data_type = &column_schema.data_type;
                columns_builders.push((
                    &column_schema.name,
                    data_type,
                    VectorBuilder::with_capacity(data_type.clone(), rows_num),
                ));
            }
        } else {
            for column_name in columns {
                let column_schema =
                    schema.column_schema_by_name(column_name).with_context(|| {
                        ColumnNotFoundSnafu {
                            table_name: table_name.clone(),
                            column_name: column_name.to_string(),
                        }
                    })?;
                let data_type = &column_schema.data_type;
                columns_builders.push((
                    column_name,
                    data_type,
                    VectorBuilder::with_capacity(data_type.clone(), rows_num),
                ));
            }
        }

        // Convert rows into columns
        for row in values {
            ensure!(
                row.len() == columns_num,
                ColumnValuesNumberMismatchSnafu {
                    columns: columns_num,
                    values: row.len(),
                }
            );

            for (i, sql_val) in row.iter().enumerate() {
                let (column_name, data_type, builder) =
                    columns_builders.get_mut(i).expect("unreachable");

                add_row_to_vector(column_name, data_type, sql_val, builder)?;
            }
        }

        Ok(SqlRequest::Insert(InsertRequest {
            table_name,
            columns_values: columns_builders
                .into_iter()
                .map(|(c, _, mut b)| (c.to_owned(), b.finish()))
                .collect(),
        }))
    }
}

fn add_row_to_vector(
    column_name: &str,
    data_type: &ConcreteDataType,
    sql_val: &SqlValue,
    builder: &mut VectorBuilder,
) -> Result<()> {
    let value = parse_sql_value(column_name, data_type, sql_val)?;
    builder.push(&value);

    Ok(())
}

fn parse_sql_value(
    column_name: &str,
    data_type: &ConcreteDataType,
    sql_val: &SqlValue,
) -> Result<Value> {
    Ok(match sql_val {
        SqlValue::Number(n, _) => sql_number_to_value(data_type, n)?,
        SqlValue::Null => Value::Null,
        SqlValue::Boolean(b) => {
            ensure!(
                data_type.is_boolean(),
                ColumnTypeMismatchSnafu {
                    column_name,
                    expect: data_type.clone(),
                    actual: ConcreteDataType::boolean_datatype(),
                }
            );

            (*b).into()
        }
        SqlValue::DoubleQuotedString(s) | SqlValue::SingleQuotedString(s) => {
            ensure!(
                data_type.is_string(),
                ColumnTypeMismatchSnafu {
                    column_name,
                    expect: data_type.clone(),
                    actual: ConcreteDataType::string_datatype(),
                }
            );

            s.to_owned().into()
        }

        _ => todo!("Other sql value"),
    })
}

macro_rules! parse_number_to_value {
    ($data_type: expr, $n: ident,  $(($Type: ident, $PrimitiveType: ident)), +) => {
        match $data_type {
            $(
                ConcreteDataType::$Type(_) => {
                    let n  = parse_sql_number::<$PrimitiveType>($n)?;
                    Ok(Value::from(n))
                },
            )+
                _ => ParseSqlValueSnafu {
                    msg: format!("Fail to parse number {}, invalid column type: {:?}",
                                 $n, $data_type
                    )}.fail(),
        }
    }
}

fn sql_number_to_value(data_type: &ConcreteDataType, n: &str) -> Result<Value> {
    parse_number_to_value!(
        data_type,
        n,
        (UInt8, u8),
        (UInt16, u16),
        (UInt32, u32),
        (UInt64, u64),
        (Int8, i8),
        (Int16, i16),
        (Int32, i32),
        (Int64, i64),
        (Float64, f64),
        (Float32, f32)
    )
}

fn parse_sql_number<R: FromStr + std::fmt::Debug>(n: &str) -> Result<R>
where
    <R as FromStr>::Err: std::fmt::Debug,
{
    match n.parse::<R>() {
        Ok(n) => Ok(n),
        Err(e) => ParseSqlValueSnafu {
            msg: format!("Fail to parse number {}, {:?}", n, e),
        }
        .fail(),
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::Arc;

    use common_query::logical_plan::Expr;
    use common_recordbatch::SendableRecordBatchStream;
    use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
    use datatypes::value::OrderedFloat;
    use query::catalog::memory;
    use query::catalog::schema::SchemaProvider;
    use query::error::Result as QueryResult;
    use query::QueryEngineFactory;
    use storage::EngineImpl;
    use table::error::Result as TableResult;
    use table::{Table, TableRef};
    use table_engine::engine::MitoEngine;

    use super::*;

    #[test]
    fn test_sql_number_to_value() {
        let v = sql_number_to_value(&ConcreteDataType::float64_datatype(), "3.0").unwrap();
        assert_eq!(Value::Float64(OrderedFloat(3.0)), v);

        let v = sql_number_to_value(&ConcreteDataType::int32_datatype(), "999").unwrap();
        assert_eq!(Value::Int32(999), v);

        let v = sql_number_to_value(&ConcreteDataType::string_datatype(), "999");
        assert!(v.is_err(), "parse value error is: {:?}", v);
    }

    #[test]
    fn test_parse_sql_value() {
        let sql_val = SqlValue::Null;
        assert_eq!(
            Value::Null,
            parse_sql_value("a", &ConcreteDataType::float64_datatype(), &sql_val).unwrap()
        );

        let sql_val = SqlValue::Boolean(true);
        assert_eq!(
            Value::Boolean(true),
            parse_sql_value("a", &ConcreteDataType::boolean_datatype(), &sql_val).unwrap()
        );

        let sql_val = SqlValue::Number("3.0".to_string(), false);
        assert_eq!(
            Value::Float64(OrderedFloat(3.0)),
            parse_sql_value("a", &ConcreteDataType::float64_datatype(), &sql_val).unwrap()
        );

        let sql_val = SqlValue::Number("3.0".to_string(), false);
        let v = parse_sql_value("a", &ConcreteDataType::boolean_datatype(), &sql_val);
        assert!(v.is_err());
        assert!(format!("{:?}", v)
            .contains("Fail to parse number 3.0, invalid column type: Boolean(BooleanType)"));

        let sql_val = SqlValue::Boolean(true);
        let v = parse_sql_value("a", &ConcreteDataType::float64_datatype(), &sql_val);
        assert!(v.is_err());
        assert!(format!("{:?}", v).contains(
            "column_name: \"a\", expect: Float64(Float64), actual: Boolean(BooleanType)"
        ));
    }

    struct DemoTable;

    #[async_trait::async_trait]
    impl Table for DemoTable {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            let column_schemas = vec![
                ColumnSchema::new("host", ConcreteDataType::string_datatype(), false),
                ColumnSchema::new("cpu", ConcreteDataType::float64_datatype(), true),
                ColumnSchema::new("memory", ConcreteDataType::float64_datatype(), true),
                ColumnSchema::new("ts", ConcreteDataType::int64_datatype(), true),
            ];

            Arc::new(Schema::new(column_schemas))
        }
        async fn scan(
            &self,
            _projection: &Option<Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> TableResult<SendableRecordBatchStream> {
            unimplemented!();
        }
    }

    struct MockSchemaProvider;

    impl SchemaProvider for MockSchemaProvider {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn table_names(&self) -> Vec<String> {
            vec!["demo".to_string()]
        }

        fn table(&self, name: &str) -> Option<TableRef> {
            assert_eq!(name, "demo");
            Some(Arc::new(DemoTable {}))
        }

        fn register_table(&self, _name: String, _table: TableRef) -> QueryResult<Option<TableRef>> {
            unimplemented!();
        }
        fn deregister_table(&self, _name: &str) -> QueryResult<Option<TableRef>> {
            unimplemented!();
        }
        fn table_exist(&self, name: &str) -> bool {
            name == "demo"
        }
    }

    #[test]
    fn test_statement_to_request() {
        let catalog_list = memory::new_memory_catalog_list().unwrap();
        let factory = QueryEngineFactory::new(catalog_list);
        let query_engine = factory.query_engine().clone();

        let sql = r#"insert into demo(host, cpu, memory, ts) values
                           ('host1', 66.6, 1024, 1655276557000),
                           ('host2', 88.8,  333.3, 1655276558000)
                           "#;

        let table_engine = MitoEngine::<EngineImpl>::new(EngineImpl::new());
        let sql_handler = SqlHandler::new(table_engine);

        let stmt = query_engine.sql_to_statement(sql).unwrap();
        let schema_provider = Arc::new(MockSchemaProvider {});
        let request = sql_handler
            .statement_to_request(schema_provider, stmt)
            .unwrap();

        match request {
            SqlRequest::Insert(req) => {
                assert_eq!(req.table_name, "demo");
                let columns_values = req.columns_values;
                assert_eq!(4, columns_values.len());

                let hosts = &columns_values["host"];
                assert_eq!(2, hosts.len());
                assert_eq!(Value::from("host1"), hosts.get(0));
                assert_eq!(Value::from("host2"), hosts.get(1));

                let cpus = &columns_values["cpu"];
                assert_eq!(2, cpus.len());
                assert_eq!(Value::from(66.6f64), cpus.get(0));
                assert_eq!(Value::from(88.8f64), cpus.get(1));

                let memories = &columns_values["memory"];
                assert_eq!(2, memories.len());
                assert_eq!(Value::from(1024f64), memories.get(0));
                assert_eq!(Value::from(333.3f64), memories.get(1));

                let ts = &columns_values["ts"];
                assert_eq!(2, ts.len());
                assert_eq!(Value::from(1655276557000i64), ts.get(0));
                assert_eq!(Value::from(1655276558000i64), ts.get(1));
            }
        }
    }
}
