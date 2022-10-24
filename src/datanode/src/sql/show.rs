use std::sync::Arc;

use common_query::Output;
use common_recordbatch::{RecordBatch, RecordBatches};
use datatypes::arrow::compute;
use datatypes::arrow_array::StringArray;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema, ColumnDefaultConstraint};
use datatypes::vectors::{Helper, StringVector, VectorRef};
use snafu::{ensure, OptionExt, ResultExt};
use sql::statements::show::{ShowDatabases, ShowKind, ShowTables, ShowCreateTable};

use crate::error::{
    ArrowComputationSnafu, CastVectorSnafu, NewRecordBatchSnafu, NewRecordBatchesSnafu, Result,
    SchemaNotFoundSnafu, UnsupportedExprSnafu,
};
use crate::sql::SqlHandler;

const TABLES_COLUMN: &str = "Tables";
const SCHEMAS_COLUMN: &str = "Schemas";

impl SqlHandler {
    fn like_utf8(names: Vec<String>, s: &str) -> Result<VectorRef> {
        let array = StringArray::from_slice(&names);

        let boolean_array =
            compute::like::like_utf8_scalar(&array, s).context(ArrowComputationSnafu)?;

        Helper::try_into_vector(
            compute::filter::filter(&array, &boolean_array).context(ArrowComputationSnafu)?,
        )
        .context(CastVectorSnafu)
    }

    pub(crate) async fn show_databases(&self, stmt: ShowDatabases) -> Result<Output> {
        // TODO(dennis): supports WHERE
        ensure!(
            matches!(stmt.kind, ShowKind::All | ShowKind::Like(_)),
            UnsupportedExprSnafu {
                name: stmt.kind.to_string(),
            }
        );

        let catalog = self.get_default_catalog()?;
        // TODO(dennis): return an iterator or stream would be better.
        let schemas = catalog.schema_names();

        let column_schemas = vec![ColumnSchema::new(
            SCHEMAS_COLUMN,
            ConcreteDataType::string_datatype(),
            false,
        )];
        let schema = Arc::new(Schema::new(column_schemas));

        let schemas_vector = if let ShowKind::Like(ident) = stmt.kind {
            Self::like_utf8(schemas, &ident.value)?
        } else {
            Arc::new(StringVector::from(schemas))
        };

        let columns: Vec<VectorRef> = vec![schemas_vector];
        let recordbatch = RecordBatch::new(schema.clone(), columns).context(NewRecordBatchSnafu)?;

        Ok(Output::RecordBatches(
            RecordBatches::try_new(schema, vec![recordbatch]).context(NewRecordBatchesSnafu)?,
        ))
    }

    pub(crate) async fn show_tables(&self, stmt: ShowTables) -> Result<Output> {
        // TODO(dennis): supports WHERE
        ensure!(
            matches!(stmt.kind, ShowKind::All | ShowKind::Like(_)),
            UnsupportedExprSnafu {
                name: stmt.kind.to_string(),
            }
        );

        let schema = if let Some(name) = &stmt.database {
            let catalog = self.get_default_catalog()?;
            catalog.schema(name).context(SchemaNotFoundSnafu { name })?
        } else {
            self.get_default_schema()?
        };
        let tables = schema.table_names();

        let column_schemas = vec![ColumnSchema::new(
            TABLES_COLUMN,
            ConcreteDataType::string_datatype(),
            false,
        )];
        let schema = Arc::new(Schema::new(column_schemas));

        let tables_vector = if let ShowKind::Like(ident) = stmt.kind {
            Self::like_utf8(tables, &ident.value)?
        } else {
            Arc::new(StringVector::from(tables))
        };

        let columns: Vec<VectorRef> = vec![tables_vector];
        let recordbatch = RecordBatch::new(schema.clone(), columns).context(NewRecordBatchSnafu)?;

        Ok(Output::RecordBatches(
            RecordBatches::try_new(schema, vec![recordbatch]).context(NewRecordBatchesSnafu)?,
        ))
    }

    pub(crate) async fn show_create_table(&self, stmt: ShowCreateTable) -> Result<Output> {
        let schema1 = self.get_default_schema()?;
        let name = stmt.tablename.as_ref().unwrap();
        ensure!(
            schema1.table_exist(name),
            UnsupportedExprSnafu{
                name: name.to_string(),
            }
        );
        let column_schemas = vec![
            ColumnSchema::new("Table", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new("Create Table", ConcreteDataType::string_datatype(), false),
        ];
        let schema = Arc::new(Schema::new(column_schemas));
        let sql1 = name.to_string();
        let tableref =schema1.table(name).as_ref();
        let table_info = tableref.unwrap().table_info();
        let table_schema = tableref.unwrap().schema();
        let mut sql2 = String::new();
        for col in table_schema.column_schemas(){
            sql2 += format!("{} {:?}",col.name, col.data_type).as_str();
            if !col.is_nullable() {
                sql2 += " NOT NULL";
            }
            if let Some(expr) = col.default_constraint() {
                match expr {
                    ColumnDefaultConstraint::Value(v) =>{
                        if (v.is_null()){
                            sql2 += format!(" DEFAULT NULL").as_str();
                        }else {
                            sql2 += format!(" DEFAULT {:?}",v).as_str();
                        }
                    },
                    ColumnDefaultConstraint::Function(f) =>{
                        sql2 += format!( "DEFAULT {}()",f).as_str();
                    }
                }
            }
            sql2 += ", ";
        }
        let keys=table_info.meta.primary_key_indices;
        let mut res = String::new();
        for iter in keys{
            res += format!("{}",table_schema.column_name_by_index(iter)).as_str();
            res += ",";
        }
        sql2 += format!("PRIMARY KEY({})",res).as_str();
        sql2 += format!("TIMESTAMP KEY({})",table_schema.timestamp_column().unwrap().name).as_str();
        let mut sql3 = table_info.meta.engine;
        let mut sql4 = "".to_string();
        let opts = table_info.meta.options;
        if !opts.is_empty() {
            let mut v: Vec<String> = opts
            .into_iter()
            .map(|(k,v)| format!("{}={}",k, v))
            .collect();
            v.sort();
            sql4 = format!(" WITH({})", v.join(", "));
        }
        let sqls = vec![format!("CREATE TABLE {} ({}) ENGINE={}{}",sql1,sql2,sql3,sql4)];
        let tables = vec![table_info.name];
        let recordbatch = RecordBatch::new(
            schema.clone(),
            vec![
                Arc::new(StringVector::from(tables)),
                Arc::new(StringVector::from(sqls)),
                ],
        )
        .context(NewRecordBatchSnafu)?;

        Ok(Output::RecordBatches(
            RecordBatches::try_new(schema, vec![recordbatch]).context(NewRecordBatchesSnafu)?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_vector(expected: Vec<&str>, actual: &VectorRef) {
        let actual = actual.as_any().downcast_ref::<StringVector>().unwrap();

        assert_eq!(*actual, StringVector::from(expected));
    }

    #[test]
    fn test_like_utf8() {
        let names: Vec<String> = vec!["greptime", "hello", "public", "world"]
            .into_iter()
            .map(|x| x.to_string())
            .collect();

        let ret = SqlHandler::like_utf8(names.clone(), "%ll%").unwrap();
        assert_vector(vec!["hello"], &ret);

        let ret = SqlHandler::like_utf8(names.clone(), "%time").unwrap();
        assert_vector(vec!["greptime"], &ret);

        let ret = SqlHandler::like_utf8(names.clone(), "%ld").unwrap();
        assert_vector(vec!["world"], &ret);

        let ret = SqlHandler::like_utf8(names, "%").unwrap();
        assert_vector(vec!["greptime", "hello", "public", "world"], &ret);
    }
}
