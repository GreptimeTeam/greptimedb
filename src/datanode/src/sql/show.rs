use std::sync::Arc;

use common_query::Output;
use common_recordbatch::{RecordBatch, RecordBatches};
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema};
use datatypes::vectors::{StringVector, VectorRef};
use snafu::{ensure, OptionExt, ResultExt};
use sql::statements::show::{ShowDatabases, ShowKind, ShowTables};

use crate::error::{
    NewRecordBatcheSnafu, NewRecordBatchesSnafu, Result, SchemaNotFoundSnafu, UnsupportedExprSnafu,
};
use crate::sql::SqlHandler;

const TABLES_COLUMN: &str = "Tables";
const SCHEMAS_COLUMN: &str = "Schemas";

impl SqlHandler {
    pub(crate) async fn show_databases(&self, stmt: ShowDatabases) -> Result<Output> {
        // TODO(dennis): supports LIKE and WHERE
        ensure!(
            stmt.kind == ShowKind::All,
            UnsupportedExprSnafu {
                name: format!("{}", stmt.kind),
            }
        );

        let catalog = self.get_default_catalog()?;
        let schemas = catalog.schema_names();

        let column_schemas = vec![ColumnSchema::new(
            SCHEMAS_COLUMN,
            ConcreteDataType::string_datatype(),
            false,
        )];
        let schema = Arc::new(Schema::new(column_schemas));
        let columns: Vec<VectorRef> = vec![Arc::new(StringVector::from(schemas))];
        let recordbatch =
            RecordBatch::new(schema.clone(), columns).context(NewRecordBatcheSnafu)?;

        Ok(Output::RecordBatches(
            RecordBatches::try_new(schema, vec![recordbatch]).context(NewRecordBatchesSnafu)?,
        ))
    }

    pub(crate) async fn show_tables(&self, stmt: ShowTables) -> Result<Output> {
        // TODO(dennis): supports LIKE and WHERE
        ensure!(
            stmt.kind == ShowKind::All,
            UnsupportedExprSnafu {
                name: format!("{}", stmt.kind),
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
        let columns: Vec<VectorRef> = vec![Arc::new(StringVector::from(tables))];
        let recordbatch =
            RecordBatch::new(schema.clone(), columns).context(NewRecordBatcheSnafu)?;

        Ok(Output::RecordBatches(
            RecordBatches::try_new(schema, vec![recordbatch]).context(NewRecordBatchesSnafu)?,
        ))
    }
}
