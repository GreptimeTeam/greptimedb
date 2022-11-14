use std::sync::Arc;

use common_query::Output;
use common_recordbatch::{RecordBatch, RecordBatches};
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema};
use datatypes::vectors::{Helper, StringVector, VectorRef};
use snafu::{ensure, OptionExt, ResultExt};
use sql::statements::show::{ShowDatabases, ShowKind, ShowTables};

use crate::error::{
    self, CatalogSnafu, NewRecordBatchSnafu, NewRecordBatchesSnafu, Result, SchemaNotFoundSnafu,
    UnsupportedExprSnafu,
};
use crate::sql::SqlHandler;

const TABLES_COLUMN: &str = "Tables";
const SCHEMAS_COLUMN: &str = "Schemas";

impl SqlHandler {
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
        let schemas = catalog.schema_names().context(CatalogSnafu)?;

        let column_schemas = vec![ColumnSchema::new(
            SCHEMAS_COLUMN,
            ConcreteDataType::string_datatype(),
            false,
        )];
        let schema = Arc::new(Schema::new(column_schemas));

        let schemas_vector = if let ShowKind::Like(ident) = stmt.kind {
            Helper::like_utf8(schemas, &ident.value).context(error::VectorComputationSnafu)?
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
            catalog
                .schema(name)
                .context(CatalogSnafu)?
                .context(SchemaNotFoundSnafu { name })?
        } else {
            self.get_default_schema()?
        };
        let tables = schema.table_names().context(CatalogSnafu)?;

        let column_schemas = vec![ColumnSchema::new(
            TABLES_COLUMN,
            ConcreteDataType::string_datatype(),
            false,
        )];
        let schema = Arc::new(Schema::new(column_schemas));

        let tables_vector = if let ShowKind::Like(ident) = stmt.kind {
            Helper::like_utf8(tables, &ident.value).context(error::VectorComputationSnafu)?
        } else {
            Arc::new(StringVector::from(tables))
        };

        let columns: Vec<VectorRef> = vec![tables_vector];
        let recordbatch = RecordBatch::new(schema.clone(), columns).context(NewRecordBatchSnafu)?;

        Ok(Output::RecordBatches(
            RecordBatches::try_new(schema, vec![recordbatch]).context(NewRecordBatchesSnafu)?,
        ))
    }
}
