use std::sync::Arc;

use common_query::Output;
use common_recordbatch::{RecordBatch, RecordBatches};
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema};
use datatypes::vectors::{Helper, StringVector, VectorRef};
use snafu::{ensure, ResultExt};
use sql::statements::show::{ShowDatabases, ShowKind};

use crate::error::{
    self, CatalogSnafu, NewRecordBatchSnafu, NewRecordBatchesSnafu, Result, UnsupportedExprSnafu,
};
use crate::sql::SqlHandler;

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
}
