// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use catalog::CatalogManagerRef;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_query::Output;
use common_recordbatch::RecordBatches;
use datatypes::prelude::*;
use datatypes::schema::{ColumnSchema, Schema};
use datatypes::vectors::{Helper, StringVector};
use snafu::{ensure, OptionExt, ResultExt};
use sql::statements::show::{ShowDatabases, ShowKind, ShowTables};

use crate::error::{self, Result};

const SCHEMAS_COLUMN: &str = "Schemas";
const TABLES_COLUMN: &str = "Tables";

pub fn show_databases(stmt: ShowDatabases, catalog_manager: CatalogManagerRef) -> Result<Output> {
    // TODO(LFC): supports WHERE
    ensure!(
        matches!(stmt.kind, ShowKind::All | ShowKind::Like(_)),
        error::UnsupportedExprSnafu {
            name: stmt.kind.to_string(),
        }
    );

    let catalog = catalog_manager
        .catalog(DEFAULT_CATALOG_NAME)
        .context(error::CatalogSnafu)?
        .context(error::CatalogNotFoundSnafu {
            catalog: DEFAULT_CATALOG_NAME,
        })?;
    let databases = catalog.schema_names().context(error::CatalogSnafu)?;

    let databases = if let ShowKind::Like(ident) = stmt.kind {
        Helper::like_utf8(databases, &ident.value).context(error::VectorComputationSnafu)?
    } else {
        Arc::new(StringVector::from(databases))
    };

    let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
        SCHEMAS_COLUMN,
        ConcreteDataType::string_datatype(),
        false,
    )]));
    let records = RecordBatches::try_from_columns(schema, vec![databases])
        .context(error::CreateRecordBatchSnafu)?;
    Ok(Output::RecordBatches(records))
}

pub fn show_tables(stmt: ShowTables, catalog_manager: CatalogManagerRef) -> Result<Output> {
    // TODO(LFC): supports WHERE
    ensure!(
        matches!(stmt.kind, ShowKind::All | ShowKind::Like(_)),
        error::UnsupportedExprSnafu {
            name: stmt.kind.to_string(),
        }
    );

    let schema = stmt.database.as_deref().unwrap_or(DEFAULT_SCHEMA_NAME);
    let schema = catalog_manager
        .schema(DEFAULT_CATALOG_NAME, schema)
        .context(error::CatalogSnafu)?
        .context(error::SchemaNotFoundSnafu { schema })?;
    let tables = schema.table_names().context(error::CatalogSnafu)?;

    let tables = if let ShowKind::Like(ident) = stmt.kind {
        Helper::like_utf8(tables, &ident.value).context(error::VectorComputationSnafu)?
    } else {
        Arc::new(StringVector::from(tables))
    };

    let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
        TABLES_COLUMN,
        ConcreteDataType::string_datatype(),
        false,
    )]));
    let records = RecordBatches::try_from_columns(schema, vec![tables])
        .context(error::CreateRecordBatchSnafu)?;
    Ok(Output::RecordBatches(records))
}
