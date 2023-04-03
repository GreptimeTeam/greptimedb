// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use catalog::CatalogManagerRef;
use common_error::prelude::BoxedError;
use common_procedure::ProcedureManagerRef;
use common_query::Output;
use common_telemetry::error;
use query::sql::{show_databases, show_tables};
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt};
use sql::statements::show::{ShowDatabases, ShowTables};
use table::engine::manager::TableEngineManagerRef;
use table::engine::{TableEngineProcedureRef, TableEngineRef, TableReference};
use table::requests::*;
use table::{Table, TableRef};

use crate::error::{
    self, CloseTableEngineSnafu, ExecuteSqlSnafu, Result, TableEngineNotFoundSnafu,
    TableNotFoundSnafu,
};
use crate::instance::sql::table_idents_to_full_name;

mod alter;
mod copy_table_from;
mod copy_table_to;
mod create;
mod drop_table;
mod flush_table;
pub(crate) mod insert;

#[derive(Debug)]
pub enum SqlRequest {
    CreateTable(CreateTableRequest),
    CreateDatabase(CreateDatabaseRequest),
    Alter(AlterTableRequest),
    DropTable(DropTableRequest),
    FlushTable(FlushTableRequest),
    ShowDatabases(ShowDatabases),
    ShowTables(ShowTables),
    CopyTable(CopyTableRequest),
}

// Handler to execute SQL except query
#[derive(Clone)]
pub struct SqlHandler {
    table_engine_manager: TableEngineManagerRef,
    catalog_manager: CatalogManagerRef,
    engine_procedure: TableEngineProcedureRef,
    procedure_manager: Option<ProcedureManagerRef>,
}

impl SqlHandler {
    pub fn new(
        table_engine_manager: TableEngineManagerRef,
        catalog_manager: CatalogManagerRef,
        engine_procedure: TableEngineProcedureRef,
        procedure_manager: Option<ProcedureManagerRef>,
    ) -> Self {
        Self {
            table_engine_manager,
            catalog_manager,
            engine_procedure,
            procedure_manager,
        }
    }

    // TODO(LFC): Refactor consideration: a context awareness "Planner".
    // Now we have some query related state (like current using database in session context), maybe
    // we could create a new struct called `Planner` that stores context and handle these queries
    // there, instead of executing here in a "static" fashion.
    pub async fn execute(&self, request: SqlRequest, query_ctx: QueryContextRef) -> Result<Output> {
        let result = match request {
            SqlRequest::CreateTable(req) => self.create_table(req).await,
            SqlRequest::CreateDatabase(req) => self.create_database(req, query_ctx.clone()).await,
            SqlRequest::Alter(req) => self.alter(req).await,
            SqlRequest::DropTable(req) => self.drop_table(req).await,
            SqlRequest::CopyTable(req) => match req.direction {
                CopyDirection::Export => self.copy_table_to(req).await,
                CopyDirection::Import => self.copy_table_from(req).await,
            },
            SqlRequest::ShowDatabases(req) => {
                show_databases(req, self.catalog_manager.clone()).context(ExecuteSqlSnafu)
            }
            SqlRequest::ShowTables(req) => {
                show_tables(req, self.catalog_manager.clone(), query_ctx.clone())
                    .context(ExecuteSqlSnafu)
            }
            SqlRequest::FlushTable(req) => self.flush_table(req).await,
        };
        if let Err(e) = &result {
            error!(e; "{query_ctx}");
        }
        result
    }

    pub async fn get_table(&self, table_ref: &TableReference<'_>) -> Result<TableRef> {
        let TableReference {
            catalog,
            schema,
            table,
        } = table_ref;
        let table = self
            .catalog_manager
            .table(catalog, schema, table)
            .await
            .context(error::CatalogSnafu)?
            .with_context(|| TableNotFoundSnafu {
                table_name: table_ref.to_string(),
            })?;
        Ok(table)
    }

    pub fn table_engine_manager(&self) -> TableEngineManagerRef {
        self.table_engine_manager.clone()
    }

    pub fn table_engine(&self, table: Arc<dyn Table>) -> Result<TableEngineRef> {
        let engine_name = &table.table_info().meta.engine;
        let engine = self
            .table_engine_manager
            .engine(engine_name)
            .context(TableEngineNotFoundSnafu { engine_name })?;
        Ok(engine)
    }

    pub async fn close(&self) -> Result<()> {
        self.table_engine_manager
            .close()
            .await
            .map_err(BoxedError::new)
            .context(CloseTableEngineSnafu)
    }
}
