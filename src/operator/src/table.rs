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

use std::time::Duration;

use async_trait::async_trait;
use common_error::ext::BoxedError;
use common_function::handlers::{AffectedRows, TableMutationHandler};
use common_query::error as query_error;
use common_query::error::Result as QueryResult;
use session::context::QueryContextRef;
use snafu::ResultExt;
use sqlparser::ast::ObjectName;
use table::requests::{DeleteRequest as TableDeleteRequest, InsertRequest as TableInsertRequest};

use crate::delete::DeleterRef;
use crate::error::{InvalidSqlSnafu, Result};
use crate::insert::InserterRef;

// TODO(LFC): Refactor consideration: move this function to some helper mod,
// could be done together or after `TableReference`'s refactoring, when issue #559 is resolved.
/// Converts maybe fully-qualified table name (`<catalog>.<schema>.<table>`) to tuple.
pub fn table_idents_to_full_name(
    obj_name: &ObjectName,
    query_ctx: &QueryContextRef,
) -> Result<(String, String, String)> {
    match &obj_name.0[..] {
        [table] => Ok((
            query_ctx.current_catalog().to_string(),
            query_ctx.current_schema().to_string(),
            table.value.clone(),
        )),
        [schema, table] => Ok((
            query_ctx.current_catalog().to_string(),
            schema.value.clone(),
            table.value.clone(),
        )),
        [catalog, schema, table] => Ok((
            catalog.value.clone(),
            schema.value.clone(),
            table.value.clone(),
        )),
        _ => InvalidSqlSnafu {
            err_msg: format!(
                "expect table name to be <catalog>.<schema>.<table>, <schema>.<table> or <table>, actual: {obj_name}",
            ),
        }.fail(),
    }
}

pub struct TableMutationOperator {
    inserter: InserterRef,
    deleter: DeleterRef,
}

impl TableMutationOperator {
    pub fn new(inserter: InserterRef, deleter: DeleterRef) -> Self {
        Self { inserter, deleter }
    }
}

#[async_trait]
impl TableMutationHandler for TableMutationOperator {
    async fn insert(
        &self,
        request: TableInsertRequest,
        ctx: QueryContextRef,
    ) -> QueryResult<AffectedRows> {
        self.inserter
            .handle_table_insert(request, ctx)
            .await
            .map_err(BoxedError::new)
            .context(query_error::TableMutationSnafu)
    }

    async fn delete(
        &self,
        request: TableDeleteRequest,
        ctx: QueryContextRef,
    ) -> QueryResult<AffectedRows> {
        self.deleter
            .handle_table_delete(request, ctx)
            .await
            .map_err(BoxedError::new)
            .context(query_error::TableMutationSnafu)
    }

    async fn migrate_region(
        &self,
        _region_id: u64,
        _from_peer: u64,
        _to_peer: u64,
        _replay_timeout: Duration,
    ) -> QueryResult<String> {
        // FIXME(dennis): implemented in the following PR.
        todo!();
    }
}
