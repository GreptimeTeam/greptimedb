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

use async_trait::async_trait;
use common_error::ext::BoxedError;
use query::error as query_error;
use query::error::Result as QueryResult;
use query::table_mutation::{AffectedRows, TableMutationHandler};
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
    query_ctx: QueryContextRef,
) -> Result<(String, String, String)> {
    match &obj_name.0[..] {
        [table] => Ok((
            query_ctx.current_catalog().to_owned(),
            query_ctx.current_schema().to_owned(),
            table.value.clone(),
        )),
        [schema, table] => Ok((
            query_ctx.current_catalog().to_owned(),
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
}
