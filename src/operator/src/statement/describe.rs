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

use common_error::ext::BoxedError;
use common_query::Output;
use common_telemetry::tracing;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt};
use sql::statements::describe::DescribeTable;
use sql::util::format_raw_object_name;

use crate::error::{
    CatalogSnafu, DescribeStatementSnafu, ExternalSnafu, Result, TableNotFoundSnafu,
};
use crate::statement::StatementExecutor;
use crate::table::table_idents_to_full_name;

impl StatementExecutor {
    #[tracing::instrument(skip_all)]
    pub(super) async fn describe_table(
        &self,
        stmt: DescribeTable,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        let (catalog, schema, table) = table_idents_to_full_name(stmt.name(), query_ctx)
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;

        let table = self
            .catalog_manager
            .table(&catalog, &schema, &table)
            .await
            .context(CatalogSnafu)?
            .with_context(|| TableNotFoundSnafu {
                table_name: format_raw_object_name(stmt.name()),
            })?;

        query::sql::describe_table(table).context(DescribeStatementSnafu)
    }
}
