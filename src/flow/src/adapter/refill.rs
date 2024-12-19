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

use std::collections::{BTreeSet, VecDeque};

use common_error::ext::BoxedError;
use common_query::OutputData;
use datatypes::value::Value;
use query::parser::QueryLanguageParser;
use session::context::QueryContext;
use snafu::ResultExt;
use table::metadata::TableId;

use crate::adapter::table_source::TableSource;
use crate::adapter::FlowWorkerManagerRef;
use crate::error::UnexpectedSnafu;
use crate::expr::error::ExternalSnafu;
use crate::{Error, FlownodeBuilder, FrontendInvoker};

impl FlownodeBuilder {
    /// Create a series of tasks to refill flow, will be transfer to flownode if
    ///
    /// tasks havn't completed, and will show up in `flows` table
    async fn start_refill_flows(&self, manager: &FlowWorkerManagerRef) -> Result<(), Error> {
        todo!()
    }
}

/// Task to refill flow with given table id and a time range
pub struct RefillTask {
    table_id: TableId,
    output_data: common_query::Output,
}

impl RefillTask {
    /// Query with "select * from table WHERE time >= range_start and time < range_end"
    pub async fn create(
        invoker: &FrontendInvoker,
        table_id: TableId,
        time_range: (
            common_time::timestamp::Timestamp,
            common_time::timestamp::Timestamp,
        ),
        time_col_name: &str,
        table_src: &TableSource,
    ) -> Result<RefillTask, Error> {
        let (table_name, table_schmea) = table_src.get_table_name_schema(&table_id).await?;
        let all_col_names: BTreeSet<_> = table_schmea
            .iter_names()
            .flatten()
            .map(|s| s.as_str())
            .collect();

        if !all_col_names.contains(time_col_name) {
            UnexpectedSnafu {
                reason: format!(
                    "Can't find column {} in table {} while refill flow",
                    time_col_name,
                    table_name.join(".")
                ),
            }
            .fail()?;
        }

        let sql = format!(
            "select * from {0} where {1} >= {2} and {1} < {3}",
            table_name.join("."),
            time_col_name,
            Value::from(time_range.0),
            Value::from(time_range.1),
        );

        // we don't need information from query context in this query so a default query context is enough
        let query_ctx = QueryContext::arc();

        let stmt = QueryLanguageParser::parse_sql(&sql, &query_ctx)
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;

        let stmt_exec = invoker.statement_executor();

        let output_data = stmt_exec
            .execute_stmt(stmt, query_ctx)
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;

        Ok(RefillTask {
            table_id,
            output_data,
        })
    }
}
