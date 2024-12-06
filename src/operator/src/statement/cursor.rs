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

use common_query::{Output, OutputData};
use common_recordbatch::cursor::RecordBatchStreamCursor;
use common_recordbatch::RecordBatches;
use common_telemetry::tracing;
use query::parser::QueryStatement;
use session::context::QueryContextRef;
use snafu::ResultExt;
use sql::statements::cursor::{CloseCursor, DeclareCursor, FetchCursor};
use sql::statements::statement::Statement;

use crate::error::{self, Result};
use crate::statement::StatementExecutor;

impl StatementExecutor {
    #[tracing::instrument(skip_all)]
    pub(super) async fn declare_cursor(
        &self,
        declare_cursor: DeclareCursor,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        let cursor_name = declare_cursor.cursor_name.to_string();

        if query_ctx.get_cursor(&cursor_name).is_some() {
            error::CursorExistsSnafu {
                name: cursor_name.to_string(),
            }
            .fail()?;
        }

        let query_stmt = Statement::Query(declare_cursor.query);

        let output = self
            .plan_exec(QueryStatement::Sql(query_stmt), query_ctx.clone())
            .await?;
        match output.data {
            OutputData::RecordBatches(rb) => {
                let rbs = rb.as_stream();
                query_ctx.insert_cursor(cursor_name, RecordBatchStreamCursor::new(rbs));
            }
            OutputData::Stream(rbs) => {
                query_ctx.insert_cursor(cursor_name, RecordBatchStreamCursor::new(rbs));
            }
            // Should not happen because we have query type ensured from parser.
            OutputData::AffectedRows(_) => error::NotSupportedSnafu {
                feat: "Non-query statement on cursor",
            }
            .fail()?,
        }

        Ok(Output::new_with_affected_rows(0))
    }

    #[tracing::instrument(skip_all)]
    pub(super) async fn fetch_cursor(
        &self,
        fetch_cursor: FetchCursor,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        let cursor_name = fetch_cursor.cursor_name.to_string();
        let fetch_size = fetch_cursor.fetch_size;
        if let Some(rb) = query_ctx.get_cursor(&cursor_name) {
            let record_batch = rb
                .take(fetch_size as usize)
                .await
                .context(error::BuildRecordBatchSnafu)?;
            let record_batches =
                RecordBatches::try_new(record_batch.schema.clone(), vec![record_batch])
                    .context(error::BuildRecordBatchSnafu)?;
            Ok(Output::new_with_record_batches(record_batches))
        } else {
            error::CursorNotFoundSnafu { name: cursor_name }.fail()
        }
    }

    #[tracing::instrument(skip_all)]
    pub(super) async fn close_cursor(
        &self,
        close_cursor: CloseCursor,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        query_ctx.remove_cursor(&close_cursor.cursor_name.to_string());
        Ok(Output::new_with_affected_rows(0))
    }
}
