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

use common_query::Output;
use session::context::QueryContextRef;
use sql::statements::cursor::{CloseCursor, DeclareCursor, FetchCursor};

use crate::statement::StatementExecutor;

use crate::error::Result;

impl StatementExecutor {
    pub(super) async fn declare_cursor(
        &self,
        declare_cursor: DeclareCursor,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        let cursor_name = fetch_cursor.cursor_name.to_string();

        Ok(Output::new_with_affected_rows(0))
    }

    pub(super) async fn fetch_cursor(
        &self,
        fetch_cursor: FetchCursor,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        let cursor_name = fetch_cursor.cursor_name.to_string();
        if let Some(rb) = query_ctx.get_cursor(&cursor_name) {
        } else {
            todo!("cursor not found")
        }
    }

    pub(super) async fn close_cursor(
        &self,
        close_cursor: CloseCursor,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        query_ctx.remove_cursor(&close_cursor.cursor_name.to_string());
        Ok(Output::new_with_affected_rows(0))
    }
}
