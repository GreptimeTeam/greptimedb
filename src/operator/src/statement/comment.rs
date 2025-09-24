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

use chrono::Utc;
use common_query::Output;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt};
use sql::ast::{Ident, ObjectName, ObjectNamePartExt};
use sql::statements::alter::{AlterTable, AlterTableOperation, KeyValueOption};
use sql::statements::comment::{Comment, CommentObject};
use table::requests::{COLUMN_COMMENT_PREFIX, COMMENT_KEY};

use crate::error::{FlowNotFoundSnafu, InvalidSqlSnafu, Result, TableMetadataManagerSnafu};
use crate::statement::StatementExecutor;

impl StatementExecutor {
    pub async fn comment(&self, stmt: Comment, query_ctx: QueryContextRef) -> Result<Output> {
        match stmt.object {
            CommentObject::Table(table) => {
                self.comment_on_table(table, stmt.comment, query_ctx).await
            }
            CommentObject::Column { table, column } => {
                self.comment_on_column(table, column, stmt.comment, query_ctx)
                    .await
            }
            CommentObject::Flow(flow) => self.comment_on_flow(flow, stmt.comment, query_ctx).await,
        }
    }

    async fn comment_on_table(
        &self,
        table: ObjectName,
        comment: Option<String>,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        let value = comment.unwrap_or_default();
        let alter = AlterTable {
            table_name: table,
            alter_operation: AlterTableOperation::SetTableOptions {
                options: vec![KeyValueOption {
                    key: COMMENT_KEY.to_string(),
                    value,
                }],
            },
        };
        self.alter_table(alter, query_ctx).await
    }

    async fn comment_on_column(
        &self,
        table: ObjectName,
        column: Ident,
        comment: Option<String>,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        // Ensure the column reference resolves so we can produce better error messages later
        // by leveraging existing alter table validation.
        let mut column_name = column.value.clone();
        if column_name.is_empty() {
            column_name = column.to_string();
        }

        let key = format!("{}{}", COLUMN_COMMENT_PREFIX, column_name);
        let value = comment.unwrap_or_default();

        let alter = AlterTable {
            table_name: table,
            alter_operation: AlterTableOperation::SetTableOptions {
                options: vec![KeyValueOption { key, value }],
            },
        };
        self.alter_table(alter, query_ctx).await
    }

    async fn comment_on_flow(
        &self,
        flow_name: ObjectName,
        comment: Option<String>,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        let (catalog_name, flow_name_str) = match &flow_name.0[..] {
            [flow] => (
                query_ctx.current_catalog().to_string(),
                flow.to_string_unquoted(),
            ),
            [catalog, flow] => (catalog.to_string_unquoted(), flow.to_string_unquoted()),
            _ => {
                return InvalidSqlSnafu {
                    err_msg: format!(
                        "expect flow name to be <catalog>.<flow_name> or <flow_name>, actual: {flow_name}"
                    ),
                }
                .fail();
            }
        };

        let flow_name_val = self
            .flow_metadata_manager
            .flow_name_manager()
            .get(&catalog_name, &flow_name_str)
            .await
            .context(TableMetadataManagerSnafu)?
            .context(FlowNotFoundSnafu {
                flow_name: &flow_name_str,
            })?;

        let flow_id = flow_name_val.flow_id();
        let flow_info = self
            .flow_metadata_manager
            .flow_info_manager()
            .get_raw(flow_id)
            .await
            .context(TableMetadataManagerSnafu)?
            .context(FlowNotFoundSnafu {
                flow_name: &flow_name_str,
            })?;

        let mut new_flow_info = flow_info.get_inner_ref().clone();
        new_flow_info.comment = comment.unwrap_or_default();
        new_flow_info.updated_time = Utc::now();

        let flow_routes = self
            .flow_metadata_manager
            .flow_route_manager()
            .routes(flow_id)
            .await
            .context(TableMetadataManagerSnafu)?
            .into_iter()
            .map(|(key, value)| (key.partition_id(), value))
            .collect::<Vec<_>>();

        self.flow_metadata_manager
            .update_flow_metadata(flow_id, &flow_info, &new_flow_info, flow_routes)
            .await
            .context(TableMetadataManagerSnafu)?;

        Ok(Output::new_with_affected_rows(0))
    }
}
