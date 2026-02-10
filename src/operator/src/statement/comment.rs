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

use api::v1::CommentOnExpr;
use common_error::ext::BoxedError;
use common_meta::procedure_executor::ExecutorContext;
use common_meta::rpc::ddl::{CommentObjectType, CommentOnTask, DdlTask, SubmitDdlTaskRequest};
use common_query::Output;
use session::context::QueryContextRef;
use session::table_name::table_idents_to_full_name;
use snafu::ResultExt;
use sql::ast::ObjectNamePartExt;
use sql::statements::comment::{Comment, CommentObject};

use crate::error::{ExecuteDdlSnafu, ExternalSnafu, InvalidSqlSnafu, Result};
use crate::statement::StatementExecutor;
use crate::utils::to_meta_query_context;

impl StatementExecutor {
    /// Adds a comment to a database object (table, column, or flow).
    ///
    /// # Arguments
    ///
    /// * `stmt`: A `Comment` struct containing the object to comment on and the comment text.
    /// * `query_ctx`: A `QueryContextRef` providing contextual information for the query.
    ///
    /// # Returns
    ///
    /// A `Result` containing the `Output` of the operation, or an error if the operation fails.
    pub async fn comment(&self, stmt: Comment, query_ctx: QueryContextRef) -> Result<Output> {
        let comment_on_task = self.create_comment_on_task_from_stmt(stmt, &query_ctx)?;

        let request = SubmitDdlTaskRequest::new(
            to_meta_query_context(query_ctx),
            DdlTask::new_comment_on(comment_on_task),
        );

        self.procedure_executor
            .submit_ddl_task(&ExecutorContext::default(), request)
            .await
            .context(ExecuteDdlSnafu)
            .map(|_| Output::new_with_affected_rows(0))
    }

    pub async fn comment_by_expr(
        &self,
        expr: CommentOnExpr,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        let comment_on_task = self.create_comment_on_task_from_expr(expr)?;

        let request = SubmitDdlTaskRequest::new(
            to_meta_query_context(query_ctx),
            DdlTask::new_comment_on(comment_on_task),
        );

        self.procedure_executor
            .submit_ddl_task(&ExecutorContext::default(), request)
            .await
            .context(ExecuteDdlSnafu)
            .map(|_| Output::new_with_affected_rows(0))
    }

    fn create_comment_on_task_from_expr(&self, expr: CommentOnExpr) -> Result<CommentOnTask> {
        let object_type = match expr.object_type {
            0 => CommentObjectType::Table,
            1 => CommentObjectType::Column,
            2 => CommentObjectType::Flow,
            _ => {
                return InvalidSqlSnafu {
                    err_msg: format!(
                        "Invalid CommentObjectType value: {}. Valid values are: 0 (Table), 1 (Column), 2 (Flow)",
                        expr.object_type
                    ),
                }
                .fail();
            }
        };

        Ok(CommentOnTask {
            catalog_name: expr.catalog_name,
            schema_name: expr.schema_name,
            object_type,
            object_name: expr.object_name,
            column_name: if expr.column_name.is_empty() {
                None
            } else {
                Some(expr.column_name)
            },
            object_id: None,
            comment: if expr.comment.is_empty() {
                None
            } else {
                Some(expr.comment)
            },
        })
    }

    fn create_comment_on_task_from_stmt(
        &self,
        stmt: Comment,
        query_ctx: &QueryContextRef,
    ) -> Result<CommentOnTask> {
        match stmt.object {
            CommentObject::Table(table) => {
                let (catalog_name, schema_name, table_name) =
                    table_idents_to_full_name(&table, query_ctx)
                        .map_err(BoxedError::new)
                        .context(ExternalSnafu)?;

                Ok(CommentOnTask {
                    catalog_name,
                    schema_name,
                    object_type: CommentObjectType::Table,
                    object_name: table_name,
                    column_name: None,
                    object_id: None,
                    comment: stmt.comment,
                })
            }
            CommentObject::Column { table, column } => {
                let (catalog_name, schema_name, table_name) =
                    table_idents_to_full_name(&table, query_ctx)
                        .map_err(BoxedError::new)
                        .context(ExternalSnafu)?;

                Ok(CommentOnTask {
                    catalog_name,
                    schema_name,
                    object_type: CommentObjectType::Column,
                    object_name: table_name,
                    column_name: Some(column.value),
                    object_id: None,
                    comment: stmt.comment,
                })
            }
            CommentObject::Flow(flow_name) => {
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

                Ok(CommentOnTask {
                    catalog_name,
                    schema_name: String::new(), // Flow doesn't use schema
                    object_type: CommentObjectType::Flow,
                    object_name: flow_name_str,
                    column_name: None,
                    object_id: None,
                    comment: stmt.comment,
                })
            }
        }
    }
}
