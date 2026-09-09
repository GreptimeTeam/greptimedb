// Copyright 2023-2026 GrepTime Inc.
//
// This file is part of the GreptimeDB Enterprise Edition and is licensed under
// the GreptimeDB Enterprise License. You may not use this file except in
// compliance with that license. A copy of the license is available at the root
// of this repository in the file LICENSE-ENTERPRISE.
//
// Unless required by applicable law or agreed to in writing, this software is
// distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied.

use common_catalog::consts::DEFAULT_PRIVATE_SCHEMA_NAME;
use common_error::ext::BoxedError;
use session::context::QueryContextRef;
use snafu::{OptionExt, ensure};
use sql::ast::ObjectName;
use sql::statements::create::bulk_load::CreateBulkLoad;
use sql::statements::delete::Delete;
use sqlparser::ast::{BinaryOperator, Expr, FromTable, TableFactor, Value};

use crate::error::{InvalidSqlSnafu, Result};

pub const BULK_LOAD_JOBS_TABLE: &str = "bulk_load_jobs";
const MAX_DELETE_JOB_IDS: usize = 100;

#[async_trait::async_trait]
pub trait BulkLoadHandler: Send + Sync {
    async fn start(
        &self,
        statement: CreateBulkLoad,
        query_ctx: QueryContextRef,
    ) -> std::result::Result<(), BoxedError>;

    async fn delete(
        &self,
        catalog: String,
        job_ids: Vec<String>,
    ) -> std::result::Result<usize, BoxedError>;
}

pub type BulkLoadHandlerRef = std::sync::Arc<dyn BulkLoadHandler>;

pub struct BulkLoadDelete {
    pub catalog: String,
    pub job_ids: Vec<String>,
}

pub fn targets_bulk_load_jobs(delete: &Delete, query_ctx: &QueryContextRef) -> bool {
    delete_target_candidate(delete).is_some_and(|name| is_private_table_name(name, query_ctx))
}

pub fn extract_bulk_load_delete(
    delete: &Delete,
    query_ctx: &QueryContextRef,
) -> Result<Option<BulkLoadDelete>> {
    let Some(name) = delete_target_candidate(delete) else {
        return Ok(None);
    };
    if !is_private_table_name(name, query_ctx) {
        return Ok(None);
    }
    let Some(catalog) = resolve_private_table(name, query_ctx) else {
        return InvalidSqlSnafu {
            err_msg: "bulk-load jobs can only be deleted from the current catalog".to_string(),
        }
        .fail();
    };
    let sqlparser::ast::Statement::Delete(sqlparser::ast::Delete {
        optimizer_hint,
        tables,
        from,
        using,
        selection,
        returning,
        order_by,
        limit,
        ..
    }) = &delete.inner
    else {
        return Ok(None);
    };
    ensure!(
        optimizer_hint.is_none()
            && tables.is_empty()
            && is_plain_delete_from(from)
            && using.is_none()
            && returning.is_none()
            && order_by.is_empty()
            && limit.is_none(),
        InvalidSqlSnafu {
            err_msg: "bulk-load job deletion only supports DELETE FROM greptime_private.bulk_load_jobs WHERE ...".to_string()
        }
    );
    let selection = selection.as_ref().context(InvalidSqlSnafu {
        err_msg: "bulk-load job deletion requires a WHERE clause".to_string(),
    })?;
    let job_ids = extract_job_ids(selection).context(InvalidSqlSnafu {
        err_msg: "bulk-load job deletion only supports job_id = '<id>' or job_id IN ('<id>', ...)"
            .to_string(),
    })?;
    ensure!(
        !job_ids.is_empty(),
        InvalidSqlSnafu {
            err_msg: "bulk-load job deletion requires at least one job id".to_string()
        }
    );
    ensure!(
        job_ids.len() <= MAX_DELETE_JOB_IDS,
        InvalidSqlSnafu {
            err_msg: format!(
                "bulk-load job deletion supports at most {MAX_DELETE_JOB_IDS} job ids"
            )
        }
    );
    Ok(Some(BulkLoadDelete { catalog, job_ids }))
}

fn delete_target_candidate(delete: &Delete) -> Option<&ObjectName> {
    let sqlparser::ast::Statement::Delete(sqlparser::ast::Delete { from, .. }) = &delete.inner
    else {
        return None;
    };
    let FromTable::WithFromKeyword(from) = from else {
        return None;
    };
    let table = from.first()?;
    let TableFactor::Table { name, .. } = &table.relation else {
        return None;
    };
    Some(name)
}

fn is_plain_delete_from(from: &FromTable) -> bool {
    let FromTable::WithFromKeyword(from) = from else {
        return false;
    };
    let [table] = from.as_slice() else {
        return false;
    };
    table.joins.is_empty()
        && matches!(
            table.relation,
            TableFactor::Table {
                alias: None,
                args: None,
                ref with_hints,
                version: None,
                ref partitions,
                ..
            } if with_hints.is_empty() && partitions.is_empty()
        )
}

fn resolve_private_table(name: &ObjectName, query_ctx: &QueryContextRef) -> Option<String> {
    let parts = name
        .0
        .iter()
        .map(|part| part.as_ident().map(|ident| ident.value.as_str()))
        .collect::<Option<Vec<_>>>()?;
    let current_schema = query_ctx.current_schema();
    let (catalog, schema, table) = match parts.as_slice() {
        [table] => (query_ctx.current_catalog(), current_schema.as_str(), *table),
        [schema, table] => (query_ctx.current_catalog(), *schema, *table),
        [catalog, schema, table] => (*catalog, *schema, *table),
        _ => return None,
    };
    (catalog == query_ctx.current_catalog()
        && schema == DEFAULT_PRIVATE_SCHEMA_NAME
        && table == BULK_LOAD_JOBS_TABLE)
        .then(|| catalog.to_string())
}

fn is_private_table_name(name: &ObjectName, query_ctx: &QueryContextRef) -> bool {
    let parts = name
        .0
        .iter()
        .map(|part| part.as_ident().map(|ident| ident.value.as_str()))
        .collect::<Option<Vec<_>>>();
    match parts.as_deref() {
        Some([table]) => {
            query_ctx.current_schema() == DEFAULT_PRIVATE_SCHEMA_NAME
                && *table == BULK_LOAD_JOBS_TABLE
        }
        Some([schema, table]) | Some([_, schema, table]) => {
            *schema == DEFAULT_PRIVATE_SCHEMA_NAME && *table == BULK_LOAD_JOBS_TABLE
        }
        _ => false,
    }
}

fn extract_job_ids(expr: &Expr) -> Option<Vec<String>> {
    match expr {
        Expr::BinaryOp { left, op, right } if *op == BinaryOperator::Eq => {
            if is_job_id(left) {
                string_literal(right).map(|id| vec![id.to_string()])
            } else if is_job_id(right) {
                string_literal(left).map(|id| vec![id.to_string()])
            } else {
                None
            }
        }
        Expr::InList {
            expr,
            list,
            negated: false,
        } if is_job_id(expr) => list
            .iter()
            .map(string_literal)
            .map(|id| id.map(str::to_string))
            .collect(),
        _ => None,
    }
}

fn is_job_id(expr: &Expr) -> bool {
    matches!(expr, Expr::Identifier(ident) if ident.value == "job_id")
}

fn string_literal(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Value(value) => match &value.value {
            Value::SingleQuotedString(value) => Some(value),
            _ => None,
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use session::context::QueryContext;
    use sql::dialect::GreptimeDbDialect;
    use sql::parser::{ParseOptions, ParserContext};
    use sql::statements::statement::Statement;

    use super::*;

    fn parse_delete(sql: &str) -> Delete {
        let statement =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap()
                .remove(0);
        let Statement::Delete(delete) = statement else {
            panic!("expected DELETE")
        };
        *delete
    }

    #[test]
    fn extracts_exact_bulk_load_job_deletes() {
        let query_ctx = QueryContext::arc();
        let request = extract_bulk_load_delete(
            &parse_delete(
                "DELETE FROM greptime_private.bulk_load_jobs WHERE job_id IN ('job-1', 'job-2')",
            ),
            &query_ctx,
        )
        .unwrap()
        .unwrap();

        assert_eq!(request.catalog, query_ctx.current_catalog());
        assert_eq!(request.job_ids, ["job-1", "job-2"]);

        let request = extract_bulk_load_delete(
            &parse_delete(
                "DELETE FROM greptime.greptime_private.bulk_load_jobs WHERE 'job-3' = job_id",
            ),
            &query_ctx,
        )
        .unwrap()
        .unwrap();
        assert_eq!(request.job_ids, ["job-3"]);
    }

    #[test]
    fn rejects_broad_or_unsupported_bulk_load_job_deletes() {
        let query_ctx = QueryContext::arc();
        for sql in [
            "DELETE FROM greptime_private.bulk_load_jobs",
            "DELETE FROM greptime_private.bulk_load_jobs WHERE state = 'finished'",
            "DELETE FROM greptime_private.bulk_load_jobs WHERE job_id <> 'job-1'",
            "DELETE FROM greptime_private.bulk_load_jobs AS jobs WHERE job_id = 'job-1'",
            "DELETE FROM other.greptime_private.bulk_load_jobs WHERE job_id = 'job-1'",
        ] {
            assert!(
                extract_bulk_load_delete(&parse_delete(sql), &query_ctx).is_err(),
                "{sql}"
            );
        }
    }

    #[test]
    fn leaves_other_delete_statements_unchanged() {
        let request = extract_bulk_load_delete(
            &parse_delete("DELETE FROM metrics WHERE host = 'web-1'"),
            &QueryContext::arc(),
        )
        .unwrap();
        assert!(request.is_none());
    }
}
