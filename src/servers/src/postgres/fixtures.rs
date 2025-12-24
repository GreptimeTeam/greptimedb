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

use std::collections::HashMap;
use std::sync::Arc;

use futures::stream;
use once_cell::sync::Lazy;
use pgwire::api::Type;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::error::PgWireResult;
use pgwire::messages::data::DataRow;
use regex::Regex;
use session::context::{QueryContext, QueryContextRef};

fn build_string_data_rows(
    schema: Arc<Vec<FieldInfo>>,
    rows: Vec<Vec<String>>,
) -> Vec<PgWireResult<DataRow>> {
    let mut encoder = DataRowEncoder::new(schema.clone());
    rows.iter()
        .map(|row| {
            for value in row {
                encoder.encode_field(&Some(value))?;
            }
            Ok(encoder.take_row())
        })
        .collect()
}

static VAR_VALUES: Lazy<HashMap<&str, &str>> = Lazy::new(|| {
    HashMap::from([
        ("default_transaction_isolation", "read committed"),
        ("transaction isolation level", "read committed"),
        ("standard_conforming_strings", "on"),
        ("client_encoding", "UTF8"),
    ])
});

static SHOW_PATTERN: Lazy<Regex> = Lazy::new(|| Regex::new("(?i)^SHOW (.*?);?$").unwrap());
static SET_TRANSACTION_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new("(?i)^SET TRANSACTION (.*?);?$").unwrap());
static START_TRANSACTION_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new("(?i)^(START TRANSACTION.*|BEGIN);?").unwrap());
static COMMIT_TRANSACTION_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new("(?i)^(COMMIT TRANSACTION|COMMIT);?").unwrap());
static ABORT_TRANSACTION_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new("(?i)^(ABORT TRANSACTION|ROLLBACK);?").unwrap());

/// Test if given query statement matches the patterns
pub(crate) fn matches(query: &str) -> bool {
    process(query, QueryContext::arc()).is_some()
}

fn set_transaction_warning(query_ctx: QueryContextRef) {
    query_ctx.set_warning("Please note transaction is not supported in GreptimeDB.".to_string());
}

/// Process unsupported SQL and return fixed result as a compatibility solution
pub(crate) fn process(query: &str, query_ctx: QueryContextRef) -> Option<Vec<Response>> {
    // Transaction directives:
    if START_TRANSACTION_PATTERN.is_match(query) {
        set_transaction_warning(query_ctx);
        if query.to_lowercase().starts_with("begin") {
            Some(vec![Response::TransactionStart(Tag::new("BEGIN"))])
        } else {
            Some(vec![Response::TransactionStart(Tag::new(
                "START TRANSACTION",
            ))])
        }
    } else if ABORT_TRANSACTION_PATTERN.is_match(query) {
        Some(vec![Response::TransactionEnd(Tag::new("ROLLBACK"))])
    } else if COMMIT_TRANSACTION_PATTERN.is_match(query) {
        Some(vec![Response::TransactionEnd(Tag::new("COMMIT"))])
    } else if let Some(show_var) = SHOW_PATTERN.captures(query) {
        let show_var = show_var[1].to_lowercase();
        if let Some(value) = VAR_VALUES.get(&show_var.as_ref()) {
            let f1 = FieldInfo::new(
                show_var.clone(),
                None,
                None,
                Type::VARCHAR,
                FieldFormat::Text,
            );
            let schema = Arc::new(vec![f1]);
            let data = stream::iter(build_string_data_rows(
                schema.clone(),
                vec![vec![value.to_string()]],
            ));

            Some(vec![Response::Query(QueryResponse::new(schema, data))])
        } else {
            None
        }
    } else if SET_TRANSACTION_PATTERN.is_match(query) {
        Some(vec![Response::Execution(Tag::new("SET"))])
    } else {
        None
    }
}

#[cfg(test)]
mod test {
    use session::context::{QueryContext, QueryContextRef};

    use super::*;

    fn assert_tag(q: &str, t: &str, query_context: QueryContextRef) {
        if let Response::Execution(tag)
        | Response::TransactionStart(tag)
        | Response::TransactionEnd(tag) = process(q, query_context.clone())
            .unwrap_or_else(|| panic!("fail to match {}", q))
            .remove(0)
        {
            assert_eq!(Tag::new(t), tag);
        } else {
            panic!("Invalid response");
        }
    }

    fn get_data(q: &str, query_context: QueryContextRef) -> QueryResponse {
        if let Response::Query(resp) = process(q, query_context.clone())
            .unwrap_or_else(|| panic!("fail to match {}", q))
            .remove(0)
        {
            resp
        } else {
            panic!("Invalid response");
        }
    }

    #[test]
    fn test_process() {
        let query_context = QueryContext::arc();

        assert_tag("BEGIN", "BEGIN", query_context.clone());
        assert_tag("BEGIN;", "BEGIN", query_context.clone());
        assert_tag("begin;", "BEGIN", query_context.clone());
        assert_tag("ROLLBACK", "ROLLBACK", query_context.clone());
        assert_tag("ROLLBACK;", "ROLLBACK", query_context.clone());
        assert_tag("rollback;", "ROLLBACK", query_context.clone());
        assert_tag("COMMIT", "COMMIT", query_context.clone());
        assert_tag("COMMIT;", "COMMIT", query_context.clone());
        assert_tag("commit;", "COMMIT", query_context.clone());
        assert_tag(
            "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
            "SET",
            query_context.clone(),
        );
        assert_tag(
            "SET TRANSACTION ISOLATION LEVEL READ COMMITTED;",
            "SET",
            query_context.clone(),
        );
        assert_tag(
            "SET transaction isolation level READ COMMITTED;",
            "SET",
            query_context.clone(),
        );
        assert_tag(
            "START TRANSACTION isolation level READ COMMITTED;",
            "START TRANSACTION",
            query_context.clone(),
        );
        assert_tag(
            "start transaction isolation level READ COMMITTED;",
            "START TRANSACTION",
            query_context.clone(),
        );
        assert_tag("abort transaction;", "ROLLBACK", query_context.clone());
        assert_tag("commit transaction;", "COMMIT", query_context.clone());
        assert_tag("COMMIT transaction;", "COMMIT", query_context.clone());

        let resp = get_data("SHOW transaction isolation level", query_context.clone());
        assert_eq!(1, resp.row_schema().len());
        let resp = get_data("show client_encoding;", query_context.clone());
        assert_eq!(1, resp.row_schema().len());
        let resp = get_data("show standard_conforming_strings;", query_context.clone());
        assert_eq!(1, resp.row_schema().len());
        let resp = get_data("show default_transaction_isolation", query_context.clone());
        assert_eq!(1, resp.row_schema().len());

        assert!(process("SELECT 1", query_context.clone()).is_none());
        assert!(process("SHOW TABLES ", query_context.clone()).is_none());
        assert!(process("SET TIME_ZONE=utc ", query_context.clone()).is_none());
    }
}
