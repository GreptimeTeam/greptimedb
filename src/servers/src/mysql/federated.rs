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

//! Use regex to filter out some MySQL federated components' emitted statements.
//! Inspired by Databend's "[mysql_federated.rs](https://github.com/datafuselabs/databend/blob/ac706bf65845e6895141c96c0a10bad6fdc2d367/src/query/service/src/servers/mysql/mysql_federated.rs)".

use std::collections::HashMap;
use std::sync::Arc;

use common_query::Output;
use common_recordbatch::RecordBatches;
use common_time::timezone::system_timezone_name;
use common_version;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema};
use datatypes::vectors::StringVector;
use once_cell::sync::Lazy;
use regex::Regex;
use regex::bytes::RegexSet;
use session::SessionRef;
use session::context::QueryContextRef;

/// Matches the optional `GLOBAL`/`SESSION`/`LOCAL` scope MySQL accepts before `VARIABLES`.
const VARIABLES_SCOPE: &str = "(GLOBAL |SESSION |LOCAL )?";

static SELECT_VAR_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new("(?i)^(SELECT\\s+@@(.*))").unwrap());
static SHOW_LOWER_CASE_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(&format!(
        "(?i)^(SHOW {VARIABLES_SCOPE}VARIABLES LIKE 'lower_case_table_names'(.*))"
    ))
    .unwrap()
});
static SHOW_VARIABLES_LIKE_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(&format!(
        "(?i)^(SHOW {VARIABLES_SCOPE}VARIABLES( LIKE (.*))?)"
    ))
    .unwrap()
});
static SHOW_WARNINGS_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new("(?i)^(SHOW WARNINGS)").unwrap());

// SELECT TIMEDIFF(NOW(), UTC_TIMESTAMP());
static SELECT_TIME_DIFF_FUNC_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new("(?i)^(SELECT TIMEDIFF\\(NOW\\(\\), UTC_TIMESTAMP\\(\\)\\))").unwrap());

// sqlalchemy < 1.4.30
static SHOW_SQL_MODE_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(&format!(
        "(?i)^(SHOW {VARIABLES_SCOPE}VARIABLES LIKE 'sql_mode'(.*))"
    ))
    .unwrap()
});

static OTHER_NOT_SUPPORTED_STMT: Lazy<RegexSet> = Lazy::new(|| {
    RegexSet::new([
        // Txn.
        "(?i)^(ROLLBACK(.*))",
        "(?i)^(COMMIT(.*))",
        "(?i)^(START(.*))",
        "(?i)^(BEGIN(.*))",

        // Set.
        "(?i)^(SET NAMES(.*))",
        "(?i)^(SET character_set_results(.*))",
        "(?i)^(SET net_write_timeout(.*))",
        "(?i)^(SET FOREIGN_KEY_CHECKS(.*))",
        "(?i)^(SET AUTOCOMMIT(.*))",
        "(?i)^(SET SQL_LOG_BIN(.*))",
        "(?i)^(SET SESSION TRANSACTION(.*))",
        "(?i)^(SET TRANSACTION(.*))",
        "(?i)^(SET sql_mode(.*))",
        "(?i)^(SET SQL_SELECT_LIMIT(.*))",
        "(?i)^(SET PROFILING(.*))",

        // mysqlclient.
        "(?i)^(SELECT \\$\\$)",

        // mysqldump.
        "(?i)^(SET SQL_QUOTE_SHOW_CREATE(.*))",
        "(?i)^(LOCK TABLES(.*))",
        "(?i)^(UNLOCK TABLES(.*))",
        "(?i)^(SELECT LOGFILE_GROUP_NAME, FILE_NAME, TOTAL_EXTENTS, INITIAL_SIZE, ENGINE, EXTRA FROM INFORMATION_SCHEMA.FILES(.*))",

        // mydumper.
        "(?i)^(/\\*!80003 SET(.*) \\*/)$",
        "(?i)^(SHOW MASTER STATUS)",
        "(?i)^(SHOW ALL SLAVES STATUS)",
        "(?i)^(LOCK BINLOG FOR BACKUP)",
        "(?i)^(LOCK TABLES FOR BACKUP)",
        "(?i)^(UNLOCK BINLOG(.*))",
        "(?i)^(/\\*!40101 SET(.*) \\*/)$",

        // DBeaver.
        "(?i)^(SHOW PLUGINS)",
        "(?i)^(SHOW ENGINES)",
        "(?i)^(SHOW @@(.*))",

        // pt-toolkit
        "(?i)^(/\\*!40101 SET(.*) \\*/)$",

        // mysqldump 5.7.16
        "(?i)^(/\\*!40100 SET(.*) \\*/)$",
        "(?i)^(/\\*!40103 SET(.*) \\*/)$",
        "(?i)^(/\\*!40111 SET(.*) \\*/)$",
        "(?i)^(/\\*!40101 SET(.*) \\*/)$",
        "(?i)^(/\\*!40014 SET(.*) \\*/)$",
        "(?i)^(/\\*!40000 SET(.*) \\*/)$",
    ]).unwrap()
});

static VAR_VALUES: Lazy<HashMap<&str, &str>> = Lazy::new(|| {
    HashMap::from([
        ("tx_isolation", "REPEATABLE-READ"),
        ("session.tx_isolation", "REPEATABLE-READ"),
        ("transaction_isolation", "REPEATABLE-READ"),
        ("session.transaction_isolation", "REPEATABLE-READ"),
        ("session.transaction_read_only", "0"),
        ("max_allowed_packet", "134217728"),
        ("interactive_timeout", "31536000"),
        ("wait_timeout", "31536000"),
        ("net_write_timeout", "31536000"),
        ("version_comment", common_version::product_name()),
    ])
});

// Recordbatches for select function.
// Format:
// |function_name|
// |value|
fn select_function(name: &str, value: &str) -> RecordBatches {
    let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
        name,
        ConcreteDataType::string_datatype(),
        true,
    )]));
    let columns = vec![Arc::new(StringVector::from(vec![value])) as _];
    RecordBatches::try_from_columns(schema, columns)
        // unwrap is safe because the schema and data are definitely able to form a recordbatch, they are all string type
        .unwrap()
}

// Recordbatches for show variable statement.
// Format is:
// | Variable_name | Value |
// | xx            | yy    |
fn show_variables(name: &str, value: &str) -> RecordBatches {
    let schema = Arc::new(Schema::new(vec![
        ColumnSchema::new("Variable_name", ConcreteDataType::string_datatype(), true),
        ColumnSchema::new("Value", ConcreteDataType::string_datatype(), true),
    ]));
    let columns = vec![
        Arc::new(StringVector::from(vec![name])) as _,
        Arc::new(StringVector::from(vec![value])) as _,
    ];
    RecordBatches::try_from_columns(schema, columns)
        // unwrap is safe because the schema and data are definitely able to form a recordbatch, they are all string type
        .unwrap()
}

fn select_variable(query: &str, query_context: QueryContextRef) -> Option<Output> {
    let mut fields = vec![];
    let mut values = vec![];

    // query like "SELECT @@aa, @@bb as cc, @dd..."
    let query = query.to_lowercase();
    let vars: Vec<&str> = query.split("@@").collect();
    if vars.len() <= 1 {
        return None;
    }

    // skip the first "select"
    for var in vars.iter().skip(1) {
        let var = var.trim_matches(|c| c == ' ' || c == ',' || c == ';');
        let var_as: Vec<&str> = var
            .split(" as ")
            .map(|x| {
                x.trim_matches(|c| c == ' ')
                    .split_whitespace()
                    .next()
                    .unwrap_or("")
            })
            .collect();

        // get value of variables from known sources or fallback to defaults
        let value = match var_as[0] {
            "session.time_zone" | "time_zone" => query_context.timezone().to_string(),
            "system_time_zone" => system_timezone_name(),
            "max_execution_time" | "session.max_execution_time" => {
                query_context.query_timeout_as_millis().to_string()
            }
            _ => VAR_VALUES
                .get(var_as[0])
                .map(|v| v.to_string())
                .unwrap_or_else(|| "0".to_owned()),
        };

        values.push(Arc::new(StringVector::from(vec![value])) as _);
        match var_as.len() {
            1 => {
                // @@aa
                // field is '@@aa'
                fields.push(ColumnSchema::new(
                    format!("@@{}", var_as[0]),
                    ConcreteDataType::string_datatype(),
                    true,
                ));
            }
            2 => {
                // @@bb as cc:
                // var is 'bb'.
                // field is 'cc'.
                fields.push(ColumnSchema::new(
                    var_as[1],
                    ConcreteDataType::string_datatype(),
                    true,
                ));
            }
            _ => return None,
        }
    }

    let schema = Arc::new(Schema::new(fields));
    // unwrap is safe because the schema and data are definitely able to form a recordbatch, they are all string type
    let batches = RecordBatches::try_from_columns(schema, values).unwrap();
    Some(Output::new_with_record_batches(batches))
}

fn check_select_variable(query: &str, query_context: QueryContextRef) -> Option<Output> {
    if SELECT_VAR_PATTERN.is_match(query) {
        select_variable(query, query_context)
    } else {
        None
    }
}

fn check_show_variables(query: &str) -> Option<Output> {
    let recordbatches = if SHOW_SQL_MODE_PATTERN.is_match(query) {
        Some(show_variables(
            "sql_mode",
            "ONLY_FULL_GROUP_BY STRICT_TRANS_TABLES NO_ZERO_IN_DATE NO_ZERO_DATE ERROR_FOR_DIVISION_BY_ZERO NO_ENGINE_SUBSTITUTION",
        ))
    } else if SHOW_LOWER_CASE_PATTERN.is_match(query) {
        Some(show_variables("lower_case_table_names", "0"))
    } else if SHOW_VARIABLES_LIKE_PATTERN.is_match(query) {
        Some(show_variables("", ""))
    } else {
        None
    };
    recordbatches.map(Output::new_with_record_batches)
}

/// Build SHOW WARNINGS result from session's warnings
fn show_warnings(session: &SessionRef) -> RecordBatches {
    let schema = Arc::new(Schema::new(vec![
        ColumnSchema::new("Level", ConcreteDataType::string_datatype(), false),
        ColumnSchema::new("Code", ConcreteDataType::uint16_datatype(), false),
        ColumnSchema::new("Message", ConcreteDataType::string_datatype(), false),
    ]));

    let warnings = session.warnings();
    let count = warnings.len();

    let columns = if count > 0 {
        vec![
            Arc::new(StringVector::from(vec!["Warning"; count])) as _,
            Arc::new(datatypes::vectors::UInt16Vector::from(vec![
                Some(1000u16);
                count
            ])) as _,
            Arc::new(StringVector::from(warnings)) as _,
        ]
    } else {
        vec![
            Arc::new(StringVector::from(Vec::<String>::new())) as _,
            Arc::new(datatypes::vectors::UInt16Vector::from(
                Vec::<Option<u16>>::new(),
            )) as _,
            Arc::new(StringVector::from(Vec::<String>::new())) as _,
        ]
    };

    RecordBatches::try_from_columns(schema, columns).unwrap()
}

fn check_show_warnings(query: &str, session: &SessionRef) -> Option<Output> {
    if SHOW_WARNINGS_PATTERN.is_match(query) {
        Some(Output::new_with_record_batches(show_warnings(session)))
    } else {
        None
    }
}

// Check for SET or others query, this is the final check of the federated query.
fn check_others(query: &str, _query_ctx: QueryContextRef) -> Option<Output> {
    if OTHER_NOT_SUPPORTED_STMT.is_match(query.as_bytes()) {
        return Some(Output::new_with_record_batches(RecordBatches::empty()));
    }

    let recordbatches = if SELECT_TIME_DIFF_FUNC_PATTERN.is_match(query) {
        Some(select_function(
            "TIMEDIFF(NOW(), UTC_TIMESTAMP())",
            "00:00:00",
        ))
    } else {
        None
    };
    recordbatches.map(Output::new_with_record_batches)
}

/// Strips leading whitespace and SQL comments.
///
/// All patterns above are anchored at the start of the statement, but JDBC clients such as
/// DataGrip and DBeaver prefix every statement they send with a `/* ApplicationName=... */`
/// comment. Without stripping it first, those statements miss every pattern and reach the
/// query engine, which rejects the ones this module exists to absorb.
fn strip_leading_comments(query: &str) -> &str {
    let mut rest = query.trim_start();
    loop {
        if let Some(tail) = rest.strip_prefix("/*") {
            // An unterminated block comment leaves no statement to match against.
            let Some(end) = tail.find("*/") else {
                return "";
            };
            rest = tail[end + 2..].trim_start();
        } else if rest.starts_with('#')
            // MySQL only treats `--` as a comment when followed by whitespace.
            || (rest.starts_with("--")
                && rest[2..].chars().next().is_none_or(|c| c.is_whitespace()))
        {
            let Some(end) = rest.find('\n') else {
                return "";
            };
            rest = rest[end + 1..].trim_start();
        } else {
            return rest;
        }
    }
}

// Check whether the query is a federated or driver setup command,
// and return some faked results if there are any.
pub(crate) fn check(
    query: &str,
    query_ctx: QueryContextRef,
    session: SessionRef,
) -> Option<Output> {
    let query = strip_leading_comments(query);

    // INSERT don't need MySQL federated check. We assume the query doesn't contain
    // federated or driver setup command if it starts with a 'INSERT' statement.
    let the_6th_index = query.char_indices().nth(6).map(|(i, _)| i);
    if let Some(index) = the_6th_index
        && query[..index].eq_ignore_ascii_case("INSERT")
    {
        return None;
    }

    // First to check the query is like "select @@variables".
    check_select_variable(query, query_ctx.clone())
        .or_else(|| check_show_variables(query))
        .or_else(|| check_show_warnings(query, &session))
        // Last check
        .or_else(|| check_others(query, query_ctx))
}

#[cfg(test)]
mod test {

    use common_query::OutputData;
    use common_time::timezone::set_default_timezone;
    use session::Session;
    use session::context::{Channel, QueryContext};

    use super::*;

    #[test]
    fn test_check_abnormal() {
        let session = Arc::new(Session::new(None, Channel::Mysql, Default::default(), 0));
        let query = "🫣一点不正常的东西🫣";
        let output = check(query, QueryContext::arc(), session.clone());

        assert!(output.is_none());
    }

    #[test]
    fn test_check() {
        let session = Arc::new(Session::new(None, Channel::Mysql, Default::default(), 0));
        let query = "select 1";
        let result = check(query, QueryContext::arc(), session.clone());
        assert!(result.is_none());

        let query = "select version";
        let output = check(query, QueryContext::arc(), session.clone());
        assert!(output.is_none());

        fn test(query: &str, expected: &str) {
            let session = Arc::new(Session::new(None, Channel::Mysql, Default::default(), 0));
            let output = check(query, QueryContext::arc(), session.clone());
            match output.unwrap().data {
                OutputData::RecordBatches(r) => {
                    assert_eq!(&r.pretty_print().unwrap(), expected)
                }
                _ => unreachable!(),
            }
        }

        let query = "SELECT @@version_comment LIMIT 1";
        let expected = "\
+-------------------+
| @@version_comment |
+-------------------+
| GreptimeDB        |
+-------------------+";
        test(query, expected);

        // variables
        let query = "select @@tx_isolation, @@session.tx_isolation";
        let expected = "\
+-----------------+------------------------+
| @@tx_isolation  | @@session.tx_isolation |
+-----------------+------------------------+
| REPEATABLE-READ | REPEATABLE-READ        |
+-----------------+------------------------+";
        test(query, expected);

        // set system timezone
        set_default_timezone(Some("Asia/Shanghai")).unwrap();
        // complex variables
        let query = "/* mysql-connector-java-8.0.17 (Revision: 16a712ddb3f826a1933ab42b0039f7fb9eebc6ec) */SELECT  @@session.auto_increment_increment AS auto_increment_increment, @@character_set_client AS character_set_client, @@character_set_connection AS character_set_connection, @@character_set_results AS character_set_results, @@character_set_server AS character_set_server, @@collation_server AS collation_server, @@collation_connection AS collation_connection, @@init_connect AS init_connect, @@interactive_timeout AS interactive_timeout, @@license AS license, @@lower_case_table_names AS lower_case_table_names, @@max_allowed_packet AS max_allowed_packet, @@net_write_timeout AS net_write_timeout, @@performance_schema AS performance_schema, @@sql_mode AS sql_mode, @@system_time_zone AS system_time_zone, @@time_zone AS time_zone, @@transaction_isolation AS transaction_isolation, @@wait_timeout AS wait_timeout;";
        let expected = "\
+--------------------------+----------------------+--------------------------+-----------------------+----------------------+------------------+----------------------+--------------+---------------------+---------+------------------------+--------------------+-------------------+--------------------+----------+------------------+---------------+-----------------------+--------------+
| auto_increment_increment | character_set_client | character_set_connection | character_set_results | character_set_server | collation_server | collation_connection | init_connect | interactive_timeout | license | lower_case_table_names | max_allowed_packet | net_write_timeout | performance_schema | sql_mode | system_time_zone | time_zone     | transaction_isolation | wait_timeout |
+--------------------------+----------------------+--------------------------+-----------------------+----------------------+------------------+----------------------+--------------+---------------------+---------+------------------------+--------------------+-------------------+--------------------+----------+------------------+---------------+-----------------------+--------------+
| 0                        | 0                    | 0                        | 0                     | 0                    | 0                | 0                    | 0            | 31536000            | 0       | 0                      | 134217728          | 31536000          | 0                  | 0        | Asia/Shanghai    | Asia/Shanghai | REPEATABLE-READ       | 31536000     |
+--------------------------+----------------------+--------------------------+-----------------------+----------------------+------------------+----------------------+--------------+---------------------+---------+------------------------+--------------------+-------------------+--------------------+----------+------------------+---------------+-----------------------+--------------+";
        test(query, expected);

        let query = "show variables";
        let expected = "\
+---------------+-------+
| Variable_name | Value |
+---------------+-------+
|               |       |
+---------------+-------+";
        test(query, expected);

        let query = "show variables like 'lower_case_table_names'";
        let expected = "\
+------------------------+-------+
| Variable_name          | Value |
+------------------------+-------+
| lower_case_table_names | 0     |
+------------------------+-------+";
        test(query, expected);

        let query = "SELECT TIMEDIFF(NOW(), UTC_TIMESTAMP())";
        let expected = "\
+----------------------------------+
| TIMEDIFF(NOW(), UTC_TIMESTAMP()) |
+----------------------------------+
| 00:00:00                         |
+----------------------------------+";
        test(query, expected);
    }

    #[test]
    fn test_show_warnings() {
        // Test SHOW WARNINGS with no warnings
        let session = Arc::new(Session::new(None, Channel::Mysql, Default::default(), 0));
        let output = check("SHOW WARNINGS", QueryContext::arc(), session.clone());
        match output.unwrap().data {
            OutputData::RecordBatches(r) => {
                assert_eq!(r.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
            }
            _ => unreachable!(),
        }

        // Test SHOW WARNINGS with a single warning
        session.add_warning("Test warning message".to_string());
        let output = check("SHOW WARNINGS", QueryContext::arc(), session.clone());
        match output.unwrap().data {
            OutputData::RecordBatches(r) => {
                let expected = "\
+---------+------+----------------------+
| Level   | Code | Message              |
+---------+------+----------------------+
| Warning | 1000 | Test warning message |
+---------+------+----------------------+";
                assert_eq!(&r.pretty_print().unwrap(), expected);
            }
            _ => unreachable!(),
        }

        // Test SHOW WARNINGS with multiple warnings
        session.clear_warnings();
        session.add_warning("First warning".to_string());
        session.add_warning("Second warning".to_string());
        let output = check("SHOW WARNINGS", QueryContext::arc(), session.clone());
        match output.unwrap().data {
            OutputData::RecordBatches(r) => {
                let expected = "\
+---------+------+----------------+
| Level   | Code | Message        |
+---------+------+----------------+
| Warning | 1000 | First warning  |
| Warning | 1000 | Second warning |
+---------+------+----------------+";
                assert_eq!(&r.pretty_print().unwrap(), expected);
            }
            _ => unreachable!(),
        }

        // Test case insensitivity
        let output = check("show warnings", QueryContext::arc(), session.clone());
        assert!(output.is_some());

        // Test with DBeaver-style comment prefix
        let output = check(
            "/* ApplicationName=DBeaver */SHOW WARNINGS",
            QueryContext::arc(),
            session.clone(),
        );
        assert!(output.is_some());
    }

    #[test]
    fn test_strip_leading_comments() {
        assert_eq!(strip_leading_comments("SELECT 1"), "SELECT 1");
        assert_eq!(strip_leading_comments("  \n\tSELECT 1"), "SELECT 1");
        assert_eq!(
            strip_leading_comments("/* ApplicationName=DataGrip 2026.2.5 */ COMMIT"),
            "COMMIT"
        );
        assert_eq!(strip_leading_comments("/* a */ /* b */COMMIT"), "COMMIT");
        assert_eq!(strip_leading_comments("-- a comment\nCOMMIT"), "COMMIT");
        assert_eq!(strip_leading_comments("# a comment\nCOMMIT"), "COMMIT");
        // `--` without trailing whitespace is not a comment.
        assert_eq!(strip_leading_comments("--x\nCOMMIT"), "--x\nCOMMIT");
        // Nothing left to match against.
        assert_eq!(strip_leading_comments("/* unterminated"), "");
        assert_eq!(strip_leading_comments("-- trailing"), "");
        // Comments inside the statement are left alone; only the prefix is stripped.
        assert_eq!(
            strip_leading_comments("/* a */SELECT /* b */ 1"),
            "SELECT /* b */ 1"
        );
    }

    /// JDBC clients prefix every statement with a comment. Those statements must still reach
    /// the federated handling, and the ones MySQL answers with a result set must keep doing so.
    #[test]
    fn test_check_comment_prefixed() {
        let session = Arc::new(Session::new(None, Channel::Mysql, Default::default(), 0));
        let prefix = "/* ApplicationName=DataGrip 2026.2.5 */ ";

        for query in [
            "SET TRANSACTION READ WRITE",
            "SET SESSION TRANSACTION READ ONLY",
            "SET NAMES utf8mb4",
            "BEGIN",
            "START TRANSACTION",
            "COMMIT",
            "ROLLBACK",
        ] {
            let output = check(
                &format!("{prefix}{query}"),
                QueryContext::arc(),
                session.clone(),
            );
            let OutputData::RecordBatches(batches) = output
                .unwrap_or_else(|| panic!("{query} was not absorbed"))
                .data
            else {
                unreachable!()
            };
            assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
        }

        // DataGrip reads the scheduler status through these two. A column-less output is written
        // as an OK packet, which the JDBC driver reports as "statement has not returned cursor".
        for query in [
            "SELECT @@GLOBAL.event_scheduler",
            "SHOW GLOBAL VARIABLES LIKE 'event_scheduler'",
        ] {
            let output = check(
                &format!("{prefix}{query}"),
                QueryContext::arc(),
                session.clone(),
            );
            let OutputData::RecordBatches(batches) = output
                .unwrap_or_else(|| panic!("{query} was not absorbed"))
                .data
            else {
                unreachable!()
            };
            assert!(!batches.schema().column_schemas().is_empty(), "{query}");
        }
    }
}
