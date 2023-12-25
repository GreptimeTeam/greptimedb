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
use std::env;
use std::sync::Arc;

use common_query::Output;
use common_recordbatch::RecordBatches;
use common_time::timezone::system_timezone_name;
use common_time::Timezone;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema};
use datatypes::vectors::StringVector;
use once_cell::sync::Lazy;
use regex::bytes::RegexSet;
use regex::Regex;
use session::context::QueryContextRef;
use session::SessionRef;

static SELECT_VAR_PATTERN: Lazy<Regex> = Lazy::new(|| Regex::new("(?i)^(SELECT @@(.*))").unwrap());
static MYSQL_CONN_JAVA_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new("(?i)^(/\\* mysql-connector-j(.*))").unwrap());
static SHOW_LOWER_CASE_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new("(?i)^(SHOW VARIABLES LIKE 'lower_case_table_names'(.*))").unwrap());
static SHOW_COLLATION_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new("(?i)^(show collation where(.*))").unwrap());
static SHOW_VARIABLES_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new("(?i)^(SHOW VARIABLES(.*))").unwrap());

static SELECT_VERSION_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)^(SELECT VERSION\(\s*\))").unwrap());
static SELECT_DATABASE_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)^(SELECT DATABASE\(\s*\))").unwrap());

// SELECT TIMEDIFF(NOW(), UTC_TIMESTAMP());
static SELECT_TIME_DIFF_FUNC_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new("(?i)^(SELECT TIMEDIFF\\(NOW\\(\\), UTC_TIMESTAMP\\(\\)\\))").unwrap());

// sqlalchemy < 1.4.30
static SHOW_SQL_MODE_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new("(?i)^(SHOW VARIABLES LIKE 'sql_mode'(.*))").unwrap());

// Timezone settings
static SET_TIME_ZONE_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)^SET TIME_ZONE\s*=\s*'(\S+)'").unwrap());

static OTHER_NOT_SUPPORTED_STMT: Lazy<RegexSet> = Lazy::new(|| {
    RegexSet::new([
        // Txn.
        "(?i)^(ROLLBACK(.*))",
        "(?i)^(COMMIT(.*))",
        "(?i)^(START(.*))",

        // Set.
        "(?i)^(SET NAMES(.*))",
        "(?i)^(SET character_set_results(.*))",
        "(?i)^(SET net_write_timeout(.*))",
        "(?i)^(SET FOREIGN_KEY_CHECKS(.*))",
        "(?i)^(SET AUTOCOMMIT(.*))",
        "(?i)^(SET SQL_LOG_BIN(.*))",
        "(?i)^(SET sql_mode(.*))",
        "(?i)^(SET SQL_SELECT_LIMIT(.*))",
        "(?i)^(SET @@(.*))",

        "(?i)^(SHOW COLLATION)",
        "(?i)^(SHOW CHARSET)",

        // mysqlclient.
        "(?i)^(SELECT \\$\\$)",

        // mysqldump.
        "(?i)^(SET SESSION(.*))",
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
        "(?i)^(SHOW WARNINGS)",
        "(?i)^(/\\* ApplicationName=(.*)SHOW WARNINGS)",
        "(?i)^(/\\* ApplicationName=(.*)SHOW PLUGINS)",
        "(?i)^(/\\* ApplicationName=(.*)SHOW COLLATION)",
        "(?i)^(/\\* ApplicationName=(.*)SHOW CHARSET)",
        "(?i)^(/\\* ApplicationName=(.*)SHOW ENGINES)",
        "(?i)^(/\\* ApplicationName=(.*)SELECT @@(.*))",
        "(?i)^(/\\* ApplicationName=(.*)SHOW @@(.*))",
        "(?i)^(/\\* ApplicationName=(.*)SET net_write_timeout(.*))",
        "(?i)^(/\\* ApplicationName=(.*)SET SQL_SELECT_LIMIT(.*))",
        "(?i)^(/\\* ApplicationName=(.*)SHOW VARIABLES(.*))",

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
        ("version_comment", "Greptime"),
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
        let var = var.trim_matches(|c| c == ' ' || c == ',');
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
            "time_zone" => query_context.timezone().to_string(),
            "system_time_zone" => system_timezone_name(),
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
                    &format!("@@{}", var_as[0]),
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
    Some(Output::RecordBatches(batches))
}

fn check_select_variable(query: &str, query_context: QueryContextRef) -> Option<Output> {
    if [&SELECT_VAR_PATTERN, &MYSQL_CONN_JAVA_PATTERN]
        .iter()
        .any(|r| r.is_match(query))
    {
        select_variable(query, query_context)
    } else {
        None
    }
}

fn check_show_variables(query: &str) -> Option<Output> {
    let recordbatches = if SHOW_SQL_MODE_PATTERN.is_match(query) {
        Some(show_variables("sql_mode", "ONLY_FULL_GROUP_BY STRICT_TRANS_TABLES NO_ZERO_IN_DATE NO_ZERO_DATE ERROR_FOR_DIVISION_BY_ZERO NO_ENGINE_SUBSTITUTION"))
    } else if SHOW_LOWER_CASE_PATTERN.is_match(query) {
        Some(show_variables("lower_case_table_names", "0"))
    } else if SHOW_COLLATION_PATTERN.is_match(query) || SHOW_VARIABLES_PATTERN.is_match(query) {
        Some(show_variables("", ""))
    } else {
        None
    };
    recordbatches.map(Output::RecordBatches)
}

// TODO(sunng87): extract this to use sqlparser for more variables
fn check_set_variables(query: &str, session: SessionRef) -> Option<Output> {
    if let Some(captures) = SET_TIME_ZONE_PATTERN.captures(query) {
        // get the capture
        let tz = captures.get(1).unwrap();
        if let Ok(timezone) = Timezone::from_tz_string(tz.as_str()) {
            session.set_timezone(timezone);
            return Some(Output::AffectedRows(0));
        }
    }

    None
}

// Check for SET or others query, this is the final check of the federated query.
fn check_others(query: &str, query_ctx: QueryContextRef) -> Option<Output> {
    if OTHER_NOT_SUPPORTED_STMT.is_match(query.as_bytes()) {
        return Some(Output::RecordBatches(RecordBatches::empty()));
    }

    let recordbatches = if SELECT_VERSION_PATTERN.is_match(query) {
        Some(select_function("version()", &get_version()))
    } else if SELECT_DATABASE_PATTERN.is_match(query) {
        let schema = query_ctx.current_schema();
        Some(select_function("database()", schema))
    } else if SELECT_TIME_DIFF_FUNC_PATTERN.is_match(query) {
        Some(select_function(
            "TIMEDIFF(NOW(), UTC_TIMESTAMP())",
            "00:00:00",
        ))
    } else {
        None
    };
    recordbatches.map(Output::RecordBatches)
}

// Check whether the query is a federated or driver setup command,
// and return some faked results if there are any.
pub(crate) fn check(
    query: &str,
    query_ctx: QueryContextRef,
    session: SessionRef,
) -> Option<Output> {
    // INSERT don't need MySQL federated check. We assume the query doesn't contain
    // federated or driver setup command if it starts with a 'INSERT' statement.
    if query.len() > 6 && query[..6].eq_ignore_ascii_case("INSERT") {
        return None;
    }

    // First to check the query is like "select @@variables".
    check_select_variable(query, query_ctx.clone())
        // Then to check "show variables like ...".
        .or_else(|| check_show_variables(query))
        .or_else(|| check_set_variables(query, session.clone()))
        // Last check
        .or_else(|| check_others(query, query_ctx))
}

// get GreptimeDB's version.
fn get_version() -> String {
    format!("{}-greptime", env!("CARGO_PKG_VERSION"))
}
#[cfg(test)]
mod test {

    use common_time::timezone::set_default_timezone;
    use session::context::{Channel, QueryContext};
    use session::Session;

    use super::*;

    #[test]
    fn test_check() {
        let session = Arc::new(Session::new(None, Channel::Mysql));
        let query = "select 1";
        let result = check(query, QueryContext::arc(), session.clone());
        assert!(result.is_none());

        let query = "select versiona";
        let output = check(query, QueryContext::arc(), session.clone());
        assert!(output.is_none());

        fn test(query: &str, expected: &str) {
            let session = Arc::new(Session::new(None, Channel::Mysql));
            let output = check(query, QueryContext::arc(), session.clone());
            match output.unwrap() {
                Output::RecordBatches(r) => {
                    assert_eq!(&r.pretty_print().unwrap(), expected)
                }
                _ => unreachable!(),
            }
        }

        let query = "select version()";
        let version = env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "unknown".to_string());
        let output = check(query, QueryContext::arc(), session.clone());
        match output.unwrap() {
            Output::RecordBatches(r) => {
                assert!(&r
                    .pretty_print()
                    .unwrap()
                    .contains(&format!("{version}-greptime")));
            }
            _ => unreachable!(),
        }

        let query = "SELECT @@version_comment LIMIT 1";
        let expected = "\
+-------------------+
| @@version_comment |
+-------------------+
| Greptime          |
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
+--------------------------+----------------------+--------------------------+-----------------------+----------------------+------------------+----------------------+--------------+---------------------+---------+------------------------+--------------------+-------------------+--------------------+----------+------------------+---------------+-----------------------+---------------+
| auto_increment_increment | character_set_client | character_set_connection | character_set_results | character_set_server | collation_server | collation_connection | init_connect | interactive_timeout | license | lower_case_table_names | max_allowed_packet | net_write_timeout | performance_schema | sql_mode | system_time_zone | time_zone     | transaction_isolation | wait_timeout; |
+--------------------------+----------------------+--------------------------+-----------------------+----------------------+------------------+----------------------+--------------+---------------------+---------+------------------------+--------------------+-------------------+--------------------+----------+------------------+---------------+-----------------------+---------------+
| 0                        | 0                    | 0                        | 0                     | 0                    | 0                | 0                    | 0            | 31536000            | 0       | 0                      | 134217728          | 31536000          | 0                  | 0        | Asia/Shanghai    | Asia/Shanghai | REPEATABLE-READ       | 31536000      |
+--------------------------+----------------------+--------------------------+-----------------------+----------------------+------------------+----------------------+--------------+---------------------+---------+------------------------+--------------------+-------------------+--------------------+----------+------------------+---------------+-----------------------+---------------+";
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

        let query = "show collation";
        let expected = "\
++
++"; // empty
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
    fn test_set_timezone() {
        // test default is UTC when no config in greptimedb
        {
            let session = Arc::new(Session::new(None, Channel::Mysql));
            let query_context = session.new_query_context();
            assert_eq!("UTC", query_context.timezone().to_string());
        }
        set_default_timezone(Some("Asia/Shanghai")).unwrap();
        let session = Arc::new(Session::new(None, Channel::Mysql));
        let query_context = session.new_query_context();
        assert_eq!("Asia/Shanghai", query_context.timezone().to_string());
        let output = check(
            "set time_zone = 'UTC'",
            QueryContext::arc(),
            session.clone(),
        );
        match output.unwrap() {
            Output::AffectedRows(rows) => {
                assert_eq!(rows, 0)
            }
            _ => unreachable!(),
        }
        let query_context = session.new_query_context();
        assert_eq!("UTC", query_context.timezone().to_string());

        let output = check("select @@time_zone", query_context.clone(), session.clone());
        match output.unwrap() {
            Output::RecordBatches(r) => {
                let expected = "\
+-------------+
| @@time_zone |
+-------------+
| UTC         |
+-------------+";
                assert_eq!(r.pretty_print().unwrap(), expected);
            }
            _ => unreachable!(),
        }
    }
}
