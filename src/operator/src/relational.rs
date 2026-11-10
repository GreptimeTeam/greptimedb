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

//! Schema inference for the relational (external table) engine: connects to a
//! remote MySQL database and maps its column types to GreptimeDB
//! [`ConcreteDataType`]s at `CREATE EXTERNAL TABLE` time.
//!
//! Types that cannot be mapped faithfully (e.g. `BIT`, spatial types, or a
//! `DECIMAL` wider than 38 digits) are rejected rather than silently degraded.

use std::collections::HashSet;
use std::net::{IpAddr, ToSocketAddrs};
use std::time::Duration;

use common_error::ext::{BoxedError, PlainError};
use common_error::status_code::StatusCode;
use datatypes::prelude::ConcreteDataType;
use mysql_async::prelude::Queryable;
use snafu::{ResultExt, ensure};
use table::requests::redact_connection;
use url::Url;

use crate::error::{
    ConnectRemoteSnafu, FetchMetadataSnafu, InvalidRemoteTableSnafu, MissingRemoteSchemaSnafu,
    NotSupportedSnafu, RemoteTableNotFoundSnafu, Result,
};

/// Connect + handshake timeout for `CREATE EXTERNAL TABLE`; a silently
/// dropping host would otherwise hang the DDL until the OS timeout.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const QUERY_TIMEOUT: Duration = Duration::from_secs(10);

/// A remote column already mapped to a GreptimeDB [`ConcreteDataType`].
#[derive(Debug, Clone, PartialEq)]
pub struct RelationalColumn {
    pub name: String,
    pub data_type: ConcreteDataType,
}

/// A MySQL column type as reported by `INFORMATION_SCHEMA.COLUMNS`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MySqlColumnType {
    /// `DATA_TYPE` in lowercase, e.g. `tinyint`, `bigint`, `decimal`, `varchar`.
    pub data_type: String,
    /// Whether `UNSIGNED` is present (from `COLUMN_TYPE`, e.g. `bigint unsigned`).
    pub unsigned: bool,
    /// `NUMERIC_PRECISION` for `decimal` / `numeric`.
    pub precision: Option<u64>,
    /// `NUMERIC_SCALE` for `decimal` / `numeric`.
    pub scale: Option<i64>,
    /// `DATETIME_PRECISION` for `time` / `datetime` / `timestamp`.
    pub fsp: Option<u8>,
}

/// A remote column read from `INFORMATION_SCHEMA.COLUMNS`, before its type is
/// mapped to a GreptimeDB [`ConcreteDataType`].
#[derive(Debug, Clone, PartialEq)]
pub struct MySqlColumn {
    pub name: String,
    pub column_type: MySqlColumnType,
}

#[cfg(test)]
impl MySqlColumnType {
    pub fn new(data_type: impl Into<String>) -> Self {
        Self {
            data_type: data_type.into(),
            unsigned: false,
            precision: None,
            scale: None,
            fsp: None,
        }
    }

    pub fn with_unsigned(mut self, unsigned: bool) -> Self {
        self.unsigned = unsigned;
        self
    }

    pub fn with_decimal(mut self, precision: u64, scale: i64) -> Self {
        self.precision = Some(precision);
        self.scale = Some(scale);
        self
    }

    pub fn with_fsp(mut self, fsp: u8) -> Self {
        self.fsp = Some(fsp);
        self
    }
}

/// Maps a MySQL column type to a GreptimeDB [`ConcreteDataType`].
pub fn map_mysql_column_type(col: &MySqlColumnType) -> Result<ConcreteDataType> {
    let unsigned = col.unsigned;
    match col.data_type.as_str() {
        "tinyint" => Ok(if unsigned {
            ConcreteDataType::uint8_datatype()
        } else {
            ConcreteDataType::int8_datatype()
        }),
        "smallint" => Ok(if unsigned {
            ConcreteDataType::uint16_datatype()
        } else {
            ConcreteDataType::int16_datatype()
        }),
        "mediumint" => Ok(if unsigned {
            ConcreteDataType::uint32_datatype()
        } else {
            ConcreteDataType::int32_datatype()
        }),
        "int" | "integer" => Ok(if unsigned {
            ConcreteDataType::uint32_datatype()
        } else {
            ConcreteDataType::int32_datatype()
        }),
        "bigint" => Ok(if unsigned {
            ConcreteDataType::uint64_datatype()
        } else {
            ConcreteDataType::int64_datatype()
        }),
        "float" => Ok(ConcreteDataType::float32_datatype()),
        // `REAL` is an alias for `DOUBLE` unless the `REAL_AS_FLOAT` SQL mode
        // is enabled, in which case it means `FLOAT` (4 bytes). We always map
        // it to float64 — a harmless widening under that mode (no data lost).
        "double" | "real" => Ok(ConcreteDataType::float64_datatype()),
        "decimal" | "numeric" => map_decimal(col.precision, col.scale),
        "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext" | "enum" | "set" => {
            Ok(ConcreteDataType::string_datatype())
        }
        "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" => {
            Ok(ConcreteDataType::binary_datatype())
        }
        "date" => Ok(ConcreteDataType::date_datatype()),
        "time" => Ok(map_time(col.fsp)),
        "datetime" | "timestamp" => Ok(map_timestamp(col.fsp)),
        "year" => Ok(ConcreteDataType::int16_datatype()),
        "json" => Ok(ConcreteDataType::json_datatype()),
        other => unsupported(other),
    }
}

fn map_decimal(precision: Option<u64>, scale: Option<i64>) -> Result<ConcreteDataType> {
    // MySQL `DECIMAL` defaults to (10, 0).
    let precision = precision.unwrap_or(10);
    let scale = scale.unwrap_or(0);
    if precision > 38 {
        return unsupported(format!("`DECIMAL({precision}, {scale})` (precision > 38)"));
    }
    if scale < 0 || scale as u64 > precision {
        return unsupported(format!("`DECIMAL({precision}, {scale})` (invalid scale)"));
    }
    Ok(ConcreteDataType::decimal128_datatype(
        precision as u8,
        scale as i8,
    ))
}

fn map_time(fsp: Option<u8>) -> ConcreteDataType {
    // NOTE: MySQL `TIME` ranges over `-838:59:59 ..= 838:59:59` (negative and
    // >24h values are legal), but GreptimeDB's `Time` is a time within a single
    // day. Such out-of-range values will fail or be distorted on read; callers
    // whose tables contain them should treat the column as unsupported.
    //
    // MySQL `TIME`/`DATETIME`/`TIMESTAMP` default to `fsp = 0`.
    match fsp.unwrap_or(0) {
        0 => ConcreteDataType::time_second_datatype(),
        1..=3 => ConcreteDataType::time_millisecond_datatype(),
        _ => ConcreteDataType::time_microsecond_datatype(),
    }
}

fn map_timestamp(fsp: Option<u8>) -> ConcreteDataType {
    // NOTE: MySQL `DATETIME` is a literal wall-clock value while `TIMESTAMP` is
    // normalized to UTC on the server (and converted back on read using the
    // session `time_zone`). Both are mapped to GreptimeDB's timezone-less
    // `Timestamp`; callers that need to distinguish them should consult the
    // original `DATA_TYPE`.
    match fsp.unwrap_or(0) {
        0 => ConcreteDataType::timestamp_second_datatype(),
        1..=3 => ConcreteDataType::timestamp_millisecond_datatype(),
        _ => ConcreteDataType::timestamp_microsecond_datatype(),
    }
}

fn unsupported(what: impl std::fmt::Display) -> Result<ConcreteDataType> {
    NotSupportedSnafu {
        feat: format!("map MySQL type {what}"),
    }
    .fail()
}

/// Parses `host` as a literal IP address, stripping the URL-level brackets that
/// wrap an IPv6 literal (e.g. `[::1]` → `::1`). Returns `None` when `host` is a
/// hostname rather than a literal.
fn parse_host_literal(host: &str) -> Option<IpAddr> {
    let stripped = host.trim_matches(|c| c == '[' || c == ']');
    stripped.parse::<IpAddr>().ok()
}

/// Resolves `host` to the full list of IP addresses the TCP dial should try, so
/// a host backed by several A/AAAA records can fall back when one is unreachable.
///
/// A literal IP address is returned as a single-element list without a DNS
/// lookup; otherwise the hostname is resolved to all of its addresses.
async fn resolve_host_ips(connection: &str, host: &str) -> Result<Vec<IpAddr>> {
    if let Some(ip) = parse_host_literal(host) {
        return Ok(vec![ip]);
    }

    let host_owned = host.to_owned();
    let addrs = tokio::time::timeout(
        CONNECT_TIMEOUT,
        tokio::task::spawn_blocking(move || {
            (host_owned.as_str(), 0)
                .to_socket_addrs()
                .map(|it| it.collect::<Vec<_>>())
        }),
    )
    .await
    .map_err(|_| box_timeout_error("resolving the remote host", CONNECT_TIMEOUT))
    .and_then(|res| res.map_err(|e| box_foreign_error(e, connection)))
    .and_then(|res| res.map_err(|e| box_foreign_error(e, connection)))
    .with_context(|_| ConnectRemoteSnafu {
        connection: redact_connection(connection),
    })?;

    let ips: Vec<IpAddr> = addrs.into_iter().map(|addr| addr.ip()).collect();
    if ips.is_empty() {
        return Err(box_foreign_error(
            std::io::Error::new(
                std::io::ErrorKind::AddrNotAvailable,
                format!("the host `{host}` resolved to no IP addresses"),
            ),
            connection,
        ))
        .context(ConnectRemoteSnafu {
            connection: redact_connection(connection),
        });
    }
    Ok(ips)
}

/// A raw `INFORMATION_SCHEMA.COLUMNS` row decoded by the driver:
/// (`name`, `data_type`, `column_type`, `precision`, `scale`, `fsp`).
type RawColumnRow = (
    String,
    String,
    String,
    Option<i64>,
    Option<i64>,
    Option<i64>,
);

/// Connects to the remote MySQL instance and reads the column list of
/// `remote_table` from `INFORMATION_SCHEMA.COLUMNS`. Each column keeps its
/// driver-reported type, so callers decide which columns to map to a
/// GreptimeDB [`ConcreteDataType`].
pub async fn fetch_mysql_columns(connection: &str, remote_table: &str) -> Result<Vec<MySqlColumn>> {
    let url = Url::parse(connection)
        .map_err(|e| box_foreign_error(e, connection))
        .with_context(|_| ConnectRemoteSnafu {
            connection: redact_connection(connection),
        })?;
    let host = url.host_str().unwrap_or_default();
    let ips = resolve_host_ips(connection, host).await?;

    // Keep the original hostname in the URL: TLS uses it for SNI and
    // certificate (SAN) validation, so it must not be replaced by an IP. The
    // resolved IPs are handed to the TCP layer separately via `resolved_ips`.
    let opts = mysql_async::Opts::from_url(url.as_str())
        .map_err(|e| box_foreign_error(e, connection))
        .with_context(|_| ConnectRemoteSnafu {
            connection: redact_connection(connection),
        })?;
    let opts =
        mysql_async::Opts::from(mysql_async::OptsBuilder::from_opts(opts).resolved_ips(Some(ips)));
    let default_schema = opts.db_name().map(str::to_string);
    let mut conn = tokio::time::timeout(CONNECT_TIMEOUT, mysql_async::Conn::new(opts))
        .await
        .map_err(|_| box_timeout_error("connecting to the remote database", CONNECT_TIMEOUT))
        .and_then(|res| res.map_err(|e| box_foreign_error(e, connection)))
        .with_context(|_| ConnectRemoteSnafu {
            connection: redact_connection(connection),
        })?;

    let (schema, table) = split_remote_table(remote_table);
    ensure!(
        !table.is_empty(),
        InvalidRemoteTableSnafu {
            remote_table: remote_table.to_string(),
        }
    );
    let schema = schema
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or(default_schema)
        .unwrap_or_default();
    ensure!(
        !schema.is_empty(),
        MissingRemoteSchemaSnafu {
            remote_table: remote_table.to_string(),
        }
    );
    let table = table.to_string();

    let sql = "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, \
               CAST(NUMERIC_PRECISION AS SIGNED), CAST(NUMERIC_SCALE AS SIGNED), \
               CAST(DATETIME_PRECISION AS SIGNED) \
               FROM INFORMATION_SCHEMA.COLUMNS \
               WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION";

    let rows: Vec<RawColumnRow> =
        tokio::time::timeout(QUERY_TIMEOUT, conn.exec(sql, (schema, table)))
            .await
            .map_err(|_| {
                box_timeout_error(
                    "fetching column metadata from the remote database",
                    QUERY_TIMEOUT,
                )
            })
            .and_then(|res| res.map_err(|e| box_foreign_error(e, connection)))
            .context(FetchMetadataSnafu {
                remote_table: remote_table.to_string(),
            })?;
    let _ = conn.disconnect().await;

    ensure!(
        !rows.is_empty(),
        RemoteTableNotFoundSnafu {
            table: remote_table.to_string(),
        }
    );

    let mut columns = Vec::with_capacity(rows.len());
    for (name, data_type, column_type, precision, scale, fsp) in rows {
        let column_type = MySqlColumnType {
            data_type: data_type.to_ascii_lowercase(),
            unsigned: column_type.to_ascii_lowercase().contains("unsigned"),
            precision: precision.map(|v| v as u64),
            scale,
            fsp: fsp.map(|v| v as u8),
        };
        columns.push(MySqlColumn { name, column_type });
    }

    Ok(columns)
}

/// Maps raw MySQL columns to GreptimeDB [`ConcreteDataType`]s via
/// [`map_mysql_column_type`].
///
/// `selected` controls which columns are mapped:
/// - `Some(names)`: map only the named columns, ignoring the rest. This is the
///   declared form of `CREATE EXTERNAL TABLE`, where the user lists a subset of
///   the remote table's columns.
/// - `None`: map every column, preserving their remote order. This is the
///   inferred form, which exposes the whole remote table.
pub fn map_mysql_columns(
    raw: &[MySqlColumn],
    selected: Option<&HashSet<&str>>,
) -> Result<Vec<RelationalColumn>> {
    raw.iter()
        .filter(|column| selected.map_or(true, |names| names.contains(column.name.as_str())))
        .map(|column| {
            Ok(RelationalColumn {
                name: column.name.clone(),
                data_type: map_mysql_column_type(&column.column_type)?,
            })
        })
        .collect()
}

/// Boxes a foreign driver error into a [`BoxedError`]. The message is scrubbed:
/// if the driver echoes the connection DSN (which carries the password), it is
/// replaced by its redacted form so credentials never leak.
fn box_foreign_error(
    e: impl std::error::Error + Send + Sync + 'static,
    connection: &str,
) -> BoxedError {
    let mut message = e.to_string();
    if message.contains(connection) {
        message = message.replace(connection, &redact_connection(connection));
    }
    BoxedError::new(PlainError::new(message, StatusCode::Internal))
}

fn box_timeout_error(what: &str, timeout: Duration) -> BoxedError {
    BoxedError::new(PlainError::new(
        format!("{what} timed out after {timeout:?}"),
        StatusCode::DeadlineExceeded,
    ))
}

/// Splits `remote_table` into an optional schema qualifier and the bare table
/// name: `"schema.table"` → `(Some("schema"), "table")`, `"table"` → `(None, "table")`.
fn split_remote_table(remote_table: &str) -> (Option<&str>, &str) {
    match remote_table.rfind('.') {
        Some(i) => (Some(&remote_table[..i]), &remote_table[i + 1..]),
        None => (None, remote_table),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_remote_table() {
        assert_eq!(split_remote_table("table"), (None, "table"));
        assert_eq!(
            split_remote_table("schema.table"),
            (Some("schema"), "table")
        );
        // Leading dot yields an empty schema; callers must treat it as "no schema".
        assert_eq!(split_remote_table(".table"), (Some(""), "table"));
        // Trailing dot yields an empty table; callers must reject it.
        assert_eq!(split_remote_table("db."), (Some("db"), ""));
        // `rfind` splits on the last dot.
        assert_eq!(split_remote_table("a.b.c"), (Some("a.b"), "c"));
    }

    #[test]
    fn test_parse_host_literal() {
        // IPv6 literals arrive from URL parsing wrapped in brackets; the brackets
        // are URL-level notation and must be stripped before parsing.
        assert_eq!(
            parse_host_literal("[::1]"),
            Some(IpAddr::V6(std::net::Ipv6Addr::LOCALHOST))
        );
        assert_eq!(
            parse_host_literal("::1"),
            Some(IpAddr::V6(std::net::Ipv6Addr::LOCALHOST))
        );
        assert_eq!(
            parse_host_literal("127.0.0.1"),
            Some(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST))
        );
        // Hostnames are left to DNS resolution.
        assert_eq!(parse_host_literal("db.example.com"), None);
        assert_eq!(parse_host_literal(""), None);
    }

    #[test]
    fn test_integer_types() {
        assert_eq!(
            map_mysql_column_type(&MySqlColumnType::new("tinyint")).unwrap(),
            ConcreteDataType::int8_datatype()
        );
        assert_eq!(
            map_mysql_column_type(&MySqlColumnType::new("tinyint").with_unsigned(true)).unwrap(),
            ConcreteDataType::uint8_datatype()
        );
        assert_eq!(
            map_mysql_column_type(&MySqlColumnType::new("mediumint")).unwrap(),
            ConcreteDataType::int32_datatype()
        );
        assert_eq!(
            map_mysql_column_type(&MySqlColumnType::new("bigint")).unwrap(),
            ConcreteDataType::int64_datatype()
        );
        assert_eq!(
            map_mysql_column_type(&MySqlColumnType::new("bigint").with_unsigned(true)).unwrap(),
            ConcreteDataType::uint64_datatype()
        );
    }

    #[test]
    fn test_float_and_decimal() {
        assert_eq!(
            map_mysql_column_type(&MySqlColumnType::new("float")).unwrap(),
            ConcreteDataType::float32_datatype()
        );
        assert_eq!(
            map_mysql_column_type(&MySqlColumnType::new("double")).unwrap(),
            ConcreteDataType::float64_datatype()
        );
        assert_eq!(
            map_mysql_column_type(&MySqlColumnType::new("real")).unwrap(),
            ConcreteDataType::float64_datatype()
        );
        assert_eq!(
            map_mysql_column_type(&MySqlColumnType::new("decimal").with_decimal(12, 2)).unwrap(),
            ConcreteDataType::decimal128_datatype(12, 2)
        );
        // Precision > 38 is rejected rather than silently degraded.
        assert!(
            map_mysql_column_type(&MySqlColumnType::new("decimal").with_decimal(65, 30)).is_err()
        );
    }

    #[test]
    fn test_string_and_binary() {
        assert_eq!(
            map_mysql_column_type(&MySqlColumnType::new("varchar")).unwrap(),
            ConcreteDataType::string_datatype()
        );
        assert_eq!(
            map_mysql_column_type(&MySqlColumnType::new("char")).unwrap(),
            ConcreteDataType::string_datatype()
        );
        assert_eq!(
            map_mysql_column_type(&MySqlColumnType::new("text")).unwrap(),
            ConcreteDataType::string_datatype()
        );
        assert_eq!(
            map_mysql_column_type(&MySqlColumnType::new("enum")).unwrap(),
            ConcreteDataType::string_datatype()
        );
        assert_eq!(
            map_mysql_column_type(&MySqlColumnType::new("set")).unwrap(),
            ConcreteDataType::string_datatype()
        );
        assert_eq!(
            map_mysql_column_type(&MySqlColumnType::new("blob")).unwrap(),
            ConcreteDataType::binary_datatype()
        );
        assert_eq!(
            map_mysql_column_type(&MySqlColumnType::new("binary")).unwrap(),
            ConcreteDataType::binary_datatype()
        );
    }

    #[test]
    fn test_datetime_types() {
        assert_eq!(
            map_mysql_column_type(&MySqlColumnType::new("date")).unwrap(),
            ConcreteDataType::date_datatype()
        );
        assert_eq!(
            map_mysql_column_type(&MySqlColumnType::new("datetime")).unwrap(),
            ConcreteDataType::timestamp_second_datatype()
        );
        assert_eq!(
            map_mysql_column_type(&MySqlColumnType::new("datetime").with_fsp(3)).unwrap(),
            ConcreteDataType::timestamp_millisecond_datatype()
        );
        assert_eq!(
            map_mysql_column_type(&MySqlColumnType::new("datetime").with_fsp(6)).unwrap(),
            ConcreteDataType::timestamp_microsecond_datatype()
        );
        assert_eq!(
            map_mysql_column_type(&MySqlColumnType::new("timestamp")).unwrap(),
            ConcreteDataType::timestamp_second_datatype()
        );
        assert_eq!(
            map_mysql_column_type(&MySqlColumnType::new("time")).unwrap(),
            ConcreteDataType::time_second_datatype()
        );
        assert_eq!(
            map_mysql_column_type(&MySqlColumnType::new("year")).unwrap(),
            ConcreteDataType::int16_datatype()
        );
    }

    #[test]
    fn test_json_and_rejected() {
        assert_eq!(
            map_mysql_column_type(&MySqlColumnType::new("json")).unwrap(),
            ConcreteDataType::json_datatype()
        );
        assert!(map_mysql_column_type(&MySqlColumnType::new("bit")).is_err());
        assert!(map_mysql_column_type(&MySqlColumnType::new("geometry")).is_err());
        assert!(map_mysql_column_type(&MySqlColumnType::new("point")).is_err());
    }

    #[test]
    fn test_map_mysql_columns_selected_subset() {
        let raw = vec![
            MySqlColumn {
                name: "id".into(),
                column_type: MySqlColumnType::new("int"),
            },
            MySqlColumn {
                name: "ts".into(),
                column_type: MySqlColumnType::new("timestamp"),
            },
            MySqlColumn {
                name: "shape".into(),
                column_type: MySqlColumnType::new("geometry"),
            },
        ];

        // Declared form: mapping a subset skips the unselected, unsupported
        // `shape` column.
        let selected: std::collections::HashSet<&str> = ["id", "ts"].into_iter().collect();
        let mapped = map_mysql_columns(&raw, Some(&selected)).unwrap();
        assert_eq!(2, mapped.len());
        let names: Vec<&str> = mapped.iter().map(|column| column.name.as_str()).collect();
        assert_eq!(names, ["id", "ts"]);

        // Declared form: selecting the unsupported column maps it and fails.
        let selected: std::collections::HashSet<&str> = ["id", "shape"].into_iter().collect();
        assert!(map_mysql_columns(&raw, Some(&selected)).is_err());

        // Inferred form: every column is mapped, so the unsupported column
        // still fails.
        assert!(map_mysql_columns(&raw, None).is_err());
    }
}
