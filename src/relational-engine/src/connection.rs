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

//! Remote connection helpers for the relational engine's scan path.
//!
//! `build_mysql_opts` / `resolve_host_ips` / `box_foreign_error` are moved from
//! `operator/src/relational.rs` verbatim (only the error type changes) so the
//! scan path does not depend on the operator crate. Keep the two copies in
//! sync — a shared crate is tracked in the tracking issue.

use std::net::{IpAddr, ToSocketAddrs};
use std::time::Duration;

use common_error::ext::{BoxedError, PlainError};
use common_error::status_code::StatusCode;
use snafu::{ResultExt, ensure};
use table::requests::redact_connection;

use crate::error::{ConnectRemoteSnafu, InvalidRemoteTableSnafu, MissingRemoteSchemaSnafu, Result};

/// Connect + handshake timeout for the scan; a silently dropping host would
/// otherwise hang the query until the OS timeout.
pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
pub(crate) const QUERY_TIMEOUT: Duration = Duration::from_secs(10);

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

/// Parses the connection DSN and, for TCP connections, resolves the host to
/// candidate IP addresses. When an explicit Unix socket is configured
/// (`?socket=/path`), mysql_async dials the socket path directly, so the host
/// is a placeholder and DNS resolution is skipped entirely.
pub(crate) async fn build_mysql_opts(connection: &str) -> Result<mysql_async::Opts> {
    let opts = mysql_async::Opts::from_url(connection)
        .map_err(|e| box_foreign_error(e, connection))
        .with_context(|_| ConnectRemoteSnafu {
            connection: redact_connection(connection),
        })?;

    // A Unix-socket connection has no TCP host to resolve; an unresolvable
    // placeholder hostname must not prevent connecting via the socket path.
    if opts.socket().is_some() {
        return Ok(opts);
    }

    let host = opts.ip_or_hostname().to_owned();
    let ips = resolve_host_ips(connection, &host).await?;
    // Keep the original hostname in the URL: TLS uses it for SNI and
    // certificate (SAN) validation, so it must not be replaced by an IP. The
    // resolved IPs are handed to the TCP layer separately via `resolved_ips`.
    Ok(mysql_async::Opts::from(
        mysql_async::OptsBuilder::from_opts(opts).resolved_ips(Some(ips)),
    ))
}

/// Boxes a foreign driver error into a [`BoxedError`]. The message is scrubbed:
/// if the driver echoes the connection DSN (which carries the password), it is
/// replaced by its redacted form so credentials never leak.
pub(crate) fn box_foreign_error(
    e: impl std::error::Error + Send + Sync + 'static,
    connection: &str,
) -> BoxedError {
    let mut message = e.to_string();
    if message.contains(connection) {
        message = message.replace(connection, &redact_connection(connection));
    }
    BoxedError::new(PlainError::new(message, StatusCode::Internal))
}

pub(crate) fn box_timeout_error(what: &str, timeout: Duration) -> BoxedError {
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

/// Quotes a MySQL identifier with backticks, escaping embedded backticks by
/// doubling them. MySQL does not use ANSI double quotes (except in
/// `ANSI_QUOTES` mode), so [`datafusion_common::utils::quote_identifier`] must
/// not be used here.
pub(crate) fn quote_ident(ident: &str) -> String {
    format!("`{}`", ident.replace('`', "``"))
}

/// Builds the `SELECT ... FROM ...` statement for a full-table scan.
///
/// Only the remote columns in `select_names` (already in region-schema order,
/// synthetic columns excluded) are selected; no `WHERE`/`LIMIT`/`ORDER BY` is
/// appended (zero pushdown). The table is schema-qualified using the
/// `remote_table` qualifier or, when absent, the connection's default database.
pub(crate) fn build_select_sql(
    remote_table: &str,
    opts: &mysql_async::Opts,
    select_names: &[String],
) -> Result<String> {
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
        .or_else(|| opts.db_name().map(str::to_string))
        .ok_or_else(|| {
            MissingRemoteSchemaSnafu {
                remote_table: remote_table.to_string(),
            }
            .build()
        })?;

    // When the projection references no real remote column (e.g. only the
    // synthetic time index, or an empty `COUNT(*)` projection), still fetch one
    // row per remote row so the count is right; the synthetic columns are filled
    // locally.
    let cols = if select_names.is_empty() {
        "1".to_string()
    } else {
        select_names
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ")
    };
    Ok(format!(
        "SELECT {cols} FROM {}.{}",
        quote_ident(&schema),
        quote_ident(table)
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quote_ident() {
        assert_eq!(quote_ident("plain"), "`plain`");
        assert_eq!(quote_ident("a`b"), "`a``b`");
    }

    #[test]
    fn test_split_remote_table() {
        assert_eq!(split_remote_table("table"), (None, "table"));
        assert_eq!(
            split_remote_table("schema.table"),
            (Some("schema"), "table")
        );
        assert_eq!(split_remote_table("a.b.c"), (Some("a.b"), "c"));
    }

    #[tokio::test]
    async fn test_build_mysql_opts_skips_dns_for_socket() {
        // `unresolvable.invalid` (RFC 2606) has no DNS record; if the socket
        // branch did not short-circuit, `resolve_host_ips` would fail here.
        let opts = build_mysql_opts(
            "mysql://root:secret@unresolvable.invalid/db?socket=%2Ftmp%2Fmysql.sock",
        )
        .await
        .unwrap();
        assert_eq!(opts.socket(), Some("/tmp/mysql.sock"));
        assert!(opts.resolved_ips().is_none());
    }

    #[test]
    fn test_build_select_sql_empty_projection() {
        let opts = mysql_async::Opts::from_url("mysql://user:pw@host:3306/mydb").unwrap();
        let sql = build_select_sql("remote_table", &opts, &[]).unwrap();
        assert_eq!(sql, "SELECT 1 FROM `mydb`.`remote_table`");
    }

    #[test]
    fn test_build_select_sql_quotes_reserved_and_backtick() {
        let opts = mysql_async::Opts::from_url("mysql://user:pw@host:3306/mydb").unwrap();
        let cols = vec!["from".to_string(), "a`b".to_string()];
        let sql = build_select_sql("select", &opts, &cols).unwrap();
        assert_eq!(sql, "SELECT `from`, `a``b` FROM `mydb`.`select`");
    }

    #[test]
    fn test_build_select_sql_schema_qualified() {
        let opts = mysql_async::Opts::from_url("mysql://user:pw@host:3306/mydb").unwrap();
        let cols = vec!["col".to_string()];
        let sql = build_select_sql("schema.table", &opts, &cols).unwrap();
        assert_eq!(sql, "SELECT `col` FROM `schema`.`table`");
    }
}
