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

#[cfg(unix)]
use std::str::FromStr;

use common_error::ext::BoxedError;
use common_meta::election::ElectionRef;
use common_meta::election::rds::postgres::{ElectionPgClient, PgElection};
use common_meta::kv_backend::KvBackendRef;
use common_meta::kv_backend::rds::PgStore;
use common_meta::kv_backend::rds::postgres::{
    TlsMode as PgTlsMode, TlsOption as PgTlsOption, create_postgres_tls_connector,
};
use common_telemetry::warn;
use deadpool_postgres::{Config, Runtime};
use servers::tls::TlsOption;
use snafu::{OptionExt, ResultExt};
use tokio_postgres::NoTls;
#[cfg(unix)]
use tokio_postgres::config::Host;

use crate::error::{self, Result};

/// Converts [`TlsOption`] to [`PgTlsOption`] to avoid circular dependencies
fn convert_tls_option(tls_option: &TlsOption) -> PgTlsOption {
    let mode = match tls_option.mode {
        servers::tls::TlsMode::Disable => PgTlsMode::Disable,
        servers::tls::TlsMode::Prefer => PgTlsMode::Prefer,
        servers::tls::TlsMode::Require => PgTlsMode::Require,
        servers::tls::TlsMode::VerifyCa => PgTlsMode::VerifyCa,
        servers::tls::TlsMode::VerifyFull => PgTlsMode::VerifyFull,
    };

    PgTlsOption {
        mode,
        cert_path: tls_option.cert_path.clone(),
        key_path: tls_option.key_path.clone(),
        ca_cert_path: tls_option.ca_cert_path.clone(),
        watch: tls_option.watch,
    }
}

/// Creates a pool for the Postgres backend with config and optional TLS.
///
/// It only use first store addr to create a pool, and use the given config to create a pool.
pub async fn create_postgres_pool(
    store_addrs: &[String],
    cfg: Option<Config>,
    tls_config: Option<TlsOption>,
) -> Result<deadpool_postgres::Pool> {
    let mut cfg = cfg.unwrap_or_default();
    let postgres_url = store_addrs.first().context(error::InvalidArgumentsSnafu {
        err_msg: "empty store addrs",
    })?;
    cfg.url = Some(postgres_url.clone());

    let is_unix_socket = is_unix_socket_url(postgres_url);
    if is_unix_socket
        && matches!(tls_config.as_ref(), Some(t) if t.mode != servers::tls::TlsMode::Disable)
    {
        warn!(
            "TLS is not supported for Unix domain socket PostgreSQL connections, falling back to NoTls"
        );
    }

    let pool = match tls_config {
        Some(tls_config)
            if tls_config.mode != servers::tls::TlsMode::Disable && !is_unix_socket =>
        {
            let pg_tls_config = convert_tls_option(&tls_config);
            let tls_connector =
                create_postgres_tls_connector(&pg_tls_config).map_err(|e| error::Error::Other {
                    source: BoxedError::new(e),
                    location: snafu::Location::new(file!(), line!(), 0),
                })?;
            cfg.create_pool(Some(Runtime::Tokio1), tls_connector)
                .context(error::CreatePostgresPoolSnafu)?
        }
        _ => cfg
            .create_pool(Some(Runtime::Tokio1), NoTls)
            .context(error::CreatePostgresPoolSnafu)?,
    };

    Ok(pool)
}

#[cfg(unix)]
fn is_unix_socket_url(url: &str) -> bool {
    tokio_postgres::Config::from_str(url)
        .map(|cfg| {
            cfg.get_hosts()
                .iter()
                .any(|host| matches!(host, Host::Unix(_)))
        })
        .unwrap_or(false)
}

#[cfg(not(unix))]
fn is_unix_socket_url(_: &str) -> bool {
    false
}

/// Builds a Postgres-backed metadata [`KvBackendRef`].
///
/// * `store_addrs` - Postgres connection URLs; only the first address is used.
/// * `cfg` - optional deadpool config to customize pool/session behavior.
/// * `tls_config` - optional TLS settings for the Postgres connection.
/// * `schema_name` - optional schema containing the metadata table.
/// * `table_name` - metadata KV table name.
/// * `max_txn_ops` - maximum operations allowed in one metadata transaction.
/// * `auto_create_schema` - whether to create `schema_name` when it is missing.
#[allow(clippy::too_many_arguments)]
pub async fn build_postgres_kv_backend(
    store_addrs: &[String],
    cfg: Option<Config>,
    tls_config: Option<TlsOption>,
    schema_name: Option<&str>,
    table_name: &str,
    max_txn_ops: usize,
    auto_create_schema: bool,
) -> Result<KvBackendRef> {
    let pool = create_postgres_pool(store_addrs, cfg, tls_config).await?;
    PgStore::with_pg_pool(
        pool,
        schema_name,
        table_name,
        max_txn_ops,
        auto_create_schema,
    )
    .await
    .context(error::KvBackendSnafu)
}

/// Builds a Postgres-backed election implementation.
///
/// * `store_addrs` - Postgres connection URLs; only the first address is used.
/// * `cfg` - optional deadpool config to customize pool/session behavior.
/// * `tls_config` - optional TLS settings for the Postgres connection.
/// * `leader_value` - advertised address of this election candidate.
/// * `store_key_prefix` - prefix for election and candidate keys.
/// * `candidate_lease_ttl` - TTL for registered candidate metadata.
/// * `meta_lease_ttl` - TTL for the elected leader metadata.
/// * `schema_name` - optional schema containing the metadata table.
/// * `table_name` - metadata KV table name used for election records.
/// * `lock_id` - Postgres advisory lock id used by the election.
#[allow(clippy::too_many_arguments)]
pub async fn build_postgres_election(
    store_addrs: &[String],
    cfg: Option<Config>,
    tls_config: Option<TlsOption>,
    leader_value: String,
    store_key_prefix: String,
    candidate_lease_ttl: std::time::Duration,
    meta_lease_ttl: std::time::Duration,
    schema_name: Option<&str>,
    table_name: &str,
    lock_id: u64,
) -> Result<ElectionRef> {
    let pool = create_postgres_pool(store_addrs, cfg, tls_config).await?;
    let election_client =
        ElectionPgClient::new(pool, meta_lease_ttl, meta_lease_ttl, meta_lease_ttl)
            .context(error::KvBackendSnafu)?;
    PgElection::with_pg_client(
        leader_value,
        election_client,
        store_key_prefix,
        candidate_lease_ttl,
        meta_lease_ttl,
        schema_name,
        table_name,
        lock_id,
    )
    .await
    .context(error::KvBackendSnafu)
}

#[cfg(test)]
mod tests {
    use super::is_unix_socket_url;

    #[test]
    fn detects_postgres_unix_socket_url() {
        #[cfg(unix)]
        {
            // libpq keyword-value form (issue #7734)
            assert!(is_unix_socket_url(
                "host=/var/run/postgresql dbname=greptime user=greptime password=secret"
            ));
            // standard postgres URL with percent-encoded unix socket directory
            assert!(is_unix_socket_url(
                "postgresql://user:pw@%2Fvar%2Frun%2Fpostgresql/mydb"
            ));
            // postgres URL with socket dir in query param
            assert!(is_unix_socket_url(
                "postgresql:///mydb?host=%2Fvar%2Frun%2Fpostgresql"
            ));
            assert!(is_unix_socket_url(
                "postgresql://user:secret@/mydb?host=%2Fvar%2Frun%2Fpostgresql"
            ));

            // TCP URLs should not be classified as unix socket
            assert!(!is_unix_socket_url(
                "postgresql://user:pw@localhost:5432/mydb"
            ));
            assert!(!is_unix_socket_url("postgresql://user@localhost/db"));
            assert!(!is_unix_socket_url(
                "host=127.0.0.1 port=5432 dbname=greptime user=greptime password=secret"
            ));
        }

        #[cfg(not(unix))]
        {
            assert!(!is_unix_socket_url(
                "host=/var/run/postgresql dbname=greptime user=greptime password=secret"
            ));
            assert!(!is_unix_socket_url(
                "postgresql://user:pw@localhost:5432/mydb"
            ));
        }
    }
}
