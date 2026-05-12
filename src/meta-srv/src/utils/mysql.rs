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

use common_meta::election::ElectionRef;
use common_meta::election::rds::mysql::{ElectionMysqlClient, MySqlElection};
use common_meta::kv_backend::KvBackendRef;
use common_meta::kv_backend::rds::MySqlStore;
use common_telemetry::info;
use servers::tls::{TlsMode, TlsOption};
use snafu::{OptionExt, ResultExt};
use sqlx::mysql::{MySqlConnectOptions, MySqlPool, MySqlSslMode};

use crate::error::{self, Result};

async fn setup_mysql_options(
    store_addrs: &[String],
    tls_config: Option<&TlsOption>,
) -> Result<MySqlConnectOptions> {
    let mysql_url = store_addrs.first().context(error::InvalidArgumentsSnafu {
        err_msg: "empty store addrs",
    })?;
    // Avoid `SET` commands in sqlx
    let opts: MySqlConnectOptions = mysql_url
        .parse()
        .context(error::ParseMySqlUrlSnafu { mysql_url })?;
    let mut opts = opts
        .no_engine_substitution(false)
        .pipes_as_concat(false)
        .timezone(None)
        .set_names(false);

    let Some(tls_config) = tls_config else {
        return Ok(opts);
    };

    match tls_config.mode {
        TlsMode::Disable => return Ok(opts),
        TlsMode::Prefer => {
            opts = opts.ssl_mode(MySqlSslMode::Preferred);
        }
        TlsMode::Require => {
            opts = opts.ssl_mode(MySqlSslMode::Required);
        }
        TlsMode::VerifyCa => {
            opts = opts.ssl_mode(MySqlSslMode::VerifyCa);
            opts = opts.ssl_ca(&tls_config.ca_cert_path);
        }
        TlsMode::VerifyFull => {
            opts = opts.ssl_mode(MySqlSslMode::VerifyIdentity);
            opts = opts.ssl_ca(&tls_config.ca_cert_path);
        }
    }
    info!(
        "Setting up MySQL options with TLS mode: {:?}",
        tls_config.mode
    );

    if !tls_config.cert_path.is_empty() && !tls_config.key_path.is_empty() {
        info!("Loading client certificate for mutual TLS");
        opts = opts.ssl_client_cert(&tls_config.cert_path);
        opts = opts.ssl_client_key(&tls_config.key_path);
    }

    Ok(opts)
}

/// Creates a MySQL pool.
pub async fn create_mysql_pool(
    store_addrs: &[String],
    tls_config: Option<&TlsOption>,
) -> Result<MySqlPool> {
    let opts = setup_mysql_options(store_addrs, tls_config).await?;
    let pool = MySqlPool::connect_with(opts)
        .await
        .context(error::CreateMySqlPoolSnafu)?;

    Ok(pool)
}

/// Builds a MySQL-backed metadata [`KvBackendRef`].
///
/// * `store_addrs` - MySQL connection URLs; only the first address is used.
/// * `tls_config` - optional TLS settings for the MySQL connection.
/// * `table_name` - metadata KV table name.
/// * `max_txn_ops` - maximum operations allowed in one metadata transaction.
pub async fn build_mysql_kv_backend(
    store_addrs: &[String],
    tls_config: Option<&TlsOption>,
    table_name: &str,
    max_txn_ops: usize,
) -> Result<KvBackendRef> {
    let pool = create_mysql_pool(store_addrs, tls_config).await?;
    MySqlStore::with_mysql_pool(pool, table_name, max_txn_ops)
        .await
        .context(error::KvBackendSnafu)
}

/// Builds a MySQL-backed election implementation.
///
/// * `store_addrs` - MySQL connection URLs; only the first address is used.
/// * `tls_config` - optional TLS settings for the MySQL connection.
/// * `leader_value` - advertised address of this election candidate.
/// * `store_key_prefix` - prefix for election and candidate keys.
/// * `candidate_lease_ttl` - TTL for registered candidate metadata.
/// * `meta_lease_ttl` - TTL for the elected leader metadata.
/// * `election_table_name` - dedicated table used for election locking and records.
/// * `innodb_lock_wait_timeout` - session lock wait timeout for election transactions.
#[allow(clippy::too_many_arguments)]
pub async fn build_mysql_election(
    store_addrs: &[String],
    tls_config: Option<&TlsOption>,
    leader_value: String,
    store_key_prefix: String,
    candidate_lease_ttl: std::time::Duration,
    meta_lease_ttl: std::time::Duration,
    election_table_name: &str,
    innodb_lock_wait_timeout: std::time::Duration,
) -> Result<ElectionRef> {
    let pool = create_mysql_pool(store_addrs, tls_config).await?;
    let election_client = ElectionMysqlClient::new(
        pool,
        meta_lease_ttl,
        meta_lease_ttl,
        innodb_lock_wait_timeout,
        meta_lease_ttl,
        election_table_name,
    );
    MySqlElection::with_mysql_client(
        leader_value,
        election_client,
        store_key_prefix,
        candidate_lease_ttl,
        meta_lease_ttl,
        election_table_name,
    )
    .await
    .context(error::KvBackendSnafu)
}
