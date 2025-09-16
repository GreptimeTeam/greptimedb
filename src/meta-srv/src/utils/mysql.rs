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
