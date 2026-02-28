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

use common_error::ext::BoxedError;
use common_meta::kv_backend::rds::postgres::{
    TlsMode as PgTlsMode, TlsOption as PgTlsOption, create_postgres_tls_connector,
    is_pg_unix_socket,
};
use common_telemetry::warn;
use deadpool_postgres::{Config, Runtime};
use servers::tls::{TlsMode, TlsOption};
use snafu::{OptionExt, ResultExt};
use tokio_postgres::NoTls;

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

    let pool = if let Some(tls_config) = tls_config {
        let is_unix = is_pg_unix_socket(postgres_url);

        match tls_config.mode {
            TlsMode::Disable => cfg
                .create_pool(Some(Runtime::Tokio1), NoTls)
                .context(error::CreatePostgresPoolSnafu)?,
            _ if is_unix => {
                // Unix sockets don't support TLS; they are inherently secure
                // via filesystem permissions.
                warn!(
                    "Connecting via Unix socket, TLS mode {:?} is not applicable, skipping TLS",
                    tls_config.mode
                );
                cfg.create_pool(Some(Runtime::Tokio1), NoTls)
                    .context(error::CreatePostgresPoolSnafu)?
            }
            _ => {
                // TCP connection with TLS enabled
                let pg_tls_config = convert_tls_option(&tls_config);
                let tls_connector = create_postgres_tls_connector(&pg_tls_config).map_err(|e| {
                    error::Error::Other {
                        source: BoxedError::new(e),
                        location: snafu::Location::new(file!(), line!(), 0),
                    }
                })?;
                cfg.create_pool(Some(Runtime::Tokio1), tls_connector)
                    .context(error::CreatePostgresPoolSnafu)?
            }
        }
    } else {
        cfg.create_pool(Some(Runtime::Tokio1), NoTls)
            .context(error::CreatePostgresPoolSnafu)?
    };

    Ok(pool)
}
