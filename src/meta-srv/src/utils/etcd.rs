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

use common_meta::kv_backend::etcd::create_etcd_tls_options;
use etcd_client::{Client, ConnectOptions};
use servers::tls::{TlsMode, TlsOption};
use snafu::ResultExt;

use crate::error::{self, BuildTlsOptionsSnafu, Result};

/// Creates an etcd client with TLS configuration.
pub async fn create_etcd_client_with_tls(
    store_addrs: &[String],
    tls_config: Option<&TlsOption>,
) -> Result<Client> {
    let etcd_endpoints = store_addrs
        .iter()
        .map(|x| x.trim())
        .filter(|x| !x.is_empty())
        .collect::<Vec<_>>();

    let connect_options = tls_config
        .map(|c| create_etcd_tls_options(&convert_tls_option(c)))
        .transpose()
        .context(BuildTlsOptionsSnafu)?
        .flatten()
        .map(|tls_options| ConnectOptions::new().with_tls(tls_options));

    Client::connect(&etcd_endpoints, connect_options)
        .await
        .context(error::ConnectEtcdSnafu)
}

fn convert_tls_option(tls_option: &TlsOption) -> common_meta::kv_backend::etcd::TlsOption {
    let mode = match tls_option.mode {
        TlsMode::Disable => common_meta::kv_backend::etcd::TlsMode::Disable,
        _ => common_meta::kv_backend::etcd::TlsMode::Require,
    };
    common_meta::kv_backend::etcd::TlsOption {
        mode,
        cert_path: tls_option.cert_path.clone(),
        key_path: tls_option.key_path.clone(),
        ca_cert_path: tls_option.ca_cert_path.clone(),
    }
}
