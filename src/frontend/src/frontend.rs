// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use anymap::AnyMap;
use meta_client::MetaClientOpts;
use serde::{Deserialize, Serialize};
use servers::auth::UserProviderRef;
use servers::http::HttpOptions;
use servers::Mode;
use snafu::prelude::*;

use crate::error::{self, Result};
use crate::grpc::GrpcOptions;
use crate::influxdb::InfluxdbOptions;
use crate::instance::FrontendInstance;
use crate::mysql::MysqlOptions;
use crate::opentsdb::OpentsdbOptions;
use crate::postgres::PostgresOptions;
use crate::prometheus::PrometheusOptions;
use crate::server::Services;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FrontendOptions {
    pub http_options: Option<HttpOptions>,
    pub grpc_options: Option<GrpcOptions>,
    pub mysql_options: Option<MysqlOptions>,
    pub postgres_options: Option<PostgresOptions>,
    pub opentsdb_options: Option<OpentsdbOptions>,
    pub influxdb_options: Option<InfluxdbOptions>,
    pub prometheus_options: Option<PrometheusOptions>,
    pub mode: Mode,
    pub meta_client_opts: Option<MetaClientOpts>,
}

impl Default for FrontendOptions {
    fn default() -> Self {
        Self {
            http_options: Some(HttpOptions::default()),
            grpc_options: Some(GrpcOptions::default()),
            mysql_options: Some(MysqlOptions::default()),
            postgres_options: Some(PostgresOptions::default()),
            opentsdb_options: Some(OpentsdbOptions::default()),
            influxdb_options: Some(InfluxdbOptions::default()),
            prometheus_options: Some(PrometheusOptions::default()),
            mode: Mode::Standalone,
            meta_client_opts: None,
        }
    }
}

#[derive(Default)]
pub struct FrontendPlugin {
    pub user_provider: Option<UserProviderRef>,
}

pub struct Frontend<T>
where
    T: FrontendInstance,
{
    opts: FrontendOptions,
    instance: Option<T>,
    plugins: AnyMap,
}

impl<T: FrontendInstance> Frontend<T> {
    pub fn new(opts: FrontendOptions, instance: T, plugins: AnyMap) -> Self {
        Self {
            opts,
            instance: Some(instance),
            plugins,
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        let mut instance = self
            .instance
            .take()
            .context(error::IllegalFrontendStateSnafu {
                err_msg: "Frontend instance not initialized",
            })?;
        instance.start().await?;

        let instance = Arc::new(instance);

        let provider = self
            .plugins
            .get::<FrontendPlugin>()
            .and_then(|f| f.user_provider.clone());

        Services::start(&self.opts, instance, provider).await
    }
}
