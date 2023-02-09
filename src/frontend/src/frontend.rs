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

use std::sync::Arc;

use common_base::Plugins;
use meta_client::MetaClientOpts;
use serde::{Deserialize, Serialize};
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
use crate::promql::PromqlOptions;
use crate::server::Services;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct FrontendOptions {
    pub http_options: Option<HttpOptions>,
    pub grpc_options: Option<GrpcOptions>,
    pub mysql_options: Option<MysqlOptions>,
    pub postgres_options: Option<PostgresOptions>,
    pub opentsdb_options: Option<OpentsdbOptions>,
    pub influxdb_options: Option<InfluxdbOptions>,
    pub prometheus_options: Option<PrometheusOptions>,
    pub promql_options: Option<PromqlOptions>,
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
            promql_options: Some(PromqlOptions::default()),
            mode: Mode::Standalone,
            meta_client_opts: None,
        }
    }
}

pub struct Frontend<T>
where
    T: FrontendInstance,
{
    opts: FrontendOptions,
    instance: Option<T>,
    plugins: Arc<Plugins>,
}

impl<T: FrontendInstance> Frontend<T> {
    pub fn new(opts: FrontendOptions, instance: T, plugins: Arc<Plugins>) -> Self {
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

        // TODO(sunng87): merge this into instance
        Services::start(&self.opts, instance, self.plugins.clone()).await
    }
}
