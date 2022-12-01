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
    pub datanode_rpc_addr: String,
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
            datanode_rpc_addr: "127.0.0.1:3001".to_string(),
            meta_client_opts: None,
        }
    }
}

impl FrontendOptions {
    pub(crate) fn datanode_grpc_addr(&self) -> String {
        self.datanode_rpc_addr.clone()
    }
}

pub struct Frontend<T>
where
    T: FrontendInstance,
{
    opts: FrontendOptions,
    instance: Option<T>,
    user_provider: Option<UserProviderRef>,
}

impl<T> Frontend<T>
where
    T: FrontendInstance,
{
    pub fn new(opts: FrontendOptions, instance: T) -> Self {
        Self {
            opts,
            instance: Some(instance),
            user_provider: None,
        }
    }

    pub fn set_user_provider(&mut self, user_provider: Option<UserProviderRef>) {
        self.user_provider = user_provider;
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
        Services::start(&self.opts, instance, self.user_provider.clone()).await
    }
}
