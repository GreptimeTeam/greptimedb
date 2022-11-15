use std::sync::Arc;

use serde::{Deserialize, Serialize};
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
    pub http_addr: Option<String>,
    pub grpc_options: Option<GrpcOptions>,
    pub mysql_options: Option<MysqlOptions>,
    pub postgres_options: Option<PostgresOptions>,
    pub opentsdb_options: Option<OpentsdbOptions>,
    pub influxdb_options: Option<InfluxdbOptions>,
    pub prometheus_options: Option<PrometheusOptions>,
    pub mode: Mode,
    pub datanode_rpc_addr: String,
    pub metasrv_addr: Option<Vec<String>>,
}

impl Default for FrontendOptions {
    fn default() -> Self {
        Self {
            http_addr: Some("0.0.0.0:4000".to_string()),
            grpc_options: Some(GrpcOptions::default()),
            mysql_options: Some(MysqlOptions::default()),
            postgres_options: Some(PostgresOptions::default()),
            opentsdb_options: Some(OpentsdbOptions::default()),
            influxdb_options: Some(InfluxdbOptions::default()),
            prometheus_options: Some(PrometheusOptions::default()),
            mode: Mode::Standalone,
            datanode_rpc_addr: "127.0.0.1:3001".to_string(),
            metasrv_addr: None,
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
}

impl<T> Frontend<T>
where
    T: FrontendInstance,
{
    pub fn new(opts: FrontendOptions, instance: T) -> Self {
        Self {
            opts,
            instance: Some(instance),
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
        Services::start(&self.opts, instance).await
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    Standalone,
    // with meta server's addr
    Distributed,
}
