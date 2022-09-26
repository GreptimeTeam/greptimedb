use std::sync::Arc;

use serde::{Deserialize, Serialize};
use snafu::prelude::*;

use crate::error::{self, Result};
use crate::instance::Instance;
use crate::mysql::MysqlOptions;
use crate::opentsdb::OpentsdbOptions;
use crate::postgres::PostgresOptions;
use crate::server::Services;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FrontendOptions {
    pub http_addr: Option<String>,
    pub grpc_addr: Option<String>,
    pub mysql_options: Option<MysqlOptions>,
    pub postgres_options: Option<PostgresOptions>,
    pub opentsdb_options: Option<OpentsdbOptions>,
}

impl Default for FrontendOptions {
    fn default() -> Self {
        Self {
            http_addr: Some("0.0.0.0:4000".to_string()),
            grpc_addr: Some("0.0.0.0:4001".to_string()),
            mysql_options: Some(MysqlOptions::default()),
            postgres_options: Some(PostgresOptions::default()),
            opentsdb_options: Some(OpentsdbOptions::default()),
        }
    }
}

impl FrontendOptions {
    // TODO(LFC) Get Datanode address from Meta.
    pub(crate) fn datanode_grpc_addr(&self) -> String {
        "http://127.0.0.1:3001".to_string()
    }
}

pub struct Frontend {
    opts: FrontendOptions,
    instance: Option<Instance>,
}

impl Frontend {
    pub fn new(opts: FrontendOptions) -> Self {
        let instance = Instance::new();
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
        instance.start(&self.opts).await?;

        let instance = Arc::new(instance);
        Services::start(&self.opts, instance).await
    }
}
