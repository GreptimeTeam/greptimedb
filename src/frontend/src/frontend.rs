use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::instance::Instance;
use crate::server::Services;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FrontendOptions {
    pub http_addr: Option<String>,
    pub grpc_addr: Option<String>,
    pub mysql_addr: Option<String>,
    pub mysql_runtime_size: u32,
}

impl Default for FrontendOptions {
    fn default() -> Self {
        Self {
            http_addr: Some("0.0.0.0:4000".to_string()),
            grpc_addr: Some("0.0.0.0:4001".to_string()),
            mysql_addr: Some("0.0.0.0:4002".to_string()),
            mysql_runtime_size: 2,
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
    services: Services,
}

impl Frontend {
    pub async fn try_new(opts: FrontendOptions) -> Result<Self> {
        let instance = Arc::new(Instance::try_new(&opts).await?);
        let services = Services::new(instance);
        Ok(Self { opts, services })
    }

    pub async fn start(&mut self) -> Result<()> {
        self.services.start(&self.opts).await
    }
}
