mod catalog;
mod error;
mod processors;
mod rpc;

use crate::error::Result;
use crate::rpc::Services;

/// DataNode service.
pub struct DataNode {
    services: Services,
}

impl DataNode {
    /// Shutdown the datanode service gracefully.
    pub async fn shutdown(&self) -> Result<()> {
        self.services.shutdown().await?;

        unimplemented!()
    }
}
