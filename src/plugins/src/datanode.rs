use std::sync::Arc;

use common_base::{Plugins, PluginsRef};
use datanode::config::DatanodeOptions;
use datanode::error::Result;

use crate::OptPlugins;

pub async fn setup_datanode_plugins(opts: DatanodeOptions) -> Result<OptPlugins<DatanodeOptions>> {
    Ok(OptPlugins {
        opts,
        plugins: Arc::new(Plugins::new()),
    })
}

pub async fn start_datanode_plugins(_plugins: PluginsRef) -> Result<()> {
    Ok(())
}
