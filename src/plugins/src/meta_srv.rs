use std::sync::Arc;

use common_base::{Plugins, PluginsRef};
use meta_srv::error::Result;
use meta_srv::metasrv::MetaSrvOptions;

use crate::OptPlugins;

pub async fn setup_meta_srv_plugins(opts: MetaSrvOptions) -> Result<OptPlugins<MetaSrvOptions>> {
    Ok(OptPlugins {
        opts,
        plugins: Arc::new(Plugins::new()),
    })
}

pub async fn start_meta_srv_plugins(_plugins: PluginsRef) -> Result<()> {
    Ok(())
}
