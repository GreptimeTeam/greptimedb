use std::sync::Arc;

use common_base::Plugins;
use datanode::datanode::DatanodeOptions;
use datanode::error::Result;

pub async fn setup_datanode_plugins(
    opts: DatanodeOptions,
) -> Result<(DatanodeOptions, Arc<Plugins>)> {
    Ok((opts, Arc::new(Plugins::new())))
}

pub async fn start_datanode_plugins(_plugins: Arc<Plugins>) -> Result<()> {
    Ok(())
}
