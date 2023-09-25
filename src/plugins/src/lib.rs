mod datanode;
mod frontend;
mod meta_srv;

use common_base::PluginsRef;
pub use datanode::{setup_datanode_plugins, start_datanode_plugins};
pub use frontend::{setup_frontend_plugins, start_frontend_plugins};
pub use meta_srv::{setup_meta_srv_plugins, start_meta_srv_plugins};

pub struct OptPlugins<T> {
    pub opts: T,
    pub plugins: PluginsRef,
}
