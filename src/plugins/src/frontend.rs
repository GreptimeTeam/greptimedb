use std::sync::Arc;

use auth::UserProviderRef;
use common_base::{Plugins, PluginsRef};
use frontend::error::{IllegalAuthConfigSnafu, Result};
use frontend::frontend::FrontendOptions;
use snafu::ResultExt;

use crate::OptPlugins;

pub async fn setup_frontend_plugins(opts: FrontendOptions) -> Result<OptPlugins<FrontendOptions>> {
    let plugins = Plugins::new();

    if let Some(user_provider) = opts.user_provider.as_ref() {
        let provider =
            auth::user_provider_from_option(user_provider).context(IllegalAuthConfigSnafu)?;
        plugins.insert::<UserProviderRef>(provider);
    }

    Ok(OptPlugins {
        opts,
        plugins: Arc::new(plugins),
    })
}

pub async fn start_frontend_plugins(_plugins: PluginsRef) -> Result<()> {
    Ok(())
}
