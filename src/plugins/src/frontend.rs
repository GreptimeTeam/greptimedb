use std::sync::Arc;

use auth::UserProviderRef;
use common_base::Plugins;
use frontend::error::{IllegalAuthConfigSnafu, Result};
use frontend::frontend::FrontendOptions;
use snafu::ResultExt;

pub async fn setup_frontend_plugins(
    opts: FrontendOptions,
) -> Result<(FrontendOptions, Arc<Plugins>)> {
    let plugins = Plugins::new();

    if let Some(user_provider) = opts.user_provider.as_ref() {
        let provider =
            auth::user_provider_from_option(user_provider).context(IllegalAuthConfigSnafu)?;
        plugins.insert::<UserProviderRef>(provider);
    }

    Ok((opts, Arc::new(plugins)))
}

pub async fn start_frontend_plugins(_plugins: Arc<Plugins>) -> Result<()> {
    Ok(())
}
