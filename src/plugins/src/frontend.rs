// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use auth::{DefaultPermissionChecker, PermissionCheckerRef, UserProviderRef};
use common_base::Plugins;
use frontend::error::{IllegalAuthConfigSnafu, Result};
use frontend::frontend::FrontendOptions;
use snafu::ResultExt;

use crate::options::PluginOptions;

#[allow(unused_mut)]
pub async fn setup_frontend_plugins(
    plugins: &mut Plugins,
    _plugin_options: &[PluginOptions],
    fe_opts: &FrontendOptions,
) -> Result<()> {
    if let Some(user_provider) = fe_opts.user_provider.as_ref() {
        let provider =
            auth::user_provider_from_option(user_provider).context(IllegalAuthConfigSnafu)?;
        let permission_checker = DefaultPermissionChecker::arc();

        plugins.insert::<PermissionCheckerRef>(permission_checker);
        plugins.insert::<UserProviderRef>(provider);
    }
    Ok(())
}

pub async fn process_meta_config(
    _meta_config: Option<PluginOptions>,
    _plugins: &mut Plugins,
) -> Result<()> {
    Ok(())
}

pub async fn start_frontend_plugins(_plugins: Plugins) -> Result<()> {
    Ok(())
}

pub mod context {
    use std::sync::Arc;

    use flow::FrontendClient;
    use meta_client::MetaClientRef;

    /// The context for [`catalog::kvbackend::CatalogManagerConfiguratorRef`] in standalone or
    /// distributed.
    pub enum CatalogManagerConfigureContext {
        Distributed(DistributedCatalogManagerConfigureContext),
        Standalone(StandaloneCatalogManagerConfigureContext),
    }

    pub struct DistributedCatalogManagerConfigureContext {
        pub meta_client: MetaClientRef,
    }

    pub struct StandaloneCatalogManagerConfigureContext {
        pub fe_client: Arc<FrontendClient>,
    }
}
