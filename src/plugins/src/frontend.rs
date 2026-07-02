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
use common_meta::cache::CacheRegistryBuilder;
use frontend::error::{IllegalAuthConfigSnafu, Result};
use frontend::frontend::FrontendOptions;
use frontend::instance::Instance;
use frontend::instance::builder::FrontendBuilder;
use snafu::ResultExt;

use crate::options::PluginOptions;

/// Sets up frontend plugins before the [`FrontendBuilder`] is constructed.
///
/// This is where "infrastructure configurators" are registered — plugins that the builder
/// consumes during construction (e.g., `CatalogManagerConfiguratorRef`, cache invalidators).
///
/// In distributed mode this is called twice:
/// 1. First without meta config (before `create_meta_client`), for plugins needed by the meta client.
/// 2. Second with meta config pulled from metasrv, for dynamic configurators.
///
/// In standalone mode it is called once with `None`.
#[allow(unused_mut)]
pub async fn setup_frontend_plugins_pre_build(
    plugins: &mut Plugins,
    _plugin_options: &[PluginOptions],
    fe_opts: &FrontendOptions,
    _meta_config: Option<&[PluginOptions]>,
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

/// Sets up frontend plugins after the [`FrontendBuilder`] is constructed
/// but before [`FrontendBuilder::try_build()`] and [`FrontendBuilder::with_plugin()`].
///
/// This is where "feature plugins" are registered — plugins that consume builder context
/// (e.g., `KvBackendRef`, `CatalogManagerRef`) to construct themselves.
pub async fn setup_frontend_plugins_post_build(
    _plugins: &mut Plugins,
    _plugin_options: &[PluginOptions],
    _builder: &FrontendBuilder,
) -> Result<()> {
    Ok(())
}

pub async fn start_frontend_plugins(_instance: &Instance) -> Result<()> {
    Ok(())
}

/// Allows frontend plugins to add cache invalidators to the layered registry.
pub fn configure_cache_registry(_plugins: &Plugins) -> Option<CacheRegistryBuilder> {
    None
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
