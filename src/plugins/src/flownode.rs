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

use common_base::Plugins;
use flow::error::Result;
use flow::{FlownodeBuilder, FlownodeInstance, FlownodeOptions};

use crate::options::PluginOptions;

/// Sets up flownode plugins before the [`FlownodeBuilder`] is constructed.
#[allow(unused_mut, unused_variables)]
pub async fn setup_flownode_plugins_pre_build(
    plugins: &mut Plugins,
    plugin_options: &[PluginOptions],
    _fn_opts: &FlownodeOptions,
) -> Result<()> {
    Ok(())
}

/// Sets up flownode plugins after the [`FlownodeBuilder`] is constructed
/// but before [`FlownodeBuilder::build()`].
///
/// Plugins can read context from the builder (e.g., opts, catalog_manager, flow_metadata_manager)
/// and insert additional plugins. After this call, [`FlownodeBuilder::set_plugins()`]
/// should be called to sync plugins into the builder.
#[allow(unused_variables)]
pub async fn setup_flownode_plugins_post_build(
    plugins: &mut Plugins,
    plugin_options: &[PluginOptions],
    builder: &FlownodeBuilder,
) -> Result<()> {
    Ok(())
}

pub async fn start_flownode_plugins(_instance: &FlownodeInstance) -> Result<()> {
    Ok(())
}

pub mod context {
    use std::sync::Arc;

    use catalog::CatalogManagerRef;
    use common_meta::FlownodeId;
    use common_meta::kv_backend::KvBackendRef;
    use flow::FrontendClient;

    /// The context for `GrpcBuilderConfiguratorRef` in flownode.
    pub struct GrpcConfigureContext {
        pub kv_backend: KvBackendRef,
        pub fe_client: Arc<FrontendClient>,
        pub flownode_id: FlownodeId,
        pub catalog_manager: CatalogManagerRef,
    }
}
