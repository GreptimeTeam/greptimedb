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
use meta_srv::bootstrap::MetasrvInstance;
use meta_srv::error::Result;
use meta_srv::metasrv::MetasrvOptions;
use meta_srv::metasrv::builder::MetasrvBuilder;

use crate::options::PluginOptions;

/// Sets up metasrv plugins before the [`MetasrvBuilder`] is constructed.
///
/// Plugins registered here are available during builder construction
/// (e.g., `SelectorFactoryRef`).
#[allow(unused_variables)]
pub async fn setup_metasrv_plugins_pre_build(
    plugins: &mut Plugins,
    plugin_options: &[PluginOptions],
    _metasrv_opts: &MetasrvOptions,
) -> Result<()> {
    Ok(())
}

/// Sets up metasrv plugins after the [`MetasrvBuilder`] is constructed
/// but before [`MetasrvBuilder::build()`].
///
/// Plugins can read context from the builder (e.g., kv_backend, options)
/// and insert additional plugins. After this call, [`MetasrvBuilder::plugins()`]
/// should be called to set plugins on the builder.
#[allow(unused_variables)]
pub async fn setup_metasrv_plugins_post_build(
    plugins: &mut Plugins,
    plugin_options: &[PluginOptions],
    builder: &MetasrvBuilder,
) -> Result<()> {
    Ok(())
}

pub async fn start_metasrv_plugins(_instance: &MetasrvInstance) -> Result<()> {
    Ok(())
}
