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
use datanode::config::DatanodeOptions;
use datanode::datanode::{Datanode, DatanodeBuilder};
use datanode::error::Result;

use crate::options::PluginOptions;

/// Sets up datanode plugins before the [`DatanodeBuilder`] is constructed.
#[allow(unused_variables)]
#[allow(unused_mut)]
pub async fn setup_datanode_plugins_pre_build(
    plugins: &mut Plugins,
    plugin_options: &[PluginOptions],
    dn_opts: &DatanodeOptions,
) -> Result<()> {
    Ok(())
}

/// Sets up datanode plugins after the [`DatanodeBuilder`] is constructed
/// but before [`DatanodeBuilder::build()`].
///
/// Plugins can read context from the builder (e.g., kv_backend, options)
/// and insert additional plugins. After this call, [`DatanodeBuilder::set_plugins()`]
/// should be called to sync plugins into the builder.
#[allow(unused_variables)]
pub async fn setup_datanode_plugins_post_build(
    plugins: &mut Plugins,
    plugin_options: &[PluginOptions],
    builder: &DatanodeBuilder,
) -> Result<()> {
    Ok(())
}

pub async fn start_datanode_plugins(_instance: &Datanode) -> Result<()> {
    Ok(())
}
