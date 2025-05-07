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
use flow::FlownodeOptions;

use crate::options::PluginOptions;

#[allow(unused_mut)]
pub async fn setup_flownode_plugins(
    _plugins: &mut Plugins,
    _plugin_options: &[PluginOptions],
    _fn_opts: &FlownodeOptions,
) -> Result<()> {
    Ok(())
}

pub async fn start_flownode_plugins(_plugins: Plugins) -> Result<()> {
    Ok(())
}
