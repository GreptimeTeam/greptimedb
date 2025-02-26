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
use meta_srv::error::Result;
use meta_srv::metasrv::MetasrvOptions;

use crate::options::PluginOptions;

#[allow(unused_variables)]
pub async fn setup_metasrv_plugins(
    _plugins: &mut Plugins,
    plugin_options: &[PluginOptions],
    metasrv_opts: &MetasrvOptions,
) -> Result<()> {
    Ok(())
}

pub async fn start_metasrv_plugins(_plugins: Plugins) -> Result<()> {
    Ok(())
}
