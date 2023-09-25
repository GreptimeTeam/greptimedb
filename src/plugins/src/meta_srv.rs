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

use std::sync::Arc;

use common_base::{Plugins, PluginsRef};
use meta_srv::error::Result;
use meta_srv::metasrv::MetaSrvOptions;

use crate::OptPlugins;

pub async fn setup_meta_srv_plugins(opts: MetaSrvOptions) -> Result<OptPlugins<MetaSrvOptions>> {
    Ok(OptPlugins {
        opts,
        plugins: Arc::new(Plugins::new()),
    })
}

pub async fn start_meta_srv_plugins(_plugins: PluginsRef) -> Result<()> {
    Ok(())
}
