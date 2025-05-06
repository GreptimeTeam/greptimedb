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

use std::any::Any;

use auth::StaticUserProvider;
use common_base::Plugins;
use flow::error::{IllegalAuthConfigSnafu, NotImplementedSnafu, Result};
use flow::{FlowAuthHeader, FlownodeOptions};
use snafu::ResultExt;

use crate::options::PluginOptions;

#[allow(unused_mut)]
pub async fn setup_flownode_plugins(
    plugins: &mut Plugins,
    _plugin_options: &[PluginOptions],
    fe_opts: &FlownodeOptions,
) -> Result<()> {
    if let Some(user_provider) = fe_opts.user_provider.as_ref() {
        let provider =
            auth::user_provider_from_option(user_provider).context(IllegalAuthConfigSnafu)?;
        // downcast to StaticUserProvider
        // TODO: extract password from static user provider and wrap in AuthScheme basic
        let Some(static_provider) = (&provider as &dyn Any).downcast_ref::<StaticUserProvider>()
        else {
            NotImplementedSnafu {
                reason: format!(
                    "flownode Only support static provider for now, get {:?}",
                    provider.type_id()
                ),
            }
            .fail()?
        };
        let (usr, pwd) = static_provider
            .get_one_user_pwd()
            .context(IllegalAuthConfigSnafu)?;
        let auth_header = FlowAuthHeader::from_user_pwd(&usr, &pwd);
        plugins.insert(auth_header);
    }
    Ok(())
}

pub async fn start_flownode_plugins(_plugins: Plugins) -> Result<()> {
    Ok(())
}
