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

use auth::UserProviderRef;
use common_base::Plugins;
use frontend::error::{IllegalAuthConfigSnafu, Result};
use frontend::frontend::FrontendOptions;
use snafu::ResultExt;

pub async fn setup_frontend_plugins(opts: &FrontendOptions) -> Result<Plugins> {
    let plugins = Plugins::new();

    if let Some(user_provider) = opts.user_provider.as_ref() {
        let provider =
            auth::user_provider_from_option(user_provider).context(IllegalAuthConfigSnafu)?;
        plugins.insert::<UserProviderRef>(provider);
    }

    Ok(plugins)
}

pub async fn start_frontend_plugins(_plugins: Plugins) -> Result<()> {
    Ok(())
}
