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

use std::collections::HashMap;

use clap::Parser;
use common_config::Configurable;
use common_plugins::options::PluginOptions;
use common_runtime::global::RuntimeOptions;
use plugins::PluginOptions;
use serde::{Deserialize, Serialize};

#[derive(Parser, Default, Debug, Clone)]
pub struct GlobalOptions {
    #[clap(long, value_name = "LOG_DIR")]
    #[arg(global = true)]
    pub log_dir: Option<String>,

    #[clap(long, value_name = "LOG_LEVEL")]
    #[arg(global = true)]
    pub log_level: Option<String>,

    #[cfg(feature = "tokio-console")]
    #[clap(long, value_name = "TOKIO_CONSOLE_ADDR")]
    #[arg(global = true)]
    pub tokio_console_addr: Option<String>,
}

// TODO(LFC): Move logging and tracing options into global options, like the runtime options.
/// All the options of GreptimeDB.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct GreptimeOptions<T> {
    /// The runtime options.
    pub runtime: RuntimeOptions,
    /// The plugin options.
    pub plugins: Vec<PluginOptions>,

    /// The options of each component (like Datanode or Standalone) of GreptimeDB.
    #[serde(flatten)]
    pub component: T,

    /// The options for plugins, keyed by their names. Using this field, we can pass into the
    /// options for multiple plugins.
    ///
    /// For example, suppose we have two plugins' options:
    ///
    /// ```rust
    /// #[derive(Debug, serde::Serialize, serde::Deserialize)]
    /// struct MyPlugin1Option {
    ///     a: String,
    /// }
    ///
    /// #[derive(Debug, serde::Serialize, serde::Deserialize)]
    /// struct MyPlugin2Option {
    ///     b: Vec<u32>,
    /// }
    /// ```
    ///
    /// First implementing [`PluginOptions`] for them:
    ///
    /// ```no_run
    /// #[typetag::serde]
    /// impl PluginOptions for MyPlugin1Option {
    ///     fn as_any(&self) -> &dyn Any {
    ///         self
    ///     }
    /// }
    ///
    /// #[typetag::serde]
    /// impl PluginOptions for MyPlugin2Option {
    ///     fn as_any(&self) -> &dyn Any {
    ///         self
    ///     }
    /// }
    /// ```
    ///
    /// Then we put their options in the config file like this:
    ///
    /// ```toml
    /// [plugins.my-plugin1]
    /// type = "MyPlugin1Option"
    /// a = "foo"
    ///
    /// [plugins.my-plugin2]
    /// type = "MyPlugin2Option"
    /// b = [1, 2]
    /// ```
    /// (Note that the `type` must equals to the name of the plugin option's definition struct.)
    ///
    /// Finally, after loading the GreptimeDB config file, we can `downcast` the
    /// [`Box<dyn PluginOptions>`] to its real type:
    ///
    /// ```no_run
    /// let options = greptime_options.plugins.remove("my-plugin1").unwrap();
    /// let plugin1_options: MyPlugin1Option = options.as_any().downcast_ref().unwrap();
    /// assert_eq(plugin1_options, "foo");
    /// ```
    pub plugins: HashMap<String, Box<dyn PluginOptions>>,
}

impl<T: Configurable> Configurable for GreptimeOptions<T> {
    fn env_list_keys() -> Option<&'static [&'static str]> {
        T::env_list_keys()
    }
}
