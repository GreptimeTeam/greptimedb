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

pub(crate) mod bare;
pub(crate) mod compat;
pub(crate) mod compat_case;
pub(crate) mod datanode_overlay;
pub(crate) mod kube;

use std::path::PathBuf;

use bare::BareCommand;
use clap::Parser;
use compat::CompatCommand;
use kube::KubeCommand;

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
pub struct Command {
    #[clap(subcommand)]
    pub subcmd: SubCommand,
}

#[derive(Parser)]
pub enum SubCommand {
    Bare(BareCommand),
    Compat(CompatCommand),
    Kube(KubeCommand),
}

#[derive(Debug, Parser)]
pub struct SqlnessConfig {
    /// Directory of test cases
    #[clap(short, long)]
    pub case_dir: Option<PathBuf>,

    /// Fail this run as soon as one case fails if true
    #[arg(short, long, default_value = "false")]
    pub fail_fast: bool,

    /// Environment Configuration File
    #[clap(short, long, default_value = "config.toml")]
    pub env_config_file: String,

    /// Name of test cases to run. Accept as a regexp.
    #[clap(short, long, default_value = ".*")]
    pub test_filter: String,

    /// Which environments to run, as a regexp matched against each environment directory name.
    ///
    /// `.*` runs every environment the case directory holds. The native PromQL route needs an
    /// in-process region transport, which only the standalone server installs, so a run that
    /// compares the two routes restricts itself to `standalone` and does not report the distributed
    /// environment as natively covered.
    #[clap(long, default_value = ".*")]
    pub env_filter: String,

    /// A query hint sent with every gRPC statement, as `key=value`. Repeatable.
    ///
    /// Hints are the per-request query context, so they select behaviour a server-startup option
    /// cannot: `query.experimental_native_promql=true` asks for the native PromQL route per
    /// statement, which is what a run comparing the two routes needs. Only the gRPC client applies
    /// them; the MySQL and Postgres protocols have no hint channel.
    #[clap(long = "query-hint", value_name = "KEY=VALUE")]
    pub query_hints: Vec<String>,
}

/// The query hints a run sends with every statement.
///
/// `enable_native_promql` adds the request half of the native PromQL route next to the server option
/// the same flag renders into the config, so the two cannot disagree. An explicit
/// `query.experimental_native_promql` argument overrides it, which keeps one run able to ask for a
/// specific value.
pub(crate) fn native_query_hints(
    enable_native_promql: bool,
    hints: Vec<(String, String)>,
) -> Vec<(String, String)> {
    const KEY: &str = "query.experimental_native_promql";
    if !enable_native_promql || hints.iter().any(|(key, _)| key == KEY) {
        return hints;
    }
    let mut hints = hints;
    hints.push((KEY.to_string(), "true".to_string()));
    hints
}

/// Splits `key=value` hint arguments into the pairs the gRPC client sends.
///
/// A malformed argument fails the run instead of being dropped: a silently ignored hint would make a
/// run that asked for the native route execute the existing one and still report green.
pub(crate) fn parse_query_hints(hints: &[String]) -> Vec<(String, String)> {
    hints
        .iter()
        .map(|hint| {
            let (key, value) = hint
                .split_once('=')
                .unwrap_or_else(|| panic!("query hint {hint:?} is not `key=value`"));
            (key.trim().to_string(), value.trim().to_string())
        })
        .collect()
}
