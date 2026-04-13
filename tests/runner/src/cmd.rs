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
    Kube(KubeCommand),
    Compat(CompatCommand),
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
}
