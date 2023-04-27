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
use common_telemetry::logging::LoggingOptions;
use datanode::datanode::DatanodeOptions;
use frontend::frontend::FrontendOptions;
use meta_srv::metasrv::MetaSrvOptions;

pub struct MixOptions {
    pub fe_opts: FrontendOptions,
    pub dn_opts: DatanodeOptions,
    pub logging: LoggingOptions,
}

pub enum Options {
    Datanode(Box<DatanodeOptions>),
    Frontend(Box<FrontendOptions>),
    Metasrv(Box<MetaSrvOptions>),
    Standalone(Box<MixOptions>),
    Cli(Box<LoggingOptions>),
}

impl Options {
    pub fn logging_options(&self) -> &LoggingOptions {
        match self {
            Options::Datanode(opts) => &opts.logging,
            Options::Frontend(opts) => &opts.logging,
            Options::Metasrv(opts) => &opts.logging,
            Options::Standalone(opts) => &opts.logging,
            Options::Cli(opts) => opts,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct TopLevelOptions {
    pub log_dir: Option<String>,
    pub log_level: Option<String>,
}
