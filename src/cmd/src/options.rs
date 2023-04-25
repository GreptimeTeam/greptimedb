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

pub enum ConfigOptions {
    Datanode(Box<DatanodeOptions>),
    Frontend(FrontendOptions),
    Metasrv(MetaSrvOptions),
    Standalone(FrontendOptions, Box<DatanodeOptions>, LoggingOptions),
    Cli(LoggingOptions),
}

impl ConfigOptions {
    pub fn logging_options(&self) -> &LoggingOptions {
        match self {
            ConfigOptions::Datanode(opts) => &opts.logging,
            ConfigOptions::Frontend(opts) => &opts.logging,
            ConfigOptions::Metasrv(opts) => &opts.logging,
            ConfigOptions::Standalone(_, _, opts) => opts,
            ConfigOptions::Cli(opts) => opts,
        }
    }
}
