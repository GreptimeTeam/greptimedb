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

use clap::Parser;
use common_telemetry::logging::{LoggingOptions, TracingOptions};
use datanode::config::DatanodeOptions;
use frontend::frontend::FrontendOptions;
use meta_srv::metasrv::MetasrvOptions;

use crate::standalone::StandaloneOptions;

pub enum Options {
    Datanode(Box<DatanodeOptions>),
    Frontend(Box<FrontendOptions>),
    Metasrv(Box<MetasrvOptions>),
    Standalone(Box<StandaloneOptions>),
    Cli(Box<LoggingOptions>),
}

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

impl GlobalOptions {
    pub fn tracing_options(&self) -> TracingOptions {
        TracingOptions {
            #[cfg(feature = "tokio-console")]
            tokio_console_addr: self.tokio_console_addr.clone(),
        }
    }
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

    pub fn node_id(&self) -> Option<String> {
        match self {
            Options::Metasrv(_) | Options::Cli(_) | Options::Standalone(_) => None,
            Options::Datanode(opt) => opt.node_id.map(|x| x.to_string()),
            Options::Frontend(opt) => opt.node_id.clone(),
        }
    }
}
