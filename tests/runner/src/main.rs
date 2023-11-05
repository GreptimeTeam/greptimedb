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

use std::path::PathBuf;

use clap::Parser;
use env::Env;
use sqlness::{ConfigBuilder, Runner};

mod env;
mod util;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
/// SQL Harness for GrepTimeDB
struct Args {
    /// Directory of test cases
    #[clap(short, long)]
    case_dir: Option<PathBuf>,

    /// Fail this run as soon as one case fails if true
    #[arg(short, long, default_value = "false")]
    fail_fast: bool,

    /// Environment Configuration File
    #[clap(short, long, default_value = "config.toml")]
    env_config_file: String,

    /// Name of test cases to run. Accept as a regexp.
    #[clap(short, long, default_value = ".*")]
    test_filter: String,

    /// Address of the server
    #[clap(short, long)]
    server_addr: Option<String>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    #[cfg(windows)]
    let data_home = std::env::temp_dir();
    #[cfg(not(windows))]
    let data_home = std::path::PathBuf::from("/tmp");

    let config = ConfigBuilder::default()
        .case_dir(util::get_case_dir(args.case_dir))
        .fail_fast(args.fail_fast)
        .test_filter(args.test_filter)
        .follow_links(true)
        .env_config_file(args.env_config_file)
        .build()
        .unwrap();
    let runner = Runner::new(config, Env::new(data_home, args.server_addr));
    runner.run().await.unwrap();
}
