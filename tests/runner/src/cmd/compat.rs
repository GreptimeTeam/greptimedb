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

use crate::compatibility_runner::CompatibilityRunner;
use crate::version::Version;

#[derive(Debug, Parser)]
pub struct CompatCommand {
    #[clap(long)]
    from: String,

    #[clap(long)]
    to: String,

    #[clap(short, long)]
    case_dir: Option<PathBuf>,

    /// Fail this run as soon as one case fails if true
    #[arg(short, long, default_value = "false")]
    fail_fast: bool,

    /// Name of test cases to run. Accept as a regexp.
    #[clap(short, long, default_value = ".*")]
    test_filter: String,

    #[clap(long)]
    preserve_state: bool,
}

impl CompatCommand {
    pub async fn run(self) {
        let from_version = match Version::parse(&self.from) {
            Ok(v) => v,
            Err(e) => {
                println!("Error parsing 'from' version: {}", e);
                std::process::exit(1);
            }
        };

        let to_version = match Version::parse(&self.to) {
            Ok(v) => v,
            Err(e) => {
                println!("Error parsing 'to' version: {}", e);
                std::process::exit(1);
            }
        };

        let temp_dir = tempfile::Builder::new()
            .prefix("compat-test")
            .tempdir()
            .unwrap();
        let data_dir = temp_dir.keep();

        let runner = match CompatibilityRunner::new(
            from_version,
            to_version,
            self.case_dir,
            data_dir.clone(),
            self.test_filter,
            self.fail_fast,
        )
        .await
        {
            Ok(r) => r,
            Err(e) => {
                println!("Failed to create compatibility runner: {}", e);
                std::process::exit(1);
            }
        };

        match runner.run().await {
            Ok(_) => {
                println!("\x1b[32mCompatibility tests passed!\x1b[0m");
            }
            Err(e) => {
                println!("\x1b[31mCompatibility tests failed: {}\x1b[0m", e);
                std::process::exit(1);
            }
        }

        if !self.preserve_state {
            println!("Stopping etcd");
            crate::util::stop_rm_etcd();
            tokio::fs::remove_dir_all(data_dir).await.unwrap();
        }
    }
}
