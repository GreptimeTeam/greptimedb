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

use env::Env;
use sqlness::{ConfigBuilder, Runner};

mod env;
mod util;

#[tokio::main]
async fn main() {
    let mut args: Vec<String> = std::env::args().collect();
    let test_filter = if args.len() > 1 {
        args.pop().unwrap()
    } else {
        "".to_string()
    };

    #[cfg(windows)]
    let data_home = std::env::temp_dir();
    #[cfg(not(windows))]
    let data_home = std::path::PathBuf::from("/tmp");

    let config = ConfigBuilder::default()
        .case_dir(util::get_case_dir())
        .fail_fast(false)
        .test_filter(test_filter)
        .follow_links(true)
        .build()
        .unwrap();
    let runner = Runner::new(config, Env::new(data_home));
    runner.run().await.unwrap();
}
