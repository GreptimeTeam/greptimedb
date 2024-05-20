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
