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

#![allow(clippy::print_stdout)]

use clap::Parser;

use crate::cmd::{Command, SubCommand};

pub mod client;
mod cmd;
mod env;
pub mod formatter;
pub mod protocol_interceptor;
mod server_mode;
mod util;

#[tokio::main]
async fn main() {
    let cmd = Command::parse();

    match cmd.subcmd {
        SubCommand::Bare(cmd) => cmd.run().await,
        SubCommand::Kube(cmd) => cmd.run().await,
    }
}
