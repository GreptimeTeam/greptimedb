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

mod bench;
mod database;
pub mod error;
mod export;
mod import;

use async_trait::async_trait;
use clap::Parser;
use common_error::ext::BoxedError;
pub use database::DatabaseClient;
use error::Result;

pub use crate::bench::BenchTableMetadataCommand;
pub use crate::export::ExportCommand;
pub use crate::import::ImportCommand;

#[async_trait]
pub trait Tool: Send + Sync {
    async fn do_work(&self) -> std::result::Result<(), BoxedError>;
}

#[derive(Debug, Parser)]
pub(crate) struct AttachCommand {
    #[clap(long)]
    pub(crate) grpc_addr: String,
    #[clap(long)]
    pub(crate) meta_addr: Option<String>,
    #[clap(long, action)]
    pub(crate) disable_helper: bool,
}

impl AttachCommand {
    #[allow(dead_code)]
    async fn build(self) -> Result<Box<dyn Tool>> {
        unimplemented!("Wait for https://github.com/GreptimeTeam/greptimedb/issues/2373")
    }
}
