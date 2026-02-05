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

use std::path::Path;

use async_trait::async_trait;
use sqlness::EnvController;

use crate::cmd::bare::ServerAddr;
use crate::env::bare;
use crate::env::bare::{StoreConfig, WalConfig};

#[derive(Clone)]
pub struct Env {
    inner: bare::Env,
}

impl Env {
    pub fn new_bare(
        data_home: std::path::PathBuf,
        server_addrs: ServerAddr,
        wal: WalConfig,
        pull_version_on_need: bool,
        bins_dir: Option<std::path::PathBuf>,
        store_config: StoreConfig,
        extra_args: Vec<String>,
    ) -> Self {
        Self {
            inner: bare::Env::new(
                data_home,
                server_addrs,
                wal,
                pull_version_on_need,
                bins_dir,
                store_config,
                extra_args,
            ),
        }
    }
}

#[async_trait]
impl EnvController for Env {
    type DB = bare::GreptimeDB;

    async fn start(&self, mode: &str, id: usize, config: Option<&Path>) -> Self::DB {
        EnvController::start(&self.inner, mode, id, config).await
    }

    async fn stop(&self, mode: &str, database: Self::DB) {
        EnvController::stop(&self.inner, mode, database).await
    }
}
