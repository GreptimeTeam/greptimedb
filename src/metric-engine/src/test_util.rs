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

//! Utilities for testing.

use mito2::config::MitoConfig;
use mito2::engine::MitoEngine;
use mito2::test_util::TestEnv as MitoTestEnv;
use object_store::util::join_dir;

/// Env to test metric engine.
pub struct TestEnv {
    mito_env: MitoTestEnv,
    mito: MitoEngine,
}

impl TestEnv {
    /// Returns a new env with empty prefix for test.
    pub async fn new() -> Self {
        Self::with_prefix("").await
    }

    /// Returns a new env with specific `prefix` for test.
    pub async fn with_prefix(prefix: &str) -> Self {
        let mut mito_env = MitoTestEnv::with_prefix(prefix);
        let mito = mito_env.create_engine(MitoConfig::default()).await;
        Self { mito_env, mito }
    }

    pub fn data_home(&self) -> String {
        let env_root = self.mito_env.data_home().to_string_lossy().to_string();
        join_dir(&env_root, "data")
    }

    /// Returns a reference to the engine.
    pub fn mito(&self) -> MitoEngine {
        self.mito.clone()
    }
}
