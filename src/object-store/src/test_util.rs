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

use std::env;

use crate::{ObjectStore, Result};

/// Temp folder for object store test
pub struct TempFolder {
    store: ObjectStore,
    // The path under root.
    path: String,
}

impl TempFolder {
    pub fn new(store: &ObjectStore, path: &str) -> Self {
        Self {
            store: store.clone(),
            path: path.to_string(),
        }
    }

    pub async fn remove_all(&self) -> Result<()> {
        self.store.remove_all(&self.path).await
    }
}

/// Test s3 config from environment variables
#[derive(Debug)]
pub struct TestS3Config {
    pub root: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub bucket: String,
    pub region: Option<String>,
}

/// Returns s3 test config, return None if not found.
pub fn s3_test_config() -> Option<TestS3Config> {
    if let Ok(b) = env::var("GT_S3_BUCKET") {
        if !b.is_empty() {
            return Some(TestS3Config {
                root: uuid::Uuid::new_v4().to_string(),
                access_key_id: env::var("GT_S3_ACCESS_KEY_ID").ok()?,
                secret_access_key: env::var("GT_S3_ACCESS_KEY").ok()?,
                bucket: env::var("GT_S3_BUCKET").ok()?,
                region: Some(env::var("GT_S3_REGION").ok()?),
            });
        }
    }

    None
}
