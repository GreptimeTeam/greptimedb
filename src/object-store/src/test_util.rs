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

use crate::{ObjectStore, Result};

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

    pub async fn remove_all(&mut self) -> Result<()> {
        self.store.remove_all(&self.path).await
    }
}
