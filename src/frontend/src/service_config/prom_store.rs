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

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PromStoreOptions {
    pub enable: bool,
}

impl Default for PromStoreOptions {
    fn default() -> Self {
        Self { enable: true }
    }
}

#[cfg(test)]
mod tests {
    use super::PromStoreOptions;

    #[test]
    fn test_prom_store_options() {
        let default = PromStoreOptions::default();
        assert!(default.enable);
    }
}
