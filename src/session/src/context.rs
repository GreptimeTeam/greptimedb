// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use arc_swap::ArcSwapOption;
use common_telemetry::info;

pub type QueryContextRef = Arc<QueryContext>;

pub struct QueryContext {
    current_schema: ArcSwapOption<String>,
}

impl Default for QueryContext {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryContext {
    pub fn new() -> Self {
        Self {
            current_schema: ArcSwapOption::new(None),
        }
    }

    pub fn with_current_schema(schema: String) -> Self {
        Self {
            current_schema: ArcSwapOption::new(Some(Arc::new(schema))),
        }
    }

    pub fn current_schema(&self) -> Option<String> {
        self.current_schema.load().as_deref().cloned()
    }

    pub fn set_current_schema(&self, schema: &str) {
        let last = self.current_schema.swap(Some(Arc::new(schema.to_string())));
        info!(
            "set new session default schema: {:?}, swap old: {:?}",
            schema, last
        )
    }
}
