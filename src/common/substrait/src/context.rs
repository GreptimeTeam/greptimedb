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

use std::collections::HashMap;

#[derive(Default)]
pub struct ConvertorContext {
    scalar_fn_names: HashMap<String, u32>,
    scalar_fn_map: HashMap<u32, String>,
}

impl ConvertorContext {
    pub fn register_scalar_fn<S: AsRef<str>>(&mut self, name: S) -> u32 {
        if let Some(anchor) = self.scalar_fn_names.get(name.as_ref()) {
            return *anchor;
        }

        let next_anchor = self.scalar_fn_map.len() as _;
        self.scalar_fn_map
            .insert(next_anchor, name.as_ref().to_string());
        self.scalar_fn_names
            .insert(name.as_ref().to_string(), next_anchor);
        next_anchor
    }

    pub fn find_scalar_fn(&self, anchor: u32) -> Option<&str> {
        self.scalar_fn_map.get(&anchor).map(|s| s.as_str())
    }
}
