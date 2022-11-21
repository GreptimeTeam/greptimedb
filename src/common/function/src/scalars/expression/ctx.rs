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

use chrono_tz::Tz;

use crate::error::Error;

pub struct EvalContext {
    _tz: Tz,
    pub error: Option<Error>,
}

impl Default for EvalContext {
    fn default() -> Self {
        let tz = "UTC".parse::<Tz>().unwrap();
        Self {
            error: None,
            _tz: tz,
        }
    }
}

impl EvalContext {
    pub fn set_error(&mut self, e: Error) {
        if self.error.is_none() {
            self.error = Some(e);
        }
    }
}
