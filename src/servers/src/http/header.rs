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

use headers::{Header, HeaderName, HeaderValue};

pub const GREPTIME_DB_HEADER_FORMAT: &str = "x-greptime-format";
pub const GREPTIME_DB_HEADER_EXECUTION_TIME: &str = "x-greptime-execution-time";

pub static GREPTIME_DB_HEADER_NAME: HeaderName = HeaderName::from_static("x-greptime-name");

pub struct GreptimeDbName(Option<String>);

impl Header for GreptimeDbName {
    fn name() -> &'static HeaderName {
        &GREPTIME_DB_HEADER_NAME
    }

    fn decode<'i, I>(values: &mut I) -> Result<Self, headers::Error>
    where
        Self: Sized,
        I: Iterator<Item = &'i HeaderValue>,
    {
        if let Some(value) = values.next() {
            let str_value = value.to_str().map_err(|_| headers::Error::invalid())?;
            Ok(Self(Some(str_value.to_owned())))
        } else {
            Ok(Self(None))
        }
    }

    fn encode<E: Extend<HeaderValue>>(&self, values: &mut E) {
        if let Some(name) = &self.0 {
            if let Ok(value) = HeaderValue::from_str(name) {
                values.extend(std::iter::once(value));
            }
        }
    }
}

impl GreptimeDbName {
    pub fn value(&self) -> Option<&String> {
        self.0.as_ref()
    }
}
