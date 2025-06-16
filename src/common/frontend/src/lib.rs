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

use std::fmt::{Display, Formatter};
use std::str::FromStr;

use snafu::OptionExt;

pub mod error;
pub mod selector;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DisplayProcessId {
    pub server_addr: String,
    pub id: u32,
}

impl Display for DisplayProcessId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.server_addr, self.id)
    }
}

impl TryFrom<&str> for DisplayProcessId {
    type Error = error::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let mut split = value.split('/');
        let server_addr = split
            .next()
            .context(error::ParseProcessIdSnafu { s: value })?
            .to_string();
        let id = split
            .next()
            .context(error::ParseProcessIdSnafu { s: value })?;
        let id = u32::from_str(id)
            .ok()
            .context(error::ParseProcessIdSnafu { s: value })?;
        Ok(DisplayProcessId { server_addr, id })
    }
}
