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

use serde::Serialize;
use sqlparser_derive::{Visit, VisitMut};

/// Arguments of `KILL` statements.
#[derive(Debug, Clone, Eq, PartialEq, Visit, VisitMut, Serialize)]
pub enum Kill {
    /// Kill a remote process id.
    ProcessId(String),
    /// Kill MySQL connection id.
    ConnectionId(u32),
}

impl Display for Kill {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Kill::ProcessId(id) => {
                write!(f, "KILL {}", id)
            }
            Kill::ConnectionId(id) => {
                write!(f, "KILL QUERY {}", id)
            }
        }
    }
}
