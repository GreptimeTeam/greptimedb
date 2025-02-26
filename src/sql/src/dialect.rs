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

pub use sqlparser::dialect::{Dialect, GenericDialect, MySqlDialect, PostgreSqlDialect};

/// GreptimeDb dialect
#[derive(Debug, Clone)]
pub struct GreptimeDbDialect {}

impl Dialect for GreptimeDbDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_alphabetic() || ch == '_' || ch == '#' || ch == '@'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_alphabetic()
            || ch.is_ascii_digit()
            || ch == '@'
            || ch == '$'
            || ch == '#'
            || ch == '_'
    }

    // Accepts both `identifier` and "identifier".
    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '`' || ch == '"'
    }

    fn supports_filter_during_aggregation(&self) -> bool {
        true
    }
}
