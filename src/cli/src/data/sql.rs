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

//! Shared SQL escaping helpers for CLI-generated statements.

pub(crate) fn escape_sql_literal(value: &str) -> String {
    value.replace('\'', "''")
}

pub(crate) fn escape_sql_identifier(value: &str) -> String {
    value.replace('"', "\"\"")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escape_sql_literal_escapes_single_quotes() {
        assert_eq!(escape_sql_literal("test_db"), "test_db");
        assert_eq!(escape_sql_literal("te'st"), "te''st");
    }

    #[test]
    fn test_escape_sql_identifier_escapes_double_quotes() {
        assert_eq!(escape_sql_identifier("test_db"), "test_db");
        assert_eq!(escape_sql_identifier(r#"te"st"#), r#"te""st"#);
    }
}
