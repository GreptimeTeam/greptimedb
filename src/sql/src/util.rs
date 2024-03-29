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

use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::LazyLock;

use regex::Regex;
use sqlparser::ast::{ObjectName, SqlOption, Value};

static SQL_SECRET_PATTERNS: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    vec![
        Regex::new(r#"(?i)access_key_id=["']([^"']*)["'].*"#).unwrap(),
        Regex::new(r#"(?i)secret_access_key=["']([^"']*)["'].*"#).unwrap(),
    ]
});

/// Format an [ObjectName] without any quote of its idents.
pub fn format_raw_object_name(name: &ObjectName) -> String {
    struct Inner<'a> {
        name: &'a ObjectName,
    }

    impl<'a> Display for Inner<'a> {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            let mut delim = "";
            for ident in self.name.0.iter() {
                write!(f, "{delim}")?;
                delim = ".";
                write!(f, "{}", ident.value)?;
            }
            Ok(())
        }
    }

    format!("{}", Inner { name })
}

pub fn parse_option_string(value: Value) -> Option<String> {
    match value {
        Value::SingleQuotedString(v) | Value::DoubleQuotedString(v) => Some(v),
        _ => None,
    }
}

/// Converts options to HashMap<String, String>.
/// All keys are lowercase.
pub fn to_lowercase_options_map(opts: &[SqlOption]) -> HashMap<String, String> {
    let mut map = HashMap::with_capacity(opts.len());
    for SqlOption { name, value } in opts {
        let value_str = match value {
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => s.clone(),
            _ => value.to_string(),
        };
        let _ = map.insert(name.value.to_lowercase().clone(), value_str);
    }
    map
}

/// Use regex to match and replace common seen secret values in SQL.
pub fn redact_sql_secrets(sql: &str) -> String {
    let mut s = sql.to_string();
    for p in SQL_SECRET_PATTERNS.iter() {
        if let Some(captures) = p.captures(&s) {
            if let Some(m) = captures.get(1) {
                s = s.replace(m.as_str(), "******");
            }
        }
    }
    s
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_redact_sql_secrets() {
        assert_eq!(
            redact_sql_secrets(
                r#"COPY 'my_table' FROM '/test.orc' WITH (FORMAT = 'orc') CONNECTION(ENDPOINT = 's3.storage.site', REGION = 'hz', ACCESS_KEY_ID='my_key_id', SECRET_ACCESS_KEY="my_access_key");"#
            ),
            r#"COPY 'my_table' FROM '/test.orc' WITH (FORMAT = 'orc') CONNECTION(ENDPOINT = 's3.storage.site', REGION = 'hz', ACCESS_KEY_ID='******', SECRET_ACCESS_KEY="******");"#
        );
        assert_eq!(
            redact_sql_secrets(
                r#"COPY 'my_table' FROM '/test.orc' WITH (FORMAT = 'orc') CONNECTION(ENDPOINT = 's3.storage.site', REGION = 'hz', ACCESS_KEY_ID='@scoped/key_id', SECRET_ACCESS_KEY="@scoped/access_key");"#
            ),
            r#"COPY 'my_table' FROM '/test.orc' WITH (FORMAT = 'orc') CONNECTION(ENDPOINT = 's3.storage.site', REGION = 'hz', ACCESS_KEY_ID='******', SECRET_ACCESS_KEY="******");"#
        );
    }
}
