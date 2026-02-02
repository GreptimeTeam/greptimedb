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

pub use common_base::hash::{FixedRandomState, partition_rule_version};

/// Escapes special characters in the provided pattern string for `LIKE`.
///
/// Specifically, it prefixes the backslash (`\`), percent (`%`), and underscore (`_`)
/// characters with an additional backslash to ensure they are treated literally.
///
/// # Examples
///
/// ```rust
/// let escaped = escape_pattern("100%_some\\path");
/// assert_eq!(escaped, "100\\%\\_some\\\\path");
/// ```
pub fn escape_like_pattern(pattern: &str) -> String {
    pattern
        .chars()
        .flat_map(|c| match c {
            '\\' | '%' | '_' => vec!['\\', c],
            _ => vec![c],
        })
        .collect::<String>()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escape_like_pattern() {
        assert_eq!(
            escape_like_pattern("100%_some\\path"),
            "100\\%\\_some\\\\path"
        );
        assert_eq!(escape_like_pattern(""), "");
        assert_eq!(escape_like_pattern("hello"), "hello");
        assert_eq!(escape_like_pattern("\\%_"), "\\\\\\%\\_");
        assert_eq!(escape_like_pattern("%%__\\\\"), "\\%\\%\\_\\_\\\\\\\\");
        assert_eq!(escape_like_pattern("abc123"), "abc123");
        assert_eq!(escape_like_pattern("%_\\"), "\\%\\_\\\\");
        assert_eq!(
            escape_like_pattern("%%__\\\\another%string"),
            "\\%\\%\\_\\_\\\\\\\\another\\%string"
        );
        assert_eq!(escape_like_pattern("foo%bar_"), "foo\\%bar\\_");
        assert_eq!(escape_like_pattern("\\_\\%"), "\\\\\\_\\\\\\%");
    }
}
