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

use std::hash::BuildHasher;

use ahash::RandomState;
use serde::{Deserialize, Serialize};

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

/// A random state with fixed seeds.
///
/// This is used to ensure that the hash values are consistent across
/// different processes, and easy to serialize and deserialize.
#[derive(Debug)]
pub struct FixedRandomState {
    state: RandomState,
}

impl FixedRandomState {
    // some random seeds
    const RANDOM_SEED_0: u64 = 0x517cc1b727220a95;
    const RANDOM_SEED_1: u64 = 0x428a2f98d728ae22;
    const RANDOM_SEED_2: u64 = 0x7137449123ef65cd;
    const RANDOM_SEED_3: u64 = 0xb5c0fbcfec4d3b2f;

    pub fn new() -> Self {
        Self {
            state: ahash::RandomState::with_seeds(
                Self::RANDOM_SEED_0,
                Self::RANDOM_SEED_1,
                Self::RANDOM_SEED_2,
                Self::RANDOM_SEED_3,
            ),
        }
    }
}

impl Default for FixedRandomState {
    fn default() -> Self {
        Self::new()
    }
}

impl BuildHasher for FixedRandomState {
    type Hasher = ahash::AHasher;

    fn build_hasher(&self) -> Self::Hasher {
        self.state.build_hasher()
    }

    fn hash_one<T: std::hash::Hash>(&self, x: T) -> u64 {
        self.state.hash_one(x)
    }
}

impl Serialize for FixedRandomState {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_unit()
    }
}

impl<'de> Deserialize<'de> for FixedRandomState {
    fn deserialize<D>(_deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(Self::new())
    }
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
