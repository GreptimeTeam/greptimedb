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

use serde::{Deserialize, Serialize};

pub mod create;
pub mod error;
pub mod search;
pub mod tokenizer;

#[cfg(test)]
mod tests;

/// Configuration for fulltext index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Config {
    /// Analyzer to use for tokenization.
    pub analyzer: Analyzer,

    /// Whether the index should be case-sensitive.
    pub case_sensitive: bool,
}

/// Analyzer to use for tokenization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum Analyzer {
    #[default]
    English,

    Chinese,
}
