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

use puffin::blob_metadata::BlobMetadata;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use tantivy::tokenizer::{LowerCaser, SimpleTokenizer, TextAnalyzer, TokenizerManager};
use tantivy_jieba::JiebaTokenizer;
pub mod create;
pub mod error;
pub mod search;
pub mod tokenizer;

pub const KEY_FULLTEXT_CONFIG: &str = "fulltext_config";

use crate::fulltext_index::error::{DeserializeFromJsonSnafu, Result};

#[cfg(test)]
mod tests;

/// Configuration for fulltext index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Config {
    /// Analyzer to use for tokenization.
    pub analyzer: Analyzer,

    /// Whether the index should be case-sensitive.
    pub case_sensitive: bool,
}

/// Analyzer to use for tokenization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum Analyzer {
    #[default]
    English,

    Chinese,
}

impl Config {
    fn build_tantivy_tokenizer(&self) -> TokenizerManager {
        let mut builder = match self.analyzer {
            Analyzer::English => TextAnalyzer::builder(SimpleTokenizer::default()).dynamic(),
            Analyzer::Chinese => TextAnalyzer::builder(JiebaTokenizer {}).dynamic(),
        };

        if !self.case_sensitive {
            builder = builder.filter_dynamic(LowerCaser);
        }

        let tokenizer = builder.build();
        let tokenizer_manager = TokenizerManager::new();
        tokenizer_manager.register("default", tokenizer);
        tokenizer_manager
    }

    /// Extracts the fulltext index configuration from the blob metadata.
    pub fn from_blob_metadata(metadata: &BlobMetadata) -> Result<Self> {
        if let Some(config) = metadata.properties.get(KEY_FULLTEXT_CONFIG) {
            let config = serde_json::from_str(config).context(DeserializeFromJsonSnafu)?;
            return Ok(config);
        }

        Ok(Self::default())
    }
}

impl Analyzer {
    pub fn to_str(&self) -> &'static str {
        match self {
            Analyzer::English => "English",
            Analyzer::Chinese => "Chinese",
        }
    }
}
