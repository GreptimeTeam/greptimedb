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

use jieba_rs::Jieba;

use crate::fulltext_index::error::Result;
use crate::fulltext_index::Bytes;

pub trait Tokenizer: Send {
    fn tokenize<'a>(&self, text: &'a str) -> Vec<&'a str>;
}

#[derive(Debug, Default)]
pub struct EnglishTokenizer;

impl Tokenizer for EnglishTokenizer {
    fn tokenize<'a>(&self, text: &'a str) -> Vec<&'a str> {
        text.split(|c: char| !c.is_alphabetic())
            .filter(|s| !s.is_empty())
            .collect()
    }
}

#[derive(Debug, Default)]
pub struct ChineseTokenizer;

impl Tokenizer for ChineseTokenizer {
    fn tokenize<'a>(&self, text: &'a str) -> Vec<&'a str> {
        let jieba = Jieba::new();
        jieba.cut(text, false)
    }
}

pub struct Analyzer {
    tokenizer: Box<dyn Tokenizer>,
    case_sensitive: bool,
}

impl Analyzer {
    pub fn new(tokenizer: Box<dyn Tokenizer>, case_sensitive: bool) -> Self {
        Self {
            tokenizer,
            case_sensitive,
        }
    }

    pub fn analyze_text(&self, text: &str) -> Result<Vec<Bytes>> {
        let res = self
            .tokenizer
            .tokenize(text)
            .iter()
            .map(|s| {
                if self.case_sensitive {
                    s.as_bytes().to_vec()
                } else {
                    s.to_lowercase().as_bytes().to_vec()
                }
            })
            .collect();
        Ok(res)
    }
}
