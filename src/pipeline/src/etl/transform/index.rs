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

const INDEX_TIMESTAMP: &str = "timeindex";
const INDEX_TAG: &str = "tag";
const INDEX_FULLTEXT: &str = "fulltext";

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Index {
    TimeIndex,
    Tag,
    Fulltext,
}

impl std::fmt::Display for Index {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let index = match self {
            Index::TimeIndex => INDEX_TIMESTAMP,
            Index::Tag => INDEX_TAG,
            Index::Fulltext => INDEX_FULLTEXT,
        };

        write!(f, "{}", index)
    }
}

impl TryFrom<String> for Index {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Index::try_from(value.as_str())
    }
}

impl TryFrom<&str> for Index {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            INDEX_TIMESTAMP => Ok(Index::TimeIndex),
            INDEX_TAG => Ok(Index::Tag),
            INDEX_FULLTEXT => Ok(Index::Fulltext),
            _ => Err(format!("unsupported index type: {}", value)),
        }
    }
}
