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

use common_config::wal::KafkaWalTopic as Topic;

/// Things need to bo inserted at the front or the back of the topic.
#[derive(Debug, Default)]
pub enum Affix {
    /// Inserts a provided string to each topic.
    Fixed(String),
    /// Computes the current time for each topic and inserts it into the topic.
    TimeNow,
    /// Nothing to be inserted.
    #[default]
    Nothing,
}

impl ToString for Affix {
    fn to_string(&self) -> String {
        match self {
            Affix::Fixed(s) => s.to_string(),
            Affix::TimeNow => chrono::Local::now().timestamp_micros().to_string(),
            Affix::Nothing => String::default(),
        }
    }
}

/// Decorates a topic with the given prefix and suffix.
pub struct TopicDecorator {
    /// A prefix to be inserted at the front of each topic.
    prefix: Affix,
    /// A suffix to be inserted at the back of each topic.
    suffix: Affix,
}

impl Default for TopicDecorator {
    fn default() -> Self {
        Self {
            prefix: Affix::Nothing,
            suffix: Affix::Nothing,
        }
    }
}

impl TopicDecorator {
    /// Overrides the current prefix with the given prefix.
    pub fn with_prefix(self, prefix: Affix) -> Self {
        Self { prefix, ..self }
    }

    /// Overrides the current suffix with the given suffix.
    pub fn with_suffix(self, suffix: Affix) -> Self {
        Self { suffix, ..self }
    }

    /// Builds a topic by inserting a prefix and a suffix into the given topic.
    pub fn decorate(&self, topic: &str) -> Topic {
        format!(
            "{}_{}_{}",
            self.prefix.to_string(),
            topic,
            self.suffix.to_string()
        )
    }
}
