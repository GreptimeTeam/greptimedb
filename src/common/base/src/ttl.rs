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

use std::fmt::Display;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Time To Live

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Default, Serialize, Deserialize)]
pub enum TimeToLive {
    /// immediately throw away on insert
    Immediate,
    /// Duration to keep the data, this duration should be non-zero
    #[serde(with = "humantime_serde")]
    Duration(Duration),
    /// Keep the data forever
    #[default]
    Forever,
}

impl Display for TimeToLive {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TimeToLive::Immediate => write!(f, "immediate"),
            TimeToLive::Duration(d) => write!(f, "Duration({})", d.as_secs()),
            TimeToLive::Forever => write!(f, "forever"),
        }
    }
}

impl TimeToLive {
    /// Parse a string into TimeToLive
    pub fn from_humantime_or_str(s: &str) -> Result<Self, String> {
        match s {
            "immediate" => Ok(TimeToLive::Immediate),
            "forever" | "" => Ok(TimeToLive::Forever),
            _ => {
                let d = humantime::parse_duration(s).map_err(|e| e.to_string())?;
                Ok(TimeToLive::Duration(d))
            }
        }
    }

    /// Print TimeToLive as string
    ///
    /// omit forever variant
    pub fn as_repr_opt(&self) -> Option<String> {
        match self {
            TimeToLive::Immediate => Some("immediate".to_string()),
            TimeToLive::Duration(d) => Some(humantime::format_duration(*d).to_string()),
            TimeToLive::Forever => None,
        }
    }

    pub fn is_immediate(&self) -> bool {
        matches!(self, TimeToLive::Immediate)
    }

    pub fn is_forever(&self) -> bool {
        matches!(self, TimeToLive::Forever)
    }

    pub fn get_duration(&self) -> Option<Duration> {
        match self {
            TimeToLive::Duration(d) => Some(*d),
            _ => None,
        }
    }
}

impl From<Duration> for TimeToLive {
    fn from(duration: Duration) -> Self {
        if duration.is_zero() {
            // compatibility with old code, and inline with cassandra's behavior when ttl set to 0
            TimeToLive::Forever
        } else {
            TimeToLive::Duration(duration)
        }
    }
}

impl From<Option<Duration>> for TimeToLive {
    fn from(duration: Option<Duration>) -> Self {
        match duration {
            Some(d) => TimeToLive::from(d),
            None => TimeToLive::Forever,
        }
    }
}

impl From<humantime::Duration> for TimeToLive {
    fn from(duration: humantime::Duration) -> Self {
        Self::from(*duration)
    }
}
