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

use crate::Timestamp;

pub const INSTANT: &str = "instant";
pub const FOREVER: &str = "forever";

/// Time To Live
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeToLive {
    /// Instantly discard upon insert
    Instant,
    /// Keep the data forever
    #[default]
    Forever,
    /// Duration to keep the data, this duration should be non-zero
    #[serde(untagged, with = "humantime_serde")]
    Duration(Duration),
}

impl Display for TimeToLive {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TimeToLive::Instant => write!(f, "{}", INSTANT),
            TimeToLive::Duration(d) => write!(f, "{}", humantime::Duration::from(*d)),
            TimeToLive::Forever => write!(f, "{}", FOREVER),
        }
    }
}

impl TimeToLive {
    /// Parse a string that is either `instant`, `forever`, or a duration to `TimeToLive`
    ///
    /// note that an empty string or a zero duration(a duration that spans no time) is treat as `forever` too
    pub fn from_humantime_or_str(s: &str) -> Result<Self, String> {
        match s.to_lowercase().as_ref() {
            INSTANT => Ok(TimeToLive::Instant),
            FOREVER | "" => Ok(TimeToLive::Forever),
            _ => {
                let d = humantime::parse_duration(s).map_err(|e| e.to_string())?;
                Ok(TimeToLive::from(d))
            }
        }
    }

    /// Check if the TimeToLive is expired
    /// with the given `created_at` and `now` timestamp
    pub fn is_expired(
        &self,
        created_at: &Timestamp,
        now: &Timestamp,
    ) -> crate::error::Result<bool> {
        Ok(match self {
            TimeToLive::Instant => true,
            TimeToLive::Forever => false,
            TimeToLive::Duration(d) => now.sub_duration(*d)? > *created_at,
        })
    }

    /// Print TimeToLive as string
    pub fn to_string_opt(&self) -> Option<String> {
        match self {
            TimeToLive::Instant => Some(INSTANT.to_string()),
            TimeToLive::Duration(d) => Some(humantime::format_duration(*d).to_string()),
            TimeToLive::Forever => Some(FOREVER.to_string()),
        }
    }

    /// is instant variant
    pub fn is_instant(&self) -> bool {
        matches!(self, TimeToLive::Instant)
    }

    /// Is the default value, which is `Forever`
    pub fn is_forever(&self) -> bool {
        matches!(self, TimeToLive::Forever)
    }

    /// Get duration if it is a duration variant, otherwise return None
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

impl From<humantime::Duration> for TimeToLive {
    fn from(duration: humantime::Duration) -> Self {
        Self::from(*duration)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_serde() {
        let cases = vec![
            ("\"instant\"", TimeToLive::Instant),
            ("\"forever\"", TimeToLive::Forever),
            ("\"10d\"", Duration::from_secs(86400 * 10).into()),
            (
                "\"10000 years\"",
                humantime::parse_duration("10000 years").unwrap().into(),
            ),
        ];

        for (s, expected) in cases {
            let serialized = serde_json::to_string(&expected).unwrap();
            let deserialized: TimeToLive = serde_json::from_str(&serialized).unwrap();
            assert_eq!(deserialized, expected);

            let deserialized: TimeToLive = serde_json::from_str(s).unwrap_or_else(|err| {
                panic!("Actual serialized: {}, s=`{s}`, err: {:?}", serialized, err)
            });
            assert_eq!(deserialized, expected);
        }
    }
}
