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
#[serde(rename_all = "snake_case")]
pub enum Ttl {
    /// Immediately throw away on insert
    Immediate,
    /// Keep the data forever
    #[default]
    Forever,
    /// Duration to keep the data, this duration should be non-zero
    #[serde(untagged, with = "humantime_serde")]
    Duration(Duration),
}

impl Display for Ttl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Ttl::Immediate => write!(f, "immediate"),
            Ttl::Duration(d) => write!(f, "{}", humantime::Duration::from(*d)),
            Ttl::Forever => write!(f, "forever"),
        }
    }
}

impl Ttl {
    /// Parse a string that is either `immediate`, `forever`, or a duration to `TimeToLive`
    ///
    /// note that a empty string is treat as `forever` too
    pub fn from_humantime_or_str(s: &str) -> Result<Self, String> {
        match s {
            "immediate" => Ok(Ttl::Immediate),
            "forever" | "" => Ok(Ttl::Forever),
            _ => {
                let d = humantime::parse_duration(s).map_err(|e| e.to_string())?;
                Ok(Ttl::from(d))
            }
        }
    }

    /// Print TimeToLive as string
    pub fn as_repr_opt(&self) -> Option<String> {
        match self {
            Ttl::Immediate => Some("immediate".to_string()),
            Ttl::Duration(d) => Some(humantime::format_duration(*d).to_string()),
            Ttl::Forever => Some("forever".to_string()),
        }
    }

    pub fn is_immediate(&self) -> bool {
        matches!(self, Ttl::Immediate)
    }

    /// Is the default value, which is `Forever`
    pub fn is_forever(&self) -> bool {
        matches!(self, Ttl::Forever)
    }

    pub fn get_duration(&self) -> Option<Duration> {
        match self {
            Ttl::Duration(d) => Some(*d),
            _ => None,
        }
    }
}

impl From<Duration> for Ttl {
    fn from(duration: Duration) -> Self {
        if duration.is_zero() {
            // compatibility with old code, and inline with cassandra's behavior when ttl set to 0
            Ttl::Forever
        } else {
            Ttl::Duration(duration)
        }
    }
}

impl From<humantime::Duration> for Ttl {
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
            ("\"immediate\"", Ttl::Immediate),
            ("\"forever\"", Ttl::Forever),
            ("\"10d\"", Duration::from_secs(86400 * 10).into()),
            (
                "\"10000 years\"",
                humantime::parse_duration("10000 years").unwrap().into(),
            ),
        ];

        for (s, expected) in cases {
            let serialized = serde_json::to_string(&expected).unwrap();
            let deserialized: Ttl = serde_json::from_str(&serialized).unwrap();
            assert_eq!(deserialized, expected);

            let deserialized: Ttl = serde_json::from_str(s).unwrap_or_else(|err| {
                panic!("Actual serialized: {}, s=`{s}`, err: {:?}", serialized, err)
            });
            assert_eq!(deserialized, expected);
        }
    }
}
