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

use serde::de::Visitor;
use serde::{Deserialize, Serialize};

/// Time To Live

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Default)]
pub enum TimeToLive {
    /// immediately throw away on insert
    Immediate,
    /// Duration to keep the data, this duration should be non-zero
    Duration(Duration),
    /// Keep the data forever
    #[default]
    Forever,
    // TODO(discord9): add a new variant
    // that can't be overridden by database level ttl? call it ForceForever?
}

impl Serialize for TimeToLive {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Immediate => serializer.serialize_str("immediate"),
            Self::Duration(d) => humantime_serde::serialize(d, serializer),
            Self::Forever => serializer.serialize_str("forever"),
        }
    }
}

impl<'de> Deserialize<'de> for TimeToLive {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct StrVisitor;
        impl Visitor<'_> for StrVisitor {
            type Value = TimeToLive;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a string of time, 'immediate', 'forever' or null")
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E> {
                Ok(TimeToLive::Forever)
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                TimeToLive::from_humantime_or_str(value).map_err(serde::de::Error::custom)
            }
        }
        // deser a string or null
        let any = deserializer.deserialize_any(StrVisitor)?;
        Ok(any)
    }
}

impl Display for TimeToLive {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TimeToLive::Immediate => write!(f, "immediate"),
            TimeToLive::Duration(d) => write!(f, "{}", humantime::Duration::from(*d)),
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
                Ok(TimeToLive::from(d))
            }
        }
    }

    /// Print TimeToLive as string
    ///
    /// omit `Forever`` variant
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

    /// Is the default value, which is `Forever`
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_serde() {
        let cases = vec![
            ("\"immediate\"", TimeToLive::Immediate),
            ("\"forever\"", TimeToLive::Forever),
            ("\"10d\"", Duration::from_secs(86400 * 10).into()),
            (
                "\"10000 years\"",
                humantime::parse_duration("10000 years").unwrap().into(),
            ),
            ("null", TimeToLive::Forever),
        ];

        for (s, expected) in cases {
            let serialized = serde_json::to_string(&expected).unwrap();
            let deserialized: TimeToLive = serde_json::from_str(&serialized).unwrap();
            assert_eq!(deserialized, expected);

            let deserialized: TimeToLive = serde_json::from_str(s).unwrap();
            assert_eq!(deserialized, expected);
        }
    }
}
