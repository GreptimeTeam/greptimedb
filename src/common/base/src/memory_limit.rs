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

use std::fmt::{self, Display};
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::readable_size::ReadableSize;

/// Memory limit configuration that supports both absolute size and percentage.
///
/// Examples:
/// - Absolute size: "2GB", "4GiB", "512MB"
/// - Percentage: "50%", "75%"
/// - Unlimited: "unlimited", "0"
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MemoryLimit {
    /// Absolute memory size.
    Size(ReadableSize),
    /// Percentage of total system memory (0-100).
    Percentage(u8),
    /// No memory limit.
    #[default]
    Unlimited,
}

impl MemoryLimit {
    /// Resolve the memory limit to bytes based on total system memory.
    /// Returns 0 if the limit is unlimited.
    pub fn resolve(&self, total_memory_bytes: u64) -> u64 {
        match self {
            MemoryLimit::Size(size) => size.as_bytes(),
            MemoryLimit::Percentage(pct) => total_memory_bytes * (*pct as u64) / 100,
            MemoryLimit::Unlimited => 0,
        }
    }

    /// Returns true if this limit is unlimited.
    pub fn is_unlimited(&self) -> bool {
        match self {
            MemoryLimit::Size(size) => size.as_bytes() == 0,
            MemoryLimit::Percentage(pct) => *pct == 0,
            MemoryLimit::Unlimited => true,
        }
    }
}

impl FromStr for MemoryLimit {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();

        if s.eq_ignore_ascii_case("unlimited") {
            return Ok(MemoryLimit::Unlimited);
        }

        if let Some(pct_str) = s.strip_suffix('%') {
            let pct = pct_str
                .trim()
                .parse::<u8>()
                .map_err(|e| format!("invalid percentage value '{}': {}", pct_str, e))?;

            if pct > 100 {
                return Err(format!("percentage must be between 0 and 100, got {}", pct));
            }

            if pct == 0 {
                Ok(MemoryLimit::Unlimited)
            } else {
                Ok(MemoryLimit::Percentage(pct))
            }
        } else {
            let size = ReadableSize::from_str(s)?;
            if size.as_bytes() == 0 {
                Ok(MemoryLimit::Unlimited)
            } else {
                Ok(MemoryLimit::Size(size))
            }
        }
    }
}

impl Display for MemoryLimit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MemoryLimit::Size(size) => write!(f, "{}", size),
            MemoryLimit::Percentage(pct) => write!(f, "{}%", pct),
            MemoryLimit::Unlimited => write!(f, "unlimited"),
        }
    }
}

impl Serialize for MemoryLimit {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for MemoryLimit {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        MemoryLimit::from_str(&s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_absolute_size() {
        assert_eq!(
            "2GB".parse::<MemoryLimit>().unwrap(),
            MemoryLimit::Size(ReadableSize(2 * 1024 * 1024 * 1024))
        );
        assert_eq!(
            "512MB".parse::<MemoryLimit>().unwrap(),
            MemoryLimit::Size(ReadableSize(512 * 1024 * 1024))
        );
        assert_eq!("0".parse::<MemoryLimit>().unwrap(), MemoryLimit::Unlimited);
    }

    #[test]
    fn test_parse_percentage() {
        assert_eq!(
            "50%".parse::<MemoryLimit>().unwrap(),
            MemoryLimit::Percentage(50)
        );
        assert_eq!(
            "75%".parse::<MemoryLimit>().unwrap(),
            MemoryLimit::Percentage(75)
        );
        assert_eq!("0%".parse::<MemoryLimit>().unwrap(), MemoryLimit::Unlimited);
    }

    #[test]
    fn test_parse_invalid() {
        assert!("150%".parse::<MemoryLimit>().is_err());
        assert!("-10%".parse::<MemoryLimit>().is_err());
        assert!("invalid".parse::<MemoryLimit>().is_err());
    }

    #[test]
    fn test_resolve() {
        let total = 8 * 1024 * 1024 * 1024; // 8GB

        assert_eq!(
            MemoryLimit::Size(ReadableSize(2 * 1024 * 1024 * 1024)).resolve(total),
            2 * 1024 * 1024 * 1024
        );
        assert_eq!(
            MemoryLimit::Percentage(50).resolve(total),
            4 * 1024 * 1024 * 1024
        );
        assert_eq!(MemoryLimit::Unlimited.resolve(total), 0);
    }

    #[test]
    fn test_is_unlimited() {
        assert!(MemoryLimit::Unlimited.is_unlimited());
        assert!(!MemoryLimit::Size(ReadableSize(1024)).is_unlimited());
        assert!(!MemoryLimit::Percentage(50).is_unlimited());
        assert!(!MemoryLimit::Percentage(1).is_unlimited());

        // Defensive: these states shouldn't exist via public API, but check anyway
        assert!(MemoryLimit::Size(ReadableSize(0)).is_unlimited());
        assert!(MemoryLimit::Percentage(0).is_unlimited());
    }

    #[test]
    fn test_parse_100_percent() {
        assert_eq!(
            "100%".parse::<MemoryLimit>().unwrap(),
            MemoryLimit::Percentage(100)
        );
    }

    #[test]
    fn test_display_percentage() {
        assert_eq!(MemoryLimit::Percentage(20).to_string(), "20%");
        assert_eq!(MemoryLimit::Percentage(50).to_string(), "50%");
        assert_eq!(MemoryLimit::Percentage(100).to_string(), "100%");
    }

    #[test]
    fn test_parse_unlimited() {
        assert_eq!(
            "unlimited".parse::<MemoryLimit>().unwrap(),
            MemoryLimit::Unlimited
        );
        assert_eq!(
            "UNLIMITED".parse::<MemoryLimit>().unwrap(),
            MemoryLimit::Unlimited
        );
        assert_eq!(
            "Unlimited".parse::<MemoryLimit>().unwrap(),
            MemoryLimit::Unlimited
        );
    }

    #[test]
    fn test_display_unlimited() {
        assert_eq!(MemoryLimit::Unlimited.to_string(), "unlimited");
    }

    #[test]
    fn test_parse_display_roundtrip() {
        let cases = vec![
            "50%",
            "100%",
            "1%",
            "2GB",
            "512MB",
            "unlimited",
            "UNLIMITED",
            "0",  // normalized to unlimited
            "0%", // normalized to unlimited
        ];

        for input in cases {
            let parsed = input.parse::<MemoryLimit>().unwrap();
            let displayed = parsed.to_string();
            let reparsed = displayed.parse::<MemoryLimit>().unwrap();
            assert_eq!(
                parsed, reparsed,
                "round-trip failed: '{}' -> '{}' -> '{:?}'",
                input, displayed, reparsed
            );
        }
    }

    #[test]
    fn test_zero_normalization() {
        // All forms of zero should normalize to Unlimited
        assert_eq!("0".parse::<MemoryLimit>().unwrap(), MemoryLimit::Unlimited);
        assert_eq!("0%".parse::<MemoryLimit>().unwrap(), MemoryLimit::Unlimited);
        assert_eq!("0B".parse::<MemoryLimit>().unwrap(), MemoryLimit::Unlimited);
        assert_eq!(
            "0KB".parse::<MemoryLimit>().unwrap(),
            MemoryLimit::Unlimited
        );

        // Unlimited always displays as "unlimited"
        assert_eq!(MemoryLimit::Unlimited.to_string(), "unlimited");
    }
}
