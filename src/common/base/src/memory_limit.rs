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
/// - Unlimited: "0"
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MemoryLimit {
    /// Absolute memory size.
    Size(ReadableSize),
    /// Percentage of total system memory (0.0 to 1.0).
    Percentage(f64),
}

// Safe to implement Eq because percentage values are always in valid range (0.0-1.0)
// and won't be NaN or Infinity.
impl Eq for MemoryLimit {}

impl MemoryLimit {
    /// Resolve the memory limit to bytes based on total system memory.
    /// Returns 0 if the limit is disabled (Size(0) or Percentage(0)).
    pub fn resolve(&self, total_memory_bytes: u64) -> u64 {
        match self {
            MemoryLimit::Size(size) => size.as_bytes(),
            MemoryLimit::Percentage(pct) => {
                if *pct <= 0.0 {
                    0
                } else {
                    (total_memory_bytes as f64 * pct) as u64
                }
            }
        }
    }

    /// Returns true if this limit is disabled (0 bytes or 0%).
    pub fn is_unlimited(&self) -> bool {
        match self {
            MemoryLimit::Size(size) => size.as_bytes() == 0,
            MemoryLimit::Percentage(pct) => *pct <= 0.0,
        }
    }
}

impl Default for MemoryLimit {
    fn default() -> Self {
        MemoryLimit::Size(ReadableSize(0))
    }
}

impl FromStr for MemoryLimit {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();

        if let Some(pct_str) = s.strip_suffix('%') {
            let pct = pct_str
                .trim()
                .parse::<f64>()
                .map_err(|e| format!("invalid percentage value '{}': {}", pct_str, e))?;

            if !(0.0..=100.0).contains(&pct) {
                return Err(format!("percentage must be between 0 and 100, got {}", pct));
            }

            Ok(MemoryLimit::Percentage(pct / 100.0))
        } else {
            let size = ReadableSize::from_str(s)?;
            Ok(MemoryLimit::Size(size))
        }
    }
}

impl Display for MemoryLimit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MemoryLimit::Size(size) => write!(f, "{}", size),
            MemoryLimit::Percentage(pct) => write!(f, "{}%", pct * 100.0),
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
        assert_eq!(
            "0".parse::<MemoryLimit>().unwrap(),
            MemoryLimit::Size(ReadableSize(0))
        );
    }

    #[test]
    fn test_parse_percentage() {
        assert_eq!(
            "50%".parse::<MemoryLimit>().unwrap(),
            MemoryLimit::Percentage(0.5)
        );
        assert_eq!(
            "75%".parse::<MemoryLimit>().unwrap(),
            MemoryLimit::Percentage(0.75)
        );
        assert_eq!(
            "0%".parse::<MemoryLimit>().unwrap(),
            MemoryLimit::Percentage(0.0)
        );
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
            MemoryLimit::Percentage(0.5).resolve(total),
            4 * 1024 * 1024 * 1024
        );
        assert_eq!(MemoryLimit::Size(ReadableSize(0)).resolve(total), 0);
        assert_eq!(MemoryLimit::Percentage(0.0).resolve(total), 0);
    }

    #[test]
    fn test_is_unlimited() {
        assert!(MemoryLimit::Size(ReadableSize(0)).is_unlimited());
        assert!(MemoryLimit::Percentage(0.0).is_unlimited());
        assert!(!MemoryLimit::Size(ReadableSize(1024)).is_unlimited());
        assert!(!MemoryLimit::Percentage(0.5).is_unlimited());
    }

    #[test]
    fn test_parse_100_percent() {
        assert_eq!(
            "100%".parse::<MemoryLimit>().unwrap(),
            MemoryLimit::Percentage(1.0)
        );
    }

    #[test]
    fn test_parse_decimal_percentage() {
        assert_eq!(
            "20.5%".parse::<MemoryLimit>().unwrap(),
            MemoryLimit::Percentage(0.205)
        );
    }

    #[test]
    fn test_display_integer_percentage() {
        assert_eq!(MemoryLimit::Percentage(0.2).to_string(), "20%");
        assert_eq!(MemoryLimit::Percentage(0.5).to_string(), "50%");
    }

    #[test]
    fn test_display_decimal_percentage() {
        assert_eq!(MemoryLimit::Percentage(0.205).to_string(), "20.5%");
    }
}
