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

use std::cmp::Ordering;
use std::fmt;
use std::str::FromStr;

use semver::Version as SemverVersion;

pub type Result<T> = std::result::Result<T, VersionError>;

#[derive(Debug)]
pub enum VersionError {
    ParseError(semver::Error),
}

impl fmt::Display for VersionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VersionError::ParseError(e) => write!(f, "Failed to parse version: {}", e),
        }
    }
}

impl std::error::Error for VersionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            VersionError::ParseError(e) => Some(e),
        }
    }
}

/// Represents a version that can be either a semantic version or a special version like "Current"
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Version {
    Semantic(SemverVersion),
    Current,
}

impl Version {
    pub fn parse(s: &str) -> Result<Self> {
        let lower = s.to_lowercase();
        if lower == "current" {
            Ok(Version::Current)
        } else {
            let inner = SemverVersion::parse(s).map_err(VersionError::ParseError)?;
            Ok(Version::Semantic(inner))
        }
    }

    #[allow(unused)]
    pub fn is_current(&self) -> bool {
        matches!(self, Version::Current)
    }

    pub fn to_semantic(&self) -> Option<SemverVersion> {
        match self {
            Version::Semantic(v) => Some(v.clone()),
            Version::Current => Self::current_version_from_cargo(),
        }
    }

    fn current_version_from_cargo() -> Option<SemverVersion> {
        const CARGO_PKG_VERSION: &str = env!("CARGO_PKG_VERSION");
        SemverVersion::parse(CARGO_PKG_VERSION).ok()
    }

    pub fn compare(&self, other: &Version) -> Ordering {
        let v1 = self.to_semantic();
        let v2 = other.to_semantic();

        match (v1, v2) {
            (Some(v1), Some(v2)) => v1.cmp(&v2),
            (None, _) | (_, None) => Ordering::Equal,
        }
    }
}

impl PartialOrd for Version {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(Ord::cmp(self, other))
    }
}

impl Ord for Version {
    fn cmp(&self, other: &Self) -> Ordering {
        self.compare(other)
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Version::Semantic(v) => write!(f, "{}", v),
            Version::Current => write!(f, "current"),
        }
    }
}

impl FromStr for Version {
    type Err = VersionError;

    fn from_str(s: &str) -> Result<Self> {
        Self::parse(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_semantic_version() {
        let v = Version::parse("0.15.0").unwrap();
        assert!(matches!(v, Version::Semantic(_)));
    }

    #[test]
    fn test_parse_current() {
        let v = Version::parse("current").unwrap();
        assert_eq!(v, Version::Current);

        let v = Version::parse("CURRENT").unwrap();
        assert_eq!(v, Version::Current);
    }

    #[test]
    fn test_semantic_comparison() {
        assert!(Version::parse("0.15.0").unwrap() < Version::parse("0.16.0").unwrap());
        assert!(Version::parse("1.0.0").unwrap() > Version::parse("0.15.0").unwrap());
        assert_eq!(
            Version::parse("0.15.0").unwrap(),
            Version::parse("0.15.0").unwrap()
        );
    }

    #[test]
    fn test_pre_release_comparison() {
        assert!(
            Version::parse("1.0.0-alpha.1").unwrap() < Version::parse("1.0.0-alpha.2").unwrap()
        );
        assert!(Version::parse("1.0.0-alpha.2").unwrap() < Version::parse("1.0.0-beta.1").unwrap());
        assert!(Version::parse("1.0.0-beta.2").unwrap() < Version::parse("1.0.0-beta.10").unwrap());
        assert!(Version::parse("1.0.0-beta.1").unwrap() < Version::parse("1.0.0-rc.1").unwrap());
        assert!(Version::parse("1.0.0-rc.1").unwrap() < Version::parse("1.0.0").unwrap());
    }

    #[test]
    fn test_current_comparison() {
        let current = Version::Current;
        assert!(current == Version::Current);

        let current_sem = current.to_semantic();
        assert!(current_sem.is_some());
    }
}
