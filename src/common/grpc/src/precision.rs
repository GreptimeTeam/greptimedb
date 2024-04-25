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

use common_time::timestamp::TimeUnit;

use crate::Error;

/// Precision represents the precision of a timestamp.
/// It is used to convert timestamps between different precisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Precision {
    Nanosecond,
    Microsecond,
    Millisecond,
    Second,
    Minute,
    Hour,
}

impl Precision {
    pub fn to_nanos(&self, amount: i64) -> Option<i64> {
        match self {
            Precision::Nanosecond => Some(amount),
            Precision::Microsecond => amount.checked_mul(1_000),
            Precision::Millisecond => amount.checked_mul(1_000_000),
            Precision::Second => amount.checked_mul(1_000_000_000),
            Precision::Minute => amount
                .checked_mul(60)
                .and_then(|a| a.checked_mul(1_000_000_000)),
            Precision::Hour => amount
                .checked_mul(3600)
                .and_then(|a| a.checked_mul(1_000_000_000)),
        }
    }

    pub fn to_millis(&self, amount: i64) -> Option<i64> {
        match self {
            Precision::Nanosecond => amount.checked_div(1_000_000),
            Precision::Microsecond => amount.checked_div(1_000),
            Precision::Millisecond => Some(amount),
            Precision::Second => amount.checked_mul(1_000),
            Precision::Minute => amount.checked_mul(60_000),
            Precision::Hour => amount.checked_mul(3_600_000),
        }
    }
}

impl Display for Precision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Precision::Nanosecond => write!(f, "Precision::Nanosecond"),
            Precision::Microsecond => write!(f, "Precision::Microsecond"),
            Precision::Millisecond => write!(f, "Precision::Millisecond"),
            Precision::Second => write!(f, "Precision::Second"),
            Precision::Minute => write!(f, "Precision::Minute"),
            Precision::Hour => write!(f, "Precision::Hour"),
        }
    }
}

impl TryFrom<Precision> for TimeUnit {
    type Error = Error;

    fn try_from(precision: Precision) -> Result<Self, Self::Error> {
        Ok(match precision {
            Precision::Second => TimeUnit::Second,
            Precision::Millisecond => TimeUnit::Millisecond,
            Precision::Microsecond => TimeUnit::Microsecond,
            Precision::Nanosecond => TimeUnit::Nanosecond,
            _ => {
                return Err(Error::NotSupported {
                    feat: format!("convert {precision} into TimeUnit"),
                })
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::precision::Precision;

    #[test]
    fn test_to_nanos() {
        assert_eq!(Precision::Nanosecond.to_nanos(1).unwrap(), 1);
        assert_eq!(Precision::Microsecond.to_nanos(1).unwrap(), 1_000);
        assert_eq!(Precision::Millisecond.to_nanos(1).unwrap(), 1_000_000);
        assert_eq!(Precision::Second.to_nanos(1).unwrap(), 1_000_000_000);
        assert_eq!(Precision::Minute.to_nanos(1).unwrap(), 60 * 1_000_000_000);
        assert_eq!(
            Precision::Hour.to_nanos(1).unwrap(),
            60 * 60 * 1_000_000_000
        );
    }

    #[test]
    fn test_to_millis() {
        assert_eq!(Precision::Nanosecond.to_millis(1_000_000).unwrap(), 1);
        assert_eq!(Precision::Microsecond.to_millis(1_000).unwrap(), 1);
        assert_eq!(Precision::Millisecond.to_millis(1).unwrap(), 1);
        assert_eq!(Precision::Second.to_millis(1).unwrap(), 1_000);
        assert_eq!(Precision::Minute.to_millis(1).unwrap(), 60 * 1_000);
        assert_eq!(Precision::Hour.to_millis(1).unwrap(), 60 * 60 * 1_000);
    }

    #[test]
    fn test_to_nanos_basic() {
        assert_eq!(Precision::Second.to_nanos(1), Some(1_000_000_000));
        assert_eq!(Precision::Minute.to_nanos(1), Some(60 * 1_000_000_000));
    }

    #[test]
    fn test_to_millis_basic() {
        assert_eq!(Precision::Second.to_millis(1), Some(1_000));
        assert_eq!(Precision::Minute.to_millis(1), Some(60_000));
    }

    #[test]
    fn test_to_nanos_overflow() {
        assert_eq!(Precision::Hour.to_nanos(i64::MAX / 100), None);
    }

    #[test]
    fn test_zero_input() {
        assert_eq!(Precision::Second.to_nanos(0), Some(0));
        assert_eq!(Precision::Minute.to_millis(0), Some(0));
    }
}
