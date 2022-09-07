use core::default::Default;
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Copy, Serialize, Deserialize)]
pub struct Timestamp {
    value: i64,
    unit: TimeUnit,
}

impl Timestamp {
    pub fn new(value: i64, unit: TimeUnit) -> Self {
        Self { unit, value }
    }

    pub fn unit(&self) -> TimeUnit {
        self.unit
    }

    pub fn value(&self) -> i64 {
        self.value
    }

    pub fn convert_to(&self, unit: TimeUnit) -> i64 {
        // TODO(hl): May result into overflow
        self.value * self.unit.factor() / unit.factor()
    }
}

impl From<i64> for Timestamp {
    fn from(v: i64) -> Self {
        Self {
            value: v,
            unit: TimeUnit::Millisecond,
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeUnit {
    #[default]
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl TimeUnit {
    pub fn factor(&self) -> i64 {
        match self {
            TimeUnit::Second => 1_000_000_000,
            TimeUnit::Millisecond => 1_000_000,
            TimeUnit::Microsecond => 1_000,
            TimeUnit::Nanosecond => 1,
        }
    }
}

impl PartialOrd for Timestamp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (self.value * self.unit.factor()).partial_cmp(&(other.value * other.unit.factor()))
    }
}

impl Ord for Timestamp {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.value * self.unit.factor()).cmp(&(other.value * other.unit.factor()))
    }
}

impl PartialEq for Timestamp {
    fn eq(&self, other: &Self) -> bool {
        self.convert_to(TimeUnit::Nanosecond) == other.convert_to(TimeUnit::Nanosecond)
    }
}

impl Eq for Timestamp {}

impl Hash for Timestamp {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_i64(self.convert_to(TimeUnit::Nanosecond));
        state.finish();
    }
}
