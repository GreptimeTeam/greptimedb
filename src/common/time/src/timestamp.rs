use core::default::Default;
use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Copy, Hash, Serialize, Deserialize)]
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

    pub fn unify_to(&self, unit: TimeUnit) -> i64 {
        // TODO(hl): May result into overflow
        self.value * self.unit.factor() / unit.factor()
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
        self.unify_to(TimeUnit::Nanosecond) == other.unify_to(TimeUnit::Nanosecond)
    }
}

impl Eq for Timestamp {}
