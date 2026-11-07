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

#[cfg(test)]
mod tests;

use serde::{Deserialize, Deserializer, Serialize};
use snafu::ensure;
use sqlparser_derive::{Visit, VisitMut};

use crate::error::{InvalidSketchOptionSnafu, Result};

/// Metadata key for independently enabled HLL statistics.
pub const HLL_KEY: &str = "greptime:hll";

/// HLL options for a column. Metadata presence enables the algorithm.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Visit, VisitMut)]
#[serde(default)]
pub struct HllOptions {
    /// Base-two logarithm of the register count, in 4..=18; defaults to 10.
    #[serde(deserialize_with = "deserialize_hll_precision")]
    pub precision: u8,
}

impl HllOptions {
    /// Validates the supported algorithm parameter range.
    pub fn validate(&self) -> Result<()> {
        ensure!(
            (4..=18).contains(&self.precision),
            InvalidSketchOptionSnafu {
                algorithm: "HLL",
                reason: "precision must be in 4..=18"
            }
        );
        Ok(())
    }
}

impl Default for HllOptions {
    fn default() -> Self {
        Self { precision: 10 }
    }
}

/// Metadata key for independently enabled KLL statistics.
pub const KLL_KEY: &str = "greptime:kll";

/// KLL options for a column. Metadata presence enables the algorithm.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Visit, VisitMut)]
#[serde(default)]
pub struct KllOptions {
    /// Per-level capacity parameter, at least 8; defaults to 64.
    #[serde(deserialize_with = "deserialize_kll_capacity")]
    pub capacity: u16,
}

impl KllOptions {
    /// Validates the supported algorithm parameter range.
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.capacity >= 8,
            InvalidSketchOptionSnafu {
                algorithm: "KLL",
                reason: "capacity must be at least 8"
            }
        );
        Ok(())
    }
}

impl Default for KllOptions {
    fn default() -> Self {
        Self { capacity: 64 }
    }
}

/// Metadata key for independently enabled MG statistics.
pub const MG_KEY: &str = "greptime:mg";

/// MG options for a column. Metadata presence enables the algorithm.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Visit, VisitMut)]
#[serde(default)]
pub struct MgOptions {
    /// Maximum number of counters, greater than zero; defaults to 64.
    #[serde(deserialize_with = "deserialize_mg_capacity")]
    pub capacity: u32,
}

impl MgOptions {
    /// Validates the supported algorithm parameter range.
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.capacity > 0,
            InvalidSketchOptionSnafu {
                algorithm: "MG",
                reason: "capacity must be positive"
            }
        );
        Ok(())
    }
}

impl Default for MgOptions {
    fn default() -> Self {
        Self { capacity: 64 }
    }
}

fn deserialize_hll_precision<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<u8, D::Error> {
    let precision = u8::deserialize(deserializer)?;
    HllOptions { precision }
        .validate()
        .map_err(serde::de::Error::custom)?;
    Ok(precision)
}

fn deserialize_kll_capacity<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<u16, D::Error> {
    let capacity = u16::deserialize(deserializer)?;
    KllOptions { capacity }
        .validate()
        .map_err(serde::de::Error::custom)?;
    Ok(capacity)
}

fn deserialize_mg_capacity<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<u32, D::Error> {
    let capacity = u32::deserialize(deserializer)?;
    MgOptions { capacity }
        .validate()
        .map_err(serde::de::Error::custom)?;
    Ok(capacity)
}
