// Copyright 2023-2026 GrepTime Inc.
//
// This file is part of the GreptimeDB Enterprise Edition and is licensed under
// the GreptimeDB Enterprise License. You may not use this file except in
// compliance with that license. A copy of the license is available at the root
// of this repository in the file LICENSE-ENTERPRISE.
//
// Unless required by applicable law or agreed to in writing, this software is
// distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied.

use serde::{Deserialize, Deserializer, Serialize};
use snafu::ensure;
use sqlparser_derive::{Visit, VisitMut};

use crate::error::{InvalidSketchOptionSnafu, Result};

/// HLL options for a column. Metadata presence enables the algorithm.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Visit, VisitMut)]
#[serde(default, rename_all = "kebab-case")]
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
                msg: format!("HLL precision must be in 4..=18, got {}", self.precision)
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

/// KLL options for a column. Metadata presence enables the algorithm.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Visit, VisitMut)]
#[serde(default, rename_all = "kebab-case")]
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
                msg: format!("KLL capacity must be at least 8, got {}", self.capacity)
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

/// MG options for a column. Metadata presence enables the algorithm.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Visit, VisitMut)]
#[serde(default, rename_all = "kebab-case")]
pub struct MgOptions {
    /// Maximum number of counters, greater than zero; defaults to 64.
    #[serde(deserialize_with = "deserialize_mg_max_counters")]
    pub max_counters: u32,
}

impl MgOptions {
    /// Validates the supported algorithm parameter range.
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.max_counters > 0,
            InvalidSketchOptionSnafu {
                msg: format!(
                    "MG max_counters must be positive, got {}",
                    self.max_counters
                )
            }
        );
        Ok(())
    }
}

impl Default for MgOptions {
    fn default() -> Self {
        Self { max_counters: 64 }
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

fn deserialize_mg_max_counters<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<u32, D::Error> {
    let max_counters = u32::deserialize(deserializer)?;
    MgOptions { max_counters }
        .validate()
        .map_err(serde::de::Error::custom)?;
    Ok(max_counters)
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::Field;

    use super::*;
    use crate::data_type::ConcreteDataType;
    use crate::schema::{ColumnSchema, HLL_KEY, KLL_KEY, MG_KEY};

    #[test]
    fn test_defaults_and_parameter_validation() {
        assert_eq!(
            HllOptions::default(),
            serde_json::from_str::<HllOptions>("{}").unwrap()
        );
        assert_eq!(
            KllOptions::default(),
            serde_json::from_str::<KllOptions>("{}").unwrap()
        );
        assert_eq!(
            MgOptions::default(),
            serde_json::from_str::<MgOptions>("{}").unwrap()
        );
        for precision in [4, 10, 18] {
            let options = HllOptions { precision };
            assert_eq!(
                options,
                serde_json::from_str::<HllOptions>(&serde_json::to_string(&options).unwrap())
                    .unwrap()
            );
        }
        for precision in [0, 3, 19, 255] {
            assert!(
                serde_json::from_str::<HllOptions>(&format!(r#"{{"precision":{precision}}}"#))
                    .is_err()
            );
        }
        for capacity in [0, 7] {
            assert!(
                serde_json::from_str::<KllOptions>(&format!(r#"{{"capacity":{capacity}}}"#))
                    .is_err()
            );
        }
        assert!(serde_json::from_str::<MgOptions>(r#"{"max-counters":0}"#).is_err());
        assert!(serde_json::from_str::<MgOptions>(r#"{"max-counters":4294967296}"#).is_err());
        for capacity in [8, u16::MAX] {
            let options = KllOptions { capacity };
            assert_eq!(
                options,
                serde_json::from_str::<KllOptions>(&serde_json::to_string(&options).unwrap())
                    .unwrap()
            );
        }
        for capacity in [1, u32::MAX] {
            let options = MgOptions {
                max_counters: capacity,
            };
            assert_eq!(
                options,
                serde_json::from_str::<MgOptions>(&serde_json::to_string(&options).unwrap())
                    .unwrap()
            );
        }
    }

    #[test]
    fn test_independent_metadata_and_roundtrip() {
        let mut column = ColumnSchema::new("v", ConcreteDataType::int64_datatype(), true);
        assert_eq!(None, column.hll_options().unwrap());
        assert_eq!(None, column.kll_options().unwrap());
        assert_eq!(None, column.mg_options().unwrap());
        column = column
            .with_hll_options(HllOptions::default())
            .unwrap()
            .with_kll_options(KllOptions::default())
            .unwrap()
            .with_mg_options(MgOptions::default())
            .unwrap();
        let field = Field::try_from(&column).unwrap();
        assert_eq!(field.metadata()[HLL_KEY], r#"{"precision":10}"#);
        assert_eq!(field.metadata()[KLL_KEY], r#"{"capacity":64}"#);
        assert_eq!(field.metadata()[MG_KEY], r#"{"max-counters":64}"#);
        assert_eq!(column, ColumnSchema::try_from(&field).unwrap());
        assert_eq!(
            column,
            serde_json::from_str::<ColumnSchema>(&serde_json::to_string(&column).unwrap()).unwrap()
        );
        let before = column.clone();
        assert!(
            column
                .set_hll_options(&HllOptions { precision: 3 })
                .is_err()
        );
        assert!(column.set_kll_options(&KllOptions { capacity: 7 }).is_err());
        assert!(
            column
                .set_mg_options(&MgOptions { max_counters: 0 })
                .is_err()
        );
        assert_eq!(before, column);
        column
            .set_hll_options(&HllOptions { precision: 12 })
            .unwrap();
        column
            .set_kll_options(&KllOptions { capacity: 128 })
            .unwrap();
        column
            .set_mg_options(&MgOptions { max_counters: 256 })
            .unwrap();
        assert_eq!(
            Some(HllOptions { precision: 12 }),
            column.hll_options().unwrap()
        );
        assert_eq!(
            Some(KllOptions { capacity: 128 }),
            column.kll_options().unwrap()
        );
        assert_eq!(
            Some(MgOptions { max_counters: 256 }),
            column.mg_options().unwrap()
        );
        column.unset_hll_options();
        assert_eq!(None, column.hll_options().unwrap());
        assert_eq!(
            Some(KllOptions { capacity: 128 }),
            column.kll_options().unwrap()
        );
        assert_eq!(
            Some(MgOptions { max_counters: 256 }),
            column.mg_options().unwrap()
        );
        column.unset_kll_options();
        assert_eq!(None, column.kll_options().unwrap());
        assert_eq!(
            Some(MgOptions { max_counters: 256 }),
            column.mg_options().unwrap()
        );
        column.unset_mg_options();
        assert_eq!(None, column.mg_options().unwrap());
    }

    #[test]
    fn test_invalid_persisted_options_are_rejected_by_getters() {
        let column = ColumnSchema::new("v", ConcreteDataType::int64_datatype(), true);
        let field = Field::try_from(&column).unwrap().with_metadata(
            [
                (HLL_KEY.to_string(), r#"{"precision":19}"#.to_string()),
                (KLL_KEY.to_string(), r#"{"capacity":0}"#.to_string()),
                (MG_KEY.to_string(), "malformed json".to_string()),
            ]
            .into(),
        );
        let column = ColumnSchema::try_from(&field).unwrap();
        assert!(column.hll_options().is_err());
        assert!(column.kll_options().is_err());
        assert!(column.mg_options().is_err());
    }

    #[test]
    fn test_mg_max_counters_serialization() {
        let expected = MgOptions { max_counters: 128 };
        assert_eq!(
            expected,
            serde_json::from_str::<MgOptions>(r#"{"max-counters":128}"#).unwrap()
        );
        assert_eq!(
            r#"{"max-counters":128}"#,
            serde_json::to_string(&expected).unwrap()
        );
        assert!(serde_json::from_str::<MgOptions>(r#"{"max-counters":0}"#).is_err());
    }

    #[test]
    fn test_validation_errors_include_values() {
        for (error, value) in [
            (HllOptions { precision: 19 }.validate().unwrap_err(), "19"),
            (KllOptions { capacity: 7 }.validate().unwrap_err(), "7"),
            (MgOptions { max_counters: 0 }.validate().unwrap_err(), "0"),
        ] {
            assert!(error.to_string().contains(value), "{error}");
        }
    }
}
