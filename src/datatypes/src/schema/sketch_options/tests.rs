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

use arrow::datatypes::Field;

use super::*;
use crate::data_type::ConcreteDataType;
use crate::schema::ColumnSchema;

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
            serde_json::from_str::<HllOptions>(&serde_json::to_string(&options).unwrap()).unwrap()
        );
    }
    for precision in [0, 3, 19, 255] {
        assert!(
            serde_json::from_str::<HllOptions>(&format!(r#"{{"precision":{precision}}}"#)).is_err()
        );
    }
    for capacity in [0, 7] {
        assert!(
            serde_json::from_str::<KllOptions>(&format!(r#"{{"capacity":{capacity}}}"#)).is_err()
        );
    }
    assert!(serde_json::from_str::<MgOptions>(r#"{"capacity":0}"#).is_err());
    assert!(serde_json::from_str::<MgOptions>(r#"{"capacity":4294967296}"#).is_err());
    for capacity in [8, u16::MAX] {
        let options = KllOptions { capacity };
        assert_eq!(
            options,
            serde_json::from_str::<KllOptions>(&serde_json::to_string(&options).unwrap()).unwrap()
        );
    }
    for capacity in [1, u32::MAX] {
        let options = MgOptions { capacity };
        assert_eq!(
            options,
            serde_json::from_str::<MgOptions>(&serde_json::to_string(&options).unwrap()).unwrap()
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
    assert_eq!(field.metadata()[MG_KEY], r#"{"capacity":64}"#);
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
    assert!(column.set_mg_options(&MgOptions { capacity: 0 }).is_err());
    assert_eq!(before, column);
    column
        .set_hll_options(&HllOptions { precision: 12 })
        .unwrap();
    column
        .set_kll_options(&KllOptions { capacity: 128 })
        .unwrap();
    column.set_mg_options(&MgOptions { capacity: 256 }).unwrap();
    assert_eq!(
        Some(HllOptions { precision: 12 }),
        column.hll_options().unwrap()
    );
    assert_eq!(
        Some(KllOptions { capacity: 128 }),
        column.kll_options().unwrap()
    );
    assert_eq!(
        Some(MgOptions { capacity: 256 }),
        column.mg_options().unwrap()
    );
    column.unset_hll_options().unwrap();
    assert_eq!(None, column.hll_options().unwrap());
    assert_eq!(
        Some(KllOptions { capacity: 128 }),
        column.kll_options().unwrap()
    );
    assert_eq!(
        Some(MgOptions { capacity: 256 }),
        column.mg_options().unwrap()
    );
    column.unset_kll_options().unwrap();
    assert_eq!(None, column.kll_options().unwrap());
    assert_eq!(
        Some(MgOptions { capacity: 256 }),
        column.mg_options().unwrap()
    );
    column.unset_mg_options().unwrap();
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
