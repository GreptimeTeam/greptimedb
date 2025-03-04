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

use std::sync::Arc;

use common_telemetry::trace;
use datafusion::arrow::array::{Array, ArrayRef, ListArray};
use datafusion::common::cast::{as_list_array, as_primitive_array};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Accumulator as DfAccumulator, AggregateUDF, Volatility};
use datafusion::physical_expr::ColumnarValue;
use datafusion::prelude::{create_udaf, ScalarValue};
use datatypes::arrow::datatypes::{
    DataType, Field, Float64Type, TimeUnit, TimestampNanosecondType,
};
use serde_json;

pub const GEO_PATH_NAME: &str = "geo_path";
pub const JSON_ENCODE_PATH_NAME: &str = "json_encode_path";

#[derive(Debug)]
pub struct GeoPathAccumulator {
    lat: Vec<Option<f64>>,
    lng: Vec<Option<f64>>,
    timestamp: Vec<Option<i64>>,
}

impl GeoPathAccumulator {
    pub fn new() -> Self {
        Self {
            lat: Vec::default(),
            lng: Vec::default(),
            timestamp: Vec::default(),
        }
    }

    pub fn udf_impl() -> AggregateUDF {
        create_udaf(
            JSON_ENCODE_PATH_NAME,
            // Input types: lat, lng, timestamp
            vec![
                DataType::Float64,
                DataType::Float64,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            ],
            // Output type: list of points [(lng, lat)]
            Arc::new(DataType::List(Arc::new(Field::new(
                "point",
                DataType::Struct(
                    vec![
                        Field::new("lng", DataType::Float64, false),
                        Field::new("lat", DataType::Float64, false),
                    ]
                    .into(),
                ),
                false,
            )))),
            Volatility::Immutable,
            // Create the accumulator
            Arc::new(|_| Ok(Box::new(GeoPathAccumulator::new()))),
            // Intermediate state types
            Arc::new(vec![
                DataType::List(Arc::new(Field::new("lng", DataType::Float64, false))),
                DataType::List(Arc::new(Field::new("lat", DataType::Float64, false))),
                DataType::List(Arc::new(Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ))),
            ]),
        )
    }
}

impl DfAccumulator for GeoPathAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion::error::Result<()> {
        if values.len() != 3 {
            return Err(DataFusionError::Internal(format!(
                "Expected 3 columns for geo_path, got {}",
                values.len()
            )));
        }

        let lat_array = as_primitive_array::<Float64Type>(&values[0])?;
        let lng_array = as_primitive_array::<Float64Type>(&values[1])?;
        let ts_array = as_primitive_array::<TimestampNanosecondType>(&values[2])?;

        let size = lat_array.len();

        for idx in 0..size {
            self.lat.push(if lat_array.is_null(idx) {
                None
            } else {
                Some(lat_array.value(idx))
            });

            self.lng.push(if lng_array.is_null(idx) {
                None
            } else {
                Some(lng_array.value(idx))
            });

            self.timestamp.push(if ts_array.is_null(idx) {
                None
            } else {
                Some(ts_array.value(idx))
            });
        }

        Ok(())
    }

    fn evaluate(&mut self) -> datafusion::error::Result<ScalarValue> {
        let mut work_vec: Vec<(&Option<f64>, &Option<f64>, &Option<i64>)> = self
            .lat
            .iter()
            .zip(self.lng.iter())
            .zip(self.timestamp.iter())
            .map(|((a, b), c)| (a, b, c))
            .collect();

        // Sort by timestamp, treat null timestamp as 0
        work_vec.sort_unstable_by_key(|tuple| tuple.2.unwrap_or(0));

        let result = serde_json::to_string(
            &work_vec
                .into_iter()
                // Note that we transform to lng,lat for geojson compatibility
                .map(|(lat, lng, _)| vec![lng, lat])
                .collect::<Vec<Vec<&Option<f64>>>>(),
        )
        .map_err(|e| {
            DataFusionError::Internal(format!("Failed to serialize geo path to JSON: {}", e))
        })?;

        Ok(ScalarValue::Utf8(Some(result)))
    }

    fn size(&self) -> usize {
        // Base size of GeoPathAccumulator struct fields
        let mut total_size = std::mem::size_of::<Self>();

        // Size of vectors (approximation)
        total_size += self.lat.capacity() * std::mem::size_of::<Option<f64>>();
        total_size += self.lng.capacity() * std::mem::size_of::<Option<f64>>();
        total_size += self.timestamp.capacity() * std::mem::size_of::<Option<i64>>();

        total_size
    }

    fn state(&mut self) -> datafusion::error::Result<Vec<ScalarValue>> {
        // Convert vectors to ScalarValues
        let lat_list = ScalarValue::List(
            Some(self.lat.iter().map(|v| ScalarValue::Float64(*v)).collect()),
            Arc::new(DataType::Float64),
        );

        let lng_list = ScalarValue::List(
            Some(self.lng.iter().map(|v| ScalarValue::Float64(*v)).collect()),
            Arc::new(DataType::Float64),
        );

        let timestamp_list = ScalarValue::List(
            Some(
                self.timestamp
                    .iter()
                    .map(|v| ScalarValue::TimestampNanosecond(*v, None))
                    .collect(),
            ),
            Arc::new(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        );

        Ok(vec![lat_list, lng_list, timestamp_list])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion::error::Result<()> {
        if states.len() != 3 {
            return Err(DataFusionError::Internal(format!(
                "Expected 3 states for geo_path, got {}",
                states.len()
            )));
        }

        // Process lat lists
        let lat_lists = as_list_array(&states[0])?;
        // Process lng lists
        let lng_lists = as_list_array(&states[1])?;
        // Process timestamp lists
        let ts_lists = as_list_array(&states[2])?;

        for i in 0..lat_lists.len() {
            if lat_lists.is_valid(i) {
                let lat_values = lat_lists.value(i);
                for j in 0..lat_values.len() {
                    if let Some(ScalarValue::Float64(val)) =
                        ColumnarValue::Array(lat_values).scalar_value(j)?
                    {
                        self.lat.push(val);
                    } else {
                        self.lat.push(None);
                    }
                }
            }

            if lng_lists.is_valid(i) {
                let lng_values = lng_lists.value(i);
                for j in 0..lng_values.len() {
                    if let Some(ScalarValue::Float64(val)) =
                        ColumnarValue::Array(lng_values).scalar_value(j)?
                    {
                        self.lng.push(val);
                    } else {
                        self.lng.push(None);
                    }
                }
            }

            if ts_lists.is_valid(i) {
                let ts_values = ts_lists.value(i);
                for j in 0..ts_values.len() {
                    if let Some(ScalarValue::TimestampNanosecond(val, _)) =
                        ColumnarValue::Array(ts_values).scalar_value(j)?
                    {
                        self.timestamp.push(val);
                    } else {
                        self.timestamp.push(None);
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Float64Array, TimestampNanosecondArray};
    use datafusion::scalar::ScalarValue;

    use super::*;

    #[test]
    fn test_geo_path_basic() {
        let mut accumulator = GeoPathAccumulator::new();

        // Create test data
        let lat_array = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]));
        let lng_array = Arc::new(Float64Array::from(vec![4.0, 5.0, 6.0]));
        let ts_array = Arc::new(TimestampNanosecondArray::from(vec![100, 200, 300]));

        // Update batch
        accumulator
            .update_batch(&[lat_array, lng_array, ts_array])
            .unwrap();

        // Evaluate
        let result = accumulator.evaluate().unwrap();
        if let ScalarValue::Utf8(Some(json)) = result {
            // Expected JSON: [[4.0,1.0],[5.0,2.0],[6.0,3.0]]
            let parsed: Vec<Vec<f64>> = serde_json::from_str(&json).unwrap();
            assert_eq!(parsed.len(), 3);
            assert_eq!(parsed[0], vec![4.0, 1.0]);
            assert_eq!(parsed[1], vec![5.0, 2.0]);
            assert_eq!(parsed[2], vec![6.0, 3.0]);
        } else {
            panic!("Expected Utf8 scalar value");
        }
    }

    #[test]
    fn test_geo_path_sort_by_timestamp() {
        let mut accumulator = GeoPathAccumulator::new();

        // Create test data with unordered timestamps
        let lat_array = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]));
        let lng_array = Arc::new(Float64Array::from(vec![4.0, 5.0, 6.0]));
        let ts_array = Arc::new(TimestampNanosecondArray::from(vec![300, 100, 200]));

        // Update batch
        accumulator
            .update_batch(&[lat_array, lng_array, ts_array])
            .unwrap();

        // Evaluate
        let result = accumulator.evaluate().unwrap();
        if let ScalarValue::Utf8(Some(json)) = result {
            // Expected JSON after sorting: [[5.0,2.0],[6.0,3.0],[4.0,1.0]]
            let parsed: Vec<Vec<f64>> = serde_json::from_str(&json).unwrap();
            assert_eq!(parsed.len(), 3);
            assert_eq!(parsed[0], vec![5.0, 2.0]); // timestamp 100
            assert_eq!(parsed[1], vec![6.0, 3.0]); // timestamp 200
            assert_eq!(parsed[2], vec![4.0, 1.0]); // timestamp 300
        } else {
            panic!("Expected Utf8 scalar value");
        }
    }

    #[test]
    fn test_geo_path_merge() {
        let mut accumulator1 = GeoPathAccumulator::new();
        let mut accumulator2 = GeoPathAccumulator::new();

        // Create test data for first accumulator
        let lat_array1 = Arc::new(Float64Array::from(vec![1.0]));
        let lng_array1 = Arc::new(Float64Array::from(vec![4.0]));
        let ts_array1 = Arc::new(TimestampNanosecondArray::from(vec![100]));

        // Create test data for second accumulator
        let lat_array2 = Arc::new(Float64Array::from(vec![2.0]));
        let lng_array2 = Arc::new(Float64Array::from(vec![5.0]));
        let ts_array2 = Arc::new(TimestampNanosecondArray::from(vec![200]));

        // Update batches
        accumulator1
            .update_batch(&[lat_array1, lng_array1, ts_array1])
            .unwrap();
        accumulator2
            .update_batch(&[lat_array2, lng_array2, ts_array2])
            .unwrap();

        // Get states
        let state1 = accumulator1.state().unwrap();
        let state2 = accumulator2.state().unwrap();

        // Create a merged accumulator
        let mut merged = GeoPathAccumulator::new();

        // Create list arrays from states
        let create_list_array = |sv: &ScalarValue| -> ArrayRef {
            match sv {
                ScalarValue::List(Some(values), _) => Arc::new(ListArray::from_iter_primitive::<
                    Float64Type,
                    _,
                >(vec![Some(
                    values
                        .iter()
                        .map(|v| match v {
                            ScalarValue::Float64(val) => *val,
                            _ => panic!("Expected Float64"),
                        })
                        .collect::<Vec<_>>(),
                )])),
                ScalarValue::List(None, _) => Arc::new(ListArray::from_iter_primitive::<
                    Float64Type,
                    _,
                >(vec![None::<Vec<f64>>])),
                _ => panic!("Expected List scalar value"),
            }
        };

        // Handle timestamp list separately
        let create_ts_list_array = |sv: &ScalarValue| -> ArrayRef {
            match sv {
                ScalarValue::List(Some(values), _) => {
                    let ts_values: Vec<Option<i64>> = values
                        .iter()
                        .map(|v| match v {
                            ScalarValue::TimestampNanosecond(val, _) => *val,
                            _ => panic!("Expected TimestampNanosecond"),
                        })
                        .collect();

                    Arc::new(
                        ListArray::from_iter_primitive::<TimestampNanosecondType, _>(vec![Some(
                            ts_values
                                .into_iter()
                                .map(|v| v.unwrap_or(0))
                                .collect::<Vec<_>>(),
                        )]),
                    )
                }
                _ => panic!("Expected List scalar value"),
            }
        };

        // Create arrays for merge
        let lat_lists = create_list_array(&state1[0]);
        let lng_lists = create_list_array(&state1[1]);
        let ts_lists = create_ts_list_array(&state1[2]);

        // Merge first batch
        merged
            .merge_batch(&[lat_lists, lng_lists, ts_lists])
            .unwrap();

        // Create arrays for second merge
        let lat_lists = create_list_array(&state2[0]);
        let lng_lists = create_list_array(&state2[1]);
        let ts_lists = create_ts_list_array(&state2[2]);

        // Merge second batch
        merged
            .merge_batch(&[lat_lists, lng_lists, ts_lists])
            .unwrap();

        // Evaluate merged result
        let result = merged.evaluate().unwrap();
        if let ScalarValue::Utf8(Some(json)) = result {
            // Expected JSON sorted by timestamp: [[4.0,1.0],[5.0,2.0]]
            let parsed: Vec<Vec<f64>> = serde_json::from_str(&json).unwrap();
            assert_eq!(parsed.len(), 2);
            assert_eq!(parsed[0], vec![4.0, 1.0]); // timestamp 100
            assert_eq!(parsed[1], vec![5.0, 2.0]); // timestamp 200
        } else {
            panic!("Expected Utf8 scalar value");
        }
    }
}
