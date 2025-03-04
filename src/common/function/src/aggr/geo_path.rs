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

use datafusion::arrow::array::{Array, ArrayRef};
use datafusion::common::cast::as_primitive_array;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::{Accumulator as DfAccumulator, AggregateUDF, Volatility};
use datafusion::prelude::create_udaf;
use datafusion_common::cast::{as_list_array, as_struct_array};
use datafusion_common::utils::SingleRowListArrayBuilder;
use datafusion_common::ScalarValue;
use datatypes::arrow::array::{Float64Array, Int64Array, ListArray, StructArray};
use datatypes::arrow::datatypes::{
    DataType, Field, Float64Type, Int64Type, TimeUnit, TimestampNanosecondType,
};
use datatypes::compute::{self, sort_to_indices};

pub const GEO_PATH_NAME: &str = "geo_path";

const LATITUDE_FIELD: &str = "lat";
const LONGITUDE_FIELD: &str = "lng";
const TIMESTAMP_FIELD: &str = "timestamp";
const DEFAULT_LIST_FIELD_NAME: &str = "item";

#[derive(Debug, Default)]
pub struct GeoPathAccumulator {
    lat: Vec<Option<f64>>,
    lng: Vec<Option<f64>>,
    timestamp: Vec<Option<i64>>,
}

impl GeoPathAccumulator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn udf_impl() -> AggregateUDF {
        create_udaf(
            GEO_PATH_NAME,
            // Input types: lat, lng, timestamp
            vec![
                DataType::Float64,
                DataType::Float64,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            ],
            // Output type: list of points {[lat], [lng]}
            Arc::new(DataType::Struct(
                vec![
                    Field::new(
                        LATITUDE_FIELD,
                        DataType::List(Arc::new(Field::new(
                            DEFAULT_LIST_FIELD_NAME,
                            DataType::Float64,
                            true,
                        ))),
                        false,
                    ),
                    Field::new(
                        LONGITUDE_FIELD,
                        DataType::List(Arc::new(Field::new(
                            DEFAULT_LIST_FIELD_NAME,
                            DataType::Float64,
                            true,
                        ))),
                        false,
                    ),
                ]
                .into(),
            )),
            Volatility::Immutable,
            // Create the accumulator
            Arc::new(|_| Ok(Box::new(GeoPathAccumulator::new()))),
            // Intermediate state types
            Arc::new(vec![DataType::Struct(
                vec![
                    Field::new(
                        LATITUDE_FIELD,
                        DataType::List(Arc::new(Field::new(
                            DEFAULT_LIST_FIELD_NAME,
                            DataType::Float64,
                            true,
                        ))),
                        false,
                    ),
                    Field::new(
                        LONGITUDE_FIELD,
                        DataType::List(Arc::new(Field::new(
                            DEFAULT_LIST_FIELD_NAME,
                            DataType::Float64,
                            true,
                        ))),
                        false,
                    ),
                    Field::new(
                        TIMESTAMP_FIELD,
                        DataType::List(Arc::new(Field::new(
                            DEFAULT_LIST_FIELD_NAME,
                            DataType::Int64,
                            true,
                        ))),
                        false,
                    ),
                ]
                .into(),
            )]),
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
        self.lat.reserve(size);
        self.lng.reserve(size);

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

    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        let unordered_lng_array = Float64Array::from(self.lng.clone());
        let unordered_lat_array = Float64Array::from(self.lat.clone());
        let ts_array = Int64Array::from(self.timestamp.clone());

        let ordered_indices = sort_to_indices(&ts_array, None, None)?;
        let lat_array = compute::take(&unordered_lat_array, &ordered_indices, None)?;
        let lng_array = compute::take(&unordered_lng_array, &ordered_indices, None)?;

        let lat_list = Arc::new(SingleRowListArrayBuilder::new(lat_array).build_list_array());
        let lng_list = Arc::new(SingleRowListArrayBuilder::new(lng_array).build_list_array());

        let result = ScalarValue::Struct(Arc::new(StructArray::new(
            vec![
                Field::new(
                    LATITUDE_FIELD,
                    DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                    false,
                ),
                Field::new(
                    LONGITUDE_FIELD,
                    DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                    false,
                ),
            ]
            .into(),
            vec![lat_list, lng_list],
            None,
        )));

        Ok(result)
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
        let lat_array = Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>(vec![
            Some(self.lat.clone()),
        ]));
        let lng_array = Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>(vec![
            Some(self.lng.clone()),
        ]));
        let ts_array = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            Some(self.timestamp.clone()),
        ]));

        let state_struct = StructArray::new(
            vec![
                Field::new(
                    LATITUDE_FIELD,
                    DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                    false,
                ),
                Field::new(
                    LONGITUDE_FIELD,
                    DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                    false,
                ),
                Field::new(
                    TIMESTAMP_FIELD,
                    DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                    false,
                ),
            ]
            .into(),
            vec![lat_array, lng_array, ts_array],
            None,
        );

        Ok(vec![ScalarValue::Struct(Arc::new(state_struct))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion::error::Result<()> {
        if states.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "Expected 1 states for geo_path, got {}",
                states.len()
            )));
        }

        for state in states {
            let state = as_struct_array(state)?;
            let lat_list = as_list_array(state.column(0))?.value(0);
            let lat_array = as_primitive_array::<Float64Type>(&lat_list)?;
            let lng_list = as_list_array(state.column(1))?.value(0);
            let lng_array = as_primitive_array::<Float64Type>(&lng_list)?;
            let ts_list = as_list_array(state.column(2))?.value(0);
            let ts_array = as_primitive_array::<Int64Type>(&ts_list)?;

            self.lat.extend(lat_array);
            self.lng.extend(lng_array);
            self.timestamp.extend(ts_array);
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
        if let ScalarValue::Struct(struct_array) = result {
            // Verify structure
            let fields = struct_array.fields().clone();
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0].name(), LATITUDE_FIELD);
            assert_eq!(fields[1].name(), LONGITUDE_FIELD);

            // Verify data
            let columns = struct_array.columns();
            assert_eq!(columns.len(), 2);

            // Check latitude values
            let lat_list = as_list_array(&columns[0]).unwrap().value(0);
            let lat_array = as_primitive_array::<Float64Type>(&lat_list).unwrap();
            assert_eq!(lat_array.len(), 3);
            assert_eq!(lat_array.value(0), 1.0);
            assert_eq!(lat_array.value(1), 2.0);
            assert_eq!(lat_array.value(2), 3.0);

            // Check longitude values
            let lng_list = as_list_array(&columns[1]).unwrap().value(0);
            let lng_array = as_primitive_array::<Float64Type>(&lng_list).unwrap();
            assert_eq!(lng_array.len(), 3);
            assert_eq!(lng_array.value(0), 4.0);
            assert_eq!(lng_array.value(1), 5.0);
            assert_eq!(lng_array.value(2), 6.0);
        } else {
            panic!("Expected Struct scalar value");
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
        if let ScalarValue::Struct(struct_array) = result {
            // Extract arrays
            let columns = struct_array.columns();

            // Check latitude values
            let lat_list = as_list_array(&columns[0]).unwrap().value(0);
            let lat_array = as_primitive_array::<Float64Type>(&lat_list).unwrap();
            assert_eq!(lat_array.len(), 3);
            assert_eq!(lat_array.value(0), 2.0); // timestamp 100
            assert_eq!(lat_array.value(1), 3.0); // timestamp 200
            assert_eq!(lat_array.value(2), 1.0); // timestamp 300

            // Check longitude values (should be sorted by timestamp)
            let lng_list = as_list_array(&columns[1]).unwrap().value(0);
            let lng_array = as_primitive_array::<Float64Type>(&lng_list).unwrap();
            assert_eq!(lng_array.len(), 3);
            assert_eq!(lng_array.value(0), 5.0); // timestamp 100
            assert_eq!(lng_array.value(1), 6.0); // timestamp 200
            assert_eq!(lng_array.value(2), 4.0); // timestamp 300
        } else {
            panic!("Expected Struct scalar value");
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

        // Extract the struct arrays from the states
        let state_array1 = match &state1[0] {
            ScalarValue::Struct(array) => array.clone(),
            _ => panic!("Expected Struct scalar value"),
        };

        let state_array2 = match &state2[0] {
            ScalarValue::Struct(array) => array.clone(),
            _ => panic!("Expected Struct scalar value"),
        };

        // Merge state arrays
        merged.merge_batch(&[state_array1]).unwrap();
        merged.merge_batch(&[state_array2]).unwrap();

        // Evaluate merged result
        let result = merged.evaluate().unwrap();
        if let ScalarValue::Struct(struct_array) = result {
            // Extract arrays
            let columns = struct_array.columns();

            // Check latitude values
            let lat_list = as_list_array(&columns[0]).unwrap().value(0);
            let lat_array = as_primitive_array::<Float64Type>(&lat_list).unwrap();
            assert_eq!(lat_array.len(), 2);
            assert_eq!(lat_array.value(0), 1.0); // timestamp 100
            assert_eq!(lat_array.value(1), 2.0); // timestamp 200

            // Check longitude values (should be sorted by timestamp)
            let lng_list = as_list_array(&columns[1]).unwrap().value(0);
            let lng_array = as_primitive_array::<Float64Type>(&lng_list).unwrap();
            assert_eq!(lng_array.len(), 2);
            assert_eq!(lng_array.value(0), 4.0); // timestamp 100
            assert_eq!(lng_array.value(1), 5.0); // timestamp 200
        } else {
            panic!("Expected Struct scalar value");
        }
    }
}
