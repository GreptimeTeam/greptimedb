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

use common_error::ext::{BoxedError, PlainError};
use common_error::status_code::StatusCode;
use common_macro::{as_aggr_func_creator, AggrFuncTypeStore};
use common_query::error::{self, InvalidInputStateSnafu, Result};
use common_query::logical_plan::accumulator::AggrFuncTypeStore;
use common_query::logical_plan::{Accumulator, AggregateFunctionCreator};
use common_query::prelude::AccumulatorCreatorFunction;
use common_time::Timestamp;
use datatypes::prelude::ConcreteDataType;
use datatypes::value::{ListValue, Value};
use datatypes::vectors::VectorRef;
use snafu::{ensure, ResultExt};

use crate::scalars::geo::helpers::{ensure_columns_len, ensure_columns_n};

/// Accumulator of lat, lng, timestamp tuples
#[derive(Debug)]
pub struct JsonPathAccumulator {
    timestamp_type: ConcreteDataType,
    lat: Vec<Option<f64>>,
    lng: Vec<Option<f64>>,
    timestamp: Vec<Option<Timestamp>>,
}

impl JsonPathAccumulator {
    fn new(timestamp_type: ConcreteDataType) -> Self {
        Self {
            lat: Vec::default(),
            lng: Vec::default(),
            timestamp: Vec::default(),
            timestamp_type,
        }
    }
}

impl Accumulator for JsonPathAccumulator {
    fn state(&self) -> Result<Vec<Value>> {
        Ok(vec![
            Value::List(ListValue::new(
                self.lat.iter().map(|i| Value::from(*i)).collect(),
                ConcreteDataType::float64_datatype(),
            )),
            Value::List(ListValue::new(
                self.lng.iter().map(|i| Value::from(*i)).collect(),
                ConcreteDataType::float64_datatype(),
            )),
            Value::List(ListValue::new(
                self.timestamp.iter().map(|i| Value::from(*i)).collect(),
                self.timestamp_type.clone(),
            )),
        ])
    }

    fn update_batch(&mut self, columns: &[VectorRef]) -> Result<()> {
        // update batch as in datafusion just provides the accumulator original
        //  input.
        //
        // columns is vec of [`lat`, `lng`, `timestamp`]
        // where
        // - `lat` is a vector of `Value::Float64` or similar type. Each item in
        //  the vector is a row in given dataset.
        // - so on so forth for `lng` and `timestamp`
        ensure_columns_n!(columns, 3);

        let lat = &columns[0];
        let lng = &columns[1];
        let ts = &columns[2];

        let size = lat.len();

        for idx in 0..size {
            self.lat.push(lat.get(idx).as_f64_lossy());
            self.lng.push(lng.get(idx).as_f64_lossy());
            self.timestamp.push(ts.get(idx).as_timestamp());
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[VectorRef]) -> Result<()> {
        // merge batch as in datafusion gives state accumulated from the data
        //  returned from child accumulators' state() call
        // In our particular implementation, the data structure is like
        //
        // states is vec of [`lat`, `lng`, `timestamp`]
        // where
        // - `lat` is a vector of `Value::List`. Each item in the list is all
        //  coordinates from a child accumulator.
        // - so on so forth for `lng` and `timestamp`

        ensure_columns_n!(states, 3);

        let lat_lists = &states[0];
        let lng_lists = &states[1];
        let ts_lists = &states[2];

        let len = lat_lists.len();

        for idx in 0..len {
            if let Some(lat_list) = lat_lists
                .get(idx)
                .as_list()
                .map_err(BoxedError::new)
                .context(error::ExecuteSnafu)?
            {
                for v in lat_list.items() {
                    self.lat.push(v.as_f64_lossy());
                }
            }

            if let Some(lng_list) = lng_lists
                .get(idx)
                .as_list()
                .map_err(BoxedError::new)
                .context(error::ExecuteSnafu)?
            {
                for v in lng_list.items() {
                    self.lng.push(v.as_f64_lossy());
                }
            }

            if let Some(ts_list) = ts_lists
                .get(idx)
                .as_list()
                .map_err(BoxedError::new)
                .context(error::ExecuteSnafu)?
            {
                for v in ts_list.items() {
                    self.timestamp.push(v.as_timestamp());
                }
            }
        }

        Ok(())
    }

    fn evaluate(&self) -> Result<Value> {
        let mut work_vec: Vec<(&Option<f64>, &Option<f64>, &Option<Timestamp>)> = self
            .lat
            .iter()
            .zip(self.lng.iter())
            .zip(self.timestamp.iter())
            .map(|((a, b), c)| (a, b, c))
            .collect();

        // sort by timestamp, we treat null timestamp as 0
        work_vec.sort_unstable_by_key(|tuple| tuple.2.unwrap_or_else(|| Timestamp::new_second(0)));

        let result = serde_json::to_string(
            &work_vec
                .into_iter()
                // note that we transform to lng,lat for geojson compatibility
                .map(|(lat, lng, _)| vec![lng, lat])
                .collect::<Vec<Vec<&Option<f64>>>>(),
        )
        .map_err(|e| {
            BoxedError::new(PlainError::new(
                format!("Serialization failure: {}", e),
                StatusCode::EngineExecuteQuery,
            ))
        })
        .context(error::ExecuteSnafu)?;

        Ok(Value::String(result.into()))
    }
}

/// This function accept rows of lat, lng and timestamp, sort with timestamp and
/// encoding them into a geojson-like path.
///
/// Example:
///
/// ```sql
/// SELECT json_encode_path(lat, lon, timestamp) FROM table [group by ...];
/// ```
///
#[as_aggr_func_creator]
#[derive(Debug, Default, AggrFuncTypeStore)]
pub struct JsonPathEncodeFunctionCreator {}

impl AggregateFunctionCreator for JsonPathEncodeFunctionCreator {
    fn creator(&self) -> AccumulatorCreatorFunction {
        let creator: AccumulatorCreatorFunction = Arc::new(move |types: &[ConcreteDataType]| {
            let ts_type = types[2].clone();
            Ok(Box::new(JsonPathAccumulator::new(ts_type)))
        });

        creator
    }

    fn output_type(&self) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::string_datatype())
    }

    fn state_types(&self) -> Result<Vec<ConcreteDataType>> {
        let input_types = self.input_types()?;
        ensure!(input_types.len() == 3, InvalidInputStateSnafu);

        let timestamp_type = input_types[2].clone();

        Ok(vec![
            ConcreteDataType::list_datatype(ConcreteDataType::float64_datatype()),
            ConcreteDataType::list_datatype(ConcreteDataType::float64_datatype()),
            ConcreteDataType::list_datatype(timestamp_type),
        ])
    }
}
