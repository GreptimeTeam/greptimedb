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
use std::time::Duration;
use std::{fmt, thread};

use common_query::error::{InvalidFuncArgsSnafu, Result};
use common_query::prelude::{Signature, Volatility};
use datatypes::prelude::ConcreteDataType;
use datatypes::value::Value;
use datatypes::vectors::{Int64Vector, VectorRef};
use snafu::ensure;

use crate::function::{Function, FunctionContext};
use crate::function_registry::FunctionRegistry;

/// Sleep function that pauses execution for specified seconds
#[derive(Clone, Debug, Default)]
pub(crate) struct SleepFunction;

impl SleepFunction {
    pub fn register(registry: &FunctionRegistry) {
        registry.register(Arc::new(SleepFunction));
    }
}

const NAME: &str = "sleep";

impl Function for SleepFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::int64_datatype())
    }

    fn signature(&self) -> Signature {
        // Accept int32, int64 and float64 types
        Signature::uniform(
            1,
            vec![
                ConcreteDataType::int32_datatype(),
                ConcreteDataType::int64_datatype(),
                ConcreteDataType::float64_datatype(),
            ],
            Volatility::Volatile,
        )
    }

    fn eval(&self, _func_ctx: FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure!(
            columns.len() == 1,
            InvalidFuncArgsSnafu {
                err_msg: format!(
                    "The length of the args is not correct, expect exactly one, have: {}",
                    columns.len()
                ),
            }
        );

        let vector = &columns[0];
        let mut result = Vec::with_capacity(vector.len());

        for i in 0..vector.len() {
            let secs = match vector.get(i) {
                Value::Int64(x) => x as f64,
                Value::Int32(x) => x as f64,
                Value::Float64(x) => x.into_inner(),
                _ => {
                    result.push(None);
                    continue;
                }
            };
            // Sleep for the specified seconds TODO: use tokio::time::sleep when the scalars are async
            thread::sleep(Duration::from_secs_f64(secs));
            result.push(Some(secs as i64));
        }

        Ok(Arc::new(Int64Vector::from(result)))
    }
}

impl fmt::Display for SleepFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SLEEP")
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use datatypes::value::Value;
    use datatypes::vectors::{Float64Vector, Int32Vector};

    use super::*;

    #[test]
    fn test_sleep() {
        let f = SleepFunction;
        assert_eq!("sleep", f.name());
        assert_eq!(
            ConcreteDataType::int64_datatype(),
            f.return_type(&[]).unwrap()
        );

        let times = vec![Some(1_i64), None, Some(2_i64)];
        let args: Vec<VectorRef> = vec![Arc::new(Int64Vector::from(times.clone()))];

        let start = Instant::now();
        let vector = f.eval(FunctionContext::default(), &args).unwrap();
        let elapsed = start.elapsed();

        assert_eq!(3, vector.len());
        assert!(elapsed.as_secs() >= 3); // Should sleep for total of 3 seconds

        assert_eq!(vector.get(0), Value::Int64(1));
        assert_eq!(vector.get(1), Value::Null);
        assert_eq!(vector.get(2), Value::Int64(2));
    }

    #[test]
    fn test_sleep_float64() {
        let f = SleepFunction;
        let times = vec![Some(0.5_f64), None, Some(1.5_f64)];
        let args: Vec<VectorRef> = vec![Arc::new(Float64Vector::from(times))];

        let start = Instant::now();
        let vector = f.eval(FunctionContext::default(), &args).unwrap();
        let elapsed = start.elapsed();

        assert_eq!(3, vector.len());
        assert!(elapsed.as_secs_f64() >= 2.0);

        assert_eq!(vector.get(0), Value::Int64(0));
        assert_eq!(vector.get(1), Value::Null);
        assert_eq!(vector.get(2), Value::Int64(1));
    }

    #[test]
    fn test_sleep_int32() {
        let f = SleepFunction;
        let times = vec![Some(1_i32), None, Some(2_i32)];
        let args: Vec<VectorRef> = vec![Arc::new(Int32Vector::from(times))];

        let start = Instant::now();
        let vector = f.eval(FunctionContext::default(), &args).unwrap();
        let elapsed = start.elapsed();

        assert_eq!(3, vector.len());
        assert!(elapsed.as_secs() >= 3);

        assert_eq!(vector.get(0), Value::Int64(1));
        assert_eq!(vector.get(1), Value::Null);
        assert_eq!(vector.get(2), Value::Int64(2));
    }
}
