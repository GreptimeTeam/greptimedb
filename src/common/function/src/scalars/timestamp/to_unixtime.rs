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

use std::fmt;
use std::sync::Arc;
use std::str::FromStr;

use common_query::error::{Result, UnsupportedInputDataTypeSnafu};
use common_query::prelude::{Signature, Volatility};
use common_time::Timestamp;
use common_time::timestamp::TimeUnit;
// use common_telemetry::error;

use datatypes::vectors::{Int64Vector, StringVector, Vector};

use datatypes::prelude::ConcreteDataType;
use datatypes::types::StringType;
use datatypes::vectors::VectorRef;

use crate::scalars::function::{Function, FunctionContext};


#[derive(Clone, Debug, Default)]
pub struct ToUnixtimeFunction;

const NAME: &str = "to_unixtime";

fn to_timestamp(arg: &str) -> Option<Timestamp> {
    match Timestamp::from_str(&arg) {
        Ok(ts) => {
            Some(ts)
        },
        Err(_err) => {
            // error!("Failed to parse {} to Timestamp value", arg);
            None
        }
    }
}

fn to_seconds(ts: Timestamp) -> i64 {
    let sec_mul = (TimeUnit::Second.factor() / ts.unit().factor()) as i64;
    ts.value().div_euclid(sec_mul)
}

impl Function for ToUnixtimeFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::int64_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::exact(
            vec![ConcreteDataType::String(StringType)],
            Volatility::Immutable,
        )
    }

    fn eval(&self, _func_ctx: FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        match columns[0].data_type() {
            ConcreteDataType::String(_) => {
                let array =  columns[0].to_arrow_array();
                
                let string_vector = StringVector::try_from_arrow_array(&array).unwrap();
                let result = 
                    (0..string_vector.len())
                    .filter_map(|i| to_timestamp(&string_vector.get(i).to_string()))
                    .map(|ts| to_seconds(ts))
                    .collect::<Vec<_>>();                

                Ok(Arc::new(Int64Vector::from_vec(result)))
            },
            _ => UnsupportedInputDataTypeSnafu {
                function: NAME,
                datatypes: columns.iter().map(|c| c.data_type()).collect::<Vec<_>>(),
            }
            .fail(),
        }
    }
}

impl fmt::Display for ToUnixtimeFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TO_UNIXTIME")
    }
}

#[cfg(test)]
mod tests {
    use common_query::prelude::TypeSignature;
    use datatypes::value::Value;
    use datatypes::{prelude::ConcreteDataType, types::StringType};
    use datatypes::vectors::StringVector;
    use crate::scalars::Function;

    use super::ToUnixtimeFunction;

    use super::*;

    #[test]
    fn test_to_unixtime() {
        let f = ToUnixtimeFunction::default();        
        assert_eq!("to_unixtime", f.name());
        assert_eq!(
            ConcreteDataType::int64_datatype(),            
            f.return_type(&[]).unwrap()
        );

        assert!(matches!(f.signature(),
                         Signature {
                            type_signature: TypeSignature::Exact(valid_types),
                            volatility: Volatility::Immutable
                         } if  valid_types == vec![ConcreteDataType::String(StringType)]
        ));

        let times = vec![Some("2023-03-01T06:35:02Z"), None, Some("2022-06-30T23:59:60Z")];
        let results: Vec<i64> = vec![1677652502, 1656633600];
        let args: Vec<VectorRef> = vec![Arc::new(StringVector::from(times.clone()))];
        let vector = f.eval(FunctionContext::default(), &args).unwrap();
        assert_eq!(2, vector.len());        
        println!("[assert vector] {:?}", vector);
        
        for i in 0..vector.len() {
            let v = vector.get(i);
            match v {
                Value::Int64(ts) => {
                    let get = *results.get(i).unwrap();
                    assert_eq!(ts, get);
                }
                _ => unreachable!(),
            }
        }
        }
}
