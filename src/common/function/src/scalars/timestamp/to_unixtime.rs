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

use common_query::error::{
    ArrowComputeSnafu, IntoVectorSnafu, Result, TypeCastSnafu, UnsupportedInputDataTypeSnafu, self,
};
use common_query::prelude::{Signature, Volatility};
use arrow::compute::kernels::cast_utils::string_to_timestamp_nanos;
use common_time::timestamp::TimeUnit;
use datafusion::arrow;
use datatypes::arrow::compute;

use datatypes::vectors::{Int64Vector, StringVector, Vector};
use datatypes::arrow::datatypes::{DataType as ArrowDatatype, Int64Type};
use datatypes::data_type::DataType;
use datatypes::prelude::ConcreteDataType;
use datatypes::types::StringType;
use datatypes::vectors::{TimestampMillisecondVector, VectorRef, self};
use snafu::{ResultExt};

use crate::scalars::function::{Function, FunctionContext};


#[derive(Clone, Debug, Default)]
pub struct ToUnixtimeFunction;

const NAME: &str = "to_unixtime";


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
                let _arrow_datatype = &self.return_type(&[]).unwrap().as_arrow_type();
                
                let string_vector = StringVector::try_from_arrow_array(&array).unwrap();
                let first = string_vector.get(0).to_string();                
                let first_timestamp = common_time::Timestamp::from_str(&first).unwrap();                
                
                let sec_mul = (TimeUnit::Second.factor() / first_timestamp.unit().factor()) as i64;
                let result = first_timestamp.value().div_euclid(sec_mul);
                println!("[result] {:?}", result); // 1677652502
                
                UnsupportedInputDataTypeSnafu {
                    function: NAME,
                    datatypes: columns.iter().map(|c| c.data_type()).collect::<Vec<_>>(),
                }.fail()
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
    use datatypes::{prelude::ConcreteDataType, types::StringType};
    use datatypes::vectors::{Int64Vector, StringVector};
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
        let args: Vec<VectorRef> = vec![Arc::new(StringVector::from(times.clone()))];
        let vector = f.eval(FunctionContext::default(), &args).unwrap();
        assert_eq!(3, vector.len());
        }
}
