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

use std::net::Ipv4Addr;
use std::str::FromStr;

use common_query::error::{InvalidFuncArgsSnafu, Result};
use common_query::prelude::{Signature, TypeSignature};
use datafusion::logical_expr::Volatility;
use datatypes::prelude::ConcreteDataType;
use datatypes::scalars::ScalarVectorBuilder;
use datatypes::vectors::{MutableVector, StringVectorBuilder, UInt32VectorBuilder, VectorRef};
use derive_more::Display;
use snafu::ensure;

use crate::function::{Function, FunctionContext};

/// Function that converts a UInt32 number to an IPv4 address string.
///
/// Interprets the number as an IPv4 address in big endian and returns
/// a string in the format A.B.C.D (dot-separated numbers in decimal form).
///
/// For example:
/// - 167772160 (0x0A000000) returns "10.0.0.0"
/// - 3232235521 (0xC0A80001) returns "192.168.0.1"
#[derive(Clone, Debug, Default, Display)]
#[display("{}", self.name())]
pub struct Ipv4NumToString;

impl Function for Ipv4NumToString {
    fn name(&self) -> &str {
        "ipv4_num_to_string"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::string_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::new(
            TypeSignature::Exact(vec![ConcreteDataType::uint32_datatype()]),
            Volatility::Immutable,
        )
    }

    fn eval(&self, _func_ctx: &FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure!(
            columns.len() == 1,
            InvalidFuncArgsSnafu {
                err_msg: format!("Expected 1 argument, got {}", columns.len())
            }
        );

        let uint_vec = &columns[0];
        let size = uint_vec.len();
        let mut results = StringVectorBuilder::with_capacity(size);

        for i in 0..size {
            let ip_num = uint_vec.get(i);
            let ip_str = match ip_num {
                datatypes::value::Value::UInt32(num) => {
                    // Convert UInt32 to IPv4 string (A.B.C.D format)
                    let a = (num >> 24) & 0xFF;
                    let b = (num >> 16) & 0xFF;
                    let c = (num >> 8) & 0xFF;
                    let d = num & 0xFF;
                    Some(format!("{}.{}.{}.{}", a, b, c, d))
                }
                _ => None,
            };

            results.push(ip_str.as_deref());
        }

        Ok(results.to_vector())
    }
}

/// Function that converts a string representation of an IPv4 address to a UInt32 number.
///
/// For example:
/// - "10.0.0.1" returns 167772161
/// - "192.168.0.1" returns 3232235521
/// - Invalid IPv4 format throws an exception
#[derive(Clone, Debug, Default, Display)]
#[display("{}", self.name())]
pub struct Ipv4StringToNum;

impl Function for Ipv4StringToNum {
    fn name(&self) -> &str {
        "ipv4_string_to_num"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::uint32_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::new(
            TypeSignature::Exact(vec![ConcreteDataType::string_datatype()]),
            Volatility::Immutable,
        )
    }

    fn eval(&self, _func_ctx: &FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure!(
            columns.len() == 1,
            InvalidFuncArgsSnafu {
                err_msg: format!("Expected 1 argument, got {}", columns.len())
            }
        );

        let ip_vec = &columns[0];
        let size = ip_vec.len();
        let mut results = UInt32VectorBuilder::with_capacity(size);

        for i in 0..size {
            let ip_str = ip_vec.get(i);
            let ip_num = match ip_str {
                datatypes::value::Value::String(s) => {
                    let ip_str = s.as_utf8();
                    let ip_addr = Ipv4Addr::from_str(ip_str).map_err(|_| {
                        InvalidFuncArgsSnafu {
                            err_msg: format!("Invalid IPv4 address format: {}", ip_str),
                        }
                        .build()
                    })?;
                    Some(u32::from(ip_addr))
                }
                _ => None,
            };

            results.push(ip_num);
        }

        Ok(results.to_vector())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::scalars::ScalarVector;
    use datatypes::vectors::{StringVector, UInt32Vector};

    use super::*;

    #[test]
    fn test_ipv4_num_to_string() {
        let func = Ipv4NumToString;
        let ctx = FunctionContext::default();

        // Test data
        let values = vec![167772161u32, 3232235521u32, 0u32, 4294967295u32];
        let input = Arc::new(UInt32Vector::from_vec(values)) as VectorRef;

        let result = func.eval(&ctx, &[input]).unwrap();
        let result = result.as_any().downcast_ref::<StringVector>().unwrap();

        assert_eq!(result.get_data(0).unwrap(), "10.0.0.1");
        assert_eq!(result.get_data(1).unwrap(), "192.168.0.1");
        assert_eq!(result.get_data(2).unwrap(), "0.0.0.0");
        assert_eq!(result.get_data(3).unwrap(), "255.255.255.255");
    }

    #[test]
    fn test_ipv4_string_to_num() {
        let func = Ipv4StringToNum;
        let ctx = FunctionContext::default();

        // Test data
        let values = vec!["10.0.0.1", "192.168.0.1", "0.0.0.0", "255.255.255.255"];
        let input = Arc::new(StringVector::from_slice(&values)) as VectorRef;

        let result = func.eval(&ctx, &[input]).unwrap();
        let result = result.as_any().downcast_ref::<UInt32Vector>().unwrap();

        assert_eq!(result.get_data(0).unwrap(), 167772161);
        assert_eq!(result.get_data(1).unwrap(), 3232235521);
        assert_eq!(result.get_data(2).unwrap(), 0);
        assert_eq!(result.get_data(3).unwrap(), 4294967295);
    }

    #[test]
    fn test_ipv4_conversions_roundtrip() {
        let to_num = Ipv4StringToNum;
        let to_string = Ipv4NumToString;
        let ctx = FunctionContext::default();

        // Test data for string to num to string
        let values = vec!["10.0.0.1", "192.168.0.1", "0.0.0.0", "255.255.255.255"];
        let input = Arc::new(StringVector::from_slice(&values)) as VectorRef;

        let num_result = to_num.eval(&ctx, &[input]).unwrap();
        let back_to_string = to_string.eval(&ctx, &[num_result]).unwrap();
        let str_result = back_to_string
            .as_any()
            .downcast_ref::<StringVector>()
            .unwrap();

        for (i, expected) in values.iter().enumerate() {
            assert_eq!(str_result.get_data(i).unwrap(), *expected);
        }
    }
}
