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

use std::net::{Ipv4Addr, Ipv6Addr};
use std::str::FromStr;

use common_query::error::{InvalidFuncArgsSnafu, Result};
use common_query::prelude::{Signature, TypeSignature};
use datafusion::logical_expr::Volatility;
use datatypes::prelude::{ConcreteDataType, Value};
use datatypes::scalars::ScalarVectorBuilder;
use datatypes::vectors::{BinaryVectorBuilder, MutableVector, StringVectorBuilder, VectorRef};
use derive_more::Display;
use snafu::ensure;

use crate::function::{Function, FunctionContext};

/// Function that converts a hex string representation of an IPv6 address to a formatted string.
///
/// For example:
/// - "20010DB8000000000000000000000001" returns "2001:db8::1"
/// - "00000000000000000000FFFFC0A80001" returns "::ffff:192.168.0.1"
#[derive(Clone, Debug, Default, Display)]
#[display("{}", self.name())]
pub struct Ipv6NumToString;

impl Function for Ipv6NumToString {
    fn name(&self) -> &str {
        "ipv6_num_to_string"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::string_datatype())
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

        let hex_vec = &columns[0];
        let size = hex_vec.len();
        let mut results = StringVectorBuilder::with_capacity(size);

        for i in 0..size {
            let hex_str = hex_vec.get(i);
            let ip_str = match hex_str {
                Value::String(s) => {
                    let hex_str = s.as_utf8().to_lowercase();

                    // Validate and convert hex string to bytes
                    let bytes = if hex_str.len() == 32 {
                        let mut bytes = [0u8; 16];
                        for i in 0..16 {
                            let byte_str = &hex_str[i * 2..i * 2 + 2];
                            bytes[i] = u8::from_str_radix(byte_str, 16).map_err(|_| {
                                InvalidFuncArgsSnafu {
                                    err_msg: format!("Invalid hex characters in '{}'", byte_str),
                                }
                                .build()
                            })?;
                        }
                        bytes
                    } else {
                        return InvalidFuncArgsSnafu {
                            err_msg: format!("Expected 32 hex characters, got {}", hex_str.len()),
                        }
                        .fail();
                    };

                    // Convert bytes to IPv6 address
                    let addr = Ipv6Addr::from(bytes);

                    // Special handling for IPv6-mapped IPv4 addresses
                    if let Some(ipv4) = addr.to_ipv4() {
                        if addr.octets()[0..10].iter().all(|&b| b == 0)
                            && addr.octets()[10] == 0xFF
                            && addr.octets()[11] == 0xFF
                        {
                            Some(format!("::ffff:{}", ipv4))
                        } else {
                            Some(addr.to_string())
                        }
                    } else {
                        Some(addr.to_string())
                    }
                }
                _ => None,
            };

            results.push(ip_str.as_deref());
        }

        Ok(results.to_vector())
    }
}

/// Function that converts a string representation of an IPv6 address to its binary representation.
///
/// For example:
/// - "2001:db8::1" returns its binary representation
/// - If the input string contains a valid IPv4 address, returns its IPv6 equivalent
/// - HEX can be uppercase or lowercase
/// - Invalid IPv6 format throws an exception
#[derive(Clone, Debug, Default, Display)]
#[display("{}", self.name())]
pub struct Ipv6StringToNum;

impl Function for Ipv6StringToNum {
    fn name(&self) -> &str {
        "ipv6_string_to_num"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::binary_datatype())
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
        let mut results = BinaryVectorBuilder::with_capacity(size);

        for i in 0..size {
            let ip_str = ip_vec.get(i);
            let ip_binary = match ip_str {
                Value::String(s) => {
                    let addr_str = s.as_utf8();

                    let addr = if let Ok(ipv6) = Ipv6Addr::from_str(addr_str) {
                        // Direct IPv6 address
                        ipv6
                    } else if let Ok(ipv4) = Ipv4Addr::from_str(addr_str) {
                        // IPv4 address to be converted to IPv6
                        ipv4.to_ipv6_mapped()
                    } else {
                        // Invalid format
                        return InvalidFuncArgsSnafu {
                            err_msg: format!("Invalid IPv6 address format: {}", addr_str),
                        }
                        .fail();
                    };

                    // Convert IPv6 address to binary (16 bytes)
                    let octets = addr.octets();
                    Some(octets.to_vec())
                }
                _ => None,
            };

            results.push(ip_binary.as_deref());
        }

        Ok(results.to_vector())
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Write;
    use std::sync::Arc;

    use datatypes::scalars::ScalarVector;
    use datatypes::vectors::{BinaryVector, StringVector, Vector};

    use super::*;

    #[test]
    fn test_ipv6_num_to_string() {
        let func = Ipv6NumToString;
        let ctx = FunctionContext::default();

        // Hex string for "2001:db8::1"
        let hex_str1 = "20010db8000000000000000000000001";

        // Hex string for IPv4-mapped IPv6 address "::ffff:192.168.0.1"
        let hex_str2 = "00000000000000000000ffffc0a80001";

        let values = vec![hex_str1, hex_str2];
        let input = Arc::new(StringVector::from_slice(&values)) as VectorRef;

        let result = func.eval(&ctx, &[input]).unwrap();
        let result = result.as_any().downcast_ref::<StringVector>().unwrap();

        assert_eq!(result.get_data(0).unwrap(), "2001:db8::1");
        assert_eq!(result.get_data(1).unwrap(), "::ffff:192.168.0.1");
    }

    #[test]
    fn test_ipv6_num_to_string_uppercase() {
        let func = Ipv6NumToString;
        let ctx = FunctionContext::default();

        // Uppercase hex string for "2001:db8::1"
        let hex_str = "20010DB8000000000000000000000001";

        let values = vec![hex_str];
        let input = Arc::new(StringVector::from_slice(&values)) as VectorRef;

        let result = func.eval(&ctx, &[input]).unwrap();
        let result = result.as_any().downcast_ref::<StringVector>().unwrap();

        assert_eq!(result.get_data(0).unwrap(), "2001:db8::1");
    }

    #[test]
    fn test_ipv6_num_to_string_error() {
        let func = Ipv6NumToString;
        let ctx = FunctionContext::default();

        // Invalid hex string - wrong length
        let hex_str = "20010db8";

        let values = vec![hex_str];
        let input = Arc::new(StringVector::from_slice(&values)) as VectorRef;

        // Should return an error
        let result = func.eval(&ctx, &[input]);
        assert!(result.is_err());

        // Check that the error message contains expected text
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Expected 32 hex characters"));
    }

    #[test]
    fn test_ipv6_string_to_num() {
        let func = Ipv6StringToNum;
        let ctx = FunctionContext::default();

        let values = vec!["2001:db8::1", "::ffff:192.168.0.1", "192.168.0.1"];
        let input = Arc::new(StringVector::from_slice(&values)) as VectorRef;

        let result = func.eval(&ctx, &[input]).unwrap();
        let result = result.as_any().downcast_ref::<BinaryVector>().unwrap();

        // Expected binary for "2001:db8::1"
        let expected_1 = [
            0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x01,
        ];

        // Expected binary for "::ffff:192.168.0.1" or "192.168.0.1" (IPv4-mapped)
        let expected_2 = [
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF, 0xFF, 0xC0, 0xA8, 0, 0x01,
        ];

        assert_eq!(result.get_data(0).unwrap(), &expected_1);
        assert_eq!(result.get_data(1).unwrap(), &expected_2);
        assert_eq!(result.get_data(2).unwrap(), &expected_2);
    }

    #[test]
    fn test_ipv6_conversions_roundtrip() {
        let to_num = Ipv6StringToNum;
        let to_string = Ipv6NumToString;
        let ctx = FunctionContext::default();

        // Test data
        let values = vec!["2001:db8::1", "::ffff:192.168.0.1"];
        let input = Arc::new(StringVector::from_slice(&values)) as VectorRef;

        // Convert IPv6 addresses to binary
        let binary_result = to_num.eval(&ctx, std::slice::from_ref(&input)).unwrap();

        // Convert binary to hex string representation (for ipv6_num_to_string)
        let mut hex_strings = Vec::new();
        let binary_vector = binary_result
            .as_any()
            .downcast_ref::<BinaryVector>()
            .unwrap();

        for i in 0..binary_vector.len() {
            let bytes = binary_vector.get_data(i).unwrap();
            let hex = bytes.iter().fold(String::new(), |mut acc, b| {
                write!(&mut acc, "{:02x}", b).unwrap();
                acc
            });
            hex_strings.push(hex);
        }

        let hex_str_refs: Vec<&str> = hex_strings.iter().map(|s| s.as_str()).collect();
        let hex_input = Arc::new(StringVector::from_slice(&hex_str_refs)) as VectorRef;

        // Now convert hex to formatted string
        let string_result = to_string.eval(&ctx, &[hex_input]).unwrap();
        let str_result = string_result
            .as_any()
            .downcast_ref::<StringVector>()
            .unwrap();

        // Compare with original input
        assert_eq!(str_result.get_data(0).unwrap(), values[0]);
        assert_eq!(str_result.get_data(1).unwrap(), values[1]);
    }

    #[test]
    fn test_ipv6_conversions_hex_roundtrip() {
        // Create a new test to verify that the string output from ipv6_num_to_string
        // can be converted back using ipv6_string_to_num
        let to_string = Ipv6NumToString;
        let to_binary = Ipv6StringToNum;
        let ctx = FunctionContext::default();

        // Hex representation of IPv6 addresses
        let hex_values = vec![
            "20010db8000000000000000000000001",
            "00000000000000000000ffffc0a80001",
        ];
        let hex_input = Arc::new(StringVector::from_slice(&hex_values)) as VectorRef;

        // Convert hex to string representation
        let string_result = to_string.eval(&ctx, &[hex_input]).unwrap();

        // Then convert string representation back to binary
        let binary_result = to_binary.eval(&ctx, &[string_result]).unwrap();
        let bin_result = binary_result
            .as_any()
            .downcast_ref::<BinaryVector>()
            .unwrap();

        // Expected binary values
        let expected_bin1 = [
            0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x01,
        ];
        let expected_bin2 = [
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF, 0xFF, 0xC0, 0xA8, 0, 0x01,
        ];

        assert_eq!(bin_result.get_data(0).unwrap(), &expected_bin1);
        assert_eq!(bin_result.get_data(1).unwrap(), &expected_bin2);
    }
}
