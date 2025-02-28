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
use datatypes::vectors::{BooleanVectorBuilder, MutableVector, VectorRef};
use derive_more::Display;
use snafu::ensure;

use crate::function::{Function, FunctionContext};

/// Function that checks if an IPv4 address is within a specified CIDR range.
///
/// Both the IP address and the CIDR range are provided as strings.
/// Returns boolean result indicating whether the IP is in the range.
///
/// Examples:
/// - ipv4_in_range('192.168.1.5', '192.168.1.0/24') -> true
/// - ipv4_in_range('192.168.2.1', '192.168.1.0/24') -> false
/// - ipv4_in_range('10.0.0.1', '10.0.0.0/8') -> true
#[derive(Clone, Debug, Default, Display)]
#[display("{}", self.name())]
pub struct Ipv4InRange;

impl Function for Ipv4InRange {
    fn name(&self) -> &str {
        "ipv4_in_range"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::boolean_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::new(
            TypeSignature::Exact(vec![
                ConcreteDataType::string_datatype(),
                ConcreteDataType::string_datatype(),
            ]),
            Volatility::Immutable,
        )
    }

    fn eval(&self, _func_ctx: FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure!(
            columns.len() == 2,
            InvalidFuncArgsSnafu {
                err_msg: format!("Expected 2 arguments, got {}", columns.len())
            }
        );

        let ip_vec = &columns[0];
        let range_vec = &columns[1];
        let size = ip_vec.len();

        ensure!(
            range_vec.len() == size,
            InvalidFuncArgsSnafu {
                err_msg: "IP addresses and CIDR ranges must have the same number of rows"
                    .to_string()
            }
        );

        let mut results = BooleanVectorBuilder::with_capacity(size);

        for i in 0..size {
            let ip = ip_vec.get(i);
            let range = range_vec.get(i);

            let in_range = match (ip, range) {
                (Value::String(ip_str), Value::String(range_str)) => {
                    let ip_str = ip_str.as_utf8().trim();
                    let range_str = range_str.as_utf8().trim();

                    if ip_str.is_empty() || range_str.is_empty() {
                        return InvalidFuncArgsSnafu {
                            err_msg: "IP address and CIDR range cannot be empty".to_string(),
                        }
                        .fail();
                    }

                    // Parse the IP address
                    let ip_addr = Ipv4Addr::from_str(ip_str).map_err(|_| {
                        InvalidFuncArgsSnafu {
                            err_msg: format!("Invalid IPv4 address: {}", ip_str),
                        }
                        .build()
                    })?;

                    // Parse the CIDR range
                    let (cidr_ip, cidr_prefix) = parse_ipv4_cidr(range_str)?;

                    // Check if the IP is in the CIDR range
                    is_ipv4_in_range(&ip_addr, &cidr_ip, cidr_prefix)
                }
                _ => None,
            };

            results.push(in_range);
        }

        Ok(results.to_vector())
    }
}

/// Function that checks if an IPv6 address is within a specified CIDR range.
///
/// Both the IP address and the CIDR range are provided as strings.
/// Returns boolean result indicating whether the IP is in the range.
///
/// Examples:
/// - ipv6_in_range('2001:db8::1', '2001:db8::/32') -> true
/// - ipv6_in_range('2001:db8:1::', '2001:db8::/32') -> true
/// - ipv6_in_range('2001:db9::1', '2001:db8::/32') -> false
/// - ipv6_in_range('::1', '::1/128') -> true
#[derive(Clone, Debug, Default, Display)]
#[display("{}", self.name())]
pub struct Ipv6InRange;

impl Function for Ipv6InRange {
    fn name(&self) -> &str {
        "ipv6_in_range"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::boolean_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::new(
            TypeSignature::Exact(vec![
                ConcreteDataType::string_datatype(),
                ConcreteDataType::string_datatype(),
            ]),
            Volatility::Immutable,
        )
    }

    fn eval(&self, _func_ctx: FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure!(
            columns.len() == 2,
            InvalidFuncArgsSnafu {
                err_msg: format!("Expected 2 arguments, got {}", columns.len())
            }
        );

        let ip_vec = &columns[0];
        let range_vec = &columns[1];
        let size = ip_vec.len();

        ensure!(
            range_vec.len() == size,
            InvalidFuncArgsSnafu {
                err_msg: "IP addresses and CIDR ranges must have the same number of rows"
                    .to_string()
            }
        );

        let mut results = BooleanVectorBuilder::with_capacity(size);

        for i in 0..size {
            let ip = ip_vec.get(i);
            let range = range_vec.get(i);

            let in_range = match (ip, range) {
                (Value::String(ip_str), Value::String(range_str)) => {
                    let ip_str = ip_str.as_utf8().trim();
                    let range_str = range_str.as_utf8().trim();

                    if ip_str.is_empty() || range_str.is_empty() {
                        return InvalidFuncArgsSnafu {
                            err_msg: "IP address and CIDR range cannot be empty".to_string(),
                        }
                        .fail();
                    }

                    // Parse the IP address
                    let ip_addr = Ipv6Addr::from_str(ip_str).map_err(|_| {
                        InvalidFuncArgsSnafu {
                            err_msg: format!("Invalid IPv6 address: {}", ip_str),
                        }
                        .build()
                    })?;

                    // Parse the CIDR range
                    let (cidr_ip, cidr_prefix) = parse_ipv6_cidr(range_str)?;

                    // Check if the IP is in the CIDR range
                    is_ipv6_in_range(&ip_addr, &cidr_ip, cidr_prefix)
                }
                _ => None,
            };

            results.push(in_range);
        }

        Ok(results.to_vector())
    }
}

// Helper functions

fn parse_ipv4_cidr(cidr: &str) -> Result<(Ipv4Addr, u8)> {
    // Split the CIDR string into IP and prefix parts
    let parts: Vec<&str> = cidr.split('/').collect();
    ensure!(
        parts.len() == 2,
        InvalidFuncArgsSnafu {
            err_msg: format!("Invalid CIDR notation: {}", cidr),
        }
    );

    // Parse the IP address part
    let ip = Ipv4Addr::from_str(parts[0]).map_err(|_| {
        InvalidFuncArgsSnafu {
            err_msg: format!("Invalid IPv4 address in CIDR: {}", parts[0]),
        }
        .build()
    })?;

    // Parse the prefix length
    let prefix = parts[1].parse::<u8>().map_err(|_| {
        InvalidFuncArgsSnafu {
            err_msg: format!("Invalid prefix length: {}", parts[1]),
        }
        .build()
    })?;

    ensure!(
        prefix <= 32,
        InvalidFuncArgsSnafu {
            err_msg: format!("IPv4 prefix length must be <= 32, got {}", prefix),
        }
    );

    Ok((ip, prefix))
}

fn parse_ipv6_cidr(cidr: &str) -> Result<(Ipv6Addr, u8)> {
    // Split the CIDR string into IP and prefix parts
    let parts: Vec<&str> = cidr.split('/').collect();
    ensure!(
        parts.len() == 2,
        InvalidFuncArgsSnafu {
            err_msg: format!("Invalid CIDR notation: {}", cidr),
        }
    );

    // Parse the IP address part
    let ip = Ipv6Addr::from_str(parts[0]).map_err(|_| {
        InvalidFuncArgsSnafu {
            err_msg: format!("Invalid IPv6 address in CIDR: {}", parts[0]),
        }
        .build()
    })?;

    // Parse the prefix length
    let prefix = parts[1].parse::<u8>().map_err(|_| {
        InvalidFuncArgsSnafu {
            err_msg: format!("Invalid prefix length: {}", parts[1]),
        }
        .build()
    })?;

    ensure!(
        prefix <= 128,
        InvalidFuncArgsSnafu {
            err_msg: format!("IPv6 prefix length must be <= 128, got {}", prefix),
        }
    );

    Ok((ip, prefix))
}

fn is_ipv4_in_range(ip: &Ipv4Addr, cidr_base: &Ipv4Addr, prefix_len: u8) -> Option<bool> {
    // Convert both IPs to integers
    let ip_int = u32::from(*ip);
    let cidr_int = u32::from(*cidr_base);

    // Calculate the mask from the prefix length
    let mask = if prefix_len == 0 {
        0
    } else {
        u32::MAX << (32 - prefix_len)
    };

    // Apply the mask to both IPs and see if they match
    let ip_network = ip_int & mask;
    let cidr_network = cidr_int & mask;

    Some(ip_network == cidr_network)
}

fn is_ipv6_in_range(ip: &Ipv6Addr, cidr_base: &Ipv6Addr, prefix_len: u8) -> Option<bool> {
    // Get the octets (16 bytes) of both IPs
    let ip_octets = ip.octets();
    let cidr_octets = cidr_base.octets();

    // Calculate how many full bytes to compare
    let full_bytes = (prefix_len / 8) as usize;

    // First, check full bytes for equality
    for i in 0..full_bytes {
        if ip_octets[i] != cidr_octets[i] {
            return Some(false);
        }
    }

    // If there's a partial byte to check
    if prefix_len % 8 != 0 && full_bytes < 16 {
        let bits_to_check = prefix_len % 8;
        let mask = 0xFF_u8 << (8 - bits_to_check);

        if (ip_octets[full_bytes] & mask) != (cidr_octets[full_bytes] & mask) {
            return Some(false);
        }
    }

    // If we got here, everything matched
    Some(true)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::scalars::ScalarVector;
    use datatypes::vectors::{BooleanVector, StringVector};

    use super::*;

    #[test]
    fn test_ipv4_in_range() {
        let func = Ipv4InRange;
        let ctx = FunctionContext::default();

        // Test IPs
        let ip_values = vec![
            "192.168.1.5",
            "192.168.2.1",
            "10.0.0.1",
            "10.1.0.1",
            "172.16.0.1",
        ];

        // Corresponding CIDR ranges
        let cidr_values = vec![
            "192.168.1.0/24",
            "192.168.1.0/24",
            "10.0.0.0/8",
            "10.0.0.0/8",
            "172.16.0.0/16",
        ];

        let ip_input = Arc::new(StringVector::from_slice(&ip_values)) as VectorRef;
        let cidr_input = Arc::new(StringVector::from_slice(&cidr_values)) as VectorRef;

        let result = func.eval(ctx, &[ip_input, cidr_input]).unwrap();
        let result = result.as_any().downcast_ref::<BooleanVector>().unwrap();

        // Expected results
        assert!(result.get_data(0).unwrap()); // 192.168.1.5 is in 192.168.1.0/24
        assert!(!result.get_data(1).unwrap()); // 192.168.2.1 is not in 192.168.1.0/24
        assert!(result.get_data(2).unwrap()); // 10.0.0.1 is in 10.0.0.0/8
        assert!(result.get_data(3).unwrap()); // 10.1.0.1 is in 10.0.0.0/8
        assert!(result.get_data(4).unwrap()); // 172.16.0.1 is in 172.16.0.0/16
    }

    #[test]
    fn test_ipv6_in_range() {
        let func = Ipv6InRange;
        let ctx = FunctionContext::default();

        // Test IPs
        let ip_values = vec![
            "2001:db8::1",
            "2001:db8:1::",
            "2001:db9::1",
            "::1",
            "fe80::1",
        ];

        // Corresponding CIDR ranges
        let cidr_values = vec![
            "2001:db8::/32",
            "2001:db8::/32",
            "2001:db8::/32",
            "::1/128",
            "fe80::/16",
        ];

        let ip_input = Arc::new(StringVector::from_slice(&ip_values)) as VectorRef;
        let cidr_input = Arc::new(StringVector::from_slice(&cidr_values)) as VectorRef;

        let result = func.eval(ctx, &[ip_input, cidr_input]).unwrap();
        let result = result.as_any().downcast_ref::<BooleanVector>().unwrap();

        // Expected results
        assert!(result.get_data(0).unwrap()); // 2001:db8::1 is in 2001:db8::/32
        assert!(result.get_data(1).unwrap()); // 2001:db8:1:: is in 2001:db8::/32
        assert!(!result.get_data(2).unwrap()); // 2001:db9::1 is not in 2001:db8::/32
        assert!(result.get_data(3).unwrap()); // ::1 is in ::1/128
        assert!(result.get_data(4).unwrap()); // fe80::1 is in fe80::/16
    }

    #[test]
    fn test_invalid_inputs() {
        let ipv4_func = Ipv4InRange;
        let ipv6_func = Ipv6InRange;
        let ctx = FunctionContext::default();

        // Invalid IPv4 address
        let invalid_ip_values = vec!["not-an-ip", "192.168.1.300"];
        let cidr_values = vec!["192.168.1.0/24", "192.168.1.0/24"];

        let invalid_ip_input = Arc::new(StringVector::from_slice(&invalid_ip_values)) as VectorRef;
        let cidr_input = Arc::new(StringVector::from_slice(&cidr_values)) as VectorRef;

        let result = ipv4_func.eval(ctx.clone(), &[invalid_ip_input, cidr_input]);
        assert!(result.is_err());

        // Invalid CIDR notation
        let ip_values = vec!["192.168.1.1", "2001:db8::1"];
        let invalid_cidr_values = vec!["192.168.1.0", "2001:db8::/129"];

        let ip_input = Arc::new(StringVector::from_slice(&ip_values)) as VectorRef;
        let invalid_cidr_input =
            Arc::new(StringVector::from_slice(&invalid_cidr_values)) as VectorRef;

        let ipv4_result =
            ipv4_func.eval(ctx.clone(), &[ip_input.clone(), invalid_cidr_input.clone()]);
        let ipv6_result = ipv6_func.eval(ctx.clone(), &[ip_input, invalid_cidr_input]);

        assert!(ipv4_result.is_err());
        assert!(ipv6_result.is_err());
    }

    #[test]
    fn test_edge_cases() {
        let ipv4_func = Ipv4InRange;
        let ctx = FunctionContext::default();

        // Edge cases like prefix length 0 (matches everything) and 32 (exact match)
        let ip_values = vec!["8.8.8.8", "192.168.1.1", "192.168.1.1"];
        let cidr_values = vec!["0.0.0.0/0", "192.168.1.1/32", "192.168.1.0/32"];

        let ip_input = Arc::new(StringVector::from_slice(&ip_values)) as VectorRef;
        let cidr_input = Arc::new(StringVector::from_slice(&cidr_values)) as VectorRef;

        let result = ipv4_func.eval(ctx, &[ip_input, cidr_input]).unwrap();
        let result = result.as_any().downcast_ref::<BooleanVector>().unwrap();

        assert!(result.get_data(0).unwrap()); // 8.8.8.8 is in 0.0.0.0/0 (matches everything)
        assert!(result.get_data(1).unwrap()); // 192.168.1.1 is in 192.168.1.1/32 (exact match)
        assert!(!result.get_data(2).unwrap()); // 192.168.1.1 is not in 192.168.1.0/32 (no match)
    }
}
