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
use std::sync::Arc;

use common_query::error::{InvalidFuncArgsSnafu, Result};
use datafusion_common::DataFusionError;
use datafusion_common::arrow::array::{Array, AsArray, BooleanBuilder};
use datafusion_common::arrow::compute;
use datafusion_common::arrow::datatypes::DataType;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, Volatility};
use derive_more::Display;
use snafu::ensure;

use crate::function::{Function, extract_args};

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

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn signature(&self) -> Signature {
        Signature::string(2, Volatility::Immutable)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [arg0, arg1] = extract_args(self.name(), &args)?;

        let arg0 = compute::cast(&arg0, &DataType::Utf8View)?;
        let ip_vec = arg0.as_string_view();
        let arg1 = compute::cast(&arg1, &DataType::Utf8View)?;
        let ranges = arg1.as_string_view();

        let size = ip_vec.len();

        let mut builder = BooleanBuilder::with_capacity(size);

        for i in 0..size {
            let ip = ip_vec.is_valid(i).then(|| ip_vec.value(i));
            let range = ranges.is_valid(i).then(|| ranges.value(i));

            let in_range = match (ip, range) {
                (Some(ip_str), Some(range_str)) => {
                    if ip_str.is_empty() || range_str.is_empty() {
                        return Err(DataFusionError::Execution(
                            "IP address or CIDR range cannot be empty".to_string(),
                        ));
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

            builder.append_option(in_range);
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
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

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn signature(&self) -> Signature {
        Signature::string(2, Volatility::Immutable)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [arg0, arg1] = extract_args(self.name(), &args)?;

        let arg0 = compute::cast(&arg0, &DataType::Utf8View)?;
        let ip_vec = arg0.as_string_view();
        let arg1 = compute::cast(&arg1, &DataType::Utf8View)?;
        let ranges = arg1.as_string_view();
        let size = ip_vec.len();
        let mut builder = BooleanBuilder::with_capacity(size);

        for i in 0..size {
            let ip = ip_vec.is_valid(i).then(|| ip_vec.value(i));
            let range = ranges.is_valid(i).then(|| ranges.value(i));

            let in_range = match (ip, range) {
                (Some(ip_str), Some(range_str)) => {
                    if ip_str.is_empty() || range_str.is_empty() {
                        return Err(DataFusionError::Execution(
                            "IP address or CIDR range cannot be empty".to_string(),
                        ));
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

            builder.append_option(in_range);
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
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

    use arrow_schema::Field;
    use datafusion_common::arrow::array::StringViewArray;

    use super::*;

    #[test]
    fn test_ipv4_in_range() {
        let func = Ipv4InRange;

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

        let arg0 = ColumnarValue::Array(Arc::new(StringViewArray::from_iter_values(&ip_values)));
        let arg1 = ColumnarValue::Array(Arc::new(StringViewArray::from_iter_values(&cidr_values)));

        let args = ScalarFunctionArgs {
            args: vec![arg0, arg1],
            arg_fields: vec![],
            number_rows: 5,
            return_field: Arc::new(Field::new("x", DataType::Boolean, false)),
            config_options: Arc::new(Default::default()),
        };
        let result = func.invoke_with_args(args).unwrap();
        let result = result.to_array(5).unwrap();
        let result = result.as_boolean();

        // Expected results
        assert!(result.value(0)); // 192.168.1.5 is in 192.168.1.0/24
        assert!(!result.value(1)); // 192.168.2.1 is not in 192.168.1.0/24
        assert!(result.value(2)); // 10.0.0.1 is in 10.0.0.0/8
        assert!(result.value(3)); // 10.1.0.1 is in 10.0.0.0/8
        assert!(result.value(4)); // 172.16.0.1 is in 172.16.0.0/16
    }

    #[test]
    fn test_ipv6_in_range() {
        let func = Ipv6InRange;

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

        let arg0 = ColumnarValue::Array(Arc::new(StringViewArray::from_iter_values(&ip_values)));
        let arg1 = ColumnarValue::Array(Arc::new(StringViewArray::from_iter_values(&cidr_values)));

        let args = ScalarFunctionArgs {
            args: vec![arg0, arg1],
            arg_fields: vec![],
            number_rows: 5,
            return_field: Arc::new(Field::new("x", DataType::Boolean, false)),
            config_options: Arc::new(Default::default()),
        };
        let result = func.invoke_with_args(args).unwrap();
        let result = result.to_array(5).unwrap();
        let result = result.as_boolean();

        // Expected results
        assert!(result.value(0)); // 2001:db8::1 is in 2001:db8::/32
        assert!(result.value(1)); // 2001:db8:1:: is in 2001:db8::/32
        assert!(!result.value(2)); // 2001:db9::1 is not in 2001:db8::/32
        assert!(result.value(3)); // ::1 is in ::1/128
        assert!(result.value(4)); // fe80::1 is in fe80::/16
    }

    #[test]
    fn test_invalid_inputs() {
        let ipv4_func = Ipv4InRange;
        let ipv6_func = Ipv6InRange;

        // Invalid IPv4 address
        let invalid_ip_values = vec!["not-an-ip", "192.168.1.300"];
        let cidr_values = vec!["192.168.1.0/24", "192.168.1.0/24"];

        let arg0 = ColumnarValue::Array(Arc::new(StringViewArray::from_iter_values(
            &invalid_ip_values,
        )));
        let arg1 = ColumnarValue::Array(Arc::new(StringViewArray::from_iter_values(&cidr_values)));

        let args = ScalarFunctionArgs {
            args: vec![arg0, arg1],
            arg_fields: vec![],
            number_rows: 2,
            return_field: Arc::new(Field::new("x", DataType::Boolean, false)),
            config_options: Arc::new(Default::default()),
        };
        let result = ipv4_func.invoke_with_args(args);
        assert!(result.is_err());

        // Invalid CIDR notation
        let ip_values = vec!["192.168.1.1", "2001:db8::1"];
        let invalid_cidr_values = vec!["192.168.1.0", "2001:db8::/129"];

        let arg0 = ColumnarValue::Array(Arc::new(StringViewArray::from_iter_values(&ip_values)));
        let arg1 = ColumnarValue::Array(Arc::new(StringViewArray::from_iter_values(
            &invalid_cidr_values,
        )));

        let args = ScalarFunctionArgs {
            args: vec![arg0, arg1],
            arg_fields: vec![],
            number_rows: 2,
            return_field: Arc::new(Field::new("x", DataType::Boolean, false)),
            config_options: Arc::new(Default::default()),
        };
        let ipv4_result = ipv4_func.invoke_with_args(args.clone());
        let ipv6_result = ipv6_func.invoke_with_args(args);

        assert!(ipv4_result.is_err());
        assert!(ipv6_result.is_err());
    }

    #[test]
    fn test_edge_cases() {
        let ipv4_func = Ipv4InRange;

        // Edge cases like prefix length 0 (matches everything) and 32 (exact match)
        let ip_values = vec!["8.8.8.8", "192.168.1.1", "192.168.1.1"];
        let cidr_values = vec!["0.0.0.0/0", "192.168.1.1/32", "192.168.1.0/32"];

        let arg0 = ColumnarValue::Array(Arc::new(StringViewArray::from_iter_values(&ip_values)));
        let arg1 = ColumnarValue::Array(Arc::new(StringViewArray::from_iter_values(&cidr_values)));

        let args = ScalarFunctionArgs {
            args: vec![arg0, arg1],
            arg_fields: vec![],
            number_rows: 3,
            return_field: Arc::new(Field::new("x", DataType::Boolean, false)),
            config_options: Arc::new(Default::default()),
        };
        let result = ipv4_func.invoke_with_args(args).unwrap();
        let result = result.to_array(3).unwrap();
        let result = result.as_boolean();

        assert!(result.value(0)); // 8.8.8.8 is in 0.0.0.0/0 (matches everything)
        assert!(result.value(1)); // 192.168.1.1 is in 192.168.1.1/32 (exact match)
        assert!(!result.value(2)); // 192.168.1.1 is not in 192.168.1.0/32 (no match)
    }
}
