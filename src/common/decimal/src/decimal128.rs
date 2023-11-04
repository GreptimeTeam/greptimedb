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

use std::fmt::Display;
use std::hash::Hash;
use std::str::FromStr;

use bigdecimal::{BigDecimal, ToPrimitive};
use rust_decimal::Decimal as RustDecimal;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::error::{
    self, BigDecimalOutOfRangeSnafu, Error, InvalidPrecisionOrScaleSnafu, ParseBigDecimalStrSnafu,
    ParseRustDecimalStrSnafu,
};

/// The maximum precision for [Decimal128] values
pub const DECIMAL128_MAX_PRECISION: u8 = 38;

/// The maximum scale for [Decimal128] values
pub const DECIMAL128_MAX_SCALE: i8 = 38;

/// The default scale for [Decimal128] values
pub const DECIMAL128_DEFAULT_SCALE: i8 = 10;

const BYTES_TO_OVERFLOW_RUST_DECIMAL: usize = 28;

/// 128bit decimal, using the i128 to represent the decimal.
///
/// **precision**: the total number of digits in the number, it's range is \[1, 38\].
///
/// **scale**: the number of digits to the right of the decimal point, it's range is \[0, precision\].
#[derive(Debug, Default, Copy, Clone, Serialize, Deserialize)]
pub struct Decimal128 {
    value: i128,
    precision: u8,
    scale: i8,
}

impl Decimal128 {
    /// Create a new Decimal128 from i128, precision and scale.
    pub fn new(value: i128, precision: u8, scale: i8) -> Self {
        Self {
            value,
            precision,
            scale,
        }
    }

    /// Create a new Decimal128 from i128, precision and scale.
    /// If the precision or scale is invalid, return error.
    pub fn new_with_check(value: i128, precision: u8, scale: i8) -> error::Result<Self> {
        valid_precision_and_scale(precision as u64, scale as i64)?;
        Ok(Self {
            value,
            precision,
            scale,
        })
    }

    pub fn val(&self) -> i128 {
        self.value
    }

    /// Returns the precision of this decimal.
    pub fn precision(&self) -> u8 {
        self.precision
    }

    /// Returns the scale of this decimal.
    pub fn scale(&self) -> i8 {
        self.scale
    }

    /// Convert to ScalarValue
    pub fn to_scalar_value(&self) -> (Option<i128>, u8, i8) {
        (Some(self.value), self.precision, self.scale)
    }
}

impl PartialEq for Decimal128 {
    fn eq(&self, other: &Self) -> bool {
        self.value.eq(&other.value)
    }
}

impl Eq for Decimal128 {}

impl PartialOrd for Decimal128 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Decimal128 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.value.cmp(&other.value)
    }
}

/// Convert from string to Decimal128
/// If the string length is less than 18, the result is accurate.
/// Otherwise, the result is a round value.
impl FromStr for Decimal128 {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let len = s.as_bytes().len();
        if len <= BYTES_TO_OVERFLOW_RUST_DECIMAL {
            let rd = RustDecimal::from_str_exact(s).context(ParseRustDecimalStrSnafu { raw: s })?;
            Ok(Self::from(rd))
        } else {
            let bd = BigDecimal::from_str(s).context(ParseBigDecimalStrSnafu { raw: s })?;
            Self::try_from(bd)
        }
    }
}

impl Display for Decimal128 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            format_decimal_str(&self.value.to_string(), self.precision as usize, self.scale)
        )
    }
}

impl Hash for Decimal128 {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write_i128(self.value);
        state.write_u8(self.precision);
        state.write_i8(self.scale);
    }
}

impl From<Decimal128> for serde_json::Value {
    fn from(decimal: Decimal128) -> Self {
        serde_json::Value::String(decimal.to_string())
    }
}

impl From<Decimal128> for i128 {
    fn from(decimal: Decimal128) -> Self {
        decimal.val()
    }
}

impl From<i128> for Decimal128 {
    fn from(value: i128) -> Self {
        Self {
            value,
            precision: DECIMAL128_MAX_PRECISION,
            scale: DECIMAL128_DEFAULT_SCALE,
        }
    }
}

/// Convert from RustDecimal to Decimal128
/// RustDecimal can represent the range is smaller than Decimal128,
/// it is safe to convert RustDecimal to Decimal128
impl From<RustDecimal> for Decimal128 {
    fn from(rd: RustDecimal) -> Self {
        let s = rd.to_string();
        let precision = (s.len() - s.matches(&['.', '-'][..]).count()) as u8;
        Self {
            value: rd.mantissa(),
            precision,
            scale: rd.scale() as i8,
        }
    }
}

/// Try from BigDecimal to Decimal128
/// The range that BigDecimal can represent is larger than Decimal128,
/// so it is not safe to convert BigDecimal to Decimal128,
/// If the BigDecimal is out of range, return error.
impl TryFrom<BigDecimal> for Decimal128 {
    type Error = Error;

    fn try_from(value: BigDecimal) -> Result<Self, Self::Error> {
        let precision = value.digits();
        let (big_int, scale) = value.as_bigint_and_exponent();
        // conver big_int to i128, if convert failed, return error
        big_int
            .to_i128()
            .map(|val| Self::new_with_check(val, precision as u8, scale as i8))
            .unwrap_or_else(|| BigDecimalOutOfRangeSnafu { value }.fail())
    }
}

/// Port from arrow-rs,
/// see https://github.com/Apache/arrow-rs/blob/master/arrow-array/src/types.rs#L1323-L1344
fn format_decimal_str(value_str: &str, precision: usize, scale: i8) -> String {
    let (sign, rest) = match value_str.strip_prefix('-') {
        Some(stripped) => ("-", stripped),
        None => ("", value_str),
    };

    let bound = precision.min(rest.len()) + sign.len();
    let value_str = &value_str[0..bound];

    if scale == 0 {
        value_str.to_string()
    } else if scale < 0 {
        let padding = value_str.len() + scale.unsigned_abs() as usize;
        format!("{value_str:0<padding$}")
    } else if rest.len() > scale as usize {
        // Decimal separator is in the middle of the string
        let (whole, decimal) = value_str.split_at(value_str.len() - scale as usize);
        format!("{whole}.{decimal}")
    } else {
        // String has to be padded
        format!("{}0.{:0>width$}", sign, rest, width = scale as usize)
    }
}

/// check whether precision and scale is valid
fn valid_precision_and_scale(precision: u64, scale: i64) -> error::Result<()> {
    if precision == 0 {
        return InvalidPrecisionOrScaleSnafu {
            reason: format!(
                "precision cannot be 0, has to be between [1, {}]",
                DECIMAL128_MAX_PRECISION
            ),
        }
        .fail();
    }
    if precision > DECIMAL128_MAX_PRECISION as u64 {
        return InvalidPrecisionOrScaleSnafu {
            reason: format!(
                "precision {} is greater than max {}",
                precision, DECIMAL128_MAX_PRECISION
            ),
        }
        .fail();
    }
    if scale > DECIMAL128_MAX_SCALE as i64 {
        return InvalidPrecisionOrScaleSnafu {
            reason: format!(
                "scale {} is greater than max {}",
                scale, DECIMAL128_MAX_SCALE
            ),
        }
        .fail();
    }
    if scale > 0 && scale as u64 > precision {
        return InvalidPrecisionOrScaleSnafu {
            reason: format!("scale {} is greater than precision {}", scale, precision),
        }
        .fail();
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_common_decimal() {
        let decimal = Decimal128::new(123456789, 9, 3);
        assert_eq!(decimal.to_string(), "123456.789");

        let decimal = Decimal128::new_with_check(123456789, 9, 0);
        assert_eq!(decimal.unwrap().to_string(), "123456789");

        let decimal = Decimal128::new_with_check(123456789, 9, 2);
        assert_eq!(decimal.unwrap().to_string(), "1234567.89");

        let decimal = Decimal128::new_with_check(123, 3, -2);
        assert_eq!(decimal.unwrap().to_string(), "12300");

        // invalid precision or scale

        // precision is 0
        let decimal = Decimal128::new_with_check(123, 0, 0);
        assert!(decimal.is_err());

        // precision is greater than 38
        let decimal = Decimal128::new_with_check(123, 39, 0);
        assert!(decimal.is_err());

        // scale is greater than 38
        let decimal = Decimal128::new_with_check(123, 38, 39);
        assert!(decimal.is_err());

        // scale is greater than precision
        let decimal = Decimal128::new_with_check(123, 3, 4);
        assert!(decimal.is_err());
    }

    #[test]
    fn test_decimal_from_str() {
        // 0 < precision <= 28
        let decimal = Decimal128::from_str("1234567890.123456789").unwrap();
        assert_eq!(decimal.to_string(), "1234567890.123456789");
        assert_eq!(decimal.precision(), 19);
        assert_eq!(decimal.scale(), 9);

        let decimal = Decimal128::from_str("1234567890.123456789012345678").unwrap();
        assert_eq!(decimal.to_string(), "1234567890.123456789012345678");
        assert_eq!(decimal.precision(), 28);
        assert_eq!(decimal.scale(), 18);

        // 28 < precision <= 38
        let decimal = Decimal128::from_str("1234567890.1234567890123456789012").unwrap();
        assert_eq!(decimal.to_string(), "1234567890.1234567890123456789012");
        assert_eq!(decimal.precision(), 32);
        assert_eq!(decimal.scale(), 22);

        let decimal = Decimal128::from_str("1234567890.1234567890123456789012345678").unwrap();
        assert_eq!(
            decimal.to_string(),
            "1234567890.1234567890123456789012345678"
        );
        assert_eq!(decimal.precision(), 38);
        assert_eq!(decimal.scale(), 28);

        // precision > 38
        let decimal = Decimal128::from_str("1234567890.12345678901234567890123456789");
        assert!(decimal.is_err());
    }

    #[test]
    #[ignore]
    fn test_parse_decimal_speed() {
        // RustDecimal::from_str: 1.124855167s
        for _ in 0..1500000 {
            let _ = RustDecimal::from_str("1234567890.123456789012345678999").unwrap();
        }

        // BigDecimal::try_from: 6.799290042s
        for _ in 0..1500000 {
            let _ = BigDecimal::from_str("1234567890.123456789012345678999").unwrap();
        }
    }

    #[test]
    fn test_decimal_precision_and_scale() {
        // precision and scale from Deicmal(1,0) to Decimal(38,38)
        for precision in 1..=38 {
            for scale in 1..=precision {
                let decimal_str = format!("0.{}", "1".repeat(scale as usize));
                let decimal = Decimal128::from_str(&decimal_str).unwrap();
                assert_eq!(decimal_str, decimal.to_string());
            }
        }
    }
}
