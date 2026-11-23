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

//! Reads decimal columns from the text of a csv or json file.
//!
//! A file reader that decodes a number into a `Float64` loses the digits a `Float64` can't
//! hold, and Arrow's own decimal decoding is wrong for some numbers in scientific notation. So
//! the readers hand over the text of such a column and [`parse_decimal_columns`] parses it.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Decimal128Array};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::error::DataFusionError;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::StreamExt;

/// Parses the string columns of `stream` that have a precision and scale in `decimals` into
/// `Decimal128` columns. `decimals` has one entry for each column of the stream.
///
/// An empty string is read as NULL, which is what an empty csv field is for a number.
pub fn parse_decimal_columns(
    stream: SendableRecordBatchStream,
    decimals: Vec<Option<(u8, i8)>>,
) -> SendableRecordBatchStream {
    if decimals.iter().all(Option::is_none) {
        return stream;
    }

    let input_schema = stream.schema();
    let fields = input_schema
        .fields()
        .iter()
        .zip(&decimals)
        .map(|(field, decimal)| match decimal {
            Some((precision, scale)) => Arc::new(
                Field::clone(field).with_data_type(DataType::Decimal128(*precision, *scale)),
            ),
            None => field.clone(),
        })
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        input_schema.metadata().clone(),
    ));

    let output_schema = schema.clone();
    let stream = stream.map(move |batch| {
        parse_decimal_batch(&batch?, &output_schema, &decimals).map_err(DataFusionError::from)
    });

    Box::pin(RecordBatchStreamAdapter::new(schema, stream))
}

fn parse_decimal_batch(
    batch: &RecordBatch,
    schema: &SchemaRef,
    decimals: &[Option<(u8, i8)>],
) -> Result<RecordBatch, ArrowError> {
    let columns = batch
        .columns()
        .iter()
        .zip(decimals)
        .map(|(column, decimal)| match decimal {
            Some((precision, scale)) => parse_decimal_array(column, *precision, *scale),
            None => Ok(column.clone()),
        })
        .collect::<Result<Vec<_>, _>>()?;

    RecordBatch::try_new(schema.clone(), columns)
}

fn parse_decimal_array(array: &ArrayRef, precision: u8, scale: i8) -> Result<ArrayRef, ArrowError> {
    let strings = array.as_string_opt::<i32>().ok_or_else(|| {
        ArrowError::CastError(format!(
            "expected a string column to parse DECIMAL({precision}, {scale}) from, got {}",
            array.data_type()
        ))
    })?;

    let values = strings
        .iter()
        .map(|text| match text {
            Some(text) if !text.is_empty() => parse_decimal128(text, precision, scale).map(Some),
            _ => Ok(None),
        })
        .collect::<Result<Decimal128Array, _>>()?
        .with_precision_and_scale(precision, scale)?;

    Ok(Arc::new(values))
}

/// Parses a decimal number written in plain or scientific notation and returns its value
/// with `scale` fractional digits, rounded half away from zero.
///
/// Every digit of the text is used, so the result is exact apart from that rounding.
pub fn parse_decimal128(text: &str, precision: u8, scale: i8) -> Result<i128, ArrowError> {
    let invalid = || {
        ArrowError::ParseError(format!(
            "can't parse the value {text} as DECIMAL({precision}, {scale})"
        ))
    };
    let overflow = || {
        ArrowError::ParseError(format!(
            "the value {text} is out of range for DECIMAL({precision}, {scale})"
        ))
    };

    let (negative, number) = match text.as_bytes().first() {
        Some(b'-') => (true, &text[1..]),
        Some(b'+') => (false, &text[1..]),
        _ => (false, text),
    };
    let (mantissa, exponent) = match number.find(['e', 'E']) {
        Some(idx) => {
            let exponent = &number[idx + 1..];
            let exponent_digits = exponent.strip_prefix(['+', '-']).unwrap_or(exponent);
            if exponent_digits.is_empty() || !exponent_digits.bytes().all(|b| b.is_ascii_digit()) {
                return Err(invalid());
            }
            // An exponent that doesn't fit is far outside the range of any decimal.
            let exponent = exponent
                .parse::<i64>()
                .unwrap_or(if exponent.starts_with('-') {
                    i64::MIN / 2
                } else {
                    i64::MAX / 2
                });
            (&number[..idx], exponent.clamp(i64::MIN / 2, i64::MAX / 2))
        }
        None => (number, 0),
    };
    let (integer, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if (integer.is_empty() && fraction.is_empty())
        || !integer
            .bytes()
            .chain(fraction.bytes())
            .all(|b| b.is_ascii_digit())
    {
        return Err(invalid());
    }

    // The value is `0.d1 d2 d3 ... * 10^point`, where the digits start at the first non-zero
    // one. The first `kept` digits make the integer part of `value * 10^scale`.
    let leading_zeros = integer
        .bytes()
        .chain(fraction.bytes())
        .take_while(|b| *b == b'0')
        .count();
    let mut digits = integer.bytes().chain(fraction.bytes()).skip(leading_zeros);
    let point = integer.len() as i64 - leading_zeros as i64 + exponent;
    let kept = point + scale as i64;

    let mut value = 0_i128;
    let mut taken = 0_i64;
    while taken < kept {
        let digit = match digits.next() {
            Some(digit) => digit - b'0',
            // The digits ran out before the decimal point.
            None if value == 0 => break,
            None => 0,
        };
        if taken >= precision as i64 {
            return Err(overflow());
        }
        value = value * 10 + digit as i128;
        taken += 1;
    }
    if taken == kept && digits.next().is_some_and(|digit| digit >= b'5') {
        value += 1;
        if value >= 10_i128.pow(precision as u32) {
            return Err(overflow());
        }
    }

    Ok(if negative { -value } else { value })
}

#[cfg(test)]
mod tests {
    use arrow::array::StringArray;
    use arrow::datatypes::Decimal128Type;
    use datafusion::physical_plan::common::collect;
    use futures::stream;

    use super::*;

    #[test]
    fn test_parse_decimal128() {
        let cases: &[(&str, u8, i8, i128)] = &[
            // Plain notation keeps every digit.
            ("123456789012345678.12", 20, 2, 12345678901234567812),
            (
                "-9999999999999999999999999999.9999999999",
                38,
                10,
                -99999999999999999999999999999999999999,
            ),
            ("7", 20, 2, 700),
            ("0", 5, 2, 0),
            ("+1.5", 5, 2, 150),
            (".5", 5, 2, 50),
            ("5.", 5, 2, 500),
            ("000123.4500", 5, 2, 12345),
            // More fractional digits than the scale are rounded half away from zero.
            ("1.005", 20, 2, 101),
            ("-1.005", 20, 2, -101),
            ("1.004", 20, 2, 100),
            ("2.999", 20, 2, 300),
            ("0.005", 5, 2, 1),
            ("0.0049", 5, 2, 0),
            ("-0.0049", 5, 2, 0),
            ("999.994", 5, 2, 99999),
            ("0.5", 3, 0, 1),
            ("-2.5", 3, 0, -3),
            // Scientific notation.
            ("1.2500000000000000000000000000000000000000e0", 38, 2, 125),
            ("0e0", 38, 2, 0),
            ("0.0e0", 5, 2, 0),
            ("1e2", 20, 2, 10000),
            ("1.5e-1", 20, 2, 15),
            ("1.25E+1", 20, 2, 1250),
            ("12345e-3", 20, 2, 1235),
            ("0.00012345e5", 5, 2, 1235),
            ("5e-3", 20, 2, 1),
            ("4.9e-3", 20, 2, 0),
            ("1e37", 38, 0, 10_i128.pow(37)),
            ("1e-999999999999999999999", 20, 2, 0),
            ("0e999999999999999999999", 20, 2, 0),
            // A negative scale.
            ("150", 3, -1, 15),
            ("144", 3, -1, 14),
        ];
        for (text, precision, scale, expected) in cases {
            assert_eq!(
                parse_decimal128(text, *precision, *scale).unwrap(),
                *expected,
                "{text} as DECIMAL({precision}, {scale})"
            );
        }

        for text in [
            // Out of range, also after rounding.
            "1000",
            "1e3",
            "999.995",
            "1e999999999999999999999",
            // Not a number.
            "",
            "-",
            ".",
            "e5",
            "1e",
            "1e+",
            "1e1.5",
            "1.2.3",
            "1,5",
            " 1",
            "--1",
            "NaN",
            "inf",
        ] {
            assert!(
                parse_decimal128(text, 5, 2).is_err(),
                "{text} as DECIMAL(5, 2)"
            );
        }
    }

    #[tokio::test]
    async fn test_parse_decimal_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, true),
            Field::new("price", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
                Arc::new(StringArray::from(vec![
                    Some("1.2500000000000000000000000000000000000000e0"),
                    Some(""),
                    None,
                    Some("-0.005"),
                ])),
            ],
        )
        .unwrap();
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            stream::iter(vec![Ok(batch.clone())]),
        ));

        let stream = parse_decimal_columns(stream, vec![None, Some((38, 2))]);
        assert_eq!(
            stream.schema().field(1),
            &Field::new("price", DataType::Decimal128(38, 2), true)
        );
        let batches = collect(stream).await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].column(0), batch.column(0));
        let prices = batches[0].column(1).as_primitive::<Decimal128Type>();
        assert_eq!(
            prices.iter().collect::<Vec<_>>(),
            vec![Some(125), None, None, Some(-1)]
        );

        let stream = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::iter(vec![Ok(batch)]),
        ));
        let stream = parse_decimal_columns(stream, vec![Some((38, 2)), None]);
        let error = collect(stream).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("can't parse the value a as DECIMAL(38, 2)"),
            "{error}"
        );
    }
}
