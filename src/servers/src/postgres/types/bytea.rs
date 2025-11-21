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

use bytes::BufMut;
use pgwire::types::ToSqlText;
use pgwire::types::format::FormatOptions;
use postgres_types::{IsNull, ToSql, Type};

#[derive(Debug)]
pub struct HexOutputBytea<'a>(pub &'a [u8]);
impl ToSqlText for HexOutputBytea<'_> {
    fn to_sql_text(
        &self,
        ty: &Type,
        out: &mut bytes::BytesMut,
        format_options: &FormatOptions,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        let _ = self.0.to_sql_text(ty, out, format_options);
        Ok(IsNull::No)
    }
}

impl ToSql for HexOutputBytea<'_> {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        self.0.to_sql(ty, out)
    }

    fn accepts(ty: &Type) -> bool
    where
        Self: Sized,
    {
        <&[u8] as ToSql>::accepts(ty)
    }

    fn to_sql_checked(
        &self,
        ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        self.0.to_sql_checked(ty, out)
    }
}
#[derive(Debug)]
pub struct EscapeOutputBytea<'a>(pub &'a [u8]);
impl ToSqlText for EscapeOutputBytea<'_> {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut bytes::BytesMut,
        _format_options: &FormatOptions,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        self.0.iter().for_each(|b| match b {
            0..=31 | 127..=255 => {
                out.put_slice(b"\\");
                out.put_slice(format!("{:03o}", b).as_bytes());
            }
            92 => out.put_slice(b"\\\\"),
            32..=126 => out.put_u8(*b),
        });
        Ok(IsNull::No)
    }
}
impl ToSql for EscapeOutputBytea<'_> {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        self.0.to_sql(ty, out)
    }

    fn accepts(ty: &Type) -> bool
    where
        Self: Sized,
    {
        <&[u8] as ToSql>::accepts(ty)
    }

    fn to_sql_checked(
        &self,
        ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        self.0.to_sql_checked(ty, out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escape_output_bytea() {
        let input: &[u8] = &[97, 98, 99, 107, 108, 109, 42, 169, 84];
        let input = EscapeOutputBytea(input);

        let expected = b"abcklm*\\251T";
        let mut out = bytes::BytesMut::new();
        let is_null = input
            .to_sql_text(&Type::BYTEA, &mut out, &FormatOptions::default())
            .unwrap();
        assert!(matches!(is_null, IsNull::No));
        assert_eq!(&out[..], expected);

        let expected = &[97, 98, 99, 107, 108, 109, 42, 169, 84];
        let mut out = bytes::BytesMut::new();
        let is_null = input.to_sql(&Type::BYTEA, &mut out).unwrap();
        assert!(matches!(is_null, IsNull::No));
        assert_eq!(&out[..], expected);
    }

    #[test]
    fn test_hex_output_bytea() {
        let input = b"hello, world!";
        let input = HexOutputBytea(input);

        let expected = b"\\x68656c6c6f2c20776f726c6421";
        let mut out = bytes::BytesMut::new();
        let is_null = input
            .to_sql_text(&Type::BYTEA, &mut out, &FormatOptions::default())
            .unwrap();
        assert!(matches!(is_null, IsNull::No));
        assert_eq!(&out[..], expected);

        let expected = b"hello, world!";
        let mut out = bytes::BytesMut::new();
        let is_null = input.to_sql(&Type::BYTEA, &mut out).unwrap();
        assert!(matches!(is_null, IsNull::No));
        assert_eq!(&out[..], expected);
    }
}
