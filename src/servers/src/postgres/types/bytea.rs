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
use postgres_types::{IsNull, ToSql, Type};

#[derive(Debug)]
pub struct HexOutputBytea<'a>(pub &'a [u8]);
impl ToSqlText for HexOutputBytea<'_> {
    fn to_sql_text(
        &self,
        ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        out.put_slice(b"\\x");
        let _ = self.0.to_sql_text(ty, out);
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
