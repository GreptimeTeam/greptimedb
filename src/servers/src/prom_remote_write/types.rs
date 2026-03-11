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

//! Shared types for Prometheus remote write decoding.

use api::prom_store::remote::Sample;
use bytes::Buf;
use prost::DecodeError;
use prost::encoding::{WireType, decode_varint};

use crate::repeated_field::Clear;

pub type RawBytes<'a> = &'a [u8];

impl Clear for Sample {
    fn clear(&mut self) {
        self.timestamp = 0;
        self.value = 0.0;
    }
}

#[derive(Default, Clone, Debug)]
pub(crate) struct PromLabel<'a> {
    pub name: RawBytes<'a>,
    pub value: RawBytes<'a>,
}

impl<'a> Clear for PromLabel<'a> {
    fn clear(&mut self) {
        self.name.clear();
        self.value.clear();
    }
}

impl<'a> PromLabel<'a> {
    pub(crate) fn merge_field(
        &mut self,
        tag: u32,
        wire_type: WireType,
        buf: &mut &'a [u8],
    ) -> Result<(), DecodeError> {
        const STRUCT_NAME: &str = "PromLabel";
        match tag {
            1u32 => {
                let value = &mut self.name;
                merge_bytes(value, buf).map_err(|mut error| {
                    error.push(STRUCT_NAME, "name");
                    error
                })
            }
            2u32 => {
                let value = &mut self.value;
                merge_bytes(value, buf).map_err(|mut error| {
                    error.push(STRUCT_NAME, "value");
                    error
                })
            }
            _ => prost::encoding::skip_field(wire_type, tag, buf, Default::default()),
        }
    }
}

/// Reads a variable-length encoded bytes field from `src` and assign it to `dst`.
#[inline(always)]
pub fn merge_bytes<'a>(dst: &mut RawBytes<'a>, src: &mut &'a [u8]) -> Result<(), DecodeError> {
    let len = decode_varint(src)? as usize;
    if len > src.remaining() {
        return Err(DecodeError::new(format!(
            "buffer underflow, len: {}, remaining: {}",
            len,
            src.remaining()
        )));
    }
    *dst = &src[..len];
    src.advance(len);
    Ok(())
}
