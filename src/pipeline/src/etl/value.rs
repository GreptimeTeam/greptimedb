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

pub mod time;

pub use time::Timestamp;

/// Value can be used as type
/// acts as value: the enclosed value is the actual value
/// acts as type: the enclosed value is the default value
#[derive(Debug, Clone, PartialEq, Default)]
pub enum Value {
    // as value: null
    // as type: no type specified
    #[default]
    Null,

    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),

    Uint8(u8),
    Uint16(u16),
    Uint32(u32),
    Uint64(u64),

    Float32(f32),
    Float64(f64),

    Boolean(bool),
    String(String),

    Timestamp(Timestamp),
}
