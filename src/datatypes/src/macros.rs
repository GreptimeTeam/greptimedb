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

//! Some helper macros for datatypes, copied from databend.

/// Apply the macro rules to all primitive types.
#[macro_export]
macro_rules! for_all_primitive_types {
    ($macro:tt $(, $x:tt)*) => {
        $macro! {
            [$($x),*],
            { i8 },
            { i16 },
            { i32 },
            { i64 },
            { u8 },
            { u16 },
            { u32 },
            { u64 },
            { f32 },
            { f64 }
        }
    };
}

/// Match the logical type and apply `$body` to all primitive types and
/// `nbody` to other types.
#[macro_export]
macro_rules! with_match_primitive_type_id {
    ($key_type:expr, | $_:tt $T:ident | $body:tt, $nbody:tt) => {{
        macro_rules! __with_ty__ {
            ( $_ $T:ident ) => {
                $body
            };
        }

        use $crate::type_id::LogicalTypeId;
        use $crate::types::{
            Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type,
            UInt32Type, UInt64Type, UInt8Type,
        };
        match $key_type {
            LogicalTypeId::Int8 => __with_ty__! { Int8Type },
            LogicalTypeId::Int16 => __with_ty__! { Int16Type },
            LogicalTypeId::Int32 => __with_ty__! { Int32Type },
            LogicalTypeId::Int64 => __with_ty__! { Int64Type },
            LogicalTypeId::UInt8 => __with_ty__! { UInt8Type },
            LogicalTypeId::UInt16 => __with_ty__! { UInt16Type },
            LogicalTypeId::UInt32 => __with_ty__! { UInt32Type },
            LogicalTypeId::UInt64 => __with_ty__! { UInt64Type },
            LogicalTypeId::Float32 => __with_ty__! { Float32Type },
            LogicalTypeId::Float64 => __with_ty__! { Float64Type },

            _ => $nbody,
        }
    }};
}
