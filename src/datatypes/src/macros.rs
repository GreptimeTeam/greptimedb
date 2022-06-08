///! Some helper macros for datatypes, copied from databend.
#[macro_export]
macro_rules! for_all_scalar_types {
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
            { f64 },
            { bool },
        }
    };
}

#[macro_export]
macro_rules! for_all_primitive_types{
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

#[macro_export]
macro_rules! with_match_primitive_type_id {
    ($key_type:expr, | $_:tt $T:ident | $body:tt,  $nbody:tt) => {{
        macro_rules! __with_ty__ {
            ( $_ $T:ident ) => {
                $body
            };
        }

        match $key_type {
            LogicalTypeId::Int8 => __with_ty__! { i8 },
            LogicalTypeId::Int16 => __with_ty__! { i16 },
            LogicalTypeId::Int32 => __with_ty__! { i32 },
            LogicalTypeId::Int64 => __with_ty__! { i64 },
            LogicalTypeId::UInt8 => __with_ty__! { u8 },
            LogicalTypeId::UInt16 => __with_ty__! { u16 },
            LogicalTypeId::UInt32 => __with_ty__! { u32 },
            LogicalTypeId::UInt64 => __with_ty__! { u64 },
            LogicalTypeId::Float32 => __with_ty__! { f32 },
            LogicalTypeId::Float64 => __with_ty__! { f64 },

            _ => $nbody,
        }
    }};
}
