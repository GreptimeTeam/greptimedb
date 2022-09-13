//! Compute operations for vectors.

// TODO(yingwen): Rename to find_dup/find_duplication
pub mod dedup;

// FIXME(yingwen): constant vector.
/// Match `vector` and apply `body` with scalar vector type `T` if `vector` is a
/// scalar vector, or apply `nbody` if the vector is a `NullVector`.
macro_rules! match_scalar_vector {
    ($vector: ident, | $_: tt $T: ident | $body: tt, $nbody: tt) => {{
        use $crate::data_type::ConcreteDataType;
        use $crate::vectors::all::*;

        macro_rules! __with_ty_scalar__ {
            ( $_ $T: ident ) => {
                $body
            };
        }

        match $vector.data_type() {
            ConcreteDataType::Null(_) => $nbody
            ConcreteDataType::Boolean(_) => __with_ty_scalar__! { BooleanVector },
            ConcreteDataType::Int8(_) => __with_ty_scalar__! { Int8Vector },
            ConcreteDataType::Int16(_) => __with_ty_scalar__! { Int16Vector },
            ConcreteDataType::Int32(_) => __with_ty_scalar__! { Int32Vector },
            ConcreteDataType::Int64(_) => __with_ty_scalar__! { Int64Vector },
            ConcreteDataType::UInt8(_) => __with_ty_scalar__! { UInt8Vector },
            ConcreteDataType::UInt16(_) => __with_ty_scalar__! { UInt16Vector },
            ConcreteDataType::UInt32(_) => __with_ty_scalar__! { UInt32Vector },
            ConcreteDataType::UInt64(_) => __with_ty_scalar__! { UInt64Vector },
            ConcreteDataType::Float32(_) => __with_ty_scalar__! { Float32Vector },
            ConcreteDataType::Float64(_) => __with_ty_scalar__! { Float64Vector },
            ConcreteDataType::Binary(_) => __with_ty_scalar__! { BinaryVector },
            ConcreteDataType::String(_) => __with_ty_scalar__! { StringVector },
            ConcreteDataType::Date(_) => __with_ty_scalar__! { DateVector },
            ConcreteDataType::DateTime(_) => __with_ty_scalar__! { DateTimeVector },
            ConcreteDataType::Timestamp(_) => __with_ty_scalar__! { TimestampVector },
            ConcreteDataType::List(_) => __with_ty_scalar__! { ListVector },
        }
    }};
}

pub(crate) use match_scalar_vector;
