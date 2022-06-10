use std::sync::Arc;

use crate::data_type::ConcreteDataType;
use crate::scalars::ScalarVectorBuilder;
use crate::value::Value;
use crate::vectors::{
    BinaryVectorBuilder, BooleanVectorBuilder, Float32VectorBuilder, Float64VectorBuilder,
    Int16VectorBuilder, Int32VectorBuilder, Int64VectorBuilder, Int8VectorBuilder, NullVector,
    StringVectorBuilder, UInt16VectorBuilder, UInt32VectorBuilder, UInt64VectorBuilder,
    UInt8VectorBuilder, VectorRef,
};

pub enum VectorBuilder {
    Null(usize),

    // Numeric types:
    Boolean(BooleanVectorBuilder),
    UInt8(UInt8VectorBuilder),
    UInt16(UInt16VectorBuilder),
    UInt32(UInt32VectorBuilder),
    UInt64(UInt64VectorBuilder),
    Int8(Int8VectorBuilder),
    Int16(Int16VectorBuilder),
    Int32(Int32VectorBuilder),
    Int64(Int64VectorBuilder),
    Float32(Float32VectorBuilder),
    Float64(Float64VectorBuilder),

    // String types:
    String(StringVectorBuilder),
    Binary(BinaryVectorBuilder),
}

impl VectorBuilder {
    pub fn new(data_type: ConcreteDataType) -> VectorBuilder {
        VectorBuilder::with_capacity(data_type, 0)
    }

    pub fn with_capacity(data_type: ConcreteDataType, capacity: usize) -> VectorBuilder {
        match data_type {
            ConcreteDataType::Null(_) => VectorBuilder::Null(0),
            ConcreteDataType::Boolean(_) => {
                VectorBuilder::Boolean(BooleanVectorBuilder::with_capacity(capacity))
            }
            ConcreteDataType::UInt8(_) => {
                VectorBuilder::UInt8(UInt8VectorBuilder::with_capacity(capacity))
            }
            ConcreteDataType::UInt16(_) => {
                VectorBuilder::UInt16(UInt16VectorBuilder::with_capacity(capacity))
            }
            ConcreteDataType::UInt32(_) => {
                VectorBuilder::UInt32(UInt32VectorBuilder::with_capacity(capacity))
            }
            ConcreteDataType::UInt64(_) => {
                VectorBuilder::UInt64(UInt64VectorBuilder::with_capacity(capacity))
            }
            ConcreteDataType::Int8(_) => {
                VectorBuilder::Int8(Int8VectorBuilder::with_capacity(capacity))
            }
            ConcreteDataType::Int16(_) => {
                VectorBuilder::Int16(Int16VectorBuilder::with_capacity(capacity))
            }
            ConcreteDataType::Int32(_) => {
                VectorBuilder::Int32(Int32VectorBuilder::with_capacity(capacity))
            }
            ConcreteDataType::Int64(_) => {
                VectorBuilder::Int64(Int64VectorBuilder::with_capacity(capacity))
            }
            ConcreteDataType::Float32(_) => {
                VectorBuilder::Float32(Float32VectorBuilder::with_capacity(capacity))
            }
            ConcreteDataType::Float64(_) => {
                VectorBuilder::Float64(Float64VectorBuilder::with_capacity(capacity))
            }
            ConcreteDataType::String(_) => {
                VectorBuilder::String(StringVectorBuilder::with_capacity(capacity))
            }
            ConcreteDataType::Binary(_) => {
                VectorBuilder::Binary(BinaryVectorBuilder::with_capacity(capacity))
            }
        }
    }

    pub fn push(&mut self, value: &Value) {
        if value.is_null() {
            self.push_null();
            return;
        }

        match (self, value) {
            (VectorBuilder::Boolean(b), Value::Boolean(v)) => b.push(Some(*v)),
            (VectorBuilder::UInt8(b), Value::UInt8(v)) => b.push(Some(*v)),
            (VectorBuilder::UInt16(b), Value::UInt16(v)) => b.push(Some(*v)),
            (VectorBuilder::UInt32(b), Value::UInt32(v)) => b.push(Some(*v)),
            (VectorBuilder::UInt64(b), Value::UInt64(v)) => b.push(Some(*v)),
            (VectorBuilder::Int8(b), Value::Int8(v)) => b.push(Some(*v)),
            (VectorBuilder::Int16(b), Value::Int16(v)) => b.push(Some(*v)),
            (VectorBuilder::Int32(b), Value::Int32(v)) => b.push(Some(*v)),
            (VectorBuilder::Int64(b), Value::Int64(v)) => b.push(Some(*v)),
            (VectorBuilder::Float32(b), Value::Float32(v)) => b.push(Some(v.into_inner())),
            (VectorBuilder::Float64(b), Value::Float64(v)) => b.push(Some(v.into_inner())),
            (VectorBuilder::String(b), Value::String(v)) => b.push(Some(v.as_utf8())),
            (VectorBuilder::Binary(b), Value::Binary(v)) => b.push(Some(v)),
            _ => panic!("Value {:?} does not match builder type", value),
        }
    }

    pub fn push_null(&mut self) {
        match self {
            VectorBuilder::Null(v) => *v += 1,
            VectorBuilder::Boolean(b) => b.push(None),
            VectorBuilder::UInt8(b) => b.push(None),
            VectorBuilder::UInt16(b) => b.push(None),
            VectorBuilder::UInt32(b) => b.push(None),
            VectorBuilder::UInt64(b) => b.push(None),
            VectorBuilder::Int8(b) => b.push(None),
            VectorBuilder::Int16(b) => b.push(None),
            VectorBuilder::Int32(b) => b.push(None),
            VectorBuilder::Int64(b) => b.push(None),
            VectorBuilder::Float32(b) => b.push(None),
            VectorBuilder::Float64(b) => b.push(None),
            VectorBuilder::String(b) => b.push(None),
            VectorBuilder::Binary(b) => b.push(None),
        }
    }

    pub fn finish(&mut self) -> VectorRef {
        match self {
            VectorBuilder::Null(v) => Arc::new(NullVector::new(*v)),
            VectorBuilder::Boolean(b) => Arc::new(b.finish()),
            VectorBuilder::UInt8(b) => Arc::new(b.finish()),
            VectorBuilder::UInt16(b) => Arc::new(b.finish()),
            VectorBuilder::UInt32(b) => Arc::new(b.finish()),
            VectorBuilder::UInt64(b) => Arc::new(b.finish()),
            VectorBuilder::Int8(b) => Arc::new(b.finish()),
            VectorBuilder::Int16(b) => Arc::new(b.finish()),
            VectorBuilder::Int32(b) => Arc::new(b.finish()),
            VectorBuilder::Int64(b) => Arc::new(b.finish()),
            VectorBuilder::Float32(b) => Arc::new(b.finish()),
            VectorBuilder::Float64(b) => Arc::new(b.finish()),
            VectorBuilder::String(b) => Arc::new(b.finish()),
            VectorBuilder::Binary(b) => Arc::new(b.finish()),
        }
    }
}

#[cfg(test)]
mod tests {
    use ordered_float::OrderedFloat;

    use super::*;

    macro_rules! impl_integer_builder_test {
        ($Type: ident, $datatype: ident) => {
            let mut builder = VectorBuilder::with_capacity(ConcreteDataType::$datatype(), 10);
            for i in 0..10 {
                builder.push(&Value::$Type(i));
            }
            let vector = builder.finish();

            for i in 0..10 {
                assert_eq!(Value::$Type(i), vector.get(i as usize));
            }

            let mut builder = VectorBuilder::new(ConcreteDataType::$datatype());
            builder.push(&Value::Null);
            builder.push(&Value::$Type(100));
            let vector = builder.finish();

            assert!(vector.is_null(0));
            assert_eq!(Value::$Type(100), vector.get(1));
        };
    }

    #[test]
    fn test_null_vector_builder() {
        let mut builder = VectorBuilder::new(ConcreteDataType::null_datatype());
        builder.push(&Value::Null);
        let vector = builder.finish();
        assert!(vector.is_null(0));
    }

    #[test]
    fn test_integer_vector_builder() {
        impl_integer_builder_test!(UInt8, uint8_datatype);
        impl_integer_builder_test!(UInt16, uint16_datatype);
        impl_integer_builder_test!(UInt32, uint32_datatype);
        impl_integer_builder_test!(UInt64, uint64_datatype);
        impl_integer_builder_test!(Int8, int8_datatype);
        impl_integer_builder_test!(Int16, int16_datatype);
        impl_integer_builder_test!(Int32, int32_datatype);
        impl_integer_builder_test!(Int64, int64_datatype);
    }

    #[test]
    fn test_float_vector_builder() {
        let mut builder = VectorBuilder::new(ConcreteDataType::float32_datatype());
        builder.push(&Value::Float32(OrderedFloat(1.0)));
        let vector = builder.finish();
        assert_eq!(Value::Float32(OrderedFloat(1.0)), vector.get(0));

        let mut builder = VectorBuilder::new(ConcreteDataType::float64_datatype());
        builder.push(&Value::Float64(OrderedFloat(2.0)));
        let vector = builder.finish();
        assert_eq!(Value::Float64(OrderedFloat(2.0)), vector.get(0));
    }

    #[test]
    fn test_binary_vector_builder() {
        let hello: &[u8] = b"hello";
        let mut builder = VectorBuilder::new(ConcreteDataType::binary_datatype());
        builder.push(&Value::Binary(hello.into()));
        let vector = builder.finish();
        assert_eq!(Value::Binary(hello.into()), vector.get(0));
    }

    #[test]
    fn test_string_vector_builder() {
        let hello = "hello";
        let mut builder = VectorBuilder::new(ConcreteDataType::string_datatype());
        builder.push(&Value::String(hello.into()));
        let vector = builder.finish();
        assert_eq!(Value::String(hello.into()), vector.get(0));
    }
}
