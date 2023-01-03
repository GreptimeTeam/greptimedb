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

use crate::prelude::*;
pub(crate) use crate::vectors::null::replicate_null;
pub(crate) use crate::vectors::primitive::replicate_primitive;

pub(crate) fn replicate_scalar<C: ScalarVector>(c: &C, offsets: &[usize]) -> VectorRef {
    assert_eq!(offsets.len(), c.len());

    if offsets.is_empty() {
        return c.slice(0, 0);
    }
    let mut builder = <<C as ScalarVector>::Builder>::with_capacity(c.len());

    let mut previous_offset = 0;
    for (i, offset) in offsets.iter().enumerate() {
        let data = c.get_data(i);
        for _ in previous_offset..*offset {
            builder.push(data);
        }
        previous_offset = *offset;
    }
    builder.to_vector()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_time::timestamp::TimeUnit;
    use common_time::{Date, DateTime, Timestamp};
    use paste::paste;

    use super::*;
    use crate::vectors::constant::ConstantVector;
    use crate::vectors::{Int32Vector, NullVector, StringVector, VectorOp};

    #[test]
    fn test_replicate_primitive() {
        let v = Int32Vector::from_iterator(0..5);
        let offsets = [0, 1, 2, 3, 4];

        let v = v.replicate(&offsets);
        assert_eq!(4, v.len());

        for i in 0..4 {
            assert_eq!(Value::Int32(i as i32 + 1), v.get(i));
        }
    }

    #[test]
    fn test_replicate_nullable_primitive() {
        let v = Int32Vector::from(vec![None, Some(1), None, Some(2)]);
        let offsets = [2, 4, 6, 8];
        let v = v.replicate(&offsets);
        assert_eq!(8, v.len());

        let expect: VectorRef = Arc::new(Int32Vector::from(vec![
            None,
            None,
            Some(1),
            Some(1),
            None,
            None,
            Some(2),
            Some(2),
        ]));
        assert_eq!(expect, v);
    }

    #[test]
    fn test_replicate_scalar() {
        let v = StringVector::from_slice(&["0", "1", "2", "3"]);
        let offsets = [1, 3, 5, 6];

        let v = v.replicate(&offsets);
        assert_eq!(6, v.len());

        let expect: VectorRef = Arc::new(StringVector::from_slice(&["0", "1", "1", "2", "2", "3"]));
        assert_eq!(expect, v);
    }

    #[test]
    fn test_replicate_constant() {
        let v = Arc::new(StringVector::from_slice(&["hello"]));
        let cv = ConstantVector::new(v.clone(), 2);
        let offsets = [1, 4];

        let cv = cv.replicate(&offsets);
        assert_eq!(4, cv.len());

        let expect: VectorRef = Arc::new(ConstantVector::new(v, 4));
        assert_eq!(expect, cv);
    }

    #[test]
    fn test_replicate_null() {
        let v = NullVector::new(0);
        let offsets = [];
        let v = v.replicate(&offsets);
        assert!(v.is_empty());

        let v = NullVector::new(3);
        let offsets = [1, 3, 5];

        let v = v.replicate(&offsets);
        assert_eq!(5, v.len());
    }

    macro_rules! impl_replicate_date_like_test {
        ($VectorType: ident, $ValueType: ident, $method: ident) => {{
            use $crate::vectors::$VectorType;

            let v = $VectorType::from_iterator((0..5).map($ValueType::$method));
            let offsets = [0, 1, 2, 3, 4];

            let v = v.replicate(&offsets);
            assert_eq!(4, v.len());

            for i in 0..4 {
                assert_eq!(
                    Value::$ValueType($ValueType::$method((i as i32 + 1).into())),
                    v.get(i)
                );
            }
        }};
    }

    macro_rules! impl_replicate_timestamp_test {
        ($unit: ident) => {{
            paste!{
                use $crate::vectors::[<Timestamp $unit Vector>];
                use $crate::timestamp::[<Timestamp $unit>];
                let v = [<Timestamp $unit Vector>]::from_iterator((0..5).map([<Timestamp $unit>]::from));
                let offsets = [0, 1, 2, 3, 4];
                let v = v.replicate(&offsets);
                assert_eq!(4, v.len());
                for i in 0..4 {
                    assert_eq!(
                        Value::Timestamp(Timestamp::new(i as i64 + 1, TimeUnit::$unit)),
                        v.get(i)
                    );
                }
            }
        }};
    }

    #[test]
    fn test_replicate_date_like() {
        impl_replicate_date_like_test!(DateVector, Date, new);
        impl_replicate_date_like_test!(DateTimeVector, DateTime, new);

        impl_replicate_timestamp_test!(Second);
        impl_replicate_timestamp_test!(Millisecond);
        impl_replicate_timestamp_test!(Microsecond);
        impl_replicate_timestamp_test!(Nanosecond);
    }
}
