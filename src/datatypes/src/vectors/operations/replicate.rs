use crate::prelude::*;
pub(crate) use crate::vectors::constant::replicate_constant;
pub(crate) use crate::vectors::date::replicate_date;
pub(crate) use crate::vectors::datetime::replicate_datetime;
pub(crate) use crate::vectors::null::replicate_null;
pub(crate) use crate::vectors::primitive::replicate_primitive;
pub(crate) use crate::vectors::timestamp::replicate_timestamp;

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
