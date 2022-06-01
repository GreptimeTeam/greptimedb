use crate::prelude::*;

pub fn replicate_scalar_vector<C: ScalarVector>(c: &C, offsets: &[usize]) -> VectorRef {
    debug_assert!(
        offsets.len() == c.len(),
        "Size of offsets must match size of vector"
    );

    if offsets.is_empty() {
        return c.slice(0, 0);
    }
    let mut builder = <<C as ScalarVector>::Builder>::with_capacity(c.len());

    let mut previous_offset: usize = 0;
    (0..c.len()).for_each(|i| {
        let offset: usize = offsets[i];
        let data = c.get_data(i);
        for _ in previous_offset..offset {
            builder.push(data);
        }
        previous_offset = offset;
    });
    builder.to_vector()
}
