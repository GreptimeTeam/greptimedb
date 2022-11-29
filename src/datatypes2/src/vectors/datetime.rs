use crate::types::DateTimeType;
use crate::vectors::{PrimitiveVector, PrimitiveVectorBuilder};

/// Vector of [`DateTime`](common_time::Date)
pub type DateTimeVector = PrimitiveVector<DateTimeType>;
/// Builder for [`DateTimeVector`].
pub type DateTimeVectorBuilder = PrimitiveVectorBuilder<DateTimeType>;
