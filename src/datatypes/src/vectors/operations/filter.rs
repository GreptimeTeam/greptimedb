pub(crate) use crate::vectors::constant::filter_constant;

macro_rules! filter_non_constant {
    ($vector: expr, $VectorType: ty, $filter: ident) => {{
        use std::sync::Arc;

        use snafu::ResultExt;

        let arrow_array = $vector.as_arrow();
        let filtered = arrow::compute::filter::filter(arrow_array, $filter.as_boolean_array())
            .context(crate::error::ArrowComputeSnafu)?;
        Ok(Arc::new(<$VectorType>::try_from_arrow_array(filtered)?))
    }};
}

pub(crate) use filter_non_constant;
