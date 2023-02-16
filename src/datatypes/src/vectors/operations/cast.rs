macro_rules! cast_non_constant {
    ($vector: expr, $VectorType: ty, $to_type: expr) => {{
        use std::sync::Arc;

        use arrow::compute;
        use snafu::ResultExt;

        use crate::data_type::DataType;

        let arrow_array = $vector.to_arrow_array();
        let casted = compute::cast(&arrow_array, &$to_type.as_arrow_type())
            .context(crate::error::ArrowComputeSnafu)?;
        Ok(Arc::new(<$VectorType>::try_from_arrow_array(casted)?))
    }};
}

pub(crate) use cast_non_constant;
