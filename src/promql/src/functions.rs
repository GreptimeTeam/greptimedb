mod idelta;
mod increase;

use datafusion::arrow::array::ArrayRef;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::ColumnarValue;
pub use idelta::IDelta;
pub use increase::Increase;

pub(crate) fn extract_array(columnar_value: &ColumnarValue) -> Result<ArrayRef, DataFusionError> {
    if let ColumnarValue::Array(array) = columnar_value {
        Ok(array.clone())
    } else {
        Err(DataFusionError::Execution(
            "expect array as input, found scalar value".to_string(),
        ))
    }
}
