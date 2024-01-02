use common_error::ext::ErrorExt;
use common_telemetry::logging::{debug, error};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct ErrorResponse {
    // deprecated - backward compatible
    r#type: &'static str,

    error: String,
    execution_time_ms: u64,
}

impl ErrorResponse {
    pub fn from_error(ty: &'static str, error: impl ErrorExt) -> Self {
        if error.status_code().should_log_error() {
            error!(error; "Failed to handle HTTP request");
        } else {
            debug!("Failed to handle HTTP request, err: {:?}", error);
        }
        Self::from_error_message(ty, error.output_msg())
    }

    pub fn from_error_message(ty: &'static str, err_msg: String) -> Self {
        ErrorResponse {
            r#type: ty,
            error: err_msg,
            execution_time_ms: 0,
        }
    }

    pub fn with_execution_time(mut self, execution_time: u64) -> Self {
        self.execution_time_ms = execution_time;
        self
    }

    pub fn execution_time_ms(&self) -> u64 {
        self.execution_time_ms
    }
}
