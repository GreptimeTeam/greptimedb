use std::sync::Arc;

use api::serde::DecodeError;
use common_error::prelude::*;
use datafusion::physical_plan::ExecutionPlan;
use datanode::server::grpc::physical_plan;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Connect failed to {}, source: {}", url, source))]
    ConnectFailed {
        url: String,
        source: tonic::transport::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Missing {}, expected {}, actual {}", name, expected, actual))]
    MissingResult {
        name: String,
        expected: usize,
        actual: usize,
    },

    #[snafu(display("Missing result header"))]
    MissingHeader,

    #[snafu(display("Tonic internal error, source: {}", source))]
    TonicStatus {
        source: tonic::Status,
        backtrace: Backtrace,
    },

    #[snafu(display("Fail to decode select result, source: {}", source))]
    DecodeSelect { source: DecodeError },

    #[snafu(display("Error occurred on the data node, code: {}, msg: {}", code, msg))]
    Datanode { code: u32, msg: String },

    #[snafu(display("Failed to encode physical plan: {:?}, source: {}", physical, source))]
    EncodePhysical {
        physical: Arc<dyn ExecutionPlan>,
        #[snafu(backtrace)]
        source: physical_plan::error::Error,
    },
}

pub type Result<T> = std::result::Result<T, Error>;
