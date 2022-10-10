use common_error::prelude::*;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Connect failed to {}, source: {}", url, source))]
    ConnectFailed {
        url: String,
        source: tonic::transport::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Illegal GRPC client state: {}", err_msg))]
    IllegalGrpcClientState {
        err_msg: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Tonic internal error, source: {}", source))]
    TonicStatus {
        source: tonic::Status,
        backtrace: Backtrace,
    },

    #[snafu(display("Fail to ask leader from all endpoints"))]
    AskLeader { backtrace: Backtrace },

    #[snafu(display("Failed to create gRPC channel, source: {}", source))]
    CreateChannel {
        source: common_grpc::error::Error,
        backtrace: Backtrace,
    },
}

#[allow(dead_code)]
pub type Result<T> = std::result::Result<T, Error>;
