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
    #[snafu(display("Missing {}, expected {}, actual {}", name, expected, actual))]
    MissingResult {
        name: String,
        expected: usize,
        actual: usize,
    },
    #[snafu(display("Invalid argument {}", msg))]
    InvalidArgument { msg: String },
    #[snafu(display("Operation is not implemented or not supported {}", msg))]
    Unimplemented { msg: String },
    #[snafu(display("The service is currently unavailable {}", msg))]
    Unavailable { msg: String },
    #[snafu(display("Unrecoverable data loss or corruption. {}", msg))]
    DataLoss { msg: String },
    #[snafu(display("Internal error {}, source: {}", msg, source))]
    Internal { msg: String, source: BoxError },
    #[snafu(display("Unknown error occurred, source: {}", source))]
    Unknown { source: BoxError },
}

pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub type Result<T> = std::result::Result<T, Error>;

impl From<tonic::Status> for Error {
    fn from(s: tonic::Status) -> Self {
        match s.code() {
            tonic::Code::InvalidArgument => Error::InvalidArgument {
                msg: s.message().into(),
            },
            tonic::Code::Unimplemented => Error::Unimplemented {
                msg: s.message().into(),
            },
            tonic::Code::Unavailable => Error::Unavailable {
                msg: s.message().into(),
            },
            tonic::Code::DataLoss => Error::DataLoss {
                msg: s.message().into(),
            },
            tonic::Code::Internal => Error::Internal {
                msg: s.message().into(),
                source: Box::new(s),
            },
            _ => Error::Unknown {
                source: Box::new(s),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tonic_status_to_error() {
        let err: Error = tonic::Status::invalid_argument("invalid_argument").into();
        let v = if let Error::InvalidArgument { msg } = err {
            Some(msg)
        } else {
            None
        };
        assert_eq!(v, Some("invalid_argument".to_string()));

        let err: Error = tonic::Status::unimplemented("unimplemented").into();
        let v = if let Error::Unimplemented { msg } = err {
            Some(msg)
        } else {
            None
        };
        assert_eq!(v, Some("unimplemented".to_string()));

        let err: Error = tonic::Status::unavailable("unavailable").into();
        let v = if let Error::Unavailable { msg } = err {
            Some(msg)
        } else {
            None
        };
        assert_eq!(v, Some("unavailable".to_string()));

        let err: Error = tonic::Status::data_loss("data_loss").into();
        let v = if let Error::DataLoss { msg } = err {
            Some(msg)
        } else {
            None
        };
        assert_eq!(v, Some("data_loss".to_string()));

        let err: Error = tonic::Status::internal("internal").into();
        let v = if let Error::Internal { msg, .. } = err {
            Some(msg)
        } else {
            None
        };
        assert_eq!(v, Some("internal".to_string()));

        let err: Error = tonic::Status::already_exists("already_exists").into();
        let v = if let Error::Unknown { .. } = err {
            Some(true)
        } else {
            None
        };
        assert_eq!(v, Some(true));
    }
}
