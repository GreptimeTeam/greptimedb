use common_error::prelude::*;
use tonic::{Code, Status};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Error stream request next is None"))]
    StreamNone { backtrace: Backtrace },

    #[snafu(display("Empty key is not allowed"))]
    EmptyKey { backtrace: Backtrace },

    #[snafu(display("Failed to execute via Etcd, source: {}", source))]
    EtcdFailed {
        source: etcd_client::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to connect to Etcd, source: {}", source))]
    ConnectEtcd {
        source: etcd_client::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to bind address {}, source: {}", addr, source))]
    TcpBind {
        addr: String,
        source: std::io::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to start gRPC server, source: {}", source))]
    StartGrpc {
        source: tonic::transport::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Empty table name"))]
    EmptyTableName { backtrace: Backtrace },
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<Error> for Status {
    fn from(err: Error) -> Self {
        Status::new(Code::Internal, err.to_string())
    }
}

impl ErrorExt for Error {
    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::StreamNone { .. }
            | Error::EtcdFailed { .. }
            | Error::ConnectEtcd { .. }
            | Error::TcpBind { .. }
            | Error::StartGrpc { .. } => StatusCode::Internal,
            Error::EmptyKey { .. } | Error::EmptyTableName { .. } => StatusCode::InvalidArguments,
        }
    }
}

// for form tonic
pub(crate) fn match_for_io_error(err_status: &Status) -> Option<&std::io::Error> {
    let mut err: &(dyn std::error::Error + 'static) = err_status;

    loop {
        if let Some(io_err) = err.downcast_ref::<std::io::Error>() {
            return Some(io_err);
        }

        // h2::Error do not expose std::io::Error with `source()`
        // https://github.com/hyperium/h2/pull/462
        if let Some(h2_err) = err.downcast_ref::<h2::Error>() {
            if let Some(io_err) = h2_err.get_io() {
                return Some(io_err);
            }
        }

        err = match err.source() {
            Some(err) => err,
            None => return None,
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type StdResult<E> = std::result::Result<(), E>;

    fn throw_none_option() -> Option<String> {
        None
    }

    fn throw_etcd_client_error() -> StdResult<etcd_client::Error> {
        Err(etcd_client::Error::InvalidArgs("".to_string()))
    }

    #[test]
    fn test_stream_node_error() {
        let e = throw_none_option().context(StreamNoneSnafu).err().unwrap();

        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::Internal);
    }

    #[test]
    fn test_empty_key_error() {
        let e = throw_none_option().context(EmptyKeySnafu).err().unwrap();

        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::InvalidArguments);
    }

    #[test]
    fn test_etcd_failed_error() {
        let e = throw_etcd_client_error()
            .context(EtcdFailedSnafu)
            .err()
            .unwrap();

        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::Internal);
    }

    #[test]
    fn test_connect_etcd_error() {
        let e = throw_etcd_client_error()
            .context(ConnectEtcdSnafu)
            .err()
            .unwrap();

        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::Internal);
    }

    #[test]
    fn test_tcp_bind_error() {
        fn throw_std_error() -> StdResult<std::io::Error> {
            Err(std::io::ErrorKind::NotFound.into())
        }
        let e = throw_std_error()
            .context(TcpBindSnafu { addr: "127.0.0.1" })
            .err()
            .unwrap();

        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::Internal);
    }

    #[test]
    fn test_start_grpc_error() {
        fn throw_tonic_error() -> StdResult<tonic::transport::Error> {
            tonic::transport::Endpoint::new("http//http").map(|_| ())
        }

        let e = throw_tonic_error().context(StartGrpcSnafu).err().unwrap();

        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::Internal);
    }

    #[test]
    fn test_empty_table_error() {
        let e = throw_none_option()
            .context(EmptyTableNameSnafu)
            .err()
            .unwrap();

        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::InvalidArguments);
    }
}
