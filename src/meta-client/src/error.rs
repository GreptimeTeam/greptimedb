use common_error::prelude::*;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to connect to {}, source: {}", url, source))]
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

    #[snafu(display("Failed to ask leader from all endpoints"))]
    AskLeader { backtrace: Backtrace },

    #[snafu(display("No leader, should ask leader first"))]
    NoLeader { backtrace: Backtrace },

    #[snafu(display("Failed to create gRPC channel, source: {}", source))]
    CreateChannel {
        #[snafu(backtrace)]
        source: common_grpc::error::Error,
    },

    #[snafu(display("{} not started", name))]
    NotStarted { name: String, backtrace: Backtrace },

    #[snafu(display("Failed to send heartbeat: {}", err_msg))]
    SendHeartbeat {
        err_msg: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed create heartbeat stream to server"))]
    CreateHeartbeatStream { backtrace: Backtrace },

    #[snafu(display("Route info corruped: {}", err_msg))]
    RouteInfoCorrupted {
        err_msg: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Illegal state from server, code: {}, error: {}", code, err_msg))]
    IllegalServerState {
        code: i32,
        err_msg: String,
        backtrace: Backtrace,
    },
}

#[allow(dead_code)]
pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::ConnectFailed { .. }
            | Error::IllegalGrpcClientState { .. }
            | Error::TonicStatus { .. }
            | Error::AskLeader { .. }
            | Error::NoLeader { .. }
            | Error::NotStarted { .. }
            | Error::SendHeartbeat { .. }
            | Error::CreateHeartbeatStream { .. }
            | Error::CreateChannel { .. }
            | Error::IllegalServerState { .. } => StatusCode::Internal,
            Error::RouteInfoCorrupted { .. } => StatusCode::Unexpected,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type StdResult<E> = std::result::Result<(), E>;

    fn throw_none_option() -> Option<String> {
        None
    }

    #[test]
    fn test_connect_failed_error() {
        fn throw_tonic_error() -> StdResult<tonic::transport::Error> {
            tonic::transport::Endpoint::new("http//http").map(|_| ())
        }
        let e = throw_tonic_error()
            .context(ConnectFailedSnafu { url: "" })
            .err()
            .unwrap();
        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::Internal);
    }

    #[test]
    fn test_illegal_grpc_client_state_error() {
        let e = throw_none_option()
            .context(IllegalGrpcClientStateSnafu { err_msg: "" })
            .err()
            .unwrap();
        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::Internal);
    }

    #[test]
    fn test_tonic_status_error() {
        fn throw_tonic_status_error() -> StdResult<tonic::Status> {
            Err(tonic::Status::new(tonic::Code::Aborted, ""))
        }
        let e = throw_tonic_status_error()
            .context(TonicStatusSnafu)
            .err()
            .unwrap();
        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::Internal);
    }

    #[test]
    fn test_ask_leader_error() {
        let e = throw_none_option().context(AskLeaderSnafu).err().unwrap();
        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::Internal);
    }

    #[test]
    fn test_no_leader_error() {
        let e = throw_none_option().context(NoLeaderSnafu).err().unwrap();
        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::Internal);
    }

    #[test]
    fn test_create_channel_error() {
        fn throw_common_grpc_error() -> StdResult<common_grpc::Error> {
            tonic::transport::Endpoint::new("http//http")
                .map(|_| ())
                .context(common_grpc::error::CreateChannelSnafu)
        }
        let e = throw_common_grpc_error()
            .context(CreateChannelSnafu)
            .err()
            .unwrap();
        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::Internal);
    }

    #[test]
    fn test_not_started_error() {
        let e = throw_none_option()
            .context(NotStartedSnafu { name: "" })
            .err()
            .unwrap();
        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::Internal);
    }

    #[test]
    fn test_send_heartbeat_error() {
        let e = throw_none_option()
            .context(SendHeartbeatSnafu { err_msg: "" })
            .err()
            .unwrap();
        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::Internal);
    }

    #[test]
    fn test_create_heartbeat_stream_error() {
        let e = throw_none_option()
            .context(CreateHeartbeatStreamSnafu)
            .err()
            .unwrap();

        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::Internal);
    }

    #[test]
    fn test_route_info_corruped_error() {
        let e = throw_none_option()
            .context(RouteInfoCorruptedSnafu { err_msg: "" })
            .err()
            .unwrap();

        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::Unexpected);
    }

    #[test]
    fn test_illegal_server_state_error() {
        let e = throw_none_option()
            .context(IllegalServerStateSnafu {
                code: 1,
                err_msg: "",
            })
            .err()
            .unwrap();

        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::Internal);
    }
}
