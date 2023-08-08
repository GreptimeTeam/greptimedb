use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use snafu::{Location, Snafu};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Invalid config value: {}, {}", value, msg))]
    InvalidConfig { value: String, msg: String },

    #[snafu(display("Illegal param: {}", msg))]
    IllegalParam { msg: String },

    #[snafu(display("Internal state error: {}", msg))]
    InternalState { msg: String },

    #[snafu(display("IO error, source: {}", source))]
    Io {
        source: std::io::Error,
        location: Location,
    },

    #[snafu(display("Auth failed, source: {}", source))]
    AuthBackend {
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("User not found, username: {}", username))]
    UserNotFound { username: String },

    #[snafu(display("Unsupported password type: {}", password_type))]
    UnsupportedPasswordType { password_type: String },

    #[snafu(display("Username and password does not match, username: {}", username))]
    UserPasswordMismatch { username: String },

    #[snafu(display(
        "Access denied for user '{}' to database '{}-{}'",
        username,
        catalog,
        schema
    ))]
    AccessDenied {
        catalog: String,
        schema: String,
        username: String,
    },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidConfig { .. } => StatusCode::InvalidArguments,
            Error::IllegalParam { .. } => StatusCode::InvalidArguments,
            Error::InternalState { .. } => StatusCode::Unexpected,
            Error::Io { .. } => StatusCode::Internal,
            Error::AuthBackend { .. } => StatusCode::Internal,

            Error::UserNotFound { .. } => StatusCode::UserNotFound,
            Error::UnsupportedPasswordType { .. } => StatusCode::UnsupportedPasswordType,
            Error::UserPasswordMismatch { .. } => StatusCode::UserPasswordMismatch,
            Error::AccessDenied { .. } => StatusCode::AccessDenied,
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

pub type Result<T> = std::result::Result<T, Error>;
