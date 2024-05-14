use common_macro::stack_trace_debug;
use snafu::{location, Location, Snafu};

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Failed to spawn a child process"))]
    SpawnChild {
        location: Location,
        #[snafu(source)]
        error: std::io::Error,
    },

    #[snafu(display("Child process exited unexpected"))]
    UnexpectedExited { location: Location },

    #[snafu(display("Unexpected: {err_msg}"))]
    Unexpected { err_msg: String, location: Location },

    #[snafu(display("Failed to kill a process"))]
    KillProcess {
        location: Location,
        #[snafu(source)]
        error: nix::Error,
    },

    #[snafu(display("Failed to create a file: {}", path))]
    CreateFile {
        path: String,
        location: Location,
        #[snafu(source)]
        error: std::io::Error,
    },

    #[snafu(display("Failed to create dir all"))]
    CreateDirAll {
        location: Location,
        #[snafu(source)]
        error: std::io::Error,
    },

    #[snafu(display("Failed to write a file: {}", path))]
    WriteFile {
        path: String,
        location: Location,
        #[snafu(source)]
        error: std::io::Error,
    },

    #[snafu(display("Failed to execute query: {}", sql))]
    ExecuteQuery {
        sql: String,
        #[snafu(source)]
        error: sqlx::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to request mysql, error: {}", err_msg))]
    RequestMysql {
        err_msg: String,
        #[snafu(source)]
        error: mysql::Error,
        location: Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<tests_fuzz::error::Error> for Error {
    fn from(e: tests_fuzz::error::Error) -> Self {
        Self::Unexpected {
            err_msg: e.to_string(),
            location: location!(),
        }
    }
}
