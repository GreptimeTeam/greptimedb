// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;

use common_error::prelude::*;
use rustyline::error::ReadlineError;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to start datanode, source: {}", source))]
    StartDatanode {
        #[snafu(backtrace)]
        source: datanode::error::Error,
    },

    #[snafu(display("Failed to start frontend, source: {}", source))]
    StartFrontend {
        #[snafu(backtrace)]
        source: frontend::error::Error,
    },

    #[snafu(display("Failed to start meta server, source: {}", source))]
    StartMetaServer {
        #[snafu(backtrace)]
        source: meta_srv::error::Error,
    },

    #[snafu(display("Failed to read config file: {}, source: {}", path, source))]
    ReadConfig {
        path: String,
        source: std::io::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to parse config, source: {}", source))]
    ParseConfig {
        source: toml::de::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Missing config, msg: {}", msg))]
    MissingConfig { msg: String, backtrace: Backtrace },

    #[snafu(display("Illegal config: {}", msg))]
    IllegalConfig { msg: String, backtrace: Backtrace },

    #[snafu(display("Illegal auth config: {}", source))]
    IllegalAuthConfig {
        #[snafu(backtrace)]
        source: servers::auth::Error,
    },

    #[snafu(display("Unsupported selector type, {} source: {}", selector_type, source))]
    UnsupportedSelectorType {
        selector_type: String,
        #[snafu(backtrace)]
        source: meta_srv::error::Error,
    },

    #[snafu(display("Invalid REPL command: {reason}"))]
    InvalidReplCommand { reason: String },

    #[snafu(display("Cannot create REPL: {}", source))]
    ReplCreation {
        source: ReadlineError,
        backtrace: Backtrace,
    },

    #[snafu(display("Error reading command: {}", source))]
    Readline {
        source: ReadlineError,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to request database, sql: {sql}, source: {source}"))]
    RequestDatabase {
        sql: String,
        #[snafu(backtrace)]
        source: client::Error,
    },

    #[snafu(display("Failed to collect RecordBatches, source: {source}"))]
    CollectRecordBatches {
        #[snafu(backtrace)]
        source: common_recordbatch::error::Error,
    },

    #[snafu(display("Failed to pretty print Recordbatches, source: {source}"))]
    PrettyPrintRecordBatches {
        #[snafu(backtrace)]
        source: common_recordbatch::error::Error,
    },

    #[snafu(display("Failed to start Meta client, source: {}", source))]
    StartMetaClient {
        #[snafu(backtrace)]
        source: meta_client::error::Error,
    },

    #[snafu(display("Failed to parse SQL: {}, source: {}", sql, source))]
    ParseSql {
        sql: String,
        #[snafu(backtrace)]
        source: query::error::Error,
    },

    #[snafu(display("Failed to plan statement, source: {}", source))]
    PlanStatement {
        #[snafu(backtrace)]
        source: query::error::Error,
    },

    #[snafu(display("Failed to encode logical plan in substrait, source: {}", source))]
    SubstraitEncodeLogicalPlan {
        #[snafu(backtrace)]
        source: substrait::error::Error,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::StartDatanode { source } => source.status_code(),
            Error::StartFrontend { source } => source.status_code(),
            Error::StartMetaServer { source } => source.status_code(),
            Error::UnsupportedSelectorType { source, .. } => source.status_code(),
            Error::ReadConfig { .. } | Error::ParseConfig { .. } | Error::MissingConfig { .. } => {
                StatusCode::InvalidArguments
            }
            Error::IllegalConfig { .. } | Error::InvalidReplCommand { .. } => {
                StatusCode::InvalidArguments
            }
            Error::IllegalAuthConfig { .. } => StatusCode::InvalidArguments,
            Error::ReplCreation { .. } | Error::Readline { .. } => StatusCode::Internal,
            Error::RequestDatabase { source, .. } => source.status_code(),
            Error::CollectRecordBatches { source } | Error::PrettyPrintRecordBatches { source } => {
                source.status_code()
            }
            Error::StartMetaClient { source } => source.status_code(),
            Error::ParseSql { source, .. } | Error::PlanStatement { source } => {
                source.status_code()
            }
            Error::SubstraitEncodeLogicalPlan { source } => source.status_code(),
        }
    }

    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type StdResult<E> = std::result::Result<(), E>;

    #[test]
    fn test_start_node_error() {
        fn throw_datanode_error() -> StdResult<datanode::error::Error> {
            datanode::error::MissingNodeIdSnafu {}.fail()
        }

        let e = throw_datanode_error()
            .context(StartDatanodeSnafu)
            .err()
            .unwrap();

        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::InvalidArguments);
    }

    #[test]
    fn test_start_frontend_error() {
        fn throw_frontend_error() -> StdResult<frontend::error::Error> {
            frontend::error::InvalidSqlSnafu { err_msg: "failed" }.fail()
        }

        let e = throw_frontend_error()
            .context(StartFrontendSnafu)
            .err()
            .unwrap();

        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::InvalidArguments);
    }

    #[test]
    fn test_start_metasrv_error() {
        fn throw_metasrv_error() -> StdResult<meta_srv::error::Error> {
            meta_srv::error::StreamNoneSnafu {}.fail()
        }

        let e = throw_metasrv_error()
            .context(StartMetaServerSnafu)
            .err()
            .unwrap();

        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::Internal);
    }

    #[test]
    fn test_read_config_error() {
        fn throw_read_config_error() -> StdResult<std::io::Error> {
            Err(std::io::ErrorKind::NotFound.into())
        }

        let e = throw_read_config_error()
            .context(ReadConfigSnafu { path: "test" })
            .err()
            .unwrap();

        assert!(e.backtrace_opt().is_some());
        assert_eq!(e.status_code(), StatusCode::InvalidArguments);
    }
}
