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

#[cfg(unix)]
pub mod nix;

pub mod error {
    use std::any::Any;

    use common_error::ext::ErrorExt;
    use common_error::status_code::StatusCode;
    use common_macro::stack_trace_debug;
    use snafu::{Location, Snafu};

    #[derive(Snafu)]
    #[stack_trace_debug]
    #[snafu(visibility(pub(crate)))]
    pub enum Error {
        #[cfg(unix)]
        #[snafu(display("Pprof error"))]
        Pprof {
            #[snafu(source)]
            error: pprof::Error,
            #[snafu(implicit)]
            location: Location,
        },

        #[snafu(display("Pprof is unsupported on this platform"))]
        Unsupported {
            #[snafu(implicit)]
            location: Location,
        },
    }

    pub type Result<T> = std::result::Result<T, Error>;

    impl ErrorExt for Error {
        fn status_code(&self) -> StatusCode {
            match self {
                #[cfg(unix)]
                Error::Pprof { .. } => StatusCode::Unexpected,
                Error::Unsupported { .. } => StatusCode::Unsupported,
            }
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }
}

#[cfg(not(unix))]
pub mod dummy {
    use std::time::Duration;

    use crate::error::{Result, UnsupportedSnafu};

    /// Dummpy CPU profiler utility.
    #[derive(Debug)]
    pub struct Profiling {}

    impl Profiling {
        /// Creates a new profiler.
        pub fn new(_duration: Duration, _frequency: i32) -> Profiling {
            Profiling {}
        }

        /// Profiles and returns a generated text.
        pub async fn dump_text(&self) -> Result<String> {
            UnsupportedSnafu {}.fail()
        }

        /// Profiles and returns a generated flamegraph.
        pub async fn dump_flamegraph(&self) -> Result<Vec<u8>> {
            UnsupportedSnafu {}.fail()
        }

        /// Profiles and returns a generated proto.
        pub async fn dump_proto(&self) -> Result<Vec<u8>> {
            UnsupportedSnafu {}.fail()
        }
    }
}

#[cfg(not(unix))]
pub use dummy::Profiling;
#[cfg(unix)]
pub use nix::Profiling;
