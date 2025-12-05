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

use common_macro::stack_trace_debug;
use snafu::{Location, Snafu};

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Failed to resolve endpoint {:?}", broker_endpoint))]
    ResolveEndpoint {
        broker_endpoint: String,
        #[snafu(source)]
        error: std::io::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to find ipv4 endpoint: {:?}", broker_endpoint))]
    EndpointIPV4NotFound {
        broker_endpoint: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to read file, path: {}", path))]
    ReadFile {
        path: String,
        #[snafu(source)]
        error: std::io::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to add root cert"))]
    AddCert {
        #[snafu(source)]
        error: rustls::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to read cert, path: {}", path))]
    ReadCerts {
        path: String,
        #[snafu(source)]
        error: std::io::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to read key, path: {}", path))]
    ReadKey {
        path: String,
        #[snafu(source)]
        error: std::io::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse key, path: {}", path))]
    KeyNotFound {
        path: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to set client auth cert"))]
    SetClientAuthCert {
        #[snafu(source)]
        error: rustls::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to load ca certs from system"))]
    LoadSystemCerts {
        #[snafu(source)]
        error: std::io::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unsupported WAL provider: {}", provider))]
    UnsupportedWalProvider {
        provider: String,
        #[snafu(implicit)]
        location: Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;
