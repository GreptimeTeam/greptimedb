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

#![feature(let_chains)]

pub mod error;
pub mod helper;

pub mod prom_store {
    pub mod remote {
        pub use greptime_proto::prometheus::remote::*;
    }
}

pub mod region;
pub mod v1;

pub use greptime_proto;
pub use prost::DecodeError;
