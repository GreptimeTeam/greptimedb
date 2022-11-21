// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(clippy::derive_partial_eq_without_eq)]
tonic::include_proto!("greptime.v1");

pub const GREPTIME_FD_SET: &[u8] = tonic::include_file_descriptor_set!("greptime_fd");

pub mod codec {
    tonic::include_proto!("greptime.v1.codec");
}

pub mod meta;
