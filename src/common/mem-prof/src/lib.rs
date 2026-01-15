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

pub mod error;

#[cfg(not(windows))]
mod jemalloc;
#[cfg(not(windows))]
pub use jemalloc::{
    activate_heap_profile, deactivate_heap_profile, dump_flamegraph, dump_pprof, dump_profile,
    is_gdump_active, is_heap_profile_active, set_gdump_active, symbolicate_jeheap,
};

#[cfg(windows)]
pub async fn dump_profile() -> error::Result<Vec<u8>> {
    error::ProfilingNotSupportedSnafu.fail()
}

#[cfg(windows)]
pub async fn dump_pprof() -> error::Result<Vec<u8>> {
    error::ProfilingNotSupportedSnafu.fail()
}

#[cfg(windows)]
pub async fn dump_flamegraph() -> error::Result<Vec<u8>> {
    error::ProfilingNotSupportedSnafu.fail()
}

#[cfg(windows)]
pub fn activate_heap_profile() -> error::Result<()> {
    error::ProfilingNotSupportedSnafu.fail()
}

#[cfg(windows)]
pub fn deactivate_heap_profile() -> error::Result<()> {
    error::ProfilingNotSupportedSnafu.fail()
}

#[cfg(windows)]
pub fn is_heap_profile_active() -> error::Result<bool> {
    error::ProfilingNotSupportedSnafu.fail()
}

#[cfg(windows)]
pub fn is_gdump_active() -> error::Result<bool> {
    error::ProfilingNotSupportedSnafu.fail()
}

#[cfg(windows)]
pub fn set_gdump_active(_: bool) -> error::Result<()> {
    error::ProfilingNotSupportedSnafu.fail()
}

#[cfg(windows)]
pub fn symbolicate_jeheap(_dump_content: &[u8]) -> error::Result<Vec<u8>> {
    error::ProfilingNotSupportedSnafu.fail()
}
