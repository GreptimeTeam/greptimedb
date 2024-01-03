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

pub mod cluster;
mod grpc;
mod influxdb;
mod instance;
mod opentsdb;
mod otlp;
mod prom_store;
pub mod test_util;

pub mod standalone;
#[cfg(test)]
mod tests;

#[cfg(test)]
// allowed because https://docs.rs/rstest_reuse/0.5.0/rstest_reuse/#use-rstest_reuse-at-the-top-of-your-crate
#[allow(clippy::single_component_path_imports)]
use rstest_reuse;
