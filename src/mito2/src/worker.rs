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

//! Structs and utilities for writing regions.

use crate::region::RegionMapRef;

/// A fixed size group of [RegionWorker]s.
///
/// The group binds each region to a specific [RegionWorker].
#[derive(Debug, Default)]
pub(crate) struct WorkerGroup {
    workers: Vec<RegionWorker>,
}

/// Worker to write and alter regions bound to it.
#[derive(Debug, Default)]
struct RegionWorker {
    regions: RegionMapRef,
}
