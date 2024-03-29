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

/// This crate is designed to be at the bottom of the depencey tree
/// to provide common and useful utils and consts to all plugin usage,
/// since `plugins` crate is at the top depending on crates like `frontend` and `datanode`
mod consts;

pub use consts::{GREPTIME_EXEC_PREFIX, GREPTIME_EXEC_READ_COST, GREPTIME_EXEC_WRITE_COST};
