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

#![feature(assert_matches)]

pub mod dist_table;
pub mod error;
pub mod metadata;
pub mod predicate;
pub mod requests;
pub mod stats;
pub mod table;
pub mod table_name;
pub mod table_reference;
pub mod test_util;

pub use crate::error::{Error, Result};
pub use crate::stats::{ColumnStatistics, TableStatistics};
pub use crate::table::{Table, TableRef};
