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

#[cfg(feature = "vector_index")]
pub mod exec;
#[cfg(feature = "vector_index")]
pub(crate) mod metrics;
#[cfg(feature = "vector_index")]
pub mod options;
#[cfg(feature = "vector_index")]
pub mod plan;
#[cfg(feature = "vector_index")]
pub mod planner;
#[cfg(all(test, feature = "vector_index"))]
pub(crate) mod test_utils;
#[cfg(feature = "vector_index")]
pub(crate) mod utils;
