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

use std::hash::Hash;

/// The namespace id.
/// Usually the namespace id is identical with the region id.
pub type Id = u64;

pub trait Namespace: Send + Sync + Clone + std::fmt::Debug + Hash + PartialEq + Eq {
    /// Returns the namespace id.
    fn id(&self) -> Id;
}
