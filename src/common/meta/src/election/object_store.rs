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

//! Object-store Authority election, Head v1 codec and conditional storage.

mod authority;
mod head;
mod probe;
mod store;
#[cfg(test)]
mod tests;

pub use authority::{
    Authority, AuthorityMutation, AuthorityPrecondition, AuthorityStatus, FencingToken,
    TakeoverOutcome,
};
pub use head::{AuthorityHead, HEAD_FORMAT_VERSION, NodeIdentity};
pub use store::{AuthorityState, HeadStore};
