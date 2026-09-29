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

pub mod create;
pub mod error;
pub mod format;
pub mod search;

/// A finite state transducer map that shares ownership of its backing bytes
/// with the reader that produced it, so the FST payload is not copied into
/// the map.
pub type FstMap = fst::Map<bytes::Bytes>;
