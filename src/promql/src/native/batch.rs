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

//! The chunk native nodes hand to each other by call; see [`EvaluatedChunk`] for the protocol.
//!
//! A series closes once per input series, not per label set: a node that rewrites labels can emit
//! two input series under one label set, so a consumer that merges inputs must accept a second
//! close of the same label set.

pub use crate::evaluated::EvaluatedChunk;
