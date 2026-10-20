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

//! The batches native nodes hand to each other.
//!
//! These are ordinary Rust values passed by call, not DataFusion streams and not encoded messages:
//! the Arrow arrays, buffers and shared schemas inside them are reused as they are.
//!
//! Two rules make a chunk self-describing:
//!
//! - A grid position is absolute on the chunk's grid. Batch-local row numbers never stand in for
//!   one, and an inner grid is never addressed with an outer grid's index.
//! - A series closes exactly once, on the chunk that carries [`EvaluatedChunk::is_last`]. A closed
//!   prefix still has to be followed by a final chunk even when that chunk holds no values, so a
//!   consumer can tell "nothing more here" from "no more input".

//!
//! The chunk type itself is [`crate::evaluated::EvaluatedChunk`]: one chunk protocol serves both the
//! evaluator that produces chunks and the native nodes that consume them. Defining a second one
//! here would let the two drift apart, and a chunk produced by the evaluator could not be handed to
//! a node without translation.

pub use crate::evaluated::EvaluatedChunk;
