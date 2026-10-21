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

//! The native PromQL evaluation model.
//!
//! A query compiles to a [`plan::NativeTree`]: nodes whose operations, fields and grids are already
//! bound. Nodes hand each other [`batch::EvaluatedChunk`]s as ordinary Rust values, so no row batch
//! is materialised between them. [`metrics`] names each node after its position in that tree.
//!
//! Three properties keep this apart from recognising a query shape in a finished physical plan:
//!
//! - Legality is a type and data-dependency question ([`plan::NativeNode::validate`]), so a new
//!   combination of supported operations is an ordinary tree, not another matcher.
//! - A window binds its input and output grids separately, so a subquery's inner grid is consumed
//!   as it is instead of being resampled onto the outer grid.
//! - A series closes exactly once, on the chunk that says so, so a consumer can tell "nothing more
//!   here" from "no more input".

pub mod batch;
mod display;
pub mod execute;
pub mod metrics;
pub mod plan;
pub(crate) mod runs;
