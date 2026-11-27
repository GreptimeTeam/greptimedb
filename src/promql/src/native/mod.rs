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
//! A query compiles to a [`plan::NativeTree`] whose nodes have their operations, fields and grids
//! already bound; legality is checked by [`plan::NativeNode::validate`]. Nodes hand each other
//! [`batch::EvaluatedChunk`]s as Rust values, and a window binds its input and output grids
//! separately, so a subquery's inner grid is consumed without resampling onto the outer grid.
//! [`metrics`] names each node after its position in the tree.

mod aggregate;
pub mod batch;
pub mod canonicalize;
pub mod display;
pub mod execute;
pub mod metrics;
pub mod plan;
pub mod segment;
pub(crate) mod split;
pub(crate) mod state;
