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

//! Metric Engine is a region engine to store timeseries data in metric monitoring
//! scenario. It is something like a multiplexer over the [Mito](mito2::engine::MitoEngine)
//! engine, which is for a more generic use case. By leveraging a synthetic wide physical
//! table (region) that offers storage for multiple logical tables, Metric Engine is able to
//! provide a more efficient storage solution that is able to handle a tremendous number of
//! small tables in scenarios like Prometheus metrics.
//!
//! For more details about implementation, please refer to [MetricEngine](crate::engine::MetricEngine).
//!
//! This new engine doesn't re-implement low level components like file R/W etc. It warps the
//! existing mito engine, with extra storage and metadata multiplexing logic. I.e., it expose
//! multiple logical regions based on two physical mito engine regions like this:
//!
//! ```plaintext
//! ┌───────────────┐ ┌───────────────┐ ┌───────────────┐
//! │ Metric Engine │ │ Metric Engine │ │ Metric Engine │
//! │   Region 1    │ │   Region 2    │ │   Region 3    │
//! └───────────────┘ └───────────────┘ └───────────────┘
//!         ▲               ▲                   ▲
//!         │               │                   │
//!         └───────────────┼───────────────────┘
//!                         │
//!               ┌─────────┴────────┐
//!               │ Metric Region    │
//!               │   Engine         │
//!               │    ┌─────────────┤
//!               │    │ Mito Region │
//!               │    │   Engine    │
//!               └────▲─────────────┘
//!                    │
//!                    │
//!              ┌─────┴───────────────┐
//!              │                     │
//!              │ Mito Engine Regions │
//!              │                     │
//!              └─────────────────────┘
//! ```

#![feature(let_chains)]

mod data_region;
#[allow(unused)]
pub mod engine;
pub mod error;
mod metadata_region;
mod metrics;
#[cfg(test)]
mod test_util;
mod utils;
