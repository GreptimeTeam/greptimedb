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

//! Core skeleton for the GreptimeDB query frontend.
//!
//! The query frontend sits in front of the query path and is intended to host
//! optimizations such as in-flight coalescing or caching in later iterations.
//! This crate only establishes the protocol-neutral building blocks; it does
//! **not** yet change any runtime behavior.
//!
//! Deliberately out of scope for this skeleton: singleflight, caching,
//! stale-while-revalidate, TTLs, timestamp/query normalization, and response
//! replay. The defaults are no-ops so wiring this crate in cannot alter the
//! current request path.
//!
//! Module map:
//! - [`request`]: protocol-neutral request DTO handed to the frontend.
//! - [`key`]: exact identity key derived from a request.
//! - [`policy`]: decides whether the frontend engages for a request.
//! - [`config`]: configuration, disabled by default.
//! - [`metrics`]: stable metric/label names for observability.
//! - [`executor`]: the boundary trait that runs the underlying query.

pub mod config;
pub mod executor;
pub mod key;
pub mod metrics;
pub mod policy;
pub mod request;
