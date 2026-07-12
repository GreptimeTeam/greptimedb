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

use std::sync::{Arc, Mutex};

use axum::Router;

/// A thread-safe wrapper for an Axum [`Router`] that allows for dynamic configuration.
#[derive(Default, Clone)]
pub struct RouterConfigurator(Arc<Mutex<Router>>);

impl RouterConfigurator {
    /// Returns a clone of the current [`Router`].
    pub fn router(&self) -> Router {
        self.0.lock().unwrap().clone()
    }

    /// Applies a configuration function to the current [`Router`].
    ///
    /// The provided closure receives the current router (cloned) and should return a new [`Router`].
    /// The internal router is then replaced with the result.
    pub fn configure(&self, f: impl FnOnce(Router) -> Router) {
        let mut router = self.0.lock().unwrap();
        *router = f(router.clone());
    }
}
