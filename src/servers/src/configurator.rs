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

use std::sync::Arc;

use async_trait::async_trait;
use axum::Router;
use tokio::sync::Mutex;

#[async_trait]
pub trait Configurator: Send + Sync {
    fn config_http(&self, route: Router) -> Router {
        route
    }
}

pub type ConfiguratorRefOption = Option<Arc<dyn Configurator>>;
pub type ConfiguratorRef = Arc<Mutex<Option<Arc<dyn Configurator>>>>;
