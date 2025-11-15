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

use std::collections::HashMap;
use std::sync::Arc;

use catalog::information_schema::InformationSchemaTableFactoryRef;
use common_error::ext::BoxedError;
use flow::FrontendClient;

/// Provides a set of factories for creating information schema tables.
///
/// For example, the enterprise version will provider some information schema
/// tables, such as `information_schema.triggers` and `information_schema.alerts`
/// etc.
#[async_trait::async_trait]
pub trait InformationSchemaTableFactoryProvider: Send + Sync {
    async fn create_factories(
        &self,
        ctx: ProviderContext,
    ) -> std::result::Result<HashMap<String, InformationSchemaTableFactoryRef>, BoxedError>;
}

pub type InformationSchemaTableFactoryProviderRef = Arc<dyn InformationSchemaTableFactoryProvider>;

pub struct ProviderContext {
    pub fe_client: Option<Arc<FrontendClient>>,
}
