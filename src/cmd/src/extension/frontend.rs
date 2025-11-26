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
use common_meta::kv_backend::KvBackendRef;
use meta_client::MetaClientRef;
#[cfg(feature = "enterprise")]
use operator::statement::TriggerQuerierRef;

/// The extension point for frontend instance.
#[derive(Default)]
pub struct Extension {
    pub info_schema_factories: Option<HashMap<String, InformationSchemaTableFactoryRef>>,
    #[cfg(feature = "enterprise")]
    pub trigger_querier: Option<TriggerQuerierRef>,
}

/// Factory trait to create Extension instances.
#[async_trait::async_trait]
pub trait ExtensionFactory: Send + Sync {
    async fn create(&self, ctx: ExtensionContext) -> Result<Extension, BoxedError>;
}

pub type FrontendExtesionFactoryRef = Arc<dyn ExtensionFactory>;

/// Context provided to ExtensionFactory during extension creation.
pub struct ExtensionContext {
    pub kv_backend: KvBackendRef,
    pub meta_client: MetaClientRef,
}
