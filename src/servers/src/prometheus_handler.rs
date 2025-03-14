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

//! prom supply the prometheus HTTP API Server compliance

use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use catalog::CatalogManagerRef;
use common_query::Output;
use promql_parser::label::Matcher;
use query::parser::PromQuery;
use session::context::QueryContextRef;

use crate::error::Result;

pub const PROMETHEUS_API_VERSION: &str = "v1";

pub type PrometheusHandlerRef = Arc<dyn PrometheusHandler + Send + Sync>;

#[async_trait]
pub trait PrometheusHandler {
    async fn do_query(&self, query: &PromQuery, query_ctx: QueryContextRef) -> Result<Output>;

    /// Query metric table names by the `__name__` matchers.
    async fn query_metric_names(
        &self,
        matchers: Vec<Matcher>,
        ctx: &QueryContextRef,
    ) -> Result<Vec<String>>;

    async fn query_label_values(
        &self,
        metric: String,
        label_name: String,
        matchers: Vec<Matcher>,
        start: SystemTime,
        end: SystemTime,
        ctx: &QueryContextRef,
    ) -> Result<Vec<String>>;

    fn catalog_manager(&self) -> CatalogManagerRef;
}
