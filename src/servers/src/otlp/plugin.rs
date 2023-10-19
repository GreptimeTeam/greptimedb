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

use api::v1::RowInsertRequests;

/// Transformer helps to transform ExportTraceServiceRequest based on logic, like:
///   - uplift some fields from Attributes (Map type) to column
pub trait TraceTransformer: Send + Sync {
    fn transform(&self, request: RowInsertRequests) -> RowInsertRequests {
        request
    }
}

pub type TraceTransformerRef = Arc<dyn TraceTransformer>;

impl TraceTransformer for Option<&TraceTransformerRef> {
    fn transform(&self, request: RowInsertRequests) -> RowInsertRequests {
        match self {
            Some(this) => this.transform(request),
            None => request,
        }
    }
}
