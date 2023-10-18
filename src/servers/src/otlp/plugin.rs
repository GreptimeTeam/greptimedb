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
