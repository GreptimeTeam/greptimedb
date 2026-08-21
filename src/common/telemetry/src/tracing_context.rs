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

//! tracing stuffs, inspired by RisingWave
use std::collections::HashMap;

use opentelemetry::propagation::TextMapPropagator;
use opentelemetry::trace::TraceContextExt;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use tracing_opentelemetry::{OpenTelemetrySpanExt, get_otel_context};

// An wrapper for `Futures` that provides tracing instrument adapters.
pub trait FutureExt: std::future::Future + Sized {
    fn trace(self, span: tracing::span::Span) -> tracing::instrument::Instrumented<Self>;
}

impl<T: std::future::Future> FutureExt for T {
    #[inline]
    fn trace(self, span: tracing::span::Span) -> tracing::instrument::Instrumented<Self> {
        tracing::instrument::Instrument::instrument(self, span)
    }
}

/// Context for tracing used for propagating tracing information in a distributed system.
///
/// Generally, the caller of a service should create a tracing context from the current tracing span
/// and pass it to the callee through the network. The callee will then attach its local tracing
/// span as a child of the tracing context, so that the external tracing service can associate them
/// in a single trace.
///
/// The tracing context must be serialized into the W3C trace context format and passed in rpc
/// message headers when communication of frontend, datanode and meta.
///
/// See [Trace Context](https://www.w3.org/TR/trace-context/) for more information.
#[derive(Debug, Clone)]
pub struct TracingContext(opentelemetry::Context);

pub type W3cTrace = HashMap<String, String>;

/// Returns valid identifiers for the current OpenTelemetry span, if one exists.
/// An absent or invalid context is deliberately represented as `None` so ordinary
/// logs never manufacture correlation identifiers.
pub fn current_trace_ids() -> Option<(String, String)> {
    current_trace_ids_for_span(tracing::Span::current().id().as_ref())
}

/// Returns identifiers for a span known by the active subscriber. This variant
/// is used by formatting layers while they are rendering an event.
pub fn current_trace_ids_for_span(id: Option<&tracing::span::Id>) -> Option<(String, String)> {
    let current = opentelemetry::Context::current();
    let span_context = if current.span().span_context().is_valid() {
        current.span().span_context().clone()
    } else {
        id.and_then(|id| {
            tracing::dispatcher::get_default(|dispatch| get_otel_context(id, dispatch))
        })
        .unwrap_or_else(|| tracing::Span::current().context())
        .span()
        .span_context()
        .clone()
    };
    span_context.is_valid().then(|| {
        (
            span_context.trace_id().to_string(),
            span_context.span_id().to_string(),
        )
    })
}

impl Default for TracingContext {
    fn default() -> Self {
        Self::new()
    }
}

type Propagator = TraceContextPropagator;

impl TracingContext {
    /// Create a new tracing context from a tracing span.
    pub fn from_span(span: &tracing::Span) -> Self {
        Self(span.context())
    }

    /// Create a new tracing context from the current tracing span considered by the subscriber.
    pub fn from_current_span() -> Self {
        Self::from_span(&tracing::Span::current())
    }

    /// Create a no-op tracing context.
    pub fn new() -> Self {
        Self(opentelemetry::Context::new())
    }

    /// Attach the given span as a child of the context. Returns the attached span.
    pub fn attach(&self, span: tracing::Span) -> tracing::Span {
        let _ = span.set_parent(self.0.clone());
        span
    }

    /// Convert the tracing context to the W3C trace context format.
    pub fn to_w3c(&self) -> W3cTrace {
        let mut fields = HashMap::new();
        Propagator::new().inject_context(&self.0, &mut fields);
        fields
    }

    /// Create a new tracing context from the W3C trace context format.
    pub fn from_w3c(fields: &W3cTrace) -> Self {
        let context = Propagator::new().extract(fields);
        Self(context)
    }

    /// Convert the tracing context to a JSON string in W3C trace context format.
    pub fn to_json(&self) -> String {
        serde_json::to_string(&self.to_w3c()).unwrap()
    }

    /// Create a new tracing context from a JSON string in W3C trace context format.
    ///
    /// Illegal json string will produce an empty tracing context and no error will be reported.
    pub fn from_json(json: &str) -> Self {
        let fields: W3cTrace = serde_json::from_str(json).unwrap_or_default();
        Self::from_w3c(&fields)
    }
}
