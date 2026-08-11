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

use std::sync::{Arc, RwLock, Weak};

use common_event_recorder::{EventRecorder, EventRecorderRef};

use crate::error::Result;
use crate::statement::admin::event::{
    ADMIN_FUNCTION_EVENT_TYPE, AdminFunctionEvent, AdminFunctionEventInput,
};
use crate::statement::admin::{
    AdminFunctionRequest, AdminFunctionResponse, AdminFunctionService, AdminFunctionServiceRef,
};

/// A composable layer over ADMIN function execution.
pub trait AdminFunctionLayer: Send + Sync {
    /// Wraps `inner` and returns the resulting ADMIN function service.
    fn layer(&self, inner: AdminFunctionServiceRef) -> AdminFunctionServiceRef;
}

/// A shared composable ADMIN function layer.
pub type AdminFunctionLayerRef = Arc<dyn AdminFunctionLayer>;

/// A late-bound weak reference to the event recorder.
///
/// The weak reference avoids retaining the frontend event handler, which owns a
/// statement executor itself.
#[derive(Clone, Default)]
pub struct AdminEventRecorderHandle {
    recorder: Arc<RwLock<Option<Weak<dyn EventRecorder>>>>,
}

impl AdminEventRecorderHandle {
    /// Installs the event recorder as a weak reference.
    pub fn install(&self, recorder: &EventRecorderRef) {
        *self.recorder.write().unwrap_or_else(|e| e.into_inner()) = Some(Arc::downgrade(recorder));
    }

    fn upgrade(&self) -> Option<EventRecorderRef> {
        self.recorder
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .and_then(Weak::upgrade)
    }
}

/// A layer that records ADMIN function inputs and outcomes as events.
#[derive(Clone)]
pub struct AdminFunctionRecordingLayer {
    recorder: AdminEventRecorderHandle,
}

impl AdminFunctionRecordingLayer {
    /// Creates a recording layer backed by the late-bound recorder handle.
    pub fn new(recorder: AdminEventRecorderHandle) -> Self {
        Self { recorder }
    }
}

impl AdminFunctionLayer for AdminFunctionRecordingLayer {
    fn layer(&self, inner: AdminFunctionServiceRef) -> AdminFunctionServiceRef {
        Arc::new(AdminFunctionRecordingService {
            inner,
            recorder: self.recorder.clone(),
        })
    }
}

#[derive(Clone)]
struct AdminFunctionRecordingService {
    inner: AdminFunctionServiceRef,
    recorder: AdminEventRecorderHandle,
}

#[async_trait::async_trait]
impl AdminFunctionService for AdminFunctionRecordingService {
    async fn call(&self, request: AdminFunctionRequest) -> Result<AdminFunctionResponse> {
        // Avoid building the event input when this event type is disabled. Do
        // not retain the recorder across ADMIN execution because its handler can
        // own the statement executor that contains this layer.
        let enabled = self.recorder.upgrade().is_some_and(|recorder| {
            recorder
                .event_type_filter()
                .allows(ADMIN_FUNCTION_EVENT_TYPE)
        });
        let mut recording = AdminFunctionRecordingGuard::new(
            enabled.then(|| AdminFunctionEventInput::from_request(&request)),
            self.recorder.clone(),
        );
        let result = self.inner.call(request).await;
        recording.record_result(&result);
        result
    }
}

/// Records an ADMIN event after completion or when an in-flight call is dropped.
struct AdminFunctionRecordingGuard {
    input: Option<AdminFunctionEventInput>,
    recorder: AdminEventRecorderHandle,
}

impl AdminFunctionRecordingGuard {
    fn new(input: Option<AdminFunctionEventInput>, recorder: AdminEventRecorderHandle) -> Self {
        Self { input, recorder }
    }

    fn record_result(&mut self, result: &Result<AdminFunctionResponse>) {
        let Some(input) = self.input.take() else {
            return;
        };
        let Some(recorder) = self.recorder.upgrade() else {
            return;
        };
        let event = match result {
            Ok(response) => AdminFunctionEvent::success(input, response.immediate_result.as_ref()),
            Err(error) => AdminFunctionEvent::failure(input, error),
        };
        recorder.record(Box::new(event));
    }
}

impl Drop for AdminFunctionRecordingGuard {
    fn drop(&mut self) {
        let Some(input) = self.input.take() else {
            return;
        };
        let Some(recorder) = self.recorder.upgrade() else {
            return;
        };
        recorder.record(Box::new(AdminFunctionEvent::cancelled(input)));
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::fmt::{Debug, Formatter};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use common_event_recorder::event_table::jsonb_value;
    use common_event_recorder::{
        Event, EventRecorder, EventRecorderRef, EventTypeFilter, EventTypeFilterRef,
    };
    use common_query::{Output, OutputData};
    use datatypes::value::Value;
    use serde_json::json;
    use session::context::QueryContext;
    use sql::dialect::GreptimeDbDialect;
    use sql::parser::{ParseOptions, ParserContext};
    use sql::statements::statement::Statement;
    use tokio::sync::Notify;

    use crate::error::Result;
    use crate::statement::admin::layer::{AdminEventRecorderHandle, AdminFunctionRecordingLayer};
    use crate::statement::admin::{
        AdminFunctionLayer, AdminFunctionRequest, AdminFunctionResponse, AdminFunctionService,
        AdminFunctionServiceRef,
    };

    #[derive(Clone)]
    struct TraceLayer {
        trace: Arc<Mutex<Vec<&'static str>>>,
        before: &'static str,
        after: &'static str,
    }

    impl AdminFunctionLayer for TraceLayer {
        fn layer(&self, inner: AdminFunctionServiceRef) -> AdminFunctionServiceRef {
            Arc::new(TraceService {
                inner,
                trace: self.trace.clone(),
                before: self.before,
                after: self.after,
            })
        }
    }

    struct TraceService {
        inner: AdminFunctionServiceRef,
        trace: Arc<Mutex<Vec<&'static str>>>,
        before: &'static str,
        after: &'static str,
    }

    #[async_trait::async_trait]
    impl AdminFunctionService for TraceService {
        async fn call(&self, request: AdminFunctionRequest) -> Result<AdminFunctionResponse> {
            self.trace.lock().unwrap().push(self.before);
            let result = self.inner.call(request).await;
            self.trace.lock().unwrap().push(self.after);
            result
        }
    }

    struct TestService {
        trace: Option<Arc<Mutex<Vec<&'static str>>>>,
        immediate_result: Option<Value>,
    }

    #[async_trait::async_trait]
    impl AdminFunctionService for TestService {
        async fn call(&self, _request: AdminFunctionRequest) -> Result<AdminFunctionResponse> {
            if let Some(trace) = &self.trace {
                trace.lock().unwrap().push("core");
            }
            Ok(response(self.immediate_result.clone()))
        }
    }

    struct PendingService {
        started: Arc<Notify>,
    }

    #[async_trait::async_trait]
    impl AdminFunctionService for PendingService {
        async fn call(&self, _request: AdminFunctionRequest) -> Result<AdminFunctionResponse> {
            self.started.notify_one();
            std::future::pending().await
        }
    }

    struct TestRecorder {
        events: Mutex<Vec<Box<dyn Event>>>,
        filter: EventTypeFilterRef,
    }

    impl TestRecorder {
        fn new(filter: EventTypeFilter) -> Self {
            Self {
                events: Mutex::new(Vec::new()),
                filter: Arc::new(filter),
            }
        }
    }

    impl Debug for TestRecorder {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("TestRecorder").finish()
        }
    }

    impl EventRecorder for TestRecorder {
        fn record(&self, event: Box<dyn Event>) {
            self.events.lock().unwrap().push(event);
        }

        fn event_type_filter(&self) -> EventTypeFilterRef {
            self.filter.clone()
        }

        fn close(&self) {}
    }

    #[tokio::test]
    async fn last_added_layer_runs_outermost() {
        let trace = Arc::new(Mutex::new(Vec::new()));
        let mut service: AdminFunctionServiceRef = Arc::new(TestService {
            trace: Some(trace.clone()),
            immediate_result: Some(Value::UInt64(0)),
        });
        for (before, after) in [("a_before", "a_after"), ("b_before", "b_after")] {
            service = TraceLayer {
                trace: trace.clone(),
                before,
                after,
            }
            .layer(service);
        }

        service.call(request()).await.unwrap();
        assert_eq!(
            *trace.lock().unwrap(),
            ["b_before", "a_before", "core", "a_after", "b_after"]
        );
    }

    #[tokio::test]
    async fn recording_respects_filter_and_weak_lifetime() {
        let enabled = Arc::new(TestRecorder::new(EventTypeFilter::All));
        let enabled_ref: EventRecorderRef = enabled.clone();
        let handle = AdminEventRecorderHandle::default();
        handle.install(&enabled_ref);
        recording_service(handle.clone())
            .call(request())
            .await
            .unwrap();
        assert_eq!(enabled.events.lock().unwrap().len(), 1);

        let disabled = Arc::new(TestRecorder::new(EventTypeFilter::Only(HashSet::new())));
        let disabled_ref: EventRecorderRef = disabled.clone();
        handle.install(&disabled_ref);
        recording_service(handle.clone())
            .call(request())
            .await
            .unwrap();
        assert!(disabled.events.lock().unwrap().is_empty());

        drop(disabled_ref);
        drop(disabled);
        assert!(handle.upgrade().is_none());
        recording_service(handle).call(request()).await.unwrap();

        let recorder = Arc::new(TestRecorder::new(EventTypeFilter::All));
        let recorder_ref: EventRecorderRef = recorder.clone();
        let handle = AdminEventRecorderHandle::default();
        handle.install(&recorder_ref);
        let service = recording_service(handle.clone());
        let future = service.call(request());
        drop(recorder_ref);
        drop(recorder);
        assert!(handle.upgrade().is_none());
        future.await.unwrap();
    }

    #[tokio::test]
    async fn recording_does_not_retain_recorder_while_pending() {
        let recorder = Arc::new(TestRecorder::new(EventTypeFilter::All));
        let recorder_ref: EventRecorderRef = recorder.clone();
        let handle = AdminEventRecorderHandle::default();
        handle.install(&recorder_ref);
        let started = Arc::new(Notify::new());
        let service =
            AdminFunctionRecordingLayer::new(handle.clone()).layer(Arc::new(PendingService {
                started: started.clone(),
            }));

        let task = tokio::spawn(async move { service.call(request()).await });
        started.notified().await;
        drop(recorder_ref);
        drop(recorder);

        assert!(handle.upgrade().is_none());
        task.abort();
        assert!(matches!(task.await, Err(error) if error.is_cancelled()));
    }

    #[tokio::test]
    async fn dropping_pending_call_records_failure() {
        let recorder = Arc::new(TestRecorder::new(EventTypeFilter::All));
        let recorder_ref: EventRecorderRef = recorder.clone();
        let handle = AdminEventRecorderHandle::default();
        handle.install(&recorder_ref);
        let started = Arc::new(Notify::new());
        let service = AdminFunctionRecordingLayer::new(handle).layer(Arc::new(PendingService {
            started: started.clone(),
        }));

        let task = tokio::spawn(async move { service.call(request()).await });
        started.notified().await;
        task.abort();
        assert!(matches!(task.await, Err(error) if error.is_cancelled()));
        assert_eq!(recorder.events.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn timing_out_pending_call_records_failure() {
        let recorder = Arc::new(TestRecorder::new(EventTypeFilter::All));
        let recorder_ref: EventRecorderRef = recorder.clone();
        let handle = AdminEventRecorderHandle::default();
        handle.install(&recorder_ref);
        let service = AdminFunctionRecordingLayer::new(handle).layer(Arc::new(PendingService {
            started: Arc::new(Notify::new()),
        }));

        assert!(
            tokio::time::timeout(Duration::from_millis(10), service.call(request()))
                .await
                .is_err()
        );
        assert_eq!(recorder.events.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn empty_immediate_result_preserves_success_output_and_records_no_result() {
        let recorder = Arc::new(TestRecorder::new(EventTypeFilter::All));
        let recorder_ref: EventRecorderRef = recorder.clone();
        let handle = AdminEventRecorderHandle::default();
        handle.install(&recorder_ref);
        let service = AdminFunctionRecordingLayer::new(handle).layer(Arc::new(TestService {
            trace: None,
            immediate_result: None,
        }));

        let response = service.call(request()).await.unwrap();
        assert!(matches!(response.output.data, OutputData::AffectedRows(0)));
        assert_eq!(response.immediate_result, None);

        let events = recorder.events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].extra_rows().unwrap()[0].values[3],
            jsonb_value(&json!({}))
        );
    }

    fn recording_service(handle: AdminEventRecorderHandle) -> AdminFunctionServiceRef {
        let core = Arc::new(TestService {
            trace: None,
            immediate_result: Some(Value::UInt64(0)),
        });
        AdminFunctionRecordingLayer::new(handle).layer(core)
    }

    fn response(immediate_result: Option<Value>) -> AdminFunctionResponse {
        AdminFunctionResponse {
            output: Output::new_with_affected_rows(0),
            immediate_result,
        }
    }

    fn request() -> AdminFunctionRequest {
        let Statement::Admin(statement) = ParserContext::create_with_dialect(
            "ADMIN flush_table('demo')",
            &GreptimeDbDialect {},
            ParseOptions::default(),
        )
        .unwrap()
        .remove(0) else {
            panic!("expected ADMIN statement")
        };
        AdminFunctionRequest {
            statement,
            query_ctx: QueryContext::arc(),
        }
    }
}
