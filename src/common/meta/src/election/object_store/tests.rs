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
use std::sync::{Arc, Mutex};

use common_error::ext::{ErrorExt, RetryHint};
use common_error::status_code::StatusCode;
use object_store::layers::RetryLayer;
use object_store::layers::mock::{
    Buffer, BytesRange, Capability, Error, ErrorKind, Metadata, MetadataBuilder, OpCopy,
    OpCreateDir, OpDelete, OpList, OpPresign, OpRead, OpRename, OpStat, OpWrite, OperationContext,
    Result, RpCreateDir, RpPresign, RpRead, RpRename, RpStat, Service, Timestamp, oio,
};
use object_store::{ObjectStore, ServiceInfo};

use super::*;

// Explicitly supplies coherent response metadata. This is a protocol simulator,
// not a claim about memory-service or real-backend semantics.
#[derive(Debug, Default)]
struct Data {
    objects: HashMap<String, (Vec<u8>, Metadata)>,
    seq: u64,
    writes: usize,
    ignore_conditions: bool,
    ignore_replace: bool,
    create_exists: bool,
    missing_metadata: bool,
    fail: Option<&'static str>,
    delete_fail: bool,
    deleted: Vec<String>,
    conflict_create: Option<ConflictMutation>,
    conflict_replace: Option<ConflictMutation>,
    corruptions: usize,
    write_barrier: Option<Arc<tokio::sync::Barrier>>,
    write_arrivals: usize,
    writes_in_flight: usize,
    max_writes_in_flight: usize,
    pause: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
}

#[derive(Debug, Clone, Copy)]
enum ConflictMutation {
    Body,
    Identity,
}

#[derive(Debug, Clone)]
struct Backend {
    data: Arc<Mutex<Data>>,
    scheme: &'static str,
    capability: Capability,
}

impl Default for Backend {
    fn default() -> Self {
        Self {
            data: Arc::default(),
            scheme: "s3",
            capability: Capability {
                read: true,
                write: true,
                list: true,
                delete: true,
                write_with_if_not_exists: true,
                write_with_if_match: true,
                ..Default::default()
            },
        }
    }
}

impl Service for Backend {
    async fn create_dir(
        &self,
        _: &OperationContext,
        _: &str,
        _: OpCreateDir,
    ) -> Result<RpCreateDir> {
        unreachable!()
    }

    async fn stat(&self, _: &OperationContext, _: &str, _: OpStat) -> Result<RpStat> {
        unreachable!("Head must not stat")
    }

    fn copy(&self, _: &OperationContext, _: &str, _: &str, _: OpCopy) -> Result<()> {
        unreachable!()
    }

    async fn rename(
        &self,
        _: &OperationContext,
        _: &str,
        _: &str,
        _: OpRename,
    ) -> Result<RpRename> {
        unreachable!()
    }

    async fn presign(&self, _: &OperationContext, _: &str, _: OpPresign) -> Result<RpPresign> {
        unreachable!()
    }

    type Reader = TestReader;
    type Writer = TestWriter;
    type Lister = TestLister;
    type Deleter = TestDeleter;
    type Copier = ();
    type Composer = ();

    fn info(&self) -> ServiceInfo {
        ServiceInfo::new(self.scheme, "/", "authority-test")
    }

    fn capability(&self) -> Capability {
        self.capability
    }

    fn read(&self, _: &OperationContext, path: &str, _: OpRead) -> Result<Self::Reader> {
        Ok(TestReader {
            data: self.data.clone(),
            path: path.into(),
        })
    }

    fn write(&self, _: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        Ok(TestWriter {
            data: self.data.clone(),
            path: path.into(),
            args,
            body: Vec::new(),
        })
    }

    fn delete(&self, _: &OperationContext) -> Result<Self::Deleter> {
        Ok(TestDeleter(self.data.clone()))
    }

    fn list(&self, _: &OperationContext, _: &str, _: OpList) -> Result<TestLister> {
        if self.data.lock().unwrap().fail == Some("list") {
            return Err(denied());
        }
        Ok(TestLister)
    }
}

struct TestLister;

impl oio::List for TestLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        Ok(None)
    }
}

fn denied() -> Error {
    Error::new(ErrorKind::PermissionDenied, "injected permission failure")
}

struct TestReader {
    data: Arc<Mutex<Data>>,
    path: String,
}

impl oio::Read for TestReader {
    async fn open(&self, _: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let data = self.data.lock().unwrap();
        let (body, meta) = data
            .objects
            .get(&self.path)
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "missing"))?;
        let rp = if data.missing_metadata {
            RpRead::default()
        } else {
            RpRead::new(meta.clone())
        };
        Ok((
            rp,
            Box::new(TestStream {
                body: Buffer::from(body.clone()),
                pause: data.pause.clone(),
            }),
        ))
    }

    async fn read(&self, _: BytesRange) -> Result<(RpRead, Buffer)> {
        unreachable!("full read must use one stream")
    }
}

struct TestStream {
    body: Buffer,
    pause: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
}

impl oio::ReadStream for TestStream {
    async fn read(&mut self) -> Result<Buffer> {
        if let Some((opened, release)) = self.pause.take() {
            opened.notify_one();
            release.notified().await;
        }
        Ok(std::mem::take(&mut self.body))
    }
}

struct TestWriter {
    data: Arc<Mutex<Data>>,
    path: String,
    args: OpWrite,
    body: Vec<u8>,
}

impl oio::Write for TestWriter {
    async fn write(&mut self, bytes: Buffer) -> Result<()> {
        self.body.extend(bytes.to_vec());
        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        let barrier = {
            let mut data = self.data.lock().unwrap();
            let barrier = data.write_barrier.clone();
            if barrier.is_some() {
                data.write_arrivals += 1;
                data.writes_in_flight += 1;
                data.max_writes_in_flight = data.max_writes_in_flight.max(data.writes_in_flight);
            }
            barrier
        };
        if let Some(barrier) = barrier {
            barrier.wait().await;
        }
        let mut data = self.data.lock().unwrap();
        if data.write_barrier.is_some() {
            data.writes_in_flight -= 1;
        }
        data.writes += 1;
        if data.fail == Some("write") {
            return Err(Error::new(ErrorKind::Unexpected, "lost response").set_temporary());
        }
        if data.fail == Some("plain")
            && !self.args.if_not_exists()
            && self.args.if_match().is_none()
        {
            return Err(denied());
        }
        if !data.ignore_conditions {
            if self.args.if_not_exists() && data.objects.contains_key(&self.path) {
                let mutation = data.conflict_create;
                corrupt_conflict(&mut data, &self.path, mutation);
                return Err(Error::new(
                    if data.create_exists {
                        ErrorKind::AlreadyExists
                    } else {
                        ErrorKind::ConditionNotMatch
                    },
                    "exists",
                ));
            }
            if !data.ignore_replace
                && let Some(version) = self.args.if_version_match()
                && data.objects.get(&self.path).and_then(|(_, m)| m.version()) != Some(version)
            {
                let mutation = data.conflict_replace;
                corrupt_conflict(&mut data, &self.path, mutation);
                return Err(Error::new(
                    ErrorKind::ConditionNotMatch,
                    "stale native version",
                ));
            }
            if !data.ignore_replace
                && let Some(etag) = self.args.if_match()
                && data.objects.get(&self.path).and_then(|(_, m)| m.etag()) != Some(etag)
            {
                let mutation = data.conflict_replace;
                corrupt_conflict(&mut data, &self.path, mutation);
                return Err(Error::new(ErrorKind::ConditionNotMatch, "stale"));
            }
        }
        data.seq += 1;
        let mut builder = MetadataBuilder::file(self.body.len() as u64);
        builder
            .etag(format!("\"{}\"", data.seq))
            .version(data.seq.to_string())
            .last_modified(Timestamp::now());
        let meta = builder.build();
        data.objects
            .insert(self.path.clone(), (self.body.clone(), meta.clone()));
        Ok(meta)
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }
}

fn corrupt_conflict(data: &mut Data, path: &str, mutation: Option<ConflictMutation>) {
    let Some(mutation) = mutation else {
        return;
    };
    let (body, metadata) = data.objects.get_mut(path).unwrap();
    match mutation {
        ConflictMutation::Body => {
            let mut head = AuthorityHead::decode(body).unwrap();
            head = head.with_next_write_seq().unwrap();
            *body = head.encode().unwrap();
        }
        ConflictMutation::Identity => {
            let mut builder = metadata.clone().into_builder();
            builder.etag("\"corrupt\"").version("corrupt");
            *metadata = builder.build();
        }
    }
    data.corruptions += 1;
}

struct TestDeleter(Arc<Mutex<Data>>);

impl oio::Delete for TestDeleter {
    async fn delete(&mut self, path: &str, _: OpDelete) -> Result<()> {
        let mut data = self.0.lock().unwrap();
        data.deleted.push(path.into());
        if data.delete_fail {
            return Err(denied());
        }
        data.objects.remove(path);
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

fn store(backend: &Backend) -> HeadStore {
    HeadStore::new_for_test(
        ObjectStore::from_parts(OperationContext::default(), Arc::new(backend.clone())),
        "cluster",
    )
}

#[tokio::test]
async fn test_head_conditional_create() {
    let b = Backend::default();
    let s = store(&b);
    s.create_initial_head().await.unwrap();
    let before = s.read_head().await.unwrap();
    assert!(s.create_initial_head().await.is_err());
    let after = s.read_head().await.unwrap();
    assert_eq!(before.head(), after.head());
    assert!(s.same_version(before.metadata(), after.metadata()).unwrap());
}

#[tokio::test]
async fn test_head_stale_replace() {
    let b = Backend::default();
    let s = store(&b);
    s.create_initial_head().await.unwrap();
    let old = s.read_head().await.unwrap();
    let next = old.head().clone().with_next_write_seq().unwrap();
    s.replace_head(&old, &next).await.unwrap();
    let before = s.read_head().await.unwrap();
    assert_eq!(
        s.replace_head(&old, &next)
            .await
            .unwrap_err()
            .authority_storage_error()
            .unwrap()
            .kind(),
        ErrorKind::ConditionNotMatch
    );
    let after = s.read_head().await.unwrap();
    assert_eq!(after.head(), &next);
    assert!(s.same_version(before.metadata(), after.metadata()).unwrap());
}

#[tokio::test]
async fn test_head_concurrent_replace() {
    let backend = Backend::default();
    let store = store(&backend);
    store.create_initial_head().await.unwrap();
    let old = store.read_head().await.unwrap();
    let first = old.head().clone().with_next_write_seq().unwrap();
    let second = first.clone().with_next_write_seq().unwrap();
    backend.data.lock().unwrap().write_barrier = Some(Arc::new(tokio::sync::Barrier::new(2)));
    let (left, right) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(
            store.replace_head(&old, &first),
            store.replace_head(&old, &second)
        )
    })
    .await
    .expect("both conditional writes must reach the barrier");
    {
        let data = backend.data.lock().unwrap();
        assert_eq!(data.write_arrivals, 2);
        assert_eq!(data.max_writes_in_flight, 2);
        assert_eq!(data.writes_in_flight, 0);
    }
    let winner = match (left, right) {
        (Ok(()), Err(error)) => {
            assert_eq!(
                error.authority_storage_error().unwrap().kind(),
                ErrorKind::ConditionNotMatch
            );
            first
        }
        (Err(error), Ok(())) => {
            assert_eq!(
                error.authority_storage_error().unwrap().kind(),
                ErrorKind::ConditionNotMatch
            );
            second
        }
        result => panic!("expected exactly one conditional winner: {result:?}"),
    };
    assert_eq!(store.read_head().await.unwrap().head(), &winner);
}

#[tokio::test]
async fn test_head_missing_no_create() {
    let b = Backend::default();
    let s = store(&b);
    let e = s.read_head().await.unwrap_err();
    assert_eq!(
        e.authority_storage_error().unwrap().kind(),
        ErrorKind::NotFound
    );
    assert_eq!(b.data.lock().unwrap().writes, 0);
}

#[tokio::test]
async fn test_head_missing_metadata() {
    let b = Backend::default();
    let s = store(&b);
    s.create_initial_head().await.unwrap();
    b.data.lock().unwrap().missing_metadata = true;
    assert!(s.read_head().await.is_err());
}

#[tokio::test]
async fn test_head_write_error_not_replayed() {
    for replace in [false, true] {
        let backend = Backend::default();
        let store = store(&backend);
        let error = failed_write(&store, &backend, replace).await;
        assert_eq!(
            error.authority_storage_error().unwrap().kind(),
            ErrorKind::Unexpected
        );
        assert!(error.authority_storage_error().unwrap().is_temporary());
        assert_eq!(error.retry_hint(), RetryHint::NonRetryable);
        assert_eq!(backend.data.lock().unwrap().writes, 1);
    }
}

// A positive control proves the injected failure exposes transparent retries.
#[tokio::test]
async fn test_head_write_retry_detection_control() {
    for replace in [false, true] {
        let backend = Backend::default();
        let operator =
            ObjectStore::from_parts(OperationContext::default(), Arc::new(backend.clone())).layer(
                RetryLayer::new()
                    .with_max_times(1)
                    .with_min_delay(std::time::Duration::ZERO)
                    .with_max_delay(std::time::Duration::ZERO),
            );
        let store = HeadStore::new_for_test(operator, "cluster");
        let _ = failed_write(&store, &backend, replace).await;
        assert_eq!(backend.data.lock().unwrap().writes, 2);
    }
}

async fn failed_write(store: &HeadStore, backend: &Backend, replace: bool) -> crate::error::Error {
    let expected = if replace {
        store.create_initial_head().await.unwrap();
        Some(store.read_head().await.unwrap())
    } else {
        None
    };
    {
        let mut data = backend.data.lock().unwrap();
        data.writes = 0;
        data.fail = Some("write");
    }
    if let Some(expected) = expected {
        let next = expected.head().clone().with_next_write_seq().unwrap();
        store.replace_head(&expected, &next).await.unwrap_err()
    } else {
        store.create_initial_head().await.unwrap_err()
    }
}

#[test]
fn test_head_version_selection() {
    let mut b = Backend::default();
    b.capability.write_with_if_version_match = true;
    let s = store(&b);
    let a = metadata_value(Some("a"), Some("v1"), true);
    let c = metadata_value(Some("b"), Some("v1"), true);
    assert!(s.same_version(&a, &c).unwrap());
    let etag = metadata_value(Some("a"), None, true);
    assert!(s.same_version(&a, &etag).is_err());
    b.capability.write_with_if_version_match = false;
    assert!(!store(&b).same_version(&a, &c).unwrap());
    assert!(store(&b).same_version(&etag, &etag).unwrap());
    assert!(
        s.same_version(&metadata_value(None, None, true), &etag)
            .is_err()
    );
}

#[tokio::test]
async fn test_probe_conditional_writes() {
    for exists in [false, true] {
        let b = Backend::default();
        b.data.lock().unwrap().create_exists = exists;
        store(&b).probe_conditional_writes().await.unwrap();
        assert!(b.data.lock().unwrap().objects.is_empty());
    }
}

#[tokio::test]
async fn test_probe_rejects_ignored_conditions() {
    let b = Backend::default();
    b.data.lock().unwrap().ignore_conditions = true;
    assert!(store(&b).probe_conditional_writes().await.is_err());
    assert!(b.data.lock().unwrap().objects.is_empty());
}

#[tokio::test]
async fn test_probe_permissions() {
    for fail in ["plain", "list"] {
        let b = Backend::default();
        b.data.lock().unwrap().fail = Some(fail);
        assert!(store(&b).probe_conditional_writes().await.is_err());
        assert!(b.data.lock().unwrap().objects.is_empty());
    }
}

#[tokio::test]
async fn test_probe_cleanup() {
    let b = Backend::default();
    b.data.lock().unwrap().delete_fail = true;
    let e = store(&b).probe_conditional_writes().await.unwrap_err();
    assert_eq!(
        e.authority_storage_error().unwrap().kind(),
        ErrorKind::PermissionDenied
    );
    b.data.lock().unwrap().fail = Some("write");
    let e = store(&b).probe_conditional_writes().await.unwrap_err();
    assert_eq!(
        e.authority_storage_error().unwrap().kind(),
        ErrorKind::Unexpected
    );
    assert_eq!(b.data.lock().unwrap().deleted.len(), 2);
}

#[tokio::test]
async fn test_probe_path_isolation() {
    let b = Backend::default();
    let s = store(&b);
    s.create_initial_head().await.unwrap();
    let before = s.read_head().await.unwrap();
    s.probe_conditional_writes().await.unwrap();
    let after = s.read_head().await.unwrap();
    assert!(s.same_version(before.metadata(), after.metadata()).unwrap());
    let data = b.data.lock().unwrap();
    assert!(
        data.deleted
            .iter()
            .all(|p| p.starts_with("cluster/probes/") && p.ends_with(".json"))
    );
    assert_eq!(data.objects.len(), 1);
}

#[test]
fn test_backend_admission() {
    let mut b = Backend::default();
    assert!(store(&b).check_capabilities().is_ok());
    for scheme in ["gcs", "oss", "fs", "memory"] {
        b.scheme = scheme;
        assert!(store(&b).check_capabilities().is_err());
    }
    b.scheme = "azblob";
    assert!(store(&b).check_capabilities().is_ok());
    b.capability.list = false;
    assert!(store(&b).check_capabilities().is_err());
}

#[tokio::test]
async fn test_head_read_coherent_metadata() {
    let b = Backend::default();
    let s = Arc::new(store(&b));
    s.create_initial_head().await.unwrap();
    let initial = s.read_head().await.unwrap();
    let opened = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    b.data.lock().unwrap().pause = Some((opened.clone(), release.clone()));
    let task = tokio::spawn({
        let s = s.clone();
        async move { s.read_head().await }
    });
    opened.notified().await;
    b.data.lock().unwrap().pause = None;
    let next = initial.head().clone().with_next_write_seq().unwrap();
    s.replace_head(&initial, &next).await.unwrap();
    release.notify_one();
    let observed = task.await.unwrap().unwrap();
    assert_eq!(observed.head(), initial.head());
    assert!(
        s.same_version(observed.metadata(), initial.metadata())
            .unwrap()
    );
    let current = s.read_head().await.unwrap();
    assert_eq!(current.head(), &next);
    assert!(
        !s.same_version(observed.metadata(), current.metadata())
            .unwrap()
    );
}

#[test]
fn test_head_empty_native_identity_rejected() {
    let mut backend = Backend::default();
    backend.capability.write_with_if_version_match = true;
    let metadata = metadata_value(Some("valid"), Some(""), true);
    assert!(store(&backend).same_version(&metadata, &metadata).is_err());
}

#[tokio::test]
async fn test_head_incomplete_metadata() {
    for missing_time in [false, true] {
        let backend = Backend::default();
        let store = store(&backend);
        store.create_initial_head().await.unwrap();
        {
            let mut data = backend.data.lock().unwrap();
            let (_, metadata) = data.objects.values_mut().next().unwrap();
            *metadata = if missing_time {
                metadata_value(Some("valid"), None, false)
            } else {
                metadata_value(None, None, true)
            };
        }
        assert!(store.read_head().await.is_err());
    }
}

#[tokio::test]
async fn test_head_state_namespace_binding() {
    let backend = Backend::default();
    let first = store(&backend);
    let other = store(&backend);
    first.create_initial_head().await.unwrap();
    let state = first.read_head().await.unwrap();
    assert!(other.replace_head(&state, state.head()).await.is_err());
    assert_eq!(backend.data.lock().unwrap().writes, 1);
}

fn metadata_value(etag: Option<&str>, version: Option<&str>, time: bool) -> Metadata {
    let mut builder = MetadataBuilder::file(0);
    if let Some(etag) = etag {
        builder.etag(etag);
    }
    if let Some(version) = version {
        builder.version(version);
    }
    if time {
        builder.last_modified(Timestamp::now());
    }
    builder.build()
}

#[tokio::test]
async fn test_head_native_version_replace() {
    let mut backend = Backend::default();
    backend.capability.write_with_if_version_match = true;
    let store = store(&backend);
    store.create_initial_head().await.unwrap();
    let old = store.read_head().await.unwrap();
    let next = old.head().clone().with_next_write_seq().unwrap();
    store.replace_head(&old, &next).await.unwrap();
    let current = store.read_head().await.unwrap();
    assert!(
        !store
            .same_version(old.metadata(), current.metadata())
            .unwrap()
    );
    assert_eq!(
        store
            .replace_head(&old, &next)
            .await
            .unwrap_err()
            .authority_storage_error()
            .unwrap()
            .kind(),
        ErrorKind::ConditionNotMatch
    );
}

#[tokio::test]
async fn test_probe_rejects_ignored_replace() {
    let backend = Backend::default();
    backend.data.lock().unwrap().ignore_replace = true;
    assert!(store(&backend).probe_conditional_writes().await.is_err());
    assert!(backend.data.lock().unwrap().objects.is_empty());
}

#[tokio::test]
async fn test_probe_metadata_cleanup() {
    let backend = Backend::default();
    backend.data.lock().unwrap().missing_metadata = true;
    assert!(store(&backend).probe_conditional_writes().await.is_err());
    let data = backend.data.lock().unwrap();
    assert!(data.objects.is_empty());
    assert_eq!(data.deleted.len(), 1);
}

#[test]
fn test_backend_required_capabilities() {
    for missing in 0..6 {
        let mut backend = Backend::default();
        match missing {
            0 => backend.capability.read = false,
            1 => backend.capability.write = false,
            2 => backend.capability.list = false,
            3 => backend.capability.delete = false,
            4 => backend.capability.write_with_if_not_exists = false,
            _ => backend.capability.write_with_if_match = false,
        }
        assert!(store(&backend).check_capabilities().is_err());
    }
}

#[tokio::test]
async fn test_probe_rejects_mutation_despite_conflict() {
    for mutation in [ConflictMutation::Body, ConflictMutation::Identity] {
        for create in [true, false] {
            for already_exists in [false, true] {
                let backend = Backend::default();
                {
                    let mut data = backend.data.lock().unwrap();
                    data.create_exists = already_exists;
                    if create {
                        data.conflict_create = Some(mutation);
                    } else {
                        data.conflict_replace = Some(mutation);
                    }
                }
                let error = store(&backend)
                    .probe_conditional_writes()
                    .await
                    .unwrap_err();
                assert!(
                    matches!(error, crate::error::Error::AuthorityProbe { .. }),
                    "{error:?}"
                );
                let data = backend.data.lock().unwrap();
                assert_eq!(data.corruptions, 1);
                assert_eq!(data.deleted.len(), 1);
                assert!(data.objects.is_empty());
            }
        }
    }
}

const INITIAL: &str =
    r#"{"format_version":1,"owner":null,"epoch":0,"lease_deadline_ms":0,"write_seq":0}"#;
const OWNED: &str = r#"{"format_version":1,"owner":{"addr":"127.0.0.1:3002","incarnation_id":"550e8400-e29b-41d4-a716-446655440000"},"epoch":1,"lease_deadline_ms":1794528010000,"write_seq":1}"#;

#[test]
fn test_head_v1_fixtures() {
    assert_eq!(
        AuthorityHead::decode(INITIAL.as_bytes()).unwrap(),
        AuthorityHead::initial()
    );
    for fixture in [INITIAL, OWNED] {
        let head = AuthorityHead::decode(fixture.as_bytes()).unwrap();
        assert_eq!(head.encode().unwrap(), fixture.trim_end().as_bytes());
    }
    let owner = AuthorityHead::decode(OWNED.as_bytes())
        .unwrap()
        .owner
        .unwrap();
    assert_eq!(owner.addr, "127.0.0.1:3002");
    assert_eq!(
        owner.incarnation_id.to_string(),
        "550e8400-e29b-41d4-a716-446655440000"
    );
    let identity = NodeIdentity::new(owner.addr.clone());
    assert_ne!(NodeIdentity::new(owner.addr), identity);
}

#[test]
fn test_head_v1_unknown_fields() {
    let mut value: serde_json::Value = serde_json::from_str(OWNED).unwrap();
    value["extra"] = true.into();
    value["owner"]["extra"] = 42.into();
    assert_eq!(
        AuthorityHead::decode(&serde_json::to_vec(&value).unwrap()).unwrap(),
        AuthorityHead::decode(OWNED.as_bytes()).unwrap()
    );
}

#[test]
fn test_head_required_owner() {
    assert!(AuthorityHead::decode(INITIAL.as_bytes()).is_ok());
    let mut value: serde_json::Value = serde_json::from_str(INITIAL).unwrap();
    value.as_object_mut().unwrap().remove("owner");
    assert!(AuthorityHead::decode(&serde_json::to_vec(&value).unwrap()).is_err());
}

#[test]
fn test_head_invalid_fields_and_version() {
    let value: serde_json::Value = serde_json::from_str(OWNED).unwrap();
    for field in [
        "format_version",
        "owner",
        "epoch",
        "lease_deadline_ms",
        "write_seq",
    ] {
        let mut missing = value.clone();
        missing.as_object_mut().unwrap().remove(field);
        assert!(AuthorityHead::decode(&serde_json::to_vec(&missing).unwrap()).is_err());
        let mut wrong = value.clone();
        wrong[field] = true.into();
        assert!(AuthorityHead::decode(&serde_json::to_vec(&wrong).unwrap()).is_err());
    }
    for field in ["addr", "incarnation_id"] {
        let mut missing = value.clone();
        missing["owner"].as_object_mut().unwrap().remove(field);
        assert!(AuthorityHead::decode(&serde_json::to_vec(&missing).unwrap()).is_err());
    }
    let mut invalid = value.clone();
    invalid["owner"]["incarnation_id"] = "invalid".into();
    assert!(AuthorityHead::decode(&serde_json::to_vec(&invalid).unwrap()).is_err());
    invalid = value;
    invalid["format_version"] = 2.into();
    assert!(AuthorityHead::decode(&serde_json::to_vec(&invalid).unwrap()).is_err());
    let mut head = AuthorityHead::initial();
    head.format_version = 2;
    assert!(head.encode().is_err());
    assert!(AuthorityHead::decode(b"{").is_err());
}

#[test]
fn test_head_counter_overflow() {
    let head = AuthorityHead::initial();
    let next = head
        .clone()
        .with_next_epoch()
        .unwrap()
        .with_next_write_seq()
        .unwrap();
    assert_eq!((head.epoch(), head.write_seq()), (0, 0));
    assert_eq!((next.epoch(), next.write_seq()), (1, 1));
    for field in ["epoch", "write_seq"] {
        let mut value = serde_json::to_value(&head).unwrap();
        value[field] = u64::MAX.into();
        let max = AuthorityHead::decode(&serde_json::to_vec(&value).unwrap()).unwrap();
        let error = if field == "epoch" {
            max.clone().with_next_epoch().unwrap_err()
        } else {
            max.clone().with_next_write_seq().unwrap_err()
        };
        assert_eq!(error.status_code(), StatusCode::IllegalState);
        assert_eq!(serde_json::to_value(&max).unwrap()[field], u64::MAX);
    }
}

#[test]
fn test_authority_error_status_codes() {
    let mut head = AuthorityHead::initial();
    head.format_version = 2;
    assert_eq!(
        head.encode().unwrap_err().status_code(),
        StatusCode::IllegalState
    );
    assert_eq!(
        AuthorityHead::decode(b"{").unwrap_err().status_code(),
        StatusCode::IllegalState
    );
    assert_eq!(
        crate::error::AuthorityProbeSnafu {
            reason: "ignored condition"
        }
        .build()
        .status_code(),
        StatusCode::Unsupported
    );
    assert_eq!(
        crate::error::AuthorityMetadataSnafu {
            reason: "missing timestamp"
        }
        .build()
        .status_code(),
        StatusCode::StorageUnavailable
    );
    assert_eq!(
        crate::error::AuthorityCapabilitySnafu {
            scheme: "gcs",
            reason: "pending qualification"
        }
        .build()
        .status_code(),
        StatusCode::Unsupported
    );
}

#[tokio::test]
async fn test_head_decode_error_context() {
    let backend = Backend::default();
    let store = store(&backend);
    store.create_initial_head().await.unwrap();
    for invalid in [b"{".as_slice(), br#"{"format_version":1}"#.as_slice()] {
        {
            let mut data = backend.data.lock().unwrap();
            let (body, metadata) = data.objects.values_mut().next().unwrap();
            *body = invalid.to_vec();
            let mut builder = metadata.clone().into_builder();
            builder.set_file(invalid.len() as u64);
            *metadata = builder.build();
        }
        let error = store.read_head().await.unwrap_err();
        assert_eq!(error.status_code(), StatusCode::IllegalState);
        assert!(error.to_string().contains("cluster/authority/head.json"));
        let mut source = std::error::Error::source(&error);
        let mut found = false;
        while let Some(cause) = source {
            if let Some(json) = cause.downcast_ref::<serde_json::Error>() {
                assert!(json.line() > 0);
                assert!(json.column() > 0);
                found = true;
            }
            source = cause.source();
        }
        assert!(found, "serde_json error source must be retained");
    }
}

#[test]
fn test_head_prefix_normalization() {
    let backend = Backend::default();
    for (prefix, head, probes) in [
        (
            "///cluster//nested/",
            "/cluster/nested/authority/head.json",
            "/cluster/nested/probes/",
        ),
        ("", "/authority/head.json", "/probes/"),
    ] {
        let store = HeadStore::new_for_test(
            ObjectStore::from_parts(OperationContext::default(), Arc::new(backend.clone())),
            prefix,
        );
        assert_eq!(store.head_path, head);
        assert_eq!(store.probe_prefix, probes);
    }
}

#[test]
fn test_backend_admission_diagnostics() {
    for (scheme, text) in [
        ("gcs", "generation"),
        ("oss", "PutObject"),
        ("fs", "atomic"),
        ("memory", "atomic"),
    ] {
        let backend = Backend {
            scheme,
            ..Default::default()
        };
        let error = store(&backend).check_capabilities().unwrap_err();
        match error {
            crate::error::Error::AuthorityCapability {
                scheme: actual,
                reason,
                ..
            } => {
                assert_eq!(actual, scheme);
                assert!(reason.contains(text), "{reason}");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}

#[tokio::test]
async fn test_head_write_success_does_not_require_readback() {
    let backend = Backend::default();
    let store = store(&backend);
    backend.data.lock().unwrap().missing_metadata = true;
    store.create_initial_head().await.unwrap();
    assert!(store.read_head().await.is_err());
    backend.data.lock().unwrap().missing_metadata = false;
    let expected = store.read_head().await.unwrap();
    backend.data.lock().unwrap().missing_metadata = true;
    store
        .replace_head(
            &expected,
            &expected.head().clone().with_next_write_seq().unwrap(),
        )
        .await
        .unwrap();
    assert!(store.read_head().await.is_err());
}
