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

use std::future::Future;
use std::sync::atomic::{AtomicI64, Ordering};
use std::task::Poll;

use common_error::ext::{ErrorExt, RetryHint};
use common_error::status_code::StatusCode;
use object_store::{Error as StorageError, ErrorKind};
use tokio::time::advance;

use super::*;
use crate::election::object_store::tests::{Backend, Request, store};

const CAPACITY: usize = 2;

struct Fixture {
    backend: Backend,
    store: Arc<HeadStore>,
    wall_ns: Arc<AtomicI64>,
}

impl Fixture {
    async fn new() -> Self {
        let backend = Backend::default();
        let store = Arc::new(store(&backend));
        store.create_initial_head().await.unwrap();
        let fixture = Self {
            backend,
            store,
            wall_ns: Arc::new(AtomicI64::new(0)),
        };
        fixture.clear_requests();
        fixture
    }

    fn node(&self, addr: &str) -> Arc<Authority> {
        let wall = self.wall_ns.clone();
        Arc::new(Authority::start(
            Arc::new(store(&self.backend)),
            NodeIdentity::new(addr.into()),
            move || i128::from(wall.load(Ordering::Relaxed)),
            CAPACITY,
            tokio::spawn,
        ))
    }

    fn clear_requests(&self) {
        self.backend.data.lock().unwrap().requests.clear();
    }

    fn puts(&self) -> Vec<AuthorityHead> {
        self.backend
            .data
            .lock()
            .unwrap()
            .requests
            .iter()
            .filter_map(|request| match request {
                Request::Get(_) => None,
                Request::Put(head) => Some(head.clone()),
            })
            .collect()
    }

    fn order(&self) -> Vec<&'static str> {
        self.backend
            .data
            .lock()
            .unwrap()
            .requests
            .iter()
            .map(|request| match request {
                Request::Get(_) => "GET",
                Request::Put(_) => "PUT",
            })
            .collect()
    }

    async fn expire_stored_lease(&self, extra_ns: i64) {
        let head = self.store.read_head().await.unwrap();
        let modified = head
            .metadata()
            .last_modified()
            .unwrap()
            .into_inner()
            .as_nanosecond();
        let at = modified + (LEASE + CLOCK_GUARD).as_nanos() as i128;
        self.wall_ns
            .store(i64::try_from(at).unwrap() + extra_ns, Ordering::Relaxed);
    }

    async fn replace_remotely(&self, update: impl FnOnce(AuthorityHead) -> AuthorityHead) {
        let base = self.store.read_head().await.unwrap();
        let head = update(base.head().clone());
        self.store.replace_head(&base, &head).await.unwrap();
    }
}

async fn acquire(node: &Authority) -> FencingToken {
    let TakeoverOutcome::Acquired(token) = node.takeover().await.unwrap() else {
        panic!("Head must be acquirable");
    };
    token
}

async fn renew(
    node: &Authority,
    token: FencingToken,
    precondition: AuthorityPrecondition,
) -> Result<()> {
    node.mutate(&token, precondition, AuthorityMutation::Renew)
        .await
}

fn takeover_task(node: &Arc<Authority>) -> JoinHandle<Result<TakeoverOutcome>> {
    let node = node.clone();
    tokio::spawn(async move { node.takeover().await })
}

fn renew_task(node: &Arc<Authority>, token: FencingToken) -> JoinHandle<Result<()>> {
    let node = node.clone();
    tokio::spawn(async move { renew(&node, token, AuthorityPrecondition::Any).await })
}

fn resign_task(node: &Arc<Authority>, token: FencingToken) -> JoinHandle<Result<()>> {
    let node = node.clone();
    tokio::spawn(async move { node.resign(&token).await })
}

fn assert_rejected<T: std::fmt::Debug>(result: Result<T>) {
    assert!(
        matches!(result, Err(Error::AuthorityRejected { .. })),
        "{result:?}"
    );
}

fn assert_write_seq_mismatch(result: Result<()>, expected: u64, actual: u64) {
    let error = result.unwrap_err();
    assert!(
        matches!(
            error,
            Error::AuthorityWriteSeqMismatch {
                expected: error_expected,
                actual: error_actual,
                ..
            } if error_expected == expected && error_actual == actual
        ),
        "{error:?}"
    );
    assert_eq!(error.status_code(), StatusCode::RequestOutdated);
    assert_eq!(error.retry_hint(), RetryHint::NonRetryable);
}

fn assert_unknown<T: std::fmt::Debug>(result: Result<T>) {
    assert!(
        matches!(result, Err(Error::AuthorityUnknown { .. })),
        "{result:?}"
    );
}

fn assert_closed<T: std::fmt::Debug>(result: Result<T>) {
    assert!(
        matches!(result, Err(Error::AuthorityClosed { .. })),
        "{result:?}"
    );
}

async fn fill_queue(
    node: &Authority,
    token: FencingToken,
    precondition: AuthorityPrecondition,
) -> Vec<Pin<Box<impl Future<Output = Result<()>> + '_>>> {
    let mut pending = Vec::new();
    for _ in 0..CAPACITY {
        let mut request = Box::pin(renew(node, token, precondition));
        assert!(matches!(futures::poll!(&mut request), Poll::Pending));
        pending.push(request);
    }
    assert_eq!(node.queue.capacity(), 0);
    pending
}

#[tokio::test(start_paused = true)]
async fn empty_head_can_be_acquired_and_resigned() {
    let fixture = Fixture::new().await;
    let leader = fixture.node("a");
    let candidate = fixture.node("b");
    let token = acquire(&leader).await;
    assert_eq!(token.epoch(), 1);
    let head = leader.head().await.unwrap();
    assert_eq!(head.head().owner.as_ref(), Some(&leader.shared.identity));
    assert!(head.metadata().etag().is_some());
    assert!(head.metadata().last_modified().is_some());
    leader.resign(&token).await.unwrap();
    let head = leader.head().await.unwrap();
    assert!(head.head().owner.is_none());
    assert_eq!((head.head().epoch(), head.head().write_seq()), (1, 2));
    assert_eq!(leader.status(), AuthorityStatus::Follower);
    assert_eq!(acquire(&candidate).await.epoch(), 2);
    leader.shutdown().await;
    candidate.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn takeover_requires_strictly_more_than_twelve_seconds() {
    let fixture = Fixture::new().await;
    let original = fixture.node("same-address");
    let replacement = fixture.node("same-address");
    let token = acquire(&original).await;
    original.shutdown().await;
    fixture.expire_stored_lease(0).await;
    fixture.clear_requests();
    assert_eq!(
        replacement.takeover().await.unwrap(),
        TakeoverOutcome::NotEligible
    );
    assert_eq!(fixture.order(), ["GET"]);
    assert_rejected(renew(&replacement, token, AuthorityPrecondition::Any).await);
    fixture.expire_stored_lease(1).await;
    let new_token = acquire(&replacement).await;
    assert_eq!(new_token.epoch(), token.epoch() + 1);
    let head = replacement.head().await.unwrap();
    let owner = head.head().owner.as_ref().unwrap();
    assert_eq!(owner.addr, original.shared.identity.addr);
    assert_ne!(
        owner.incarnation_id,
        original.shared.identity.incarnation_id
    );
    fixture.clear_requests();
    assert_rejected(renew(&replacement, token, AuthorityPrecondition::Any).await);
    assert!(fixture.puts().is_empty());
    replacement.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn candidates_racing_the_same_version_have_one_winner() {
    let fixture = Fixture::new().await;
    let first = fixture.node("first");
    let second = fixture.node("second");
    let mut first_put = fixture.backend.defer_next_write();
    let first_result = takeover_task(&first);
    first_put.arrived.notified().await;
    let mut second_put = fixture.backend.defer_next_write();
    let second_result = takeover_task(&second);
    second_put.arrived.notified().await;
    assert_eq!(fixture.order(), ["GET", "PUT", "GET", "PUT"]);
    assert!(first_put.commit(true).await);
    assert!(!second_put.commit(true).await);
    assert!(first_put.respond());
    assert!(second_put.respond());
    assert!(matches!(
        first_result.await.unwrap().unwrap(),
        TakeoverOutcome::Acquired(_)
    ));
    let error = second_result.await.unwrap().unwrap_err();
    assert_eq!(
        error.authority_storage_error().unwrap().kind(),
        ErrorKind::ConditionNotMatch
    );
    assert_eq!(fixture.order(), ["GET", "PUT", "GET", "PUT", "GET"]);
    assert_eq!(second.status(), AuthorityStatus::Follower);
    first.shutdown().await;
    second.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn temporary_conflict_returns_before_deadline_without_replaying_put() {
    let fixture = Fixture::new().await;
    let node = fixture.node("a");
    let token = acquire(&node).await;
    let (_, deadline) = node.status().term().unwrap();
    let base = fixture.store.read_head().await.unwrap();
    fixture.clear_requests();
    fixture.backend.fail_next_write(
        StorageError::new(ErrorKind::Conflict, "injected conditional conflict").set_temporary(),
    );

    let error = renew(&node, token, AuthorityPrecondition::Any)
        .await
        .unwrap_err();
    assert!(matches!(error, Error::AuthorityStorage { .. }), "{error:?}");
    let source = error.authority_storage_error().unwrap();
    assert_eq!(source.kind(), ErrorKind::Conflict);
    assert!(source.is_temporary());
    assert!(Instant::now() < deadline);
    assert_eq!(fixture.order(), ["GET", "PUT", "GET"]);
    let observed = fixture.store.read_head().await.unwrap();
    assert_eq!(observed.head(), base.head());
    assert!(
        fixture
            .store
            .same_version(base.metadata(), observed.metadata())
            .unwrap()
    );
    assert_eq!(node.status(), AuthorityStatus::Follower);
    node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn same_address_with_another_uuid_cannot_write_an_existing_term() {
    let fixture = Fixture::new().await;
    let leader = fixture.node("same-address");
    let token = acquire(&leader).await;
    fixture
        .replace_remotely(|head| {
            let mut next = head.with_next_write_seq().unwrap();
            next.owner = Some(NodeIdentity::new("same-address".into()));
            next
        })
        .await;
    fixture.clear_requests();
    assert_rejected(renew(&leader, token, AuthorityPrecondition::Any).await);
    assert_eq!(fixture.order(), ["GET"]);
    assert_eq!(leader.status(), AuthorityStatus::Follower);
    leader.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn overflow_is_rejected_before_put() {
    for field in ["epoch", "write_seq"] {
        let fixture = Fixture::new().await;
        fixture
            .replace_remotely(|head| {
                let mut json = serde_json::to_value(head).unwrap();
                json[field] = u64::MAX.into();
                AuthorityHead::decode(&serde_json::to_vec(&json).unwrap()).unwrap()
            })
            .await;
        let node = fixture.node("a");
        fixture.clear_requests();
        let error = node.takeover().await.unwrap_err();
        assert!(matches!(error, Error::AuthorityOverflow { counter, .. } if counter == field));
        assert_eq!(fixture.order(), ["GET"]);
        node.shutdown().await;
    }
}

#[tokio::test(start_paused = true)]
async fn write_seq_mismatch_rejects_put_without_retiring_term() {
    let fixture = Fixture::new().await;
    let node = fixture.node("a");
    let token = acquire(&node).await;
    let active = node.status();
    fixture.clear_requests();

    assert_write_seq_mismatch(
        renew(&node, token, AuthorityPrecondition::WriteSeq(0)).await,
        0,
        1,
    );
    assert_eq!(fixture.order(), ["GET"]);
    assert_eq!(node.status(), active);

    renew(&node, token, AuthorityPrecondition::WriteSeq(1))
        .await
        .unwrap();
    assert_eq!(fixture.order(), ["GET", "GET", "PUT"]);
    assert_eq!(fixture.puts()[0].write_seq(), 2);
    node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn due_renewal_overtakes_a_prepared_request_and_a_full_queue() {
    let fixture = Fixture::new().await;
    let node = fixture.node("a");
    let token = acquire(&node).await;
    fixture.clear_requests();
    advance(RENEW_INTERVAL - Duration::from_millis(200)).await;
    let read = fixture.backend.pause_next_read();
    let condition = AuthorityPrecondition::WriteSeq(1);
    let mut prepared = Box::pin(renew(&node, token, condition));
    assert!(futures::poll!(&mut prepared).is_pending());
    read.arrived.notified().await;
    let waiting = fill_queue(&node, token, condition).await;
    let mut scheduled = fixture.backend.defer_next_write();

    advance(Duration::from_millis(200)).await;
    read.release.notify_one();
    scheduled.arrived.notified().await;
    assert_eq!(fixture.order(), ["GET", "GET", "PUT"]);
    assert!(scheduled.commit(true).await);
    assert!(scheduled.respond());
    assert_write_seq_mismatch(prepared.await, 1, 2);
    for request in waiting {
        assert_write_seq_mismatch(request.await, 1, 2);
    }
    assert_eq!(fixture.order(), ["GET", "GET", "PUT", "GET", "GET", "GET"]);
    let puts = fixture.puts();
    assert_eq!((puts[0].epoch(), puts[0].write_seq()), (1, 2));
    node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_preparation_timeout_crossing_renewal_retries_against_the_renewed_head() {
    let fixture = Fixture::new().await;
    let node = fixture.node("a");
    let token = acquire(&node).await;
    fixture.clear_requests();
    advance(RENEW_INTERVAL - Duration::from_millis(200)).await;
    let read = fixture.backend.pause_next_read();
    let mut scheduled = fixture.backend.defer_next_write();
    let mut queued = Box::pin(renew(&node, token, AuthorityPrecondition::WriteSeq(1)));
    assert!(futures::poll!(&mut queued).is_pending());
    read.arrived.notified().await;

    advance(GET_TIMEOUT).await;
    scheduled.arrived.notified().await;
    assert_eq!(fixture.order(), ["GET", "GET", "PUT"]);
    assert!(scheduled.commit(true).await);
    assert!(scheduled.respond());
    assert_write_seq_mismatch(queued.await, 1, 2);
    assert_eq!(fixture.order(), ["GET", "GET", "PUT", "GET"]);
    node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn renewal_does_not_overlap_an_authorized_put() {
    let fixture = Fixture::new().await;
    let node = fixture.node("a");
    let token = acquire(&node).await;
    fixture.clear_requests();
    advance(Duration::from_secs(4)).await;
    let mut ordinary = fixture.backend.defer_next_write();
    let pending = renew_task(&node, token);
    ordinary.arrived.notified().await;
    let sent = Instant::now();
    let scheduled = fixture.backend.defer_next_write();
    advance(Duration::from_secs(1)).await;
    assert_eq!(fixture.order(), ["GET", "PUT"]);
    assert!(ordinary.commit(true).await);
    assert!(ordinary.respond());
    pending.await.unwrap().unwrap();
    assert_eq!(
        node.status(),
        AuthorityStatus::Leader {
            epoch: 1,
            deadline: sent + LEASE
        }
    );

    advance(Duration::from_secs(4)).await;
    scheduled.arrived.notified().await;
    assert_eq!(fixture.order(), ["GET", "PUT", "GET", "PUT"]);
    assert_eq!(fixture.puts()[1].write_seq(), 3);
    node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn first_takeover_get_times_out_without_put() {
    let fixture = Fixture::new().await;
    let node = fixture.node("a");
    let read = fixture.backend.pause_next_read();
    let attempt = takeover_task(&node);
    read.arrived.notified().await;
    advance(GET_TIMEOUT - Duration::from_millis(1)).await;
    assert!(!attempt.is_finished());
    advance(Duration::from_millis(1)).await;
    assert!(matches!(
        attempt.await.unwrap(),
        Err(Error::AuthorityTimeout {
            operation: "read",
            ..
        })
    ));
    assert_eq!(fixture.order(), ["GET"]);
    assert_eq!(node.status(), AuthorityStatus::Follower);
    node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn shutdown_interrupts_the_first_takeover_get_without_put() {
    let fixture = Fixture::new().await;
    let node = fixture.node("a");
    let read = fixture.backend.pause_next_read();
    let attempt = takeover_task(&node);
    read.arrived.notified().await;
    node.shutdown().await;
    assert_closed(attempt.await.unwrap());
    assert_eq!(fixture.order(), ["GET"]);
    assert!(node.writer.lock().await.is_none());
}

#[tokio::test(start_paused = true)]
async fn takeover_success_at_deadline_does_not_issue_a_token() {
    let fixture = Fixture::new().await;
    let node = fixture.node("a");
    let mut put = fixture.backend.defer_next_write();
    let attempt = takeover_task(&node);
    put.arrived.notified().await;
    assert!(put.commit(true).await);

    assert!(put.respond());
    advance(LEASE).await;
    assert_eq!(
        attempt.await.unwrap().unwrap(),
        TakeoverOutcome::AppliedButInactive
    );
    assert_eq!(node.status(), AuthorityStatus::Follower);
    assert_eq!(fixture.order(), ["GET", "PUT"]);
    node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn renewal_success_at_deadline_stays_applied_but_retires_once() {
    let fixture = Fixture::new().await;
    let node = fixture.node("a");
    let token = acquire(&node).await;
    let mut status = node.shared.status.subscribe();
    fixture.clear_requests();
    let mut put = fixture.backend.defer_next_write();
    let attempt = renew_task(&node, token);
    put.arrived.notified().await;
    let waiting = fill_queue(&node, token, AuthorityPrecondition::Any).await;
    assert!(put.commit(true).await);

    assert!(put.respond());
    advance(LEASE).await;
    attempt.await.unwrap().unwrap();
    status.changed().await.unwrap();
    assert_eq!(*status.borrow_and_update(), AuthorityStatus::Follower);
    for request in waiting {
        assert_rejected(request.await);
    }
    advance(LEASE).await;
    assert!(!status.has_changed().unwrap());
    assert_eq!(fixture.order(), ["GET", "PUT"]);
    node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn same_base_takeover_cannot_claim_an_earlier_late_put() {
    let fixture = Fixture::new().await;
    let node = fixture.node("a");
    let mut first = fixture.backend.defer_next_write();
    let a = takeover_task(&node);
    first.arrived.notified().await;
    advance(LEASE).await;
    assert_unknown(a.await.unwrap());
    assert_eq!(node.status(), AuthorityStatus::Follower);

    let mut second = fixture.backend.defer_next_write();
    let b = takeover_task(&node);
    second.arrived.notified().await;
    assert_eq!(fixture.puts()[0], fixture.puts()[1]);
    assert!(first.commit(true).await);
    assert!(!first.respond(), "the first client has stopped waiting");
    advance(Duration::from_secs(2)).await;
    assert_unknown(b.await.unwrap());
    assert!(
        !second.commit(true).await,
        "the late first PUT won the same CAS"
    );
    assert!(!second.respond());
    assert_eq!(node.status(), AuthorityStatus::Follower);

    // The ambiguity belongs to the old base version. A new version can be attributed.
    fixture.expire_stored_lease(1).await;
    let mut later = fixture.backend.defer_next_write();
    let c = takeover_task(&node);
    later.arrived.notified().await;
    assert!(later.commit(true).await);
    drop(later);
    assert!(
        matches!(c.await.unwrap().unwrap(), TakeoverOutcome::Acquired(token) if token.epoch() == 2)
    );
    node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn unchanged_version_stays_unknown_through_readback_backoff() {
    let fixture = Fixture::new().await;
    let node = fixture.node("a");
    let put = fixture.backend.defer_next_write();
    let attempt = takeover_task(&node);
    put.arrived.notified().await;
    let start = Instant::now();
    assert_unknown(attempt.await.unwrap());
    let reads: Vec<_> = fixture
        .backend
        .data
        .lock()
        .unwrap()
        .requests
        .iter()
        .filter_map(|request| match request {
            Request::Get(at) => Some(*at),
            Request::Put(_) => None,
        })
        .collect();
    assert_eq!(&fixture.order()[..3], ["GET", "PUT", "GET"]);
    let mut previous = start;
    for (&at, delay) in reads[1..].iter().zip([2_000, 50, 100, 200, 400, 400]) {
        let actual = at - previous;
        // Tokio's timer wheel can round a deadline up to the next millisecond.
        assert!(
            (Duration::from_millis(delay)..=Duration::from_millis(delay + 1)).contains(&actual)
        );
        previous = at;
    }
    assert!(reads.len() > 7);
    assert!(reads.last().unwrap() < &(start + LEASE));
    assert!(start.elapsed() >= LEASE);
    assert_eq!(fixture.puts().len(), 1);
    assert_eq!(node.status(), AuthorityStatus::Follower);
    node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_higher_epoch_cannot_confirm_a_superseded_put() {
    let fixture = Fixture::new().await;
    let node = fixture.node("a");
    let mut put = fixture.backend.defer_next_write();
    let attempt = takeover_task(&node);
    put.arrived.notified().await;
    assert!(put.commit(true).await);
    fixture
        .replace_remotely(|head| {
            let mut next = head
                .with_next_epoch()
                .unwrap()
                .with_next_write_seq()
                .unwrap();
            next.owner = Some(NodeIdentity::new("b".into()));
            next
        })
        .await;
    drop(put);
    let error = attempt.await.unwrap().unwrap_err();
    assert!(matches!(error, Error::AuthorityUnknown { .. }));
    assert_eq!(
        error.authority_storage_error().unwrap().kind(),
        ErrorKind::Unexpected
    );
    assert_eq!(node.status(), AuthorityStatus::Follower);
    assert_eq!(node.head().await.unwrap().head().epoch(), 2);
    assert_eq!(fixture.puts().len(), 2);
    node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn missing_head_during_readback_requires_recovery_and_keeps_unknown() {
    let fixture = Fixture::new().await;
    let node = fixture.node("a");
    let _put = fixture.backend.defer_next_write();
    let attempt = takeover_task(&node);
    _put.arrived.notified().await;
    fixture.backend.data.lock().unwrap().objects.clear();
    advance(Duration::from_secs(2)).await;
    assert_unknown(attempt.await.unwrap());
    assert_eq!(node.status(), AuthorityStatus::RecoveryRequired);
    assert_eq!(fixture.order(), ["GET", "PUT", "GET"]);
    assert!(matches!(
        node.takeover().await,
        Err(Error::AuthorityRecoveryRequired { .. })
    ));
    assert!(matches!(
        node.head().await,
        Err(Error::AuthorityRecoveryRequired { .. })
    ));
    assert_eq!(fixture.order(), ["GET", "PUT", "GET"]);
    node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_coherent_resign_readback_can_confirm_after_a_successor_is_observed() {
    let fixture = Fixture::new().await;
    let node = fixture.node("a");
    let candidate = fixture.node("b");
    let token = acquire(&node).await;
    let mut resign = fixture.backend.defer_next_write();
    let pending = resign_task(&node, token);
    resign.arrived.notified().await;
    assert!(resign.commit(true).await);

    let readback = fixture.backend.pause_next_read_response();
    drop(resign);
    readback.arrived.notified().await;
    assert_eq!(acquire(&candidate).await.epoch(), 2);
    assert_eq!(node.head().await.unwrap().head().epoch(), 2);
    readback.release.notify_one();
    pending.await.unwrap().unwrap();
    assert_eq!(node.status(), AuthorityStatus::Follower);
    assert_eq!(node.head().await.unwrap().head().epoch(), 2);
    assert_eq!(fixture.puts().len(), 3);
    node.shutdown().await;
    candidate.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn confirmed_write_survives_a_missing_head_on_the_next_read() {
    let fixture = Fixture::new().await;
    let node = fixture.node("a");
    let token = acquire(&node).await;
    let mut put = fixture.backend.defer_next_write();
    let mutation = renew_task(&node, token);
    put.arrived.notified().await;
    assert!(put.commit(true).await);
    fixture.backend.data.lock().unwrap().objects.clear();
    assert!(matches!(
        node.head().await,
        Err(Error::AuthorityMissing { .. })
    ));
    assert_eq!(node.status(), AuthorityStatus::RecoveryRequired);
    assert!(put.respond());
    mutation.await.unwrap().unwrap();
    assert_eq!(node.status(), AuthorityStatus::RecoveryRequired);
    assert_eq!(fixture.puts().len(), 2);
    node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn late_confirmation_of_unknown_renewal_does_not_restore_term() {
    let fixture = Fixture::new().await;
    let node = fixture.node("a");
    let token = acquire(&node).await;
    let mut status = node.shared.status.subscribe();
    fixture.clear_requests();
    let mut put = fixture.backend.defer_next_write();
    let attempt = renew_task(&node, token);
    put.arrived.notified().await;
    let waiting = fill_queue(&node, token, AuthorityPrecondition::Any).await;

    advance(Duration::from_secs(2)).await;
    status.changed().await.unwrap();
    assert_eq!(*status.borrow_and_update(), AuthorityStatus::Follower);
    assert_eq!(fixture.order(), ["GET", "PUT", "GET"]);
    assert!(put.commit(true).await);
    assert!(!put.respond());
    advance(Duration::from_millis(50)).await;
    attempt.await.unwrap().unwrap();
    for request in waiting {
        assert_rejected(request.await);
    }
    assert!(!status.has_changed().unwrap());
    assert_eq!(fixture.order(), ["GET", "PUT", "GET", "GET"]);
    node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn confirmed_write_invalidates_its_base_even_when_refresh_fails() {
    let fixture = Fixture::new().await;
    let node = fixture.node("a");
    let token = acquire(&node).await;
    fixture.clear_requests();
    let mut put = fixture.backend.defer_next_write();
    let first = renew_task(&node, token);
    put.arrived.notified().await;
    assert!(put.commit(true).await);
    fixture.backend.fail_next_read();
    assert!(put.respond());
    first.await.unwrap().unwrap();
    assert_eq!(fixture.order(), ["GET", "PUT"]);

    let error = renew(&node, token, AuthorityPrecondition::Any)
        .await
        .unwrap_err();
    assert_eq!(
        error.authority_storage_error().unwrap().kind(),
        ErrorKind::PermissionDenied
    );
    assert_eq!(fixture.order(), ["GET", "PUT", "GET"]);
    assert_eq!(node.status(), AuthorityStatus::Follower);
    assert_eq!(
        fixture.store.read_head().await.unwrap().head().write_seq(),
        2
    );
    assert_rejected(renew(&node, token, AuthorityPrecondition::WriteSeq(2)).await);
    assert_eq!(fixture.order(), ["GET", "PUT", "GET", "GET"]);
    node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn confirmed_write_rejects_a_rolled_back_snapshot() {
    let fixture = Fixture::new().await;
    let node = fixture.node("a");
    let token = acquire(&node).await;
    let stale = fixture.backend.data.lock().unwrap().objects.clone();
    renew(&node, token, AuthorityPrecondition::Any)
        .await
        .unwrap();
    fixture.clear_requests();
    fixture.backend.data.lock().unwrap().objects = stale;

    let error = renew(&node, token, AuthorityPrecondition::Any)
        .await
        .unwrap_err();
    assert!(matches!(error, Error::AuthorityMetadata { .. }));
    assert_eq!(fixture.order(), ["GET"]);
    assert_eq!(node.status(), AuthorityStatus::Follower);
    node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn failed_prepare_get_retires_both_renewal_paths_once() {
    for scheduled in [false, true] {
        let fixture = Fixture::new().await;
        let node = fixture.node("a");
        let token = acquire(&node).await;
        let (_, deadline) = node.status().term().unwrap();
        let mut status = node.shared.status.subscribe();
        fixture.clear_requests();
        fixture.backend.fail_next_read();

        if scheduled {
            advance(RENEW_INTERVAL).await;
        } else {
            let error = renew(&node, token, AuthorityPrecondition::Any)
                .await
                .unwrap_err();
            assert_eq!(
                error.authority_storage_error().unwrap().kind(),
                ErrorKind::PermissionDenied
            );
            assert_eq!(node.status(), AuthorityStatus::Follower);
        }

        status.changed().await.unwrap();
        assert_eq!(*status.borrow_and_update(), AuthorityStatus::Follower);
        assert!(Instant::now() < deadline);
        assert_rejected(renew(&node, token, AuthorityPrecondition::Any).await);
        advance(LEASE).await;
        assert!(!status.has_changed().unwrap());
        assert_eq!(fixture.order(), ["GET"]);
        node.shutdown().await;
    }
}

#[tokio::test(start_paused = true)]
async fn renewal_retries_a_timed_out_prepare_get_with_the_original_deadline() {
    for scheduled in [false, true] {
        let fixture = Fixture::new().await;
        let node = fixture.node("a");
        let token = acquire(&node).await;
        let active = node.status();
        let (epoch, original_deadline) = active.term().unwrap();
        let mut status = node.shared.status.subscribe();
        fixture.clear_requests();
        let read = fixture.backend.pause_next_read();
        let mut put = fixture.backend.defer_next_write();
        let attempt = if scheduled {
            advance(RENEW_INTERVAL).await;
            None
        } else {
            Some(renew_task(&node, token))
        };
        read.arrived.notified().await;
        let started_at = Instant::now();

        // A regression that retires on the first timeout must fail in virtual time.
        tokio::time::timeout(Duration::from_secs(2), put.arrived.notified())
            .await
            .unwrap();
        assert!(started_at.elapsed() > GET_TIMEOUT);
        assert_eq!(fixture.order(), ["GET", "GET", "PUT"]);
        assert_eq!(node.status(), active);
        assert_eq!(
            (fixture.puts()[0].epoch(), fixture.puts()[0].write_seq()),
            (epoch, 2)
        );
        assert!(put.commit(true).await);
        assert!(put.respond());

        status.changed().await.unwrap();
        assert!(matches!(
            *status.borrow_and_update(),
            AuthorityStatus::Leader { epoch: renewed, deadline }
                if renewed == epoch && deadline > original_deadline
        ));
        if let Some(attempt) = attempt {
            attempt.await.unwrap().unwrap();
        }
        node.shutdown().await;
    }
}

#[tokio::test(start_paused = true)]
async fn renewal_retries_a_temporary_prepare_get_error() {
    let fixture = Fixture::new().await;
    let node = fixture.node("a");
    let token = acquire(&node).await;
    fixture.clear_requests();
    fixture.backend.fail_next_read_with(
        StorageError::new(ErrorKind::Unexpected, "injected temporary read error").set_temporary(),
    );

    renew(&node, token, AuthorityPrecondition::Any)
        .await
        .unwrap();
    assert_eq!(fixture.order(), ["GET", "GET", "PUT"]);
    let head = &fixture.puts()[0];
    assert_eq!((head.epoch(), head.write_seq()), (token.epoch(), 2));
    node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn renewal_prepare_timeouts_exhaust_the_budget_without_a_put() {
    let fixture = Fixture::new().await;
    let node = fixture.node("a");
    let token = acquire(&node).await;
    let (_, deadline) = node.status().term().unwrap();
    let budget_end = deadline - Duration::from_secs(1);
    let mut status = node.shared.status.subscribe();
    fixture.clear_requests();
    let first = fixture.backend.pause_next_read();
    for _ in 0..10 {
        fixture.backend.pause_next_read();
    }
    advance(RENEW_INTERVAL).await;
    first.arrived.notified().await;
    status.changed().await.unwrap();

    assert_eq!(*status.borrow_and_update(), AuthorityStatus::Follower);
    assert!(Instant::now() >= budget_end && Instant::now() < deadline);
    assert_rejected(renew(&node, token, AuthorityPrecondition::Any).await);
    let order = fixture.order();
    assert!(order.len() > 1);
    assert!(order.iter().all(|request| *request == "GET"));
    assert!(
        fixture
            .backend
            .data
            .lock()
            .unwrap()
            .requests
            .iter()
            .all(|request| matches!(request, Request::Get(started_at) if *started_at < budget_end))
    );
    node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn conditional_resign_rejects_mismatched_sequence_without_retiring_term() {
    let fixture = Fixture::new().await;
    let node = fixture.node("a");
    let token = acquire(&node).await;
    let active = node.status();
    let before = node.head().await.unwrap();
    fixture.clear_requests();

    for expected in [0, 2] {
        let result = node
            .mutate(
                &token,
                AuthorityPrecondition::WriteSeq(expected),
                AuthorityMutation::Resign,
            )
            .await;
        assert_write_seq_mismatch(result, expected, 1);
        assert_eq!(node.status(), active);
    }
    assert_eq!(fixture.order(), ["GET", "GET"]);
    let after = node.head().await.unwrap();
    assert_eq!(before.head(), after.head());
    assert!(
        fixture
            .store
            .same_version(before.metadata(), after.metadata())
            .unwrap()
    );
    renew(&node, token, AuthorityPrecondition::WriteSeq(1))
        .await
        .unwrap();
    assert_eq!(fixture.puts().len(), 1);
    assert_eq!(fixture.puts()[0].epoch(), token.epoch());
    node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn conditional_resign_stops_admission_at_put_authorization() {
    let fixture = Fixture::new().await;
    let node = fixture.node("a");
    let token = acquire(&node).await;
    let active = node.status();
    let (epoch, deadline) = active.term().unwrap();
    fixture.clear_requests();
    let read = fixture.backend.pause_next_read();
    let mut put = fixture.backend.defer_next_write();
    let mut resign = Box::pin(node.mutate(
        &token,
        AuthorityPrecondition::WriteSeq(1),
        AuthorityMutation::Resign,
    ));
    assert!(futures::poll!(&mut resign).is_pending());
    read.arrived.notified().await;
    assert_eq!(node.status(), active);
    let waiting = fill_queue(&node, token, AuthorityPrecondition::Any).await;

    read.release.notify_one();
    put.arrived.notified().await;
    assert_eq!(
        node.status(),
        AuthorityStatus::Resigning { epoch, deadline }
    );
    assert_rejected(renew(&node, token, AuthorityPrecondition::Any).await);
    for request in waiting {
        assert_rejected(request.await);
    }
    assert_eq!(fixture.order(), ["GET", "PUT"]);
    assert!(put.commit(true).await);
    assert!(put.respond());
    resign.await.unwrap();
    assert_eq!(node.status(), AuthorityStatus::Follower);
    let head = node.head().await.unwrap();
    assert!(head.head().owner.is_none());
    assert_eq!((head.head().epoch(), head.head().write_seq()), (epoch, 2));
    node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn conditional_resign_rechecks_sequence_after_a_due_renewal() {
    let fixture = Fixture::new().await;
    let node = fixture.node("a");
    let token = acquire(&node).await;
    fixture.clear_requests();
    advance(RENEW_INTERVAL - Duration::from_millis(200)).await;
    let read = fixture.backend.pause_next_read();
    let mut resign = Box::pin(node.mutate(
        &token,
        AuthorityPrecondition::WriteSeq(1),
        AuthorityMutation::Resign,
    ));
    assert!(futures::poll!(&mut resign).is_pending());
    read.arrived.notified().await;

    advance(Duration::from_millis(200)).await;
    read.release.notify_one();
    assert_write_seq_mismatch(resign.await, 1, 2);
    assert_eq!(fixture.order(), ["GET", "GET", "PUT", "GET"]);
    assert!(fixture.puts()[0].owner.is_some());
    assert!(
        matches!(node.status(), AuthorityStatus::Leader { epoch, .. } if epoch == token.epoch())
    );
    node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn resign_cancels_a_preparing_get_before_authorization() {
    let fixture = Fixture::new().await;
    let node = fixture.node("a");
    let token = acquire(&node).await;
    fixture.clear_requests();
    let read = fixture.backend.pause_next_read();
    let prepared = renew_task(&node, token);
    read.arrived.notified().await;
    node.resign(&token).await.unwrap();
    assert_rejected(prepared.await.unwrap());
    assert_eq!(fixture.order(), ["GET", "GET", "PUT"]);
    let writes = fixture.puts();
    assert!(writes[0].owner.is_none());
    assert_eq!(node.status(), AuthorityStatus::Follower);
    node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn resign_wins_before_a_full_queue_can_authorize_a_put() {
    let fixture = Fixture::new().await;
    let node = fixture.node("a");
    let token = acquire(&node).await;
    fixture.clear_requests();
    let waiting = fill_queue(&node, token, AuthorityPrecondition::Any).await;
    let mut resign = Box::pin(node.resign(&token));
    assert!(futures::poll!(&mut resign).is_pending());
    assert!(matches!(node.status(), AuthorityStatus::Resigning { .. }));
    resign.await.unwrap();
    for request in waiting {
        assert_rejected(request.await);
    }
    assert_eq!(fixture.order(), ["GET", "PUT"]);
    assert!(fixture.puts()[0].owner.is_none());
    node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn resign_stops_full_queue_but_preserves_authorized_put_results() {
    for outcome in ["applied", "failed", "unknown"] {
        let fixture = Fixture::new().await;
        let node = fixture.node("a");
        let token = acquire(&node).await;
        let mut put = fixture.backend.defer_next_write();
        let sent = renew_task(&node, token);
        put.arrived.notified().await;
        let waiting = fill_queue(&node, token, AuthorityPrecondition::Any).await;
        let mut resign = Box::pin(node.resign(&token));
        assert!(futures::poll!(&mut resign).is_pending());
        assert!(matches!(
            node.status(),
            AuthorityStatus::Resigning { epoch: 1, .. }
        ));

        match outcome {
            "applied" => {
                assert!(put.commit(true).await);
                assert!(put.respond());
                sent.await.unwrap().unwrap();
                resign.await.unwrap();
                let writes = fixture.puts();
                assert_eq!(writes.len(), 3);
                assert!(writes[2].owner.is_none());
                assert_eq!((writes[2].epoch(), writes[2].write_seq()), (1, 3));
            }
            "failed" => {
                assert!(!put.commit(false).await);
                assert!(put.respond());
                let error = sent.await.unwrap().unwrap_err();
                assert_eq!(
                    error.authority_storage_error().unwrap().kind(),
                    ErrorKind::PermissionDenied
                );
                assert_rejected(resign.await);
                assert_eq!(fixture.puts().len(), 2);
            }
            _ => {
                advance(LEASE).await;
                assert_unknown(sent.await.unwrap());
                assert_rejected(resign.await);
                assert_eq!(fixture.puts().len(), 2);
            }
        }
        for request in waiting {
            assert_rejected(request.await);
        }
        assert_eq!(node.status(), AuthorityStatus::Follower);
        node.shutdown().await;
    }
}

#[tokio::test(start_paused = true)]
async fn resignation_does_not_discard_a_takeover_queued_after_retirement() {
    let fixture = Fixture::new().await;
    let node = fixture.node("a");
    let token = acquire(&node).await;
    let mut put = fixture.backend.defer_next_write();
    let sent = renew_task(&node, token);
    put.arrived.notified().await;
    let mut resign = Box::pin(node.resign(&token));
    assert!(futures::poll!(&mut resign).is_pending());

    advance(Duration::from_secs(2)).await;
    node.shared
        .wait_until(|status| status == AuthorityStatus::Follower)
        .await;
    fixture.expire_stored_lease(1).await;
    let mut takeover = Box::pin(node.takeover());
    assert!(futures::poll!(&mut takeover).is_pending());
    assert_eq!(node.queue.capacity(), CAPACITY - 1);

    advance(Duration::from_secs(8)).await;
    assert_unknown(sent.await.unwrap());
    assert_rejected(resign.await);
    assert!(
        matches!(takeover.await.unwrap(), TakeoverOutcome::Acquired(token) if token.epoch() == 2)
    );
    assert!(!put.commit(true).await);
    assert!(!put.respond());
    node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn resign_freezes_old_deadline_while_scheduled_renewal_is_in_flight() {
    let fixture = Fixture::new().await;
    let node = fixture.node("a");
    let token = acquire(&node).await;
    let AuthorityStatus::Leader { deadline, .. } = node.status() else {
        panic!("acquisition must activate the term");
    };
    let mut renewing = fixture.backend.defer_next_write();
    advance(RENEW_INTERVAL).await;
    renewing.arrived.notified().await;
    assert!(renewing.commit(true).await);
    let reading_to_resign = fixture.backend.pause_next_read();
    let mut resign = Box::pin(node.resign(&token));
    assert!(futures::poll!(&mut resign).is_pending());

    assert!(renewing.respond());
    // A paused process can observe success after the PUT timer, before the old deadline.
    advance(deadline - Instant::now() - Duration::from_secs(1)).await;
    reading_to_resign.arrived.notified().await;
    assert_eq!(
        node.status(),
        AuthorityStatus::Resigning { epoch: 1, deadline }
    );
    reading_to_resign.release.notify_one();
    assert!(matches!(
        resign.await,
        Err(Error::AuthorityTimeout {
            operation: "write budget",
            ..
        })
    ));
    assert_eq!(fixture.puts().len(), 2);
    assert_eq!(node.status(), AuthorityStatus::Follower);
    node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn shutdown_closes_a_full_queue_and_preserves_unknown_in_flight_put() {
    let fixture = Fixture::new().await;
    let node = fixture.node("a");
    let token = acquire(&node).await;
    fixture.clear_requests();
    let mut put = fixture.backend.defer_next_write();
    let sent = renew_task(&node, token);
    put.arrived.notified().await;
    let waiting = fill_queue(&node, token, AuthorityPrecondition::Any).await;
    let mut blocked_send = Box::pin(renew(&node, token, AuthorityPrecondition::Any));
    assert!(futures::poll!(&mut blocked_send).is_pending());

    tokio::join!(node.shutdown(), node.shutdown());
    assert_unknown(sent.await.unwrap());
    assert_closed(blocked_send.await);
    for request in waiting {
        assert_closed(request.await);
    }
    assert!(node.writer.lock().await.is_none());
    assert_eq!(node.status(), AuthorityStatus::Closed);
    assert_eq!(fixture.order(), ["GET", "PUT"]);
    assert!(
        put.commit(true).await,
        "the simulated server can finish after shutdown"
    );
    assert!(!put.respond());
    let head = fixture.store.read_head().await.unwrap();
    assert_eq!(head.head().owner.as_ref(), Some(&node.shared.identity));
    assert_eq!(head.head().write_seq(), 2);
}

#[tokio::test(start_paused = true)]
async fn shutdown_preserves_a_put_response_ready_at_cancellation() {
    let fixture = Fixture::new().await;
    let node = fixture.node("a");
    let token = acquire(&node).await;
    let mut put = fixture.backend.defer_next_write();
    let sent = renew_task(&node, token);
    put.arrived.notified().await;
    assert!(put.commit(true).await);
    assert!(put.respond());
    node.shutdown().await;
    sent.await.unwrap().unwrap();
    assert_eq!(node.status(), AuthorityStatus::Closed);
    assert!(node.writer.lock().await.is_none());
    assert_eq!(fixture.puts().len(), 2);
}

#[tokio::test(start_paused = true)]
async fn shutdown_cancels_renewal_preparation() {
    let fixture = Fixture::new().await;
    let node = fixture.node("a");
    let token = acquire(&node).await;
    fixture.clear_requests();
    let read = fixture.backend.pause_next_read();
    let renewal = renew_task(&node, token);
    read.arrived.notified().await;
    node.shutdown().await;
    assert_closed(renewal.await.unwrap());
    assert_eq!(node.status(), AuthorityStatus::Closed);
    assert_eq!(fixture.order(), ["GET"]);
}

#[tokio::test(start_paused = true)]
async fn shutdown_cancels_resign_preparation_and_keeps_the_stored_owner() {
    let fixture = Fixture::new().await;
    let node = fixture.node("a");
    let token = acquire(&node).await;
    let read = fixture.backend.pause_next_read();
    let resign = resign_task(&node, token);
    read.arrived.notified().await;
    node.shutdown().await;
    assert_closed(resign.await.unwrap());
    assert!(node.writer.lock().await.is_none());
    assert_eq!(fixture.puts().len(), 1);
    let head = fixture.store.read_head().await.unwrap();
    assert_eq!(head.head().owner.as_ref(), Some(&node.shared.identity));
}
