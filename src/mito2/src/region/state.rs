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

use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use store_api::region_engine::RegionRole;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegionLeaderState {
    /// The region is opened and is writable.
    Writable,
    /// The region is in staging mode - writable but no checkpoint/compaction.
    Staging,
    /// The region is stepping down.
    Downgrading,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegionRoleState {
    Leader(RegionLeaderState),
    Follower,
}

impl RegionRoleState {
    /// Converts the region role state to leader state if it is a leader state.
    pub fn into_leader_state(self) -> Option<RegionLeaderState> {
        match self {
            RegionRoleState::Leader(leader_state) => Some(leader_state),
            RegionRoleState::Follower => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegionRequestPolicy {
    /// Accept incoming requests.
    Accept,
    /// Stall incoming requests until the current guarded operation finishes.
    Stall,
    /// Reject incoming requests with a structured reason.
    Reject(RegionRequestRejectReason),
}

/// Reason why a region rejects incoming requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegionRequestRejectReason {
    /// The leader is downgrading and no longer accepts region requests.
    DowngradingLeader,
    /// The region is a follower and cannot serve leader-only requests.
    Follower,
    /// The region is being dropped.
    Dropping,
}

#[derive(Debug)]
struct RegionControlStateInner {
    #[allow(dead_code)]
    role: RegionRole,
    request_policy: RegionRequestPolicy,
    active_guard: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RequestPolicyGuardError {
    AlreadyAcquired,
    InvalidTransition {
        role: RegionRole,
        current_policy: RegionRequestPolicy,
        next_policy: RegionRequestPolicy,
    },
}

/// Controls the durable region role and the transient request admission policy.
///
/// The region role determines the default request policy. A writable or staging
/// leader may use [`RegionRequestPolicyGuard`] to temporarily downgrade the
/// request policy for in-flight operations such as region edit or drop. When the
/// guard is dropped, it restores the previous policy only if the region role and
/// active policy still match the acquisition context, so it won't overwrite a
/// newer role transition.
#[derive(Debug, Clone)]
pub(crate) struct RegionControlState {
    inner: Arc<Mutex<RegionControlStateInner>>,
}

/// Guard that sets the region request policy when created and resets it to the previous value when dropped.
pub(crate) struct RegionRequestPolicyGuard {
    control_state: Arc<Mutex<RegionControlStateInner>>,
    previous_role: RegionRole,
    previous_policy: RegionRequestPolicy,
    active_policy: RegionRequestPolicy,
}

impl Debug for RegionRequestPolicyGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegionRequestPolicyGuard")
            .field("previous_role", &self.previous_role)
            .field("active_policy", &self.active_policy)
            .field("previous_policy", &self.previous_policy)
            .finish()
    }
}

impl Drop for RegionRequestPolicyGuard {
    fn drop(&mut self) {
        let mut inner = self.control_state.lock().unwrap();
        if inner.role == self.previous_role && inner.request_policy == self.active_policy {
            inner.request_policy = self.previous_policy;
        }
        inner.active_guard = false;
    }
}

impl RegionControlState {
    fn role_to_role_state(role: RegionRole) -> RegionRoleState {
        match role {
            RegionRole::Leader => RegionRoleState::Leader(RegionLeaderState::Writable),
            RegionRole::Follower => RegionRoleState::Follower,
            RegionRole::StagingLeader => RegionRoleState::Leader(RegionLeaderState::Staging),
            RegionRole::DowngradingLeader => {
                RegionRoleState::Leader(RegionLeaderState::Downgrading)
            }
        }
    }

    fn role_state_to_role(state: RegionRoleState) -> RegionRole {
        match state {
            RegionRoleState::Follower => RegionRole::Follower,
            RegionRoleState::Leader(RegionLeaderState::Staging) => RegionRole::StagingLeader,
            RegionRoleState::Leader(RegionLeaderState::Downgrading) => {
                RegionRole::DowngradingLeader
            }
            RegionRoleState::Leader(_) => RegionRole::Leader,
        }
    }

    fn role_to_request_policy(role: RegionRole) -> RegionRequestPolicy {
        match role {
            RegionRole::Leader | RegionRole::StagingLeader => RegionRequestPolicy::Accept,
            RegionRole::DowngradingLeader => {
                RegionRequestPolicy::Reject(RegionRequestRejectReason::DowngradingLeader)
            }
            RegionRole::Follower => {
                RegionRequestPolicy::Reject(RegionRequestRejectReason::Follower)
            }
        }
    }

    fn can_acquire_request_policy_guard(
        role: RegionRole,
        current_policy: RegionRequestPolicy,
        next_policy: RegionRequestPolicy,
    ) -> bool {
        if current_policy == next_policy {
            return false;
        }

        matches!(role, RegionRole::Leader | RegionRole::StagingLeader)
            && matches!(
                next_policy,
                RegionRequestPolicy::Stall | RegionRequestPolicy::Reject(_)
            )
    }

    pub(crate) fn new(role: RegionRole) -> Self {
        let request_policy = Self::role_to_request_policy(role);
        let inner = RegionControlStateInner {
            role,
            request_policy,
            active_guard: false,
        };
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    /// Returns the current region role.
    pub(crate) fn role(&self) -> RegionRole {
        let inner = self.inner.lock().unwrap();
        inner.role
    }

    /// Returns the current region role state.
    pub(crate) fn role_state(&self) -> RegionRoleState {
        Self::role_to_role_state(self.role())
    }

    /// Compares and exchanges the region role state.
    pub(crate) fn compare_exchange_role_state(
        &self,
        current: RegionRoleState,
        next: RegionRoleState,
    ) -> std::result::Result<RegionRoleState, RegionRoleState> {
        let mut inner = self.inner.lock().unwrap();
        let actual = Self::role_to_role_state(inner.role);

        if actual != current {
            return Err(actual);
        }

        let next_role = Self::role_state_to_role(next);
        if inner.role != next_role {
            inner.role = next_role;
            inner.request_policy = Self::role_to_request_policy(next_role);
        }

        Ok(actual)
    }

    /// Updates the region role state by applying `f` to the current role state.
    pub(crate) fn fetch_update_role_state<F>(
        &self,
        f: F,
    ) -> std::result::Result<RegionRoleState, RegionRoleState>
    where
        F: FnOnce(RegionRoleState) -> Option<RegionRoleState>,
    {
        let mut inner = self.inner.lock().unwrap();
        let current = Self::role_to_role_state(inner.role);

        let Some(next) = f(current) else {
            return Err(current);
        };

        let next_role = Self::role_state_to_role(next);
        if inner.role != next_role {
            inner.role = next_role;
            inner.request_policy = Self::role_to_request_policy(next_role);
        }

        Ok(current)
    }

    /// Acquires a guard to set the region request policy.
    ///
    /// The guard will reset the policy to the previous value when dropped.
    pub(crate) fn try_acquire_request_policy_guard(
        &self,
        request_policy: RegionRequestPolicy,
    ) -> std::result::Result<RegionRequestPolicyGuard, RequestPolicyGuardError> {
        let mut inner = self.inner.lock().unwrap();
        if inner.active_guard {
            return Err(RequestPolicyGuardError::AlreadyAcquired);
        }
        if !Self::can_acquire_request_policy_guard(inner.role, inner.request_policy, request_policy)
        {
            return Err(RequestPolicyGuardError::InvalidTransition {
                role: inner.role,
                current_policy: inner.request_policy,
                next_policy: request_policy,
            });
        }
        let previous_role = inner.role;
        let previous_policy = inner.request_policy;
        inner.request_policy = request_policy;
        inner.active_guard = true;
        Ok(RegionRequestPolicyGuard {
            control_state: self.inner.clone(),
            previous_role,
            previous_policy,
            active_policy: request_policy,
        })
    }

    /// Returns the current region request policy.
    pub(crate) fn current_request_policy(&self) -> RegionRequestPolicy {
        let inner = self.inner.lock().unwrap();
        inner.request_policy
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_policy_guard_acquisition_rules() {
        let leader = RegionControlState::new(RegionRole::Leader);
        assert_invalid_request_policy_transition(
            leader.try_acquire_request_policy_guard(RegionRequestPolicy::Accept),
            RegionRole::Leader,
            RegionRequestPolicy::Accept,
            RegionRequestPolicy::Accept,
        );

        let guard = leader
            .try_acquire_request_policy_guard(RegionRequestPolicy::Stall)
            .unwrap();
        assert_eq!(leader.current_request_policy(), RegionRequestPolicy::Stall);
        assert!(matches!(
            leader.try_acquire_request_policy_guard(RegionRequestPolicy::Reject(
                RegionRequestRejectReason::Dropping,
            )),
            Err(RequestPolicyGuardError::AlreadyAcquired)
        ));
        drop(guard);
        assert_eq!(leader.current_request_policy(), RegionRequestPolicy::Accept);

        let guard = leader
            .try_acquire_request_policy_guard(RegionRequestPolicy::Reject(
                RegionRequestRejectReason::Dropping,
            ))
            .unwrap();
        assert_eq!(
            leader.current_request_policy(),
            RegionRequestPolicy::Reject(RegionRequestRejectReason::Dropping)
        );
        drop(guard);
        assert_eq!(leader.current_request_policy(), RegionRequestPolicy::Accept);

        let staging_leader = RegionControlState::new(RegionRole::StagingLeader);
        staging_leader
            .try_acquire_request_policy_guard(RegionRequestPolicy::Stall)
            .unwrap();

        let downgrading_leader = RegionControlState::new(RegionRole::DowngradingLeader);
        assert_guard_not_allowed(
            &downgrading_leader,
            RegionRole::DowngradingLeader,
            RegionRequestPolicy::Reject(RegionRequestRejectReason::DowngradingLeader),
        );

        let follower = RegionControlState::new(RegionRole::Follower);
        assert_guard_not_allowed(
            &follower,
            RegionRole::Follower,
            RegionRequestPolicy::Reject(RegionRequestRejectReason::Follower),
        );
    }

    fn assert_guard_not_allowed(
        control_state: &RegionControlState,
        role: RegionRole,
        current_policy: RegionRequestPolicy,
    ) {
        for next_policy in [
            RegionRequestPolicy::Accept,
            RegionRequestPolicy::Stall,
            RegionRequestPolicy::Reject(RegionRequestRejectReason::Dropping),
        ] {
            assert_invalid_request_policy_transition(
                control_state.try_acquire_request_policy_guard(next_policy),
                role,
                current_policy,
                next_policy,
            );
        }
    }

    fn assert_invalid_request_policy_transition(
        result: std::result::Result<RegionRequestPolicyGuard, RequestPolicyGuardError>,
        role: RegionRole,
        current_policy: RegionRequestPolicy,
        next_policy: RegionRequestPolicy,
    ) {
        assert!(matches!(
            result,
            Err(RequestPolicyGuardError::InvalidTransition {
                role: actual_role,
                current_policy: actual_current_policy,
                next_policy: actual_next_policy,
            }) if actual_role == role
                && actual_current_policy == current_policy
                && actual_next_policy == next_policy
        ));
    }
}
