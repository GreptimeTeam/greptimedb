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

use std::sync::{Arc, RwLock};

pub type StateRef = Arc<RwLock<State>>;

/// State transition.
/// ```text
///                     +------------------------------+
///                     |                              |
///                     |                              |
///                     |                              |
/// +-------------------v--------------------+         |
/// | LeaderState{enable_leader_cache:false} |         |
/// +-------------------+--------------------+         |
///                     |                              |
///                     |                              |
///           +---------v---------+                    |
///           | Init Leader Cache |                    |
///           +---------+---------+                    |
///                     |                              |
///                     |                              |
/// +-------------------v-------------------+          |
/// | LeaderState{enable_leader_cache:true} |          |
/// +-------------------+-------------------+          |
///                     |                              |
///                     |                              |
///             +-------v-------+                      |
///             | FollowerState |                      |
///             +-------+-------+                      |
///                     |                              |
///                     |                              |
///                     +------------------------------+
///```
#[derive(Debug, Clone)]
pub enum State {
    Leader(LeaderState),
    Follower(FollowerState),
}

#[derive(Debug, Clone)]
pub struct LeaderState {
    // Disables the leader cache during initiation
    pub enable_leader_cache: bool,

    pub server_addr: String,
}

#[derive(Debug, Clone)]
pub struct FollowerState {
    pub server_addr: String,
}

impl State {
    pub fn follower(server_addr: String) -> State {
        Self::Follower(FollowerState { server_addr })
    }

    pub fn leader(server_addr: String, enable_leader_cache: bool) -> State {
        Self::Leader(LeaderState {
            enable_leader_cache,
            server_addr,
        })
    }

    pub fn enable_leader_cache(&self) -> bool {
        match &self {
            State::Leader(leader) => leader.enable_leader_cache,
            State::Follower(_) => false,
        }
    }

    pub fn next_state<F>(&mut self, f: F)
    where
        F: FnOnce(&State) -> State,
    {
        *self = f(self);
    }
}

pub fn become_leader(enable_leader_cache: bool) -> impl FnOnce(&State) -> State {
    move |prev| match prev {
        State::Leader(leader) => State::Leader(LeaderState { ..leader.clone() }),
        State::Follower(follower) => State::Leader(LeaderState {
            server_addr: follower.server_addr.to_string(),
            enable_leader_cache,
        }),
    }
}

pub fn become_follower() -> impl FnOnce(&State) -> State {
    move |prev| match prev {
        State::Leader(leader) => State::Follower(FollowerState {
            server_addr: leader.server_addr.to_string(),
        }),
        State::Follower(follower) => State::Follower(FollowerState { ..follower.clone() }),
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use crate::state::{become_follower, become_leader, FollowerState, LeaderState, State};

    #[tokio::test]
    async fn test_next_state() {
        let mut state = State::follower("test".to_string());

        state.next_state(become_leader(false));

        assert_matches!(
            state,
            State::Leader(LeaderState {
                enable_leader_cache: false,
                ..
            })
        );

        state.next_state(become_leader(false));

        assert_matches!(
            state,
            State::Leader(LeaderState {
                enable_leader_cache: false,
                ..
            })
        );

        state.next_state(become_follower());

        assert_matches!(state, State::Follower(FollowerState { .. }));

        state.next_state(become_follower());

        assert_matches!(state, State::Follower(FollowerState { .. }));
    }
}
