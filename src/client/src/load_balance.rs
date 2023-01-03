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

use enum_dispatch::enum_dispatch;
use rand::seq::SliceRandom;

#[enum_dispatch]
pub trait LoadBalance {
    fn get_peer<'a>(&self, peers: &'a [String]) -> Option<&'a String>;
}

#[enum_dispatch(LoadBalance)]
#[derive(Debug)]
pub enum Loadbalancer {
    Random,
}

impl Default for Loadbalancer {
    fn default() -> Self {
        Loadbalancer::from(Random)
    }
}

#[derive(Debug)]
pub struct Random;

impl LoadBalance for Random {
    fn get_peer<'a>(&self, peers: &'a [String]) -> Option<&'a String> {
        peers.choose(&mut rand::thread_rng())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::{LoadBalance, Random};

    #[test]
    fn test_random_lb() {
        let peers = vec![
            "127.0.0.1:3001".to_string(),
            "127.0.0.1:3002".to_string(),
            "127.0.0.1:3003".to_string(),
            "127.0.0.1:3004".to_string(),
        ];
        let all: HashSet<String> = peers.clone().into_iter().collect();

        let random = Random;
        for _ in 0..100 {
            let peer = random.get_peer(&peers).unwrap();
            all.contains(peer);
        }
    }
}
