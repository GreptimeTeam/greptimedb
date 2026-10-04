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
use rand::seq::IndexedRandom;

#[enum_dispatch]
pub trait LoadBalance {
    fn get_index<'a>(&self, candidates: &'a [usize]) -> Option<&'a usize>;
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
    fn get_index<'a>(&self, candidates: &'a [usize]) -> Option<&'a usize> {
        candidates.choose(&mut rand::rng())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::{LoadBalance, Random};

    #[test]
    fn test_random_lb() {
        let candidates = vec![0, 1, 2, 3];
        let all: HashSet<usize> = candidates.iter().copied().collect();

        let random = Random;
        for _ in 0..100 {
            let index = random.get_index(&candidates).unwrap();
            assert!(all.contains(index));
        }
    }
}
