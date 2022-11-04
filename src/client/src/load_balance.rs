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
