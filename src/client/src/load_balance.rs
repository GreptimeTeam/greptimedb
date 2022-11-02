use rand::seq::SliceRandom;

pub trait LoadBalance {
    fn get_peer<'a>(&self, peers: &'a [String]) -> Option<&'a String>;
}

#[derive(Debug)]
pub struct Random;

impl LoadBalance for Random {
    fn get_peer<'a>(&self, peers: &'a [String]) -> Option<&'a String> {
        peers.choose(&mut rand::thread_rng())
    }
}

#[derive(Debug)]
pub enum LB {
    Random(Random),
}

impl LoadBalance for LB {
    fn get_peer<'a>(&self, peers: &'a [String]) -> Option<&'a String> {
        match self {
            LB::Random(random) => random.get_peer(peers),
        }
    }
}

impl Default for LB {
    fn default() -> Self {
        LB::Random(Random)
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
