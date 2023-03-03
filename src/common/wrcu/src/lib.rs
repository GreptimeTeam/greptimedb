mod wcu_calc;
mod wrcu_stat;

pub use wrcu_stat::WrcuStat;
pub use wrcu_stat::Statistics;
pub use wrcu_stat::StatKey;

/// An interface for computing WCUs for insert request
pub trait WcuCalc {
    fn wcu_num(&self) -> u32;
}

// TODO(fys): find the most suitable value through practice later
const BYTES_PER_WCU: u32 = 1024;
const BYTES_PER_RCU: u32 = 1024 * 4;

pub fn wcu(byte_num: u32) -> u32 {
    if byte_num <= BYTES_PER_WCU {
        return 1;
    }

    byte_num / BYTES_PER_WCU + 1
}

pub fn rcu(byte_num: u32) -> u32 {
    if byte_num <= BYTES_PER_RCU {
        return 1;
    }

    byte_num / BYTES_PER_RCU + 1
}

#[cfg(test)]
mod tests {
    use crate::{rcu, wcu};

    #[test]
    fn test_wcu() {
        assert_eq!(1, wcu(1022));
        assert_eq!(1, wcu(1024));
        assert_eq!(2, wcu(1025));
        assert_eq!(10, wcu(1024 * 10 - 1));
        assert_eq!(11, wcu(1024 * 10 + 1));
    }

    #[test]
    fn test_rcu() {
        assert_eq!(1, rcu(1024 * 4 - 1));
        assert_eq!(1, rcu(1024 * 4));
        assert_eq!(2, rcu(1024 * 4 + 1));
        assert_eq!(10, rcu(1024 * 40 - 1));
        assert_eq!(11, rcu(1024 * 40 + 1));
    }
}
