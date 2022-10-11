use rand::Rng;

pub fn random_get<T, F>(len: usize, func: F) -> Option<T>
where
    F: FnOnce(usize) -> Option<T>,
{
    if len == 0 {
        return None;
    }

    let mut rng = rand::thread_rng();
    let i = rng.gen_range(0..len);

    func(i)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_random_get() {
        for i in 1..100 {
            let res = random_get(i, |index| Some(2 * index));
            assert!(res.unwrap() < 2 * i);
        }
    }

    #[test]
    fn test_random_get_none() {
        let res = random_get(0, |index| Some(2 * index));
        assert!(res.is_none());
    }
}
