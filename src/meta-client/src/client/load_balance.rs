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
