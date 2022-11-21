// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/// Get prefix end key of `key`.
#[inline]
pub fn get_prefix_end_key(key: &[u8]) -> Vec<u8> {
    for (i, v) in key.iter().enumerate().rev() {
        if *v < 0xFF {
            let mut end = Vec::from(&key[..=i]);
            end[i] = *v + 1;
            return end;
        }
    }

    // next prefix does not exist (e.g., 0xffff);
    vec![0]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_prefix() {
        let key = b"testa";
        assert_eq!(b"testb".to_vec(), get_prefix_end_key(key));

        let key = vec![0, 0, 26];
        assert_eq!(vec![0, 0, 27], get_prefix_end_key(&key));

        let key = vec![0, 0, 255];
        assert_eq!(vec![0, 1], get_prefix_end_key(&key));

        let key = vec![0, 255, 255];
        assert_eq!(vec![1], get_prefix_end_key(&key));
    }
}
