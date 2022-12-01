use sha1::Digest;

// hashed_value: passed from client, calculated by "sha1(pwd) xor sha1(slat + sha1(sha1(pwd)))"
// hashed_stage2: "sha1(sha1(pwd))"
pub fn mysql_native_pwd_auth1(hashed_value: &[u8], salt: &[u8], hashed_stage2: &[u8]) -> bool {
    // sha1(hashed_value xor sha1(salt + hashed_stage2)) == hashed_stage2
    &sha1(simple_xor(
        &mut sha1_multi(salt, hashed_stage2),
        hashed_value,
    ))[..]
        == hashed_stage2
}

// hashed_value: passed from client, calculated by "sha1(pwd) xor sha1(slat + sha1(sha1(pwd)))"
pub fn mysql_native_pwd_auth2(hashed_value: &[u8], salt: &[u8], pwd: &[u8]) -> bool {
    // hashed_value == sha1(pwd) xor sha1(slat + sha1(sha1(pwd)))
    let mut hash_stage1 = sha1(pwd);
    let hash_stage2 = sha1(&hash_stage1);
    hashed_value == simple_xor(&mut hash_stage1, &sha1_multi(salt, &hash_stage2))
}

/// A function to simple xor. This function does not need to allocate additional space, the final result
/// will be assigned to v1. Then the reference of v1 as the return value of this function.
///
/// Note: The caller needs to guarantee: v1.len <= v2.len, Otherwise it will panic.
fn simple_xor<'a>(v1: &'a mut [u8], v2: &[u8]) -> &'a [u8] {
    for i in 0..v1.len() {
        v1[i] ^= v2[i];
    }
    v1
}

/// A function to compute SHA1 message digest for two messages in order to emulate sha1(v1, v2).
fn sha1_multi(v1: &[u8], v2: &[u8]) -> [u8; 20] {
    let mut m = sha1::Sha1::new();
    m.update(v1);
    m.update(v2);
    m.finalize().into()
}

/// A function to compute SHA1 message digest.
fn sha1(v: &[u8]) -> [u8; 20] {
    let mut m = sha1::Sha1::new();
    m.update(v);
    m.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::{mysql_native_pwd_auth1, sha1, sha1_multi, simple_xor};
    use crate::auth::mysql_native_pwd_auth2;

    #[test]
    fn test_mysql_native_pwd_auth() {
        let pwd = b"123456";
        let mut hashed_stage1 = sha1(pwd);
        let hashed_stage2 = sha1(&hashed_stage1);

        let salt = b"1213hjkasdhjkashdjka";

        let hashed_from_client = simple_xor(&mut hashed_stage1, &sha1_multi(salt, &hashed_stage2));

        assert!(mysql_native_pwd_auth1(
            hashed_from_client,
            salt,
            &hashed_stage2
        ));

        assert!(mysql_native_pwd_auth2(hashed_from_client, salt, pwd));
    }

    #[test]
    fn test_simple_xor() {
        let v1: &mut [u8] = &mut [1, 2, 3];
        let v2: &[u8] = &[1, 2, 3];
        let ret = simple_xor(v1, v2);
        assert_eq!(vec![0, 0, 0], ret);

        // 0000_0101 0000_0110 0000_0111
        let v1: &mut [u8] = &mut [5, 6, 7];
        // 0000_0011 0000_0010 0000_0001
        let v2: &[u8] = &[3, 2, 1];
        // 0000_0110 0000_0100 0000_0110
        let ret = simple_xor(v1, v2);
        assert_eq!(vec![6, 4, 6], ret);
    }

    #[test]
    fn test_sha1_multi() {
        let v1: &[u8] = &mut [1, 2, 3];
        let v2: &[u8] = &mut [3, 2, 1];
        let expected = vec![
            198, 228, 31, 61, 21, 45, 81, 127, 218, 80, 81, 245, 245, 151, 64, 234, 121, 250, 160,
            154,
        ];
        assert_eq!(expected, sha1_multi(v1, v2));
    }

    #[test]
    fn test_sha1() {
        let v: &[u8] = &mut [1, 2, 3];
        let expected = vec![
            112, 55, 128, 113, 152, 194, 42, 125, 43, 8, 7, 55, 29, 118, 55, 121, 168, 79, 223, 207,
        ];
        assert_eq!(expected, sha1(v));
    }
}
