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

pub struct EscapeOutputBytea<'a>(pub(crate) &'a [u8]);

impl<'a> std::fmt::Display for EscapeOutputBytea<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let bytes = self.0;
        let mut result = String::with_capacity(3 * bytes.len());
        for byte in bytes {
            match byte {
                b'\\' => result.push_str(r#"\\"#),
                b'\'' => result.push_str(r#"'"),
                b'\t' => result.push_str(r#"\t"#),
                b'\r' => result.push_str(r#"\r"#),
                _ if byte.is_ascii() && !byte.is_ascii_graphic() => result.push(format!(r#"\{:02X}"#, byte)),
                _ => result.push(byte),
            }
        }
        write!(f, "{}", result)
    }
}
END
