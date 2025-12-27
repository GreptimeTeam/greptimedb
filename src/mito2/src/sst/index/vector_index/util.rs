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

//! Utility functions for vector index operations.

use std::borrow::Cow;

/// Converts a byte slice to f32 slice, handling unaligned data gracefully.
/// Returns `Cow::Borrowed` for aligned data (zero-copy) or `Cow::Owned` for unaligned data.
///
/// # Panics
///
/// Panics if the byte slice length is not a multiple of 4.
pub fn bytes_to_f32_slice(bytes: &[u8]) -> Cow<'_, [f32]> {
    assert!(
        bytes.len().is_multiple_of(4),
        "Vector bytes length {} is not a multiple of 4",
        bytes.len()
    );

    if bytes.is_empty() {
        return Cow::Borrowed(&[]);
    }

    let ptr = bytes.as_ptr();
    if (ptr as usize).is_multiple_of(std::mem::align_of::<f32>()) {
        // Fast path: data is properly aligned, zero-copy
        // Safety: We've verified alignment and length requirements
        Cow::Borrowed(unsafe { std::slice::from_raw_parts(ptr as *const f32, bytes.len() / 4) })
    } else {
        // Slow path: data is not aligned, copy to aligned buffer
        // This should be rare in practice but handles edge cases safely
        let floats: Vec<f32> = bytes
            .chunks_exact(4)
            .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
            .collect();
        Cow::Owned(floats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytes_to_f32_slice() {
        let floats = [1.0f32, 2.0, 3.0, 4.0];
        let bytes: Vec<u8> = floats.iter().flat_map(|f| f.to_le_bytes()).collect();

        let result = bytes_to_f32_slice(&bytes);
        assert_eq!(result.len(), 4);
        assert_eq!(result[0], 1.0);
        assert_eq!(result[1], 2.0);
        assert_eq!(result[2], 3.0);
        assert_eq!(result[3], 4.0);
    }

    #[test]
    fn test_bytes_to_f32_slice_unaligned() {
        // Create a buffer with an extra byte at the start to force misalignment
        let floats = [1.0f32, 2.0, 3.0, 4.0];
        let mut bytes: Vec<u8> = vec![0u8]; // padding byte
        bytes.extend(floats.iter().flat_map(|f| f.to_le_bytes()));

        // Take a slice starting at offset 1 (unaligned)
        let unaligned_bytes = &bytes[1..];

        // Verify it's actually unaligned
        let ptr = unaligned_bytes.as_ptr();
        let is_aligned = (ptr as usize).is_multiple_of(std::mem::align_of::<f32>());

        // The function should work regardless of alignment
        let result = bytes_to_f32_slice(unaligned_bytes);
        assert_eq!(result.len(), 4);
        assert_eq!(result[0], 1.0);
        assert_eq!(result[1], 2.0);
        assert_eq!(result[2], 3.0);
        assert_eq!(result[3], 4.0);

        // If it was unaligned, it should return an owned Vec (Cow::Owned)
        if !is_aligned {
            assert!(matches!(result, Cow::Owned(_)));
        }
    }

    #[test]
    fn test_bytes_to_f32_slice_empty() {
        let bytes: &[u8] = &[];
        let result = bytes_to_f32_slice(bytes);
        assert!(result.is_empty());
    }
}
