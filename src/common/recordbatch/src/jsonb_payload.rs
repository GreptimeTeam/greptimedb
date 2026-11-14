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

//! Normalization of the payloads written into a JSON column.
//!
//! A JSON column of the `Jsonb` format is exposed as `Binary` in Arrow, but its
//! values are the canonical binary jsonb encoding. Payloads reaching a write path
//! can still be JSON *text*: COPY reads text from CSV, JSON and Parquet files, and
//! an INSERT can cast a string expression to `Binary` before the value reaches the
//! column. Text stored verbatim only appears to work because the read-side
//! `jsonb::from_slice` falls back to parsing text; the strict jsonb functions such
//! as `json_get_*` then reject the value.
//!
//! The text branch below uses the *extended* jsonb parser, the same one the read
//! side falls back to, so a payload that a legacy text write produced can also be
//! written back; SQL literals keep their standard-mode path in
//! `common_sql::convert`.
//!
//! [`normalize_jsonb_payload`] is the single place that turns such a payload into
//! canonical jsonb. Both write paths of the column share it: the COPY schema
//! adapter ([`crate::adapter`]) and the insert boundary
//! ([`crate::RecordBatch::column_vectors`]).
//!
//! The layout constants and the number/extension width tables below mirror the
//! pinned `jsonb` crate (`core/databend/constants.rs` and `core/databend/util.rs`).
//! They live next to their only two call sites on purpose: this walk re-implements
//! a dependency's internal layout precisely because that dependency is unsafe on
//! untrusted input, and the whole module is meant to be deleted once the crate
//! exposes a validator. `test_is_binary_jsonb_accepts_encoded_values` is the check
//! to run when the fork is bumped: it feeds every shape the encoder produces
//! through the walk.

use std::borrow::Cow;
use std::sync::Arc;

use datatypes::arrow::array::{Array, ArrayRef, BinaryArray};

use crate::error::{self, Result};

/// Normalizes one payload that is being written into a jsonb column.
///
/// - A structurally valid binary jsonb payload is returned unchanged, which makes
///   the normalization idempotent and never re-encodes native jsonb.
/// - An empty payload is returned unchanged: an empty CSV field is how a NULL JSON
///   value is exported, and it is stored as an empty jsonb value.
/// - JSON text is parsed and re-encoded as canonical jsonb.
/// - Anything else is rejected instead of being stored.
fn normalize_jsonb_payload(bytes: &[u8]) -> Result<Cow<'_, [u8]>> {
    match encode_if_text(bytes)? {
        None => Ok(Cow::Borrowed(bytes)),
        Some(jsonb) => Ok(Cow::Owned(jsonb)),
    }
}

/// Validates one payload that is being written into a jsonb column without
/// rewriting it.
///
/// A key column keeps its bytes, but it still has to hold a value the read side can
/// decode: binary jsonb or JSON text. Anything else is rejected with the same error
/// [`normalize_jsonb_payload`] produces, so a key column and a non-key column
/// accept exactly the same inputs.
fn validate_jsonb_payload(bytes: &[u8]) -> Result<()> {
    encode_if_text(bytes).map(|_| ())
}

/// Classifies one payload written into a jsonb column.
///
/// Returns `None` when the payload is binary jsonb (or empty) and therefore has to
/// be kept as it is, and the encoded jsonb when it is JSON text.
fn encode_if_text(bytes: &[u8]) -> Result<Option<Vec<u8>>> {
    if bytes.is_empty() || is_binary_jsonb(bytes) {
        return Ok(None);
    }

    let text = std::str::from_utf8(bytes).map_err(|e| {
        error::InvalidJsonbPayloadSnafu {
            reason: format!("the payload is neither binary jsonb nor valid UTF-8: {e}"),
        }
        .build()
    })?;
    // The *extended* parser, i.e. the one `jsonb::from_slice` falls back to when it
    // reads a text payload back. Anything a legacy text payload can be read as can
    // therefore also be written; the stricter standard-mode parser is what SQL
    // literals go through (`common_sql::convert`), not this branch.
    let jsonb = jsonb::parse_value(text.as_bytes())
        .map(|value| value.to_vec())
        .map_err(|e| {
            error::InvalidJsonbPayloadSnafu {
                reason: format!("the payload is neither binary jsonb nor parseable JSON text: {e}"),
            }
            .build()
        })?;

    Ok(Some(jsonb))
}

/// Normalizes every payload of a binary array that is written into a jsonb column.
///
/// Returns the input array unchanged when no payload needs re-encoding, so
/// canonical jsonb data is never copied.
pub(crate) fn normalize_jsonb_array(array: &ArrayRef) -> Result<ArrayRef> {
    let binary_array = binary_payloads(array)?;

    // Allocated on the first payload that has to be re-encoded; before that the
    // array is still byte-identical to the input.
    let mut rebuilt: Option<Vec<Option<Vec<u8>>>> = None;
    for (index, payload) in binary_array.iter().enumerate() {
        let Some(payload) = payload else {
            if let Some(rebuilt) = &mut rebuilt {
                rebuilt.push(None);
            }
            continue;
        };

        match normalize_jsonb_payload(payload)? {
            Cow::Borrowed(_) => {
                if let Some(rebuilt) = &mut rebuilt {
                    rebuilt.push(Some(payload.to_vec()));
                }
            }
            Cow::Owned(jsonb) => {
                let rebuilt = rebuilt.get_or_insert_with(|| {
                    (0..index)
                        .map(|previous| {
                            if binary_array.is_null(previous) {
                                None
                            } else {
                                Some(binary_array.value(previous).to_vec())
                            }
                        })
                        .collect()
                });
                rebuilt.push(Some(jsonb));
            }
        }
    }

    match rebuilt {
        None => Ok(Arc::clone(array)),
        Some(rebuilt) => Ok(Arc::new(BinaryArray::from_iter(rebuilt))),
    }
}

// The jsonb layout constants below mirror the pinned `jsonb` crate
// (`core/databend/constants.rs`).
const CONTAINER_HEADER_TYPE_MASK: u32 = 0xE000_0000;
const CONTAINER_HEADER_LEN_MASK: u32 = 0x1FFF_FFFF;
const SCALAR_CONTAINER_TAG: u32 = 0x2000_0000;
const OBJECT_CONTAINER_TAG: u32 = 0x4000_0000;
const ARRAY_CONTAINER_TAG: u32 = 0x8000_0000;
const JENTRY_OFFSET_FLAG: u32 = 0x8000_0000;
const JENTRY_TYPE_MASK: u32 = 0x7000_0000;
const JENTRY_LEN_MASK: u32 = 0x0FFF_FFFF;
const NULL_TAG: u32 = 0x0000_0000;
const STRING_TAG: u32 = 0x1000_0000;
const NUMBER_TAG: u32 = 0x2000_0000;
const FALSE_TAG: u32 = 0x3000_0000;
const TRUE_TAG: u32 = 0x4000_0000;
const CONTAINER_TAG: u32 = 0x5000_0000;
const EXTENSION_TAG: u32 = 0x6000_0000;

/// Maximum container nesting accepted by [`is_binary_jsonb`].
///
/// The walk is recursive, and the jsonb decoder recurses through nested
/// containers as well, so a payload nested deeper than this cannot be handled
/// safely (or read back) anyway.
const MAX_JSONB_DEPTH: usize = 512;

/// Validates every payload of a binary array written into a jsonb column without
/// rewriting it; see [`validate_jsonb_payload`].
pub(crate) fn validate_jsonb_array(array: &ArrayRef) -> Result<()> {
    for payload in binary_payloads(array)?.iter().flatten() {
        validate_jsonb_payload(payload)?;
    }
    Ok(())
}

/// Returns the payloads of a binary array, or an error when `array` is not binary.
fn binary_payloads(array: &ArrayRef) -> Result<&BinaryArray> {
    array.as_any().downcast_ref::<BinaryArray>().ok_or_else(|| {
        error::InvalidJsonbPayloadSnafu {
            reason: format!(
                "expected a binary array for a jsonb column, got {:?}",
                array.data_type()
            ),
        }
        .build()
    })
}

/// Returns whether `bytes` is a structurally valid binary jsonb payload.
///
/// This is a bounds-checked walk over the jsonb container/jentry layout: it never
/// allocates, never reads out of bounds and validates every string payload
/// (object keys included) as UTF-8, so it is safe on arbitrary input. The pinned
/// `jsonb` crate must not be used for this decision:
///
/// - `jsonb::from_slice` falls back to parsing text, which is exactly what lets a
///   text payload masquerade as jsonb (`12345678` even decodes as `false`).
/// - The binary decoder is unsafe on untrusted input: it allocates from
///   header-declared entry counts (`[1,2]` starts with bytes that read as an
///   object header claiming ~456 million entries), sizes number payloads with
///   `assert!(len > 0)` plus fixed-width `unwrap()`s, and builds strings and
///   object keys with `from_utf8_unchecked`.
///
/// The walk therefore rejects a scalar container header that is not exactly
/// [`SCALAR_CONTAINER_TAG`], a declared entry count or payload length that does not
/// fit the buffer, offset-form jentries (which the pinned encoder never emits and
/// the decoder does not support), non-string object keys, empty or wrong-width
/// number/extension payloads, invalid UTF-8 strings and trailing bytes.
fn is_binary_jsonb(bytes: &[u8]) -> bool {
    match jsonb_value_len(bytes, 0, 0) {
        Some(len) => len == bytes.len(),
        None => false,
    }
}

/// Returns the length of the jsonb value starting at `offset`, or `None` when it
/// is not structurally valid.
fn jsonb_value_len(bytes: &[u8], offset: usize, depth: usize) -> Option<usize> {
    if depth > MAX_JSONB_DEPTH {
        return None;
    }

    let header = read_be_u32(bytes, offset)?;
    let container_tag = header & CONTAINER_HEADER_TYPE_MASK;
    let count = (header & CONTAINER_HEADER_LEN_MASK) as usize;
    // `(entries, jentry block size)`. A scalar container carries exactly one
    // jentry, and its header is exactly `SCALAR_CONTAINER_TAG`: the encoder never
    // sets length bits on it, and JSON text whose first byte maps to the scalar
    // type always has non-zero ones, so requiring the exact header rejects such
    // text before a jentry read from its bytes can pass the bounds check (the
    // pinned decoder rejects it too). An object carries a key and a value jentry
    // per entry.
    let (entries, jentry_bytes) = match container_tag {
        SCALAR_CONTAINER_TAG if header == SCALAR_CONTAINER_TAG => (1, 4),
        ARRAY_CONTAINER_TAG => (count, count.checked_mul(4)?),
        OBJECT_CONTAINER_TAG => (count.checked_mul(2)?, count.checked_mul(8)?),
        _ => return None,
    };

    let jentries_offset = offset.checked_add(4)?;
    let mut payload_offset = jentries_offset.checked_add(jentry_bytes)?;
    // The declared number of entries must fit the buffer before anything is read
    // or allocated.
    if payload_offset > bytes.len() {
        return None;
    }

    for entry in 0..entries {
        let jentry = read_be_u32(bytes, jentries_offset.checked_add(entry.checked_mul(4)?)?)?;
        if jentry & JENTRY_OFFSET_FLAG != 0 {
            return None;
        }

        let type_code = jentry & JENTRY_TYPE_MASK;
        let length = (jentry & JENTRY_LEN_MASK) as usize;
        let payload_end = payload_offset.checked_add(length)?;
        if payload_end > bytes.len() {
            return None;
        }
        let payload = &bytes[payload_offset..payload_end];

        let is_object_key = container_tag == OBJECT_CONTAINER_TAG && entry < count;
        if is_object_key && type_code != STRING_TAG {
            return None;
        }
        let valid = match type_code {
            NULL_TAG | TRUE_TAG | FALSE_TAG => length == 0,
            // String values and object keys are read with `from_utf8_unchecked`.
            STRING_TAG => std::str::from_utf8(payload).is_ok(),
            NUMBER_TAG => is_number_payload(payload),
            EXTENSION_TAG => is_extension_payload(payload),
            CONTAINER_TAG => jsonb_value_len(bytes, payload_offset, depth + 1) == Some(length),
            _ => false,
        };
        if !valid {
            return None;
        }

        payload_offset = payload_end;
    }

    Some(payload_offset - offset)
}

fn read_be_u32(bytes: &[u8], offset: usize) -> Option<u32> {
    let end = offset.checked_add(4)?;
    let raw: [u8; 4] = bytes.get(offset..end)?.try_into().ok()?;
    Some(u32::from_be_bytes(raw))
}

/// Returns whether `payload` is a number encoding the jsonb decoder can read.
///
/// Mirrors `Number::decode`, which asserts a non-empty payload and unwraps the
/// width of every fixed-size variant.
fn is_number_payload(payload: &[u8]) -> bool {
    let Some((&tag, body)) = payload.split_first() else {
        return false;
    };
    match tag {
        // zero, NaN, +inf, -inf
        0x00 | 0x10 | 0x20 | 0x30 => body.is_empty(),
        // int64, uint64
        0x40 | 0x50 => matches!(body.len(), 1 | 2 | 4 | 8),
        // float64
        0x60 => body.len() == 8,
        // decimal64, decimal128 (current and deprecated), decimal256 (idem)
        0x70 => matches!(body.len(), 9 | 17 | 18 | 33 | 34),
        _ => false,
    }
}

/// Returns whether `payload` is an extension encoding the jsonb decoder can read.
fn is_extension_payload(payload: &[u8]) -> bool {
    let Some((&tag, body)) = payload.split_first() else {
        return false;
    };
    match tag {
        // binary of any length, date, timestamp, timestamp with time zone, interval
        0x00 => true,
        0x10 => body.len() == 4,
        0x20 => body.len() == 8,
        0x30 => matches!(body.len(), 9 | 12),
        0x40 => body.len() == 16,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use datatypes::types::jsonb_to_string;

    use super::*;

    fn jsonb_of(json: &str) -> Vec<u8> {
        // The encoder's standard-mode output, which is what SQL literals produce.
        datatypes::types::parse_string_to_jsonb(json).unwrap()
    }

    /// Every payload the jsonb encoder produces must be recognized, otherwise a
    /// native jsonb value would be mistaken for text and rejected.
    #[test]
    fn test_is_binary_jsonb_accepts_encoded_values() {
        for text in [
            "12345678",
            "-1",
            "0",
            "1.5",
            "1e300",
            "-0.0",
            r#"{"a":"x"}"#,
            r#"{"a":{"b":[1,2,{"c":null}]},"d":true,"e":false}"#,
            r#"{"unicode":"héllo \u00e9 \u2028","empty_obj":{},"empty_arr":[]}"#,
            r#"[1,2,3]"#,
            "[]",
            "{}",
            r#"[[]]"#,
            r#"[{}]"#,
            r#"[[1,[2,[3]]]]"#,
            r#""text""#,
            r#""""#,
            "null",
            "true",
            "false",
            r#"[null,true,false,0,"s",[],{}]"#,
            // The value the issue is about, and a long string.
            "12345678",
            &format!("\"{}\"", "x".repeat(4096)),
        ] {
            let encoded = jsonb_of(text);
            assert!(
                is_binary_jsonb(&encoded),
                "{text} must be recognized as binary jsonb"
            );
            assert!(
                !is_binary_jsonb(text.as_bytes()),
                "{text} must not be taken for binary jsonb"
            );
        }
    }

    /// The validator runs on bytes from files and clients, so it has to be total:
    /// it must reject every one of these instead of decoding them.
    #[test]
    fn test_is_binary_jsonb_rejects_adversarial_payloads() {
        let canonical = jsonb_of(r#"{"a":"x"}"#);
        let mut trailing = canonical.clone();
        trailing.push(0);
        // A canonical object payload whose value string is not UTF-8.
        let mut invalid_utf8_string = jsonb_of(r#"{"a":"x"}"#);
        invalid_utf8_string[13] = 0xFF;
        // An object whose key jentry is not a string.
        let mut non_string_key = jsonb_of(r#"{"a":"x"}"#);
        non_string_key[4..8].copy_from_slice(&0x2000_0001u32.to_be_bytes());
        // A number payload with no body (`Number::decode` asserts it is non-empty).
        let empty_number = [
            0x20, 0x00, 0x00, 0x00, // scalar container
            0x20, 0x00, 0x00, 0x00, // number jentry of length 0
        ];
        // A scalar container header with length bits set. The decoder requires the
        // header to be exactly `SCALAR_CONTAINER_TAG`, so this is not jsonb even
        // though its single jentry and payload would fit the buffer.
        let scalar_header_with_length = [
            0x20, 0x00, 0x00, 0x01, // scalar container claiming one entry
            0x30, 0x00, 0x00, 0x00, // `false` jentry, no payload
        ];
        // A 4-byte header claiming more entries than the buffer can hold.
        let huge_count = [0x40, 0xFF, 0xFF, 0xFF];
        // A jentry claiming a payload longer than the buffer.
        let huge_length = [
            0x40, 0x00, 0x00, 0x01, // object with one entry
            0x10, 0x0F, 0xFF, 0xFF, // key of length 0x0FFFFFFF
            0x10, 0x00, 0x00, 0x01, // value of length 1
        ];
        // A nested container that does not fill its declared length.
        let truncated_nested = [
            0x80, 0x00, 0x00, 0x01, // array with one entry
            0x50, 0x00, 0x00, 0x08, // nested container of length 8
            0x20, 0x00, 0x00, 0x00, // truncated: no jentry follows
        ];
        // Offset-form jentries are not supported by the pinned decoder.
        let offset_form = [
            0x20, 0x00, 0x00, 0x00, // scalar container
            0x90, 0x00, 0x00, 0x01, // string jentry with the offset flag set
            b'x',
        ];

        for (name, payload) in [
            ("text \"2000\"", b"2000".as_slice()),
            // Ordered before `[1,2]` on purpose: a decoder-based check panics here
            // deterministically, while `[1,2]` makes it allocate gigabytes.
            ("empty number payload", empty_number.as_slice()),
            ("text [1,2]", b"[1,2]".as_slice()),
            ("empty", b"".as_slice()),
            ("garbage", b"\x01\x02\x03\x04".as_slice()),
            ("invalid utf-8", b"\xff\xfe\xfd\xfc".as_slice()),
            ("canonical + trailing byte", trailing.as_slice()),
            ("invalid utf-8 string", invalid_utf8_string.as_slice()),
            ("non-string object key", non_string_key.as_slice()),
            (
                "scalar header with length bits",
                scalar_header_with_length.as_slice(),
            ),
            ("huge entry count", huge_count.as_slice()),
            ("huge payload length", huge_length.as_slice()),
            ("truncated nested container", truncated_nested.as_slice()),
            ("offset-form jentry", offset_form.as_slice()),
            // Truncations of a valid payload must be rejected too.
            ("truncated object", &canonical[..canonical.len() - 1]),
            ("header only", &canonical[..4]),
        ] {
            assert!(
                !is_binary_jsonb(payload),
                "{name} must not be accepted as binary jsonb"
            );
        }
    }

    /// The payloads of a binary column.
    type Payloads = Vec<Option<Vec<u8>>>;

    /// Reads the payloads of a binary array back for assertions.
    fn payloads(array: &ArrayRef) -> Payloads {
        let binary = array.as_any().downcast_ref::<BinaryArray>().unwrap();
        (0..binary.len())
            .map(|index| {
                if binary.is_null(index) {
                    None
                } else {
                    Some(binary.value(index).to_vec())
                }
            })
            .collect()
    }

    /// A batch can mix canonical jsonb, JSON text and nulls; rebuilding the array
    /// must not disturb the payloads it did not have to re-encode.
    #[test]
    fn test_normalize_jsonb_array_mixed_payloads() {
        let canonical = jsonb_of(r#"{"a":"x"}"#);
        let canonical_number = jsonb_of("12345678");
        let text = br#"{"b":"y"}"#.to_vec();
        let text_number = b"12345678".to_vec();

        let cases: Vec<(Payloads, Payloads)> = vec![
            // The payload before the first re-encoded one must be preserved.
            (
                vec![Some(canonical.clone()), Some(text.clone())],
                vec![Some(canonical.clone()), Some(jsonb_of(r#"{"b":"y"}"#))],
            ),
            // ... and so must the ones after it.
            (
                vec![Some(text.clone()), Some(canonical_number.clone())],
                vec![
                    Some(jsonb_of(r#"{"b":"y"}"#)),
                    Some(canonical_number.clone()),
                ],
            ),
            (
                vec![None, Some(text_number.clone())],
                vec![None, Some(canonical_number.clone())],
            ),
            (
                vec![Some(canonical.clone()), None, Some(text_number.clone())],
                vec![
                    Some(canonical.clone()),
                    None,
                    Some(canonical_number.clone()),
                ],
            ),
            (
                vec![
                    Some(canonical.clone()),
                    Some(text.clone()),
                    Some(canonical_number.clone()),
                    None,
                    Some(text_number.clone()),
                ],
                vec![
                    Some(canonical.clone()),
                    Some(jsonb_of(r#"{"b":"y"}"#)),
                    Some(canonical_number.clone()),
                    None,
                    Some(canonical_number.clone()),
                ],
            ),
        ];

        for (input, expected) in cases {
            let array: ArrayRef = Arc::new(BinaryArray::from_iter(
                input.iter().map(|payload| payload.as_deref()),
            ));
            let normalized = normalize_jsonb_array(&array).unwrap();
            assert_eq!(expected, payloads(&normalized), "input: {input:?}");
        }
    }

    /// An array that needs no re-encoding is returned as is.
    #[test]
    fn test_normalize_jsonb_array_keeps_canonical_array() {
        let canonical = jsonb_of(r#"{"a":"x"}"#);
        let array: ArrayRef = Arc::new(BinaryArray::from_iter([
            Some(canonical.as_slice()),
            None,
            Some(canonical.as_slice()),
        ]));

        let normalized = normalize_jsonb_array(&array).unwrap();
        assert!(Arc::ptr_eq(&array, &normalized));
        assert_eq!(
            vec![Some(canonical.clone()), None, Some(canonical)],
            payloads(&normalized)
        );
    }

    /// The text branch uses the extended parser, i.e. the one the read side falls
    /// back to, so legacy text with extended syntax can be written back.
    ///
    /// The expected strings are what the read side renders under the pinned `jsonb`
    /// crate, which is not a round trip of the input text for every form.
    #[test]
    fn test_normalize_jsonb_payload_accepts_extended_syntax() {
        for (text, expected) in [
            ("+1", "1"),
            (".5", "0.5"),
            ("01", "1"),
            // A non-finite number is stored as such and renders as `null` in this
            // format, so NaN/±Infinity are accepted but do not read back as themselves.
            ("NaN", "null"),
            ("Infinity", "null"),
            ("-Infinity", "null"),
            ("TRUE", "true"),
            ("[1,,2]", "[1,null,2]"),
            ("'single quoted'", r#""single quoted""#),
        ] {
            let normalized = normalize_jsonb_payload(text.as_bytes())
                .unwrap_or_else(|e| panic!("{text} must be accepted: {e}"));
            assert!(
                matches!(normalized, Cow::Owned(_)),
                "{text} must be encoded"
            );
            assert!(is_binary_jsonb(&normalized), "{text} must encode to jsonb");
            assert_eq!(
                expected,
                jsonb_to_string(&normalized).unwrap(),
                "unexpected encoding for {text}"
            );
        }
    }

    /// The validating path accepts exactly what the normalizing path accepts, so a
    /// key column and a non-key column reject the same payloads with the same error.
    #[test]
    fn test_validate_jsonb_payload() {
        for payload in [
            jsonb_of(r#"{"a":"x"}"#),
            jsonb_of("12345678"),
            br#"{"a":"x"}"#.to_vec(),
            b"12345678".to_vec(),
            Vec::new(),
        ] {
            assert!(
                validate_jsonb_payload(&payload).is_ok(),
                "unexpected rejection: {payload:?}"
            );
        }

        for payload in [
            br#"{"a":}"#.to_vec(),
            b"\xff\xfe".to_vec(),
            b"not json".to_vec(),
        ] {
            let validate_err = validate_jsonb_payload(&payload).unwrap_err();
            let normalize_err = normalize_jsonb_payload(&payload).unwrap_err();
            assert_eq!(validate_err.to_string(), normalize_err.to_string());
            assert!(
                validate_err.to_string().contains("neither binary jsonb"),
                "{validate_err}"
            );
        }
    }

    #[test]
    fn test_normalize_jsonb_payload() {
        // Text is encoded.
        let text = normalize_jsonb_payload(br#"{"a":"x"}"#).unwrap();
        assert!(matches!(text, Cow::Owned(_)));
        assert_eq!(r#"{"a":"x"}"#, jsonb_to_string(&text).unwrap());
        assert!(is_binary_jsonb(&text));

        let number = normalize_jsonb_payload(b"12345678").unwrap();
        assert!(matches!(number, Cow::Owned(_)));
        assert_eq!("12345678", jsonb_to_string(&number).unwrap());

        // Native jsonb is kept byte-identical.
        let canonical = jsonb_of(r#"{"a":"x"}"#);
        let normalized = normalize_jsonb_payload(&canonical).unwrap();
        assert!(matches!(normalized, Cow::Borrowed(_)));
        assert_eq!(canonical, normalized.to_vec());

        // An empty payload is kept.
        assert!(matches!(
            normalize_jsonb_payload(b"").unwrap(),
            Cow::Borrowed(_)
        ));

        // Anything else fails instead of being stored.
        let err = normalize_jsonb_payload(b"{\"a\":}").unwrap_err();
        assert!(err.to_string().contains("neither binary jsonb"), "{err}");
        let err = normalize_jsonb_payload(b"\xff\xfe").unwrap_err();
        assert!(err.to_string().contains("neither binary jsonb"), "{err}");
    }
}
