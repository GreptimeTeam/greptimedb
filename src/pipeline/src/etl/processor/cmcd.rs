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

use std::collections::BTreeMap;

use ahash::HashSet;
use snafu::{OptionExt, ResultExt};
use urlencoding::decode;

use crate::etl::error::{
    CmcdMissingKeySnafu, CmcdMissingValueSnafu, Error, FailedToParseFloatKeySnafu,
    FailedToParseIntKeySnafu, KeyMustBeStringSnafu, ProcessorExpectStringSnafu,
    ProcessorMissingFieldSnafu, Result,
};
use crate::etl::field::{Field, Fields, InputFieldInfo, OneInputMultiOutputField};
use crate::etl::find_key_index;
use crate::etl::processor::{
    yaml_bool, yaml_new_field, yaml_new_fields, Processor, ProcessorBuilder, ProcessorKind,
    FIELDS_NAME, FIELD_NAME, IGNORE_MISSING_NAME,
};
use crate::etl::value::Value;

pub(crate) const PROCESSOR_CMCD: &str = "cmcd";

const CMCD_KEY_BR: &str = "br"; // Encoded bitrate, Integer kbps
const CMCD_KEY_BL: &str = "bl"; // Buffer length, Integer milliseconds
const CMCD_KEY_BS: &str = "bs"; // Buffer starvation, Boolean
const CMCD_KEY_CID: &str = "cid"; // Content ID, String
const CMCD_KEY_D: &str = "d"; // Object duration, Integer milliseconds
const CMCD_KEY_DL: &str = "dl"; // Deadline, Integer milliseconds
const CMCD_KEY_MTP: &str = "mtp"; // Measured throughput, Integer kbps
const CMCD_KEY_NOR: &str = "nor"; // Next object request, String
const CMCD_KEY_NRR: &str = "nrr"; // Next request range, String, "<range-start>-<range-end>"
const CMCD_KEY_OT: &str = "ot"; // Object type, Token - one of [m,a,v,av,i,c,tt,k,o]
const CMCD_KEY_PR: &str = "pr"; // Playback rate, Decimal
const CMCD_KEY_RTP: &str = "rtp"; // Requested maximum throughput, Integer kbps
const CMCD_KEY_SF: &str = "sf"; // Stall frequency, Token - one of [d,h,s,o]
const CMCD_KEY_SID: &str = "sid"; // Session ID, String
const CMCD_KEY_ST: &str = "st"; // Stream type, Token - one of [v,l]
const CMCD_KEY_SU: &str = "su"; // Startup, Boolean
const CMCD_KEY_TB: &str = "tb"; // Top bitrate, Integer kbps
const CMCD_KEY_V: &str = "v"; // Version

const CMCD_KEYS: [&str; 18] = [
    CMCD_KEY_BR,
    CMCD_KEY_BL,
    CMCD_KEY_BS,
    CMCD_KEY_CID,
    CMCD_KEY_D,
    CMCD_KEY_DL,
    CMCD_KEY_MTP,
    CMCD_KEY_NOR,
    CMCD_KEY_NRR,
    CMCD_KEY_OT,
    CMCD_KEY_PR,
    CMCD_KEY_RTP,
    CMCD_KEY_SF,
    CMCD_KEY_SID,
    CMCD_KEY_ST,
    CMCD_KEY_SU,
    CMCD_KEY_TB,
    CMCD_KEY_V,
];

/// CmcdProcessorBuilder is a builder for CmcdProcessor
/// parse from raw yaml
#[derive(Debug, Default)]
pub struct CmcdProcessorBuilder {
    fields: Fields,
    output_keys: HashSet<String>,
    ignore_missing: bool,
}

impl CmcdProcessorBuilder {
    /// build_cmcd_outputs build cmcd output info
    /// generate index and function for each output
    pub(super) fn build_cmcd_outputs(
        field: &Field,
        intermediate_keys: &[String],
    ) -> Result<(BTreeMap<String, usize>, Vec<CmcdOutputInfo>)> {
        let mut output_index = BTreeMap::new();
        let mut cmcd_field_outputs = Vec::with_capacity(CMCD_KEYS.len());
        for cmcd in CMCD_KEYS {
            let final_key = generate_key(field.target_or_input_field(), cmcd);
            let index = find_key_index(intermediate_keys, &final_key, "cmcd")?;
            output_index.insert(final_key.clone(), index);
            match cmcd {
                CMCD_KEY_BS | CMCD_KEY_SU => {
                    let output_info = CmcdOutputInfo::new(final_key, cmcd, index, bs_su);
                    cmcd_field_outputs.push(output_info);
                }
                CMCD_KEY_BR | CMCD_KEY_BL | CMCD_KEY_D | CMCD_KEY_DL | CMCD_KEY_MTP
                | CMCD_KEY_RTP | CMCD_KEY_TB => {
                    let output_info = CmcdOutputInfo::new(final_key, cmcd, index, br_tb);
                    cmcd_field_outputs.push(output_info);
                }
                CMCD_KEY_CID | CMCD_KEY_NRR | CMCD_KEY_OT | CMCD_KEY_SF | CMCD_KEY_SID
                | CMCD_KEY_ST | CMCD_KEY_V => {
                    let output_info = CmcdOutputInfo::new(final_key, cmcd, index, cid_v);
                    cmcd_field_outputs.push(output_info);
                }
                CMCD_KEY_NOR => {
                    let output_info = CmcdOutputInfo::new(final_key, cmcd, index, nor);
                    cmcd_field_outputs.push(output_info);
                }
                CMCD_KEY_PR => {
                    let output_info = CmcdOutputInfo::new(final_key, cmcd, index, pr);
                    cmcd_field_outputs.push(output_info);
                }
                _ => {}
            }
        }
        Ok((output_index, cmcd_field_outputs))
    }

    /// build CmcdProcessor from CmcdProcessorBuilder
    pub fn build(self, intermediate_keys: &[String]) -> Result<CmcdProcessor> {
        let mut real_fields = vec![];
        let mut cmcd_outputs = Vec::with_capacity(CMCD_KEYS.len());
        for field in self.fields.into_iter() {
            let input_index = find_key_index(intermediate_keys, field.input_field(), "cmcd")?;

            let input_field_info = InputFieldInfo::new(field.input_field(), input_index);

            let (_, cmcd_field_outputs) = Self::build_cmcd_outputs(&field, intermediate_keys)?;

            cmcd_outputs.push(cmcd_field_outputs);

            let real_field = OneInputMultiOutputField::new(input_field_info, field.target_field);
            real_fields.push(real_field);
        }
        Ok(CmcdProcessor {
            fields: real_fields,
            cmcd_outputs,
            ignore_missing: self.ignore_missing,
        })
    }
}

impl ProcessorBuilder for CmcdProcessorBuilder {
    fn output_keys(&self) -> HashSet<&str> {
        self.output_keys.iter().map(|s| s.as_str()).collect()
    }

    fn input_keys(&self) -> HashSet<&str> {
        self.fields.iter().map(|f| f.input_field()).collect()
    }

    fn build(self, intermediate_keys: &[String]) -> Result<ProcessorKind> {
        self.build(intermediate_keys).map(ProcessorKind::Cmcd)
    }
}

fn generate_key(prefix: &str, key: &str) -> String {
    format!("{}_{}", prefix, key)
}

/// CmcdOutputInfo is a struct to store output info
#[derive(Debug)]
pub(super) struct CmcdOutputInfo {
    /// {input_field}_{cmcd_key}
    final_key: String,
    /// cmcd key
    key: &'static str,
    /// index in intermediate_keys
    index: usize,
    /// function to resolve value
    f: fn(&str, &str, Option<&str>) -> Result<Value>,
}

impl CmcdOutputInfo {
    fn new(
        final_key: String,
        key: &'static str,
        index: usize,
        f: fn(&str, &str, Option<&str>) -> Result<Value>,
    ) -> Self {
        Self {
            final_key,
            key,
            index,
            f,
        }
    }
}

impl Default for CmcdOutputInfo {
    fn default() -> Self {
        Self {
            final_key: String::default(),
            key: "",
            index: 0,
            f: |_, _, _| Ok(Value::Null),
        }
    }
}

/// function to resolve CMCD_KEY_BS | CMCD_KEY_SU
fn bs_su(_: &str, _: &str, _: Option<&str>) -> Result<Value> {
    Ok(Value::Boolean(true))
}

/// function to resolve CMCD_KEY_BR | CMCD_KEY_BL | CMCD_KEY_D | CMCD_KEY_DL | CMCD_KEY_MTP | CMCD_KEY_RTP | CMCD_KEY_TB
fn br_tb(s: &str, k: &str, v: Option<&str>) -> Result<Value> {
    let v = v.context(CmcdMissingValueSnafu { k, s })?;
    let val: i64 = v
        .parse()
        .context(FailedToParseIntKeySnafu { key: k, value: v })?;
    Ok(Value::Int64(val))
}

/// function to resolve CMCD_KEY_CID | CMCD_KEY_NRR | CMCD_KEY_OT | CMCD_KEY_SF | CMCD_KEY_SID | CMCD_KEY_V
fn cid_v(s: &str, k: &str, v: Option<&str>) -> Result<Value> {
    let v = v.context(CmcdMissingValueSnafu { k, s })?;
    Ok(Value::String(v.to_string()))
}

/// function to resolve CMCD_KEY_NOR
fn nor(s: &str, k: &str, v: Option<&str>) -> Result<Value> {
    let v = v.context(CmcdMissingValueSnafu { k, s })?;
    let val = match decode(v) {
        Ok(val) => val.to_string(),
        Err(_) => v.to_string(),
    };
    Ok(Value::String(val))
}

/// function to resolve CMCD_KEY_PR
fn pr(s: &str, k: &str, v: Option<&str>) -> Result<Value> {
    let v = v.context(CmcdMissingValueSnafu { k, s })?;
    let val: f64 = v
        .parse()
        .context(FailedToParseFloatKeySnafu { key: k, value: v })?;
    Ok(Value::Float64(val))
}

/// Common Media Client Data Specification:
/// https://cdn.cta.tech/cta/media/media/resources/standards/pdfs/cta-5004-final.pdf
///
///
/// The data payload for Header and Query Argument transmission consists of a series of
/// key/value pairs constructed according to the following rules:
/// 1. All information in the payload MUST be represented as <key>=<value> pairs.
/// 2. The key and value MUST be separated by an equals sign Unicode 0x3D. If the
///    value type is BOOLEAN and the value is TRUE, then the equals sign and the value
///    MUST be omitted.
/// 3. Successive key/value pairs MUST be delimited by a comma Unicode 0x2C.
/// 4. The key names described in this specification are reserved. Custom key names
///    may be used, but they MUST carry a hyphenated prefix to ensure that there will
///    not be a namespace collision with future revisions to this specification. Clients
///    SHOULD use a reverse-DNS syntax when defining their own prefix.
/// 5. If headers are used for data transmission, then custom keys SHOULD be
///    allocated to one of the four defined header names based upon their expected
///    level of variability:
///       a. CMCD-Request: keys whose values vary with each request.
///       b. CMCD-Object: keys whose values vary with the object being requested.
///       c. CMCD-Status: keys whose values do not vary with every request or object.
///       d. CMCD-Session: keys whose values are expected to be invariant over the life of the session.
/// 6. All key names are case-sensitive.
/// 7. Any value of type String MUST be enclosed by opening and closing double
///    quotes Unicode 0x22. Double quotes and backslashes MUST be escaped using a
///    backslash "\" Unicode 0x5C character. Any value of type Token does not require
///    quoting.
/// 8. All keys are OPTIONAL.
/// 9. Key-value pairs SHOULD be sequenced in alphabetical order of the key name in
///    order to reduce the fingerprinting surface exposed by the player.
/// 10. If the data payload is transmitted as a query argument, then the entire payload
///     string MUST be URLEncoded per [5]. Data payloads transmitted via headers
///     MUST NOT be URLEncoded.
/// 11. The data payload syntax is intended to be compliant with Structured Field Values for HTTP [6].
/// 12. Transport Layer Security SHOULD be used to protect all transmission of CMCD data.
#[derive(Debug, Default)]
pub struct CmcdProcessor {
    fields: Vec<OneInputMultiOutputField>,
    cmcd_outputs: Vec<Vec<CmcdOutputInfo>>,

    ignore_missing: bool,
}

impl CmcdProcessor {
    fn generate_key(prefix: &str, key: &str) -> String {
        format!("{}_{}", prefix, key)
    }

    fn parse(&self, field_index: usize, s: &str) -> Result<Vec<(usize, Value)>> {
        let parts = s.split(',');
        let mut result = Vec::new();
        for part in parts {
            let mut kv = part.split('=');
            let k = kv.next().context(CmcdMissingKeySnafu { part, s })?;
            let v = kv.next();

            for cmcd_key in self.cmcd_outputs[field_index].iter() {
                if cmcd_key.key == k {
                    let val = (cmcd_key.f)(s, k, v)?;
                    result.push((cmcd_key.index, val));
                }
            }
        }

        Ok(result)
    }
}

impl TryFrom<&yaml_rust::yaml::Hash> for CmcdProcessorBuilder {
    type Error = Error;

    fn try_from(value: &yaml_rust::yaml::Hash) -> Result<Self> {
        let mut fields = Fields::default();
        let mut ignore_missing = false;

        for (k, v) in value.iter() {
            let key = k
                .as_str()
                .with_context(|| KeyMustBeStringSnafu { k: k.clone() })?;
            match key {
                FIELD_NAME => {
                    fields = Fields::one(yaml_new_field(v, FIELD_NAME)?);
                }
                FIELDS_NAME => {
                    fields = yaml_new_fields(v, FIELDS_NAME)?;
                }

                IGNORE_MISSING_NAME => {
                    ignore_missing = yaml_bool(v, IGNORE_MISSING_NAME)?;
                }

                _ => {}
            }
        }

        let output_keys = fields
            .iter()
            .flat_map(|f| {
                CMCD_KEYS
                    .iter()
                    .map(|cmcd_key| generate_key(f.target_or_input_field(), cmcd_key))
            })
            .collect();

        let builder = CmcdProcessorBuilder {
            fields,
            output_keys,
            ignore_missing,
        };

        Ok(builder)
    }
}

impl Processor for CmcdProcessor {
    fn kind(&self) -> &str {
        PROCESSOR_CMCD
    }

    fn ignore_missing(&self) -> bool {
        self.ignore_missing
    }

    fn exec_mut(&self, val: &mut Vec<Value>) -> Result<()> {
        for (field_index, field) in self.fields.iter().enumerate() {
            let field_value_index = field.input_index();
            match val.get(field_value_index) {
                Some(Value::String(v)) => {
                    let result_list = self.parse(field_index, v)?;
                    for (output_index, v) in result_list {
                        val[output_index] = v;
                    }
                }
                Some(Value::Null) | None => {
                    if !self.ignore_missing {
                        return ProcessorMissingFieldSnafu {
                            processor: self.kind().to_string(),
                            field: field.input_name().to_string(),
                        }
                        .fail();
                    }
                }
                Some(v) => {
                    return ProcessorExpectStringSnafu {
                        processor: self.kind().to_string(),
                        v: v.clone(),
                    }
                    .fail();
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use urlencoding::decode;

    use super::{CmcdProcessorBuilder, CMCD_KEYS};
    use crate::etl::field::{Field, Fields};
    use crate::etl::value::{Map, Value};

    #[test]
    fn test_cmcd() {
        let ss = [
            (
                "sid%3D%226e2fb550-c457-11e9-bb97-0800200c9a66%22",
                vec![(
                    "prefix_sid",
                    Value::String("\"6e2fb550-c457-11e9-bb97-0800200c9a66\"".into()),
                )],
            ),
            (
                "br%3D3200%2Cbs%2Cd%3D4004%2Cmtp%3D25400%2Cot%3Dv%2Crtp%3D15000%2Csid%3D%226e2fb550-c457-11e9-bb97-0800200c9a66%22%2Ctb%3D6000",
                vec![
                    ("prefix_bs", Value::Boolean(true)),
                    ("prefix_ot", Value::String("v".into())),
                    ("prefix_rtp", Value::Int64(15000)),
                    ("prefix_br", Value::Int64(3200)),
                    ("prefix_tb", Value::Int64(6000)),
                    ("prefix_d", Value::Int64(4004)),
                    (
                        "prefix_sid",
                        Value::String("\"6e2fb550-c457-11e9-bb97-0800200c9a66\"".into()),
                    ),
                    ("prefix_mtp", Value::Int64(25400)),
                ],
            ),
            (
                // we not resolve `b` key
                "b%2Crtp%3D15000%2Csid%3D%226e2fb550-c457-11e9-bb97-0800200c9a66%22",
                vec![
                    (
                        "prefix_sid",
                        Value::String("\"6e2fb550-c457-11e9-bb97-0800200c9a66\"".into()),
                    ),
                    ("prefix_rtp", Value::Int64(15000)),
                ],
            ),
            (
                "bs%2Csu",
                vec![
                    ("prefix_su", Value::Boolean(true)),
                    ("prefix_bs", Value::Boolean(true)),
                ],
            ),
            (
                // we not resolve custom key
                "d%3D4004%2Ccom.example-myNumericKey%3D500%2Ccom.examplemyStringKey%3D%22myStringValue%22",
                vec![
                    // (
                    //     "prefix_com.example-myNumericKey",
                    //     Value::String("500".into()),
                    // ),
                    // (
                    //     "prefix_com.examplemyStringKey",
                    //     Value::String("\"myStringValue\"".into()),
                    // ),
                    ("prefix_d", Value::Int64(4004)),
                ],
            ),
            (
                "nor%3D%22..%252F300kbps%252Fsegment35.m4v%22%2Csid%3D%226e2fb550-c457-11e9-bb97-0800200c9a66%22",
                vec![
                    (
                        "prefix_sid",
                        Value::String("\"6e2fb550-c457-11e9-bb97-0800200c9a66\"".into()),
                    ),
                    (
                        "prefix_nor",
                        Value::String("\"../300kbps/segment35.m4v\"".into()),

                    ),
                ],
            ),
            (
                "nrr%3D%2212323-48763%22%2Csid%3D%226e2fb550-c457-11e9-bb97-0800200c9a66%22",
                vec![
                    ("prefix_nrr", Value::String("\"12323-48763\"".into())),
                    (
                        "prefix_sid",
                        Value::String("\"6e2fb550-c457-11e9-bb97-0800200c9a66\"".into()),
                    ),
                ],
            ),
            (
                "nor%3D%22..%252F300kbps%252Ftrack.m4v%22%2Cnrr%3D%2212323-48763%22%2Csid%3D%226e2fb550-c457-11e9-bb97-0800200c9a66%22",
                vec![
                    ("prefix_nrr", Value::String("\"12323-48763\"".into())),
                    (
                        "prefix_sid",
                        Value::String("\"6e2fb550-c457-11e9-bb97-0800200c9a66\"".into()),
                    ),
                    (
                        "prefix_nor",
                        Value::String("\"../300kbps/track.m4v\"".into()),
                    ),
                ],
            ),
            (
                "bl%3D21300%2Cbr%3D3200%2Cbs%2Ccid%3D%22faec5fc2-ac30-11eabb37-0242ac130002%22%2Cd%3D4004%2Cdl%3D18500%2Cmtp%3D48100%2Cnor%3D%22..%252F300kbps%252Ftrack.m4v%22%2Cnrr%3D%2212323-48763%22%2Cot%3Dv%2Cpr%3D1.08%2Crtp%3D12000%2Csf%3Dd%2Csid%3D%226e2fb550-c457-11e9-bb97-0800200c9a66%22%2Cst%3Dv%2Csu%2Ctb%3D6000",
                vec![
                    ("prefix_bl", Value::Int64(21300)),
                    ("prefix_bs", Value::Boolean(true)),
                    ("prefix_st", Value::String("v".into())),
                    ("prefix_ot", Value::String("v".into())),
                    (
                        "prefix_sid",
                        Value::String("\"6e2fb550-c457-11e9-bb97-0800200c9a66\"".into()),
                    ),
                    ("prefix_tb", Value::Int64(6000)),
                    ("prefix_d", Value::Int64(4004)),
                    (
                        "prefix_cid",
                        Value::String("\"faec5fc2-ac30-11eabb37-0242ac130002\"".into()),
                    ),
                    ("prefix_mtp", Value::Int64(48100)),
                    ("prefix_rtp", Value::Int64(12000)),
                    (
                        "prefix_nor",
                        Value::String("\"../300kbps/track.m4v\"".into()),
                    ),
                    ("prefix_sf", Value::String("d".into())),
                    ("prefix_br", Value::Int64(3200)),
                    ("prefix_nrr", Value::String("\"12323-48763\"".into())),
                    ("prefix_pr", Value::Float64(1.08)),
                    ("prefix_su", Value::Boolean(true)),
                    ("prefix_dl", Value::Int64(18500)),
                ],
            ),
        ];

        let field = Field::new("prefix", None);

        let output_keys = CMCD_KEYS
            .iter()
            .map(|k| format!("prefix_{}", k))
            .collect::<Vec<String>>();

        let mut intermediate_keys = vec!["prefix".to_string()];
        intermediate_keys.append(&mut (output_keys.clone()));

        let builder = CmcdProcessorBuilder {
            fields: Fields::new(vec![field]),
            output_keys: output_keys.iter().map(|s| s.to_string()).collect(),
            ignore_missing: false,
        };

        let processor = builder.build(&intermediate_keys).unwrap();

        for (s, vec) in ss.into_iter() {
            let decoded = decode(s).unwrap().to_string();

            let values = vec
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect::<BTreeMap<String, Value>>();
            let expected = Map { values };

            let actual = processor.parse(0, &decoded).unwrap();
            let actual = actual
                .into_iter()
                .map(|(index, value)| (intermediate_keys[index].clone(), value))
                .collect::<BTreeMap<String, Value>>();
            let actual = Map { values: actual };
            assert_eq!(actual, expected);
        }
    }
}
