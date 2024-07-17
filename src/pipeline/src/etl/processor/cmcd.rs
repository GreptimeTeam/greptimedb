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

use ahash::HashSet;
use urlencoding::decode;

use crate::etl::field::{Field, Fields};
use crate::etl::processor::{
    yaml_bool, yaml_field, yaml_fields, FIELDS_NAME, FIELD_NAME, IGNORE_MISSING_NAME,
};
use crate::etl::value::{Map, Value};

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
pub struct CMCDProcessor {
    fields: Fields,

    ignore_missing: bool,
}

impl CMCDProcessor {
    fn with_fields(&mut self, mut fields: Fields) {
        Self::update_output_keys(&mut fields);
        self.fields = fields;
    }

    fn with_ignore_missing(&mut self, ignore_missing: bool) {
        self.ignore_missing = ignore_missing;
    }

    fn generate_key(prefix: &str, key: &str) -> String {
        format!("{}_{}", prefix, key)
    }

    fn parse(prefix: &str, s: &str) -> Result<Map, String> {
        let mut map = Map::default();
        let parts = s.split(',');
        for part in parts {
            let mut kv = part.split('=');
            let k = kv.next().ok_or(format!("{part} missing key in {s}"))?;
            let v = kv.next();

            let key = Self::generate_key(prefix, k);
            match k {
                CMCD_KEY_BS | CMCD_KEY_SU => {
                    map.insert(key, Value::Boolean(true));
                }
                CMCD_KEY_BR | CMCD_KEY_BL | CMCD_KEY_D | CMCD_KEY_DL | CMCD_KEY_MTP
                | CMCD_KEY_RTP | CMCD_KEY_TB => {
                    let v = v.ok_or(format!("{k} missing value in {s}"))?;
                    let val: i64 = v
                        .parse()
                        .map_err(|_| format!("failed to parse {v} as i64"))?;
                    map.insert(key, Value::Int64(val));
                }
                CMCD_KEY_CID | CMCD_KEY_NRR | CMCD_KEY_OT | CMCD_KEY_SF | CMCD_KEY_SID
                | CMCD_KEY_ST | CMCD_KEY_V => {
                    let v = v.ok_or(format!("{k} missing value in {s}"))?;
                    map.insert(key, Value::String(v.to_string()));
                }
                CMCD_KEY_NOR => {
                    let v = v.ok_or(format!("{k} missing value in {s}"))?;
                    let val = match decode(v) {
                        Ok(val) => val.to_string(),
                        Err(_) => v.to_string(),
                    };
                    map.insert(key, Value::String(val));
                }
                CMCD_KEY_PR => {
                    let v = v.ok_or(format!("{k} missing value in {s}"))?;
                    let val: f64 = v
                        .parse()
                        .map_err(|_| format!("failed to parse {v} as f64"))?;
                    map.insert(key, Value::Float64(val));
                }
                _ => match v {
                    Some(v) => map.insert(key, Value::String(v.to_string())),
                    None => map.insert(k, Value::Boolean(true)),
                },
            }
        }

        Ok(map)
    }

    fn process_field(&self, val: &str, field: &Field) -> Result<Map, String> {
        let prefix = field.get_renamed_field();

        Self::parse(prefix, val)
    }

    fn update_output_keys(fields: &mut Fields) {
        for field in fields.iter_mut() {
            for key in CMCD_KEYS.iter() {
                field
                    .output_fields
                    .insert(Self::generate_key(field.get_renamed_field(), key), 0);
            }
        }
    }
}

impl TryFrom<&yaml_rust::yaml::Hash> for CMCDProcessor {
    type Error = String;

    fn try_from(value: &yaml_rust::yaml::Hash) -> Result<Self, Self::Error> {
        let mut processor = CMCDProcessor::default();

        for (k, v) in value.iter() {
            let key = k
                .as_str()
                .ok_or(format!("key must be a string, but got {k:?}"))?;
            match key {
                FIELD_NAME => {
                    processor.with_fields(Fields::one(yaml_field(v, FIELD_NAME)?));
                }
                FIELDS_NAME => {
                    processor.with_fields(yaml_fields(v, FIELDS_NAME)?);
                }

                IGNORE_MISSING_NAME => {
                    processor.with_ignore_missing(yaml_bool(v, IGNORE_MISSING_NAME)?);
                }

                _ => {}
            }
        }

        Ok(processor)
    }
}

impl crate::etl::processor::Processor for CMCDProcessor {
    fn kind(&self) -> &str {
        PROCESSOR_CMCD
    }

    fn ignore_missing(&self) -> bool {
        self.ignore_missing
    }

    fn fields(&self) -> &Fields {
        &self.fields
    }

    fn fields_mut(&mut self) -> &mut Fields {
        &mut self.fields
    }

    fn output_keys(&self) -> HashSet<String> {
        self.fields
            .iter()
            .map(|field| {
                field
                    .renamed_field
                    .clone()
                    .unwrap_or_else(|| field.get_field_name().to_string())
            })
            .flat_map(|keys| {
                CMCD_KEYS
                    .iter()
                    .map(move |key| format!("{}_{}", keys, *key))
            })
            .collect()
    }

    fn exec_field(&self, val: &Value, field: &Field) -> Result<Map, String> {
        match val {
            Value::String(val) => self.process_field(val, field),
            _ => Err(format!(
                "{} processor: expect string value, but got {val:?}",
                self.kind()
            )),
        }
    }

    fn exec_mut(&self, val: &mut Vec<Value>) -> Result<(), String> {
        for field in self.fields.iter() {
            match val.get(field.input_field.index) {
                Some(Value::String(v)) => {
                    let map = self.process_field(v, field)?;
                    for (k, v) in map.values.into_iter() {
                        field.output_fields.get(&k).map(|index| {
                            val.insert(*index, v);
                        });
                    }
                }
                Some(_) => {}
                None => {}
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use ahash::HashMap;
    use urlencoding::decode;

    use super::CMCDProcessor;
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
                "b%2Crtp%3D15000%2Csid%3D%226e2fb550-c457-11e9-bb97-0800200c9a66%22",
                vec![
                    (
                        "prefix_sid",
                        Value::String("\"6e2fb550-c457-11e9-bb97-0800200c9a66\"".into()),
                    ),
                    ("prefix_rtp", Value::Int64(15000)),
                    ("b", Value::Boolean(true)),
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
                "d%3D4004%2Ccom.example-myNumericKey%3D500%2Ccom.examplemyStringKey%3D%22myStringValue%22",
                vec![
                    (
                        "prefix_com.example-myNumericKey",
                        Value::String("500".into()),
                    ),
                    (
                        "prefix_com.examplemyStringKey",
                        Value::String("\"myStringValue\"".into()),
                    ),
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

        for (s, vec) in ss.into_iter() {
            let decoded = decode(s).unwrap().to_string();

            let values = vec
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect::<HashMap<String, Value>>();
            let expected = Map { values };

            let actual = CMCDProcessor::parse("prefix", &decoded).unwrap();
            assert_eq!(actual, expected);
        }
    }
}
