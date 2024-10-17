use jsonpath_rust::path::{JsonLike, ObjectField, Path};
use jsonpath_rust::{jsp_idx, jsp_obj, JsonPathValue};
use ref_cast::RefCast;
use regex::Regex;

#[derive(Default, Debug, Clone, PartialEq, RefCast)]
#[repr(transparent)]
pub struct Jsonb(pub jsonb::Value<'static>);

impl Jsonb {
    pub fn new(value: jsonb::Value<'static>) -> Self {
        Jsonb(value)
    }
}

impl From<String> for Jsonb {
    fn from(s: String) -> Self {
        Jsonb(jsonb::Value::String(s.into()))
    }
}

impl From<Vec<String>> for Jsonb {
    fn from(s: Vec<String>) -> Self {
        Jsonb(jsonb::Value::Array(
            s.into_iter().map(|s| s.into()).collect(),
        ))
    }
}

impl From<bool> for Jsonb {
    fn from(s: bool) -> Self {
        Jsonb(jsonb::Value::Bool(s))
    }
}

impl From<i64> for Jsonb {
    fn from(s: i64) -> Self {
        Jsonb(jsonb::Value::Number(jsonb::Number::Int64(s)))
    }
}

impl From<f64> for Jsonb {
    fn from(s: f64) -> Self {
        Jsonb(jsonb::Value::Number(jsonb::Number::Float64(s)))
    }
}

impl From<Vec<Jsonb>> for Jsonb {
    fn from(s: Vec<Jsonb>) -> Self {
        Jsonb(jsonb::Value::Array(s.into_iter().map(|s| s.0).collect()))
    }
}

impl From<&str> for Jsonb {
    fn from(s: &str) -> Self {
        Jsonb(jsonb::Value::String(s.to_string().into()))
    }
}

impl<'a> From<&'a jsonb::Value<'static>> for &'a Jsonb {
    fn from(s: &'a jsonb::Value<'static>) -> Self {
        Jsonb::ref_cast(s)
    }
}

impl JsonLike for Jsonb {
    fn get(&self, key: &str) -> Option<&Self> {
        let result = self.0.get_by_name_ignore_case(key);
        unsafe { std::mem::transmute(result) }
    }

    fn itre(&self, pref: String) -> Vec<jsonpath_rust::JsonPathValue<'_, Self>> {
        let res = match &self.0 {
            jsonb::Value::Array(elems) => {
                let mut res = vec![];
                for (idx, el) in elems.iter().enumerate() {
                    res.push(JsonPathValue::Slice(el.into(), jsp_idx(&pref, idx)));
                }
                res
            }
            jsonb::Value::Object(elems) => {
                let mut res = vec![];
                for (key, el) in elems.iter() {
                    res.push(JsonPathValue::Slice(el.into(), jsp_obj(&pref, key)));
                }
                res
            }
            _ => vec![],
        };
        if res.is_empty() {
            vec![JsonPathValue::NoValue]
        } else {
            res
        }
    }

    fn array_len(&self) -> JsonPathValue<'static, Jsonb> {
        match &self.0 {
            jsonb::Value::Array(elems) => {
                JsonPathValue::NewValue(Jsonb::init_with_usize(elems.len()))
            }
            _ => JsonPathValue::NoValue,
        }
    }

    fn init_with_usize(cnt: usize) -> Self {
        Jsonb(jsonb::Value::Number(jsonb::Number::UInt64(cnt as u64)))
    }

    fn deep_flatten(&self, pref: String) -> Vec<(&Self, String)> {
        let mut acc = vec![];
        match &self.0 {
            jsonb::Value::Object(elems) => {
                for (f, v) in elems.iter() {
                    let pref = jsp_obj(&pref, f);
                    acc.push((v.into(), pref.clone()));
                    acc.append(
                        &mut <&jsonb::Value<'_> as Into<&Jsonb>>::into(v).deep_flatten(pref),
                    );
                }
            }
            jsonb::Value::Array(elems) => {
                for (i, v) in elems.iter().enumerate() {
                    let pref = jsp_idx(&pref, i);
                    acc.push((v.into(), pref.clone()));
                    acc.append(
                        &mut <&jsonb::Value<'_> as Into<&Jsonb>>::into(v).deep_flatten(pref),
                    );
                }
            }
            _ => (),
        }
        acc
    }

    fn deep_path_by_key<'a>(
        &'a self,
        key: ObjectField<'a, Self>,
        pref: String,
    ) -> Vec<(&'a Self, String)> {
        let mut result: Vec<(&'a Self, String)> =
            JsonPathValue::vec_as_pair(key.find(JsonPathValue::new_slice(self, pref.clone())));
        match &self.0 {
            jsonb::Value::Object(elems) => {
                let mut next_levels: Vec<(&'a Self, String)> = elems
                    .iter()
                    .flat_map(|(k, v)| {
                        <&jsonb::Value<'_> as Into<&Jsonb>>::into(v)
                            .deep_path_by_key(key.clone(), jsp_obj(&pref, k))
                    })
                    .collect();
                result.append(&mut next_levels);
                result
            }
            jsonb::Value::Array(elems) => {
                let mut next_levels: Vec<(&'a Self, String)> = elems
                    .iter()
                    .enumerate()
                    .flat_map(|(i, v)| {
                        <&jsonb::Value<'_> as Into<&Jsonb>>::into(v)
                            .deep_path_by_key(key.clone(), jsp_idx(&pref, i))
                    })
                    .collect();
                result.append(&mut next_levels);
                result
            }
            _ => result,
        }
    }

    fn as_u64(&self) -> Option<u64> {
        self.0.as_u64()
    }

    fn is_array(&self) -> bool {
        self.0.is_array()
    }

    fn as_array(&self) -> Option<&Vec<Self>> {
        self.0
            .as_array()
            .map(|v| unsafe { std::mem::transmute::<&Vec<jsonb::Value<'static>>, &Vec<Self>>(v) })
    }

    fn size(left: Vec<&Self>, right: Vec<&Self>) -> bool {
        if let Some(Jsonb(jsonb::Value::Number(n))) = right.first() {
            if let Some(sz) = n.as_f64() {
                for el in left.iter() {
                    match &el.0 {
                        jsonb::Value::String(v) if v.len() == sz as usize => true,
                        jsonb::Value::Array(elems) if elems.len() == sz as usize => true,
                        jsonb::Value::Object(fields) if fields.len() == sz as usize => true,
                        _ => return false,
                    };
                }
                return true;
            }
        }
        false
    }

    fn sub_set_of(left: Vec<&Self>, right: Vec<&Self>) -> bool {
        if left.is_empty() {
            return true;
        }
        if right.is_empty() {
            return false;
        }

        if let Some(elems) = left.first().and_then(|e| e.as_array()) {
            if let Some(Jsonb(jsonb::Value::Array(right_elems))) = right.first() {
                if right_elems.is_empty() {
                    return false;
                }

                for el in elems {
                    let mut res = false;

                    for r in right_elems.iter() {
                        if el.0.eq(r) {
                            res = true
                        }
                    }
                    if !res {
                        return false;
                    }
                }
                return true;
            }
        }
        false
    }

    fn any_of(left: Vec<&Self>, right: Vec<&Self>) -> bool {
        if left.is_empty() {
            return true;
        }
        if right.is_empty() {
            return false;
        }

        if let Some(Jsonb(jsonb::Value::Array(elems))) = right.first() {
            if elems.is_empty() {
                return false;
            }

            for el in left.iter() {
                if let Some(left_elems) = el.as_array() {
                    for l in left_elems.iter() {
                        for r in elems.iter() {
                            if l.0.eq(r) {
                                return true;
                            }
                        }
                    }
                } else {
                    for r in elems.iter() {
                        if el.0.eq(r) {
                            return true;
                        }
                    }
                }
            }
        }

        false
    }

    fn regex(left: Vec<&Self>, right: Vec<&Self>) -> bool {
        if left.is_empty() || right.is_empty() {
            return false;
        }

        match right.first() {
            Some(Jsonb(jsonb::Value::String(str))) => {
                if let Ok(regex) = Regex::new(str) {
                    for el in left.iter() {
                        if let Some(v) = el.0.as_str() {
                            if regex.is_match(v) {
                                return true;
                            }
                        }
                    }
                }
                false
            }
            _ => false,
        }
    }

    fn inside(left: Vec<&Self>, right: Vec<&Self>) -> bool {
        if left.is_empty() {
            return false;
        }

        match right.first() {
            Some(Jsonb(jsonb::Value::Array(elems))) => {
                for el in left.iter() {
                    if elems.contains(&el.0) {
                        return true;
                    }
                }
                false
            }
            Some(Jsonb(jsonb::Value::Object(elems))) => {
                for el in left.iter() {
                    for r in elems.values() {
                        if el.0.eq(r) {
                            return true;
                        }
                    }
                }
                false
            }
            _ => false,
        }
    }

    fn less(left: Vec<&Self>, right: Vec<&Self>) -> bool {
        if left.len() == 1 && right.len() == 1 {
            match (left.first(), right.first()) {
                (Some(Jsonb(jsonb::Value::Number(l))), Some(Jsonb(jsonb::Value::Number(r)))) => l
                    .as_f64()
                    .and_then(|v1| r.as_f64().map(|v2| v1 < v2))
                    .unwrap_or(false),
                _ => false,
            }
        } else {
            false
        }
    }

    fn eq(left: Vec<&Self>, right: Vec<&Self>) -> bool {
        if left.len() != right.len() {
            false
        } else {
            left.iter().zip(right).map(|(a, b)| a.eq(&b)).all(|a| a)
        }
    }

    fn null() -> Self {
        Jsonb(jsonb::Value::Null)
    }

    fn array(data: Vec<Self>) -> Self {
        Jsonb(jsonb::Value::Array(data.into_iter().map(|x| x.0).collect()))
    }
}

#[cfg(test)]
mod test {
    use jsonpath_rust::JsonPath;

    use super::Jsonb;

    #[test]
    fn test_jsonb() {
        let json = r#"
        {
            "name":"Fred",
            "phones":[
                {
                    "type":"home",
                    "number":3720453
                },
                {
                    "type": "work",
                    "number":5062051
                }
            ]
        }"#;
        let data = Jsonb(jsonb::parse_value(json.as_bytes()).unwrap());
        let path: JsonPath<Jsonb> =
            JsonPath::try_from(r#"$.phones[?(@.number == 3720453)]"#).unwrap();
        let search_result = path.find(&data);

        assert_eq!(search_result.0.as_array().unwrap().len(), 1);
        assert_eq!(
            search_result,
            Jsonb(jsonb::Value::Array(vec![jsonb::Value::Object(
                jsonb::Object::from_iter(vec![
                    ("type".to_string(), jsonb::Value::String("home".into())),
                    (
                        "number".to_string(),
                        jsonb::Value::Number(jsonb::Number::Int64(3720453))
                    )
                ])
            )]))
        );
    }
}
