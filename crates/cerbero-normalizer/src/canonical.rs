use std::collections::BTreeMap;

use cerbero_common::contracts::sha256_lower_hex;
use serde_json::{Map, Value};

/// Returns a stable UTF-8 JSON representation with object keys sorted recursively and no
/// insignificant whitespace. CERBERO v1 restricts normalized hashing to this representation of
/// `NormalizedEvent.ocsf_event`.
#[must_use]
pub fn canonical_json_bytes(value: &Value) -> Vec<u8> {
    serde_json::to_vec(&canonicalize(value)).expect("serializing serde_json::Value cannot fail")
}

/// Returns the lowercase SHA-256 of the canonical OCSF JSON bytes.
#[must_use]
pub fn canonical_json_hash(value: &Value) -> String {
    sha256_lower_hex(&canonical_json_bytes(value))
}

fn canonicalize(value: &Value) -> Value {
    match value {
        Value::Object(object) => {
            let sorted: BTreeMap<_, _> = object
                .iter()
                .map(|(key, value)| (key.clone(), canonicalize(value)))
                .collect();
            let mut output = Map::new();
            for (key, value) in sorted {
                output.insert(key, value);
            }
            Value::Object(output)
        }
        Value::Array(values) => Value::Array(values.iter().map(canonicalize).collect()),
        _ => value.clone(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn canonical_json_sorts_nested_object_keys() {
        let left = json!({"z": 1, "a": {"y": 2, "b": 3}});
        let right = json!({"a": {"b": 3, "y": 2}, "z": 1});
        assert_eq!(canonical_json_bytes(&left), canonical_json_bytes(&right));
        assert_eq!(
            String::from_utf8(canonical_json_bytes(&left)).unwrap(),
            r#"{"a":{"b":3,"y":2},"z":1}"#
        );
        assert_eq!(canonical_json_hash(&left), canonical_json_hash(&right));
    }
}
