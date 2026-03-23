//! Request ID type for LSP JSON-RPC messages.
//!
//! Per the LSP specification, request IDs can be either integers or strings.
//! This module provides the [`RequestId`] enum that handles both cases with
//! correct JSON serialization using `#[serde(untagged)]`.

use serde::{Deserialize, Serialize};
use std::fmt;

/// A request identifier that can be either an integer or a string.
///
/// JSON-RPC 2.0 and LSP allow request IDs to be either numbers or strings.
/// This enum uses `#[serde(untagged)]` to deserialize from either format
/// and serialize back to the original format.
///
/// # Examples
///
/// ```
/// use lsp_server_tokio::RequestId;
///
/// let int_id = RequestId::from(42);
/// let str_id = RequestId::from("abc-123".to_string());
///
/// // Both serialize to JSON correctly
/// assert_eq!(serde_json::to_string(&int_id).unwrap(), "42");
/// assert_eq!(serde_json::to_string(&str_id).unwrap(), "\"abc-123\"");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RequestId {
    /// An integer request ID (most common in practice).
    Integer(i32),
    /// A string request ID.
    String(String),
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RequestId::Integer(n) => write!(f, "{n}"),
            RequestId::String(s) => write!(f, "{s}"),
        }
    }
}

impl From<i32> for RequestId {
    fn from(n: i32) -> Self {
        RequestId::Integer(n)
    }
}

impl From<String> for RequestId {
    fn from(s: String) -> Self {
        RequestId::String(s)
    }
}

impl From<&str> for RequestId {
    fn from(s: &str) -> Self {
        RequestId::String(s.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integer_serialization_roundtrip() {
        let id = RequestId::Integer(42);
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "42");
        let parsed: RequestId = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, id);
    }

    #[test]
    fn string_serialization_roundtrip() {
        let id = RequestId::String("abc-123".to_string());
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "\"abc-123\"");
        let parsed: RequestId = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, id);
    }

    #[test]
    fn integer_deserialization_from_json_number() {
        let id: RequestId = serde_json::from_str("42").unwrap();
        assert_eq!(id, RequestId::Integer(42));
    }

    #[test]
    fn string_deserialization_from_json_string() {
        let id: RequestId = serde_json::from_str("\"abc\"").unwrap();
        assert_eq!(id, RequestId::String("abc".to_string()));
    }

    #[test]
    fn display_integer() {
        let id = RequestId::Integer(42);
        assert_eq!(format!("{}", id), "42");
    }

    #[test]
    fn display_string() {
        let id = RequestId::String("abc-123".to_string());
        assert_eq!(format!("{}", id), "abc-123");
    }

    #[test]
    fn from_i32() {
        let id: RequestId = 42.into();
        assert_eq!(id, RequestId::Integer(42));
    }

    #[test]
    fn from_string() {
        let id: RequestId = "abc".to_string().into();
        assert_eq!(id, RequestId::String("abc".to_string()));
    }

    #[test]
    fn from_str() {
        let id: RequestId = "test".into();
        assert_eq!(id, RequestId::String("test".to_string()));
    }

    #[test]
    fn hash_works_for_hashmap_keys() {
        use std::collections::HashMap;
        let mut map = HashMap::new();
        map.insert(RequestId::Integer(1), "first");
        map.insert(RequestId::String("abc".to_string()), "second");
        assert_eq!(map.get(&RequestId::Integer(1)), Some(&"first"));
        assert_eq!(map.get(&RequestId::String("abc".to_string())), Some(&"second"));
    }

    #[test]
    fn integer_and_string_with_same_value_are_not_equal() {
        // Per JSON-RPC spec, integer 1 and string "1" are different IDs
        let int_id = RequestId::Integer(1);
        let str_id = RequestId::String("1".to_string());
        assert_ne!(int_id, str_id);
    }
}
