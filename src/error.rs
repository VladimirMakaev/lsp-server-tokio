//! Error types for LSP JSON-RPC messages.
//!
//! This module provides the [`ErrorCode`] enum with all LSP specification error codes
//! and the [`ResponseError`] struct for constructing error responses.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// LSP and JSON-RPC 2.0 error codes.
///
/// These error codes are defined by the JSON-RPC 2.0 specification and the LSP specification.
/// The codes are represented as i32 values using `#[repr(i32)]`.
///
/// # Error Code Ranges
///
/// - `-32700` to `-32600`: JSON-RPC 2.0 defined errors
/// - `-32099` to `-32000`: JSON-RPC reserved for implementation-defined server errors
/// - `-32899` to `-32800`: LSP reserved for LSP-specific errors
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
#[repr(i32)]
pub enum ErrorCode {
    // JSON-RPC 2.0 defined errors
    /// Invalid JSON was received by the server.
    ParseError = -32700,
    /// The JSON sent is not a valid Request object.
    InvalidRequest = -32600,
    /// The method does not exist / is not available.
    MethodNotFound = -32601,
    /// Invalid method parameter(s).
    InvalidParams = -32602,
    /// Internal JSON-RPC error.
    InternalError = -32603,

    // JSON-RPC reserved range (used by LSP)
    /// The server has not been initialized.
    ServerNotInitialized = -32002,
    /// Unknown error code (used for unmapped errors).
    UnknownErrorCode = -32001,

    // LSP specific (3.17.0+)
    /// A request failed but it was syntactically correct.
    RequestFailed = -32803,
    /// The server cancelled the request.
    ServerCancelled = -32802,
    /// The server detected that the content of a document got modified outside normal conditions.
    ContentModified = -32801,
    /// The client has canceled a request and a server has detected the cancel.
    RequestCancelled = -32800,
}

impl From<ErrorCode> for i32 {
    fn from(code: ErrorCode) -> Self {
        code as i32
    }
}

impl TryFrom<i32> for ErrorCode {
    type Error = i32;

    /// Attempts to convert an i32 to an `ErrorCode`.
    ///
    /// Returns `Err(code)` if the code is not a known error code.
    fn try_from(code: i32) -> Result<Self, Self::Error> {
        match code {
            -32700 => Ok(ErrorCode::ParseError),
            -32600 => Ok(ErrorCode::InvalidRequest),
            -32601 => Ok(ErrorCode::MethodNotFound),
            -32602 => Ok(ErrorCode::InvalidParams),
            -32603 => Ok(ErrorCode::InternalError),
            -32002 => Ok(ErrorCode::ServerNotInitialized),
            -32001 => Ok(ErrorCode::UnknownErrorCode),
            -32803 => Ok(ErrorCode::RequestFailed),
            -32802 => Ok(ErrorCode::ServerCancelled),
            -32801 => Ok(ErrorCode::ContentModified),
            -32800 => Ok(ErrorCode::RequestCancelled),
            _ => Err(code),
        }
    }
}

/// An error response in JSON-RPC format.
///
/// Contains a numeric error code, a human-readable message, and optional additional data.
/// The `code` field is stored as `i32` rather than `ErrorCode` to support unknown error codes
/// from clients or other servers.
///
/// # Examples
///
/// ```
/// use lsp_server_tokio::{ResponseError, ErrorCode};
///
/// // Create a simple error
/// let error = ResponseError::new(ErrorCode::MethodNotFound, "Method not found");
///
/// // Create an error with additional data
/// let error_with_data = ResponseError::new(ErrorCode::InvalidParams, "Missing required field")
///     .with_data(serde_json::json!({"field": "uri"}));
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseError {
    /// A number indicating the error type.
    pub code: i32,
    /// A string providing a short description of the error.
    pub message: String,
    /// A primitive or structured value with additional information.
    /// This may be omitted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl ResponseError {
    /// Creates a new `ResponseError` with the given error code and message.
    ///
    /// The `data` field is set to `None`.
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code: code as i32,
            message: message.into(),
            data: None,
        }
    }

    /// Creates a new `ResponseError` from a raw error code and message.
    ///
    /// Use this when dealing with unknown or custom error codes.
    pub fn new_raw(code: i32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }

    /// Adds additional data to this error.
    ///
    /// Consumes self and returns a new `ResponseError` with the data attached.
    #[must_use]
    pub fn with_data(mut self, data: Value) -> Self {
        self.data = Some(data);
        self
    }

    /// Returns the error code as an `ErrorCode` enum, if it maps to a known code.
    #[must_use]
    pub fn error_code(&self) -> Option<ErrorCode> {
        ErrorCode::try_from(self.code).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_code_to_i32() {
        assert_eq!(i32::from(ErrorCode::ParseError), -32700);
        assert_eq!(i32::from(ErrorCode::InvalidRequest), -32600);
        assert_eq!(i32::from(ErrorCode::MethodNotFound), -32601);
        assert_eq!(i32::from(ErrorCode::InvalidParams), -32602);
        assert_eq!(i32::from(ErrorCode::InternalError), -32603);
        assert_eq!(i32::from(ErrorCode::ServerNotInitialized), -32002);
        assert_eq!(i32::from(ErrorCode::UnknownErrorCode), -32001);
        assert_eq!(i32::from(ErrorCode::RequestFailed), -32803);
        assert_eq!(i32::from(ErrorCode::ServerCancelled), -32802);
        assert_eq!(i32::from(ErrorCode::ContentModified), -32801);
        assert_eq!(i32::from(ErrorCode::RequestCancelled), -32800);
    }

    #[test]
    fn i32_to_error_code() {
        assert_eq!(ErrorCode::try_from(-32700), Ok(ErrorCode::ParseError));
        assert_eq!(ErrorCode::try_from(-32601), Ok(ErrorCode::MethodNotFound));
        assert_eq!(ErrorCode::try_from(-32800), Ok(ErrorCode::RequestCancelled));
        assert_eq!(ErrorCode::try_from(-99999), Err(-99999));
    }

    #[test]
    fn response_error_construction() {
        let error = ResponseError::new(ErrorCode::MethodNotFound, "Method not found");
        assert_eq!(error.code, -32601);
        assert_eq!(error.message, "Method not found");
        assert!(error.data.is_none());
    }

    #[test]
    fn response_error_with_data() {
        let error = ResponseError::new(ErrorCode::InvalidParams, "Invalid params")
            .with_data(serde_json::json!({"field": "uri"}));
        assert_eq!(error.code, -32602);
        assert!(error.data.is_some());
        assert_eq!(error.data.unwrap()["field"], "uri");
    }

    #[test]
    fn response_error_serialization_without_data() {
        let error = ResponseError::new(ErrorCode::ParseError, "Parse error");
        let json = serde_json::to_string(&error).unwrap();
        // data field should be omitted when None
        assert!(!json.contains("data"));
        assert!(json.contains("-32700"));
        assert!(json.contains("Parse error"));
    }

    #[test]
    fn response_error_serialization_with_data() {
        let error = ResponseError::new(ErrorCode::InvalidParams, "Missing field")
            .with_data(serde_json::json!({"missing": "uri"}));
        let json = serde_json::to_string(&error).unwrap();
        assert!(json.contains("data"));
        assert!(json.contains("missing"));
    }

    #[test]
    fn response_error_deserialization() {
        let json = r#"{"code":-32601,"message":"Method not found"}"#;
        let error: ResponseError = serde_json::from_str(json).unwrap();
        assert_eq!(error.code, -32601);
        assert_eq!(error.message, "Method not found");
        assert!(error.data.is_none());
    }

    #[test]
    fn response_error_deserialization_with_data() {
        let json = r#"{"code":-32602,"message":"Invalid","data":{"field":"uri"}}"#;
        let error: ResponseError = serde_json::from_str(json).unwrap();
        assert_eq!(error.code, -32602);
        assert!(error.data.is_some());
    }

    #[test]
    fn response_error_raw_code() {
        let error = ResponseError::new_raw(-99999, "Custom error");
        assert_eq!(error.code, -99999);
        assert!(error.error_code().is_none());
    }

    #[test]
    fn skip_serializing_if_works() {
        let error = ResponseError::new(ErrorCode::InternalError, "Error");
        let json = serde_json::to_value(&error).unwrap();
        let obj = json.as_object().unwrap();
        // The "data" key should not exist when data is None
        assert!(!obj.contains_key("data"));
    }
}
