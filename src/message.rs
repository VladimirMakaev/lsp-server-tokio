//! Core message types for LSP JSON-RPC communication.
//!
//! This module provides the [`Message`] enum that discriminates between
//! [`Request`], [`Response`], and [`Notification`] types according to the
//! JSON-RPC 2.0 and LSP specifications.
//!
//! # Message Discrimination
//!
//! JSON-RPC 2.0 messages are discriminated by their fields:
//! - **Request**: Has `id` AND `method` fields
//! - **Response**: Has `id` AND (`result` OR `error`) fields
//! - **Notification**: Has `method` field but NO `id` field

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::ResponseError;
use crate::request_id::RequestId;

/// A JSON-RPC 2.0 request message.
///
/// Requests have an `id` that is used to correlate responses, a `method` name,
/// and optional `params`. The server MUST respond to every request.
///
/// # Examples
///
/// ```
/// use lsp_server_tokio::Request;
/// use serde_json::json;
///
/// // Create a request with params
/// let req = Request::new(1, "textDocument/completion", Some(json!({"textDocument": {"uri": "file:///test.rs"}})));
/// assert_eq!(req.id, 1.into());
/// assert_eq!(req.method, "textDocument/completion");
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
    /// The JSON-RPC protocol version. Always "2.0".
    pub jsonrpc: String,
    /// The request id used to correlate request and response.
    pub id: RequestId,
    /// The method to be invoked.
    pub method: String,
    /// The method's params.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

impl Request {
    /// Creates a new Request with the given id, method, and optional params.
    ///
    /// The `jsonrpc` field is automatically set to "2.0".
    pub fn new(id: impl Into<RequestId>, method: impl Into<String>, params: Option<Value>) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            id: id.into(),
            method: method.into(),
            params,
        }
    }
}

/// A JSON-RPC 2.0 response message.
///
/// Responses are sent from the server to the client in reply to a request.
/// A response MUST contain either a `result` (for success) or an `error` (for failure),
/// but never both.
///
/// The `id` field is `None` only for parse error responses where the request id
/// could not be determined (per JSON-RPC 2.0 specification).
///
/// # Examples
///
/// ```
/// use lsp_server_tokio::{Response, ResponseError, ErrorCode};
/// use serde_json::json;
///
/// // Success response
/// let ok = Response::ok(1, json!({"items": []}));
/// assert!(ok.result.is_some());
/// assert!(ok.error.is_none());
///
/// // Error response
/// let err = Response::err(1, ResponseError::new(ErrorCode::MethodNotFound, "Method not found"));
/// assert!(err.result.is_none());
/// assert!(err.error.is_some());
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    /// The JSON-RPC protocol version. Always "2.0".
    pub jsonrpc: String,
    /// The request id. None for parse error responses.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<RequestId>,
    /// The result of the request (mutually exclusive with error).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    /// The error that occurred (mutually exclusive with result).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ResponseError>,
}

impl Response {
    /// Creates a successful response with the given id and result.
    pub fn ok(id: impl Into<RequestId>, result: Value) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            id: Some(id.into()),
            result: Some(result),
            error: None,
        }
    }

    /// Creates an error response with the given id and error.
    pub fn err(id: impl Into<RequestId>, error: ResponseError) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            id: Some(id.into()),
            result: None,
            error: Some(error),
        }
    }

    /// Creates a parse error response where the request id could not be determined.
    ///
    /// Per JSON-RPC 2.0 spec, the id MUST be null when the request id cannot be parsed.
    pub fn parse_error(error: ResponseError) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            id: None,
            result: None,
            error: Some(error),
        }
    }
}

/// A JSON-RPC 2.0 notification message.
///
/// Notifications are messages sent from client to server (or vice versa) that
/// do not require a response. They do NOT have an `id` field.
///
/// # Examples
///
/// ```
/// use lsp_server_tokio::Notification;
/// use serde_json::json;
///
/// // Create a notification
/// let notif = Notification::new("textDocument/didOpen", Some(json!({"textDocument": {"uri": "file:///test.rs"}})));
/// assert_eq!(notif.method, "textDocument/didOpen");
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Notification {
    /// The JSON-RPC protocol version. Always "2.0".
    pub jsonrpc: String,
    /// The method to be invoked.
    pub method: String,
    /// The notification's params.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

impl Notification {
    /// Creates a new Notification with the given method and optional params.
    ///
    /// The `jsonrpc` field is automatically set to "2.0".
    pub fn new(method: impl Into<String>, params: Option<Value>) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            method: method.into(),
            params,
        }
    }
}

/// A JSON-RPC 2.0 message that can be a Request, Response, or Notification.
///
/// This enum uses `#[serde(untagged)]` for automatic discrimination based on
/// the presence of specific fields:
///
/// - **Request**: Has `id` AND `method` fields
/// - **Response**: Has `id` AND (`result` OR `error`) fields
/// - **Notification**: Has `method` field but NO `id` field
///
/// # Variant Order
///
/// The variant order is critical for correct deserialization:
/// 1. Request (most specific - has both `id` and `method`)
/// 2. Response (has `id` with `result` or `error`)
/// 3. Notification (has `method` but no `id`)
///
/// # Examples
///
/// ```
/// use lsp_server_tokio::Message;
///
/// // Request discrimination
/// let json = r#"{"jsonrpc":"2.0","id":1,"method":"test"}"#;
/// let msg: Message = serde_json::from_str(json).unwrap();
/// assert!(msg.is_request());
///
/// // Response discrimination
/// let json = r#"{"jsonrpc":"2.0","id":1,"result":null}"#;
/// let msg: Message = serde_json::from_str(json).unwrap();
/// assert!(msg.is_response());
///
/// // Notification discrimination
/// let json = r#"{"jsonrpc":"2.0","method":"test"}"#;
/// let msg: Message = serde_json::from_str(json).unwrap();
/// assert!(msg.is_notification());
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Message {
    /// A request message (has `id` and `method`).
    Request(Request),
    /// A response message (has `id` with `result` or `error`).
    Response(Response),
    /// A notification message (has `method` but no `id`).
    Notification(Notification),
}

impl Message {
    /// Returns `true` if this is a Request message.
    pub fn is_request(&self) -> bool {
        matches!(self, Message::Request(_))
    }

    /// Returns `true` if this is a Response message.
    pub fn is_response(&self) -> bool {
        matches!(self, Message::Response(_))
    }

    /// Returns `true` if this is a Notification message.
    pub fn is_notification(&self) -> bool {
        matches!(self, Message::Notification(_))
    }
}
