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

use serde::{ser::SerializeStruct, Deserialize, Serialize};
use serde_json::Value;

use crate::error::ResponseError;
use crate::request_id::RequestId;

const JSONRPC_VERSION: &str = "2.0";

fn validate_jsonrpc_field<E>(jsonrpc: &str) -> Result<(), E>
where
    E: serde::de::Error,
{
    if jsonrpc == JSONRPC_VERSION {
        Ok(())
    } else {
        Err(E::custom(format!(
            "expected jsonrpc field to equal \"{JSONRPC_VERSION}\", got \"{jsonrpc}\""
        )))
    }
}

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
#[derive(Debug, Clone)]
pub struct Request {
    /// The request id used to correlate request and response.
    pub id: RequestId,
    /// The method to be invoked.
    pub method: String,
    /// The method's params.
    pub params: Option<Value>,
}

impl Request {
    /// Creates a new Request with the given id, method, and optional params.
    pub fn new(id: impl Into<RequestId>, method: impl Into<String>, params: Option<Value>) -> Self {
        Self {
            id: id.into(),
            method: method.into(),
            params,
        }
    }
}

impl Serialize for Request {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let field_count = if self.params.is_some() { 4 } else { 3 };
        let mut state = serializer.serialize_struct("Request", field_count)?;
        state.serialize_field("jsonrpc", JSONRPC_VERSION)?;
        state.serialize_field("id", &self.id)?;
        state.serialize_field("method", &self.method)?;
        if let Some(params) = &self.params {
            state.serialize_field("params", params)?;
        }
        state.end()
    }
}

impl<'de> Deserialize<'de> for Request {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RequestRepr {
            jsonrpc: String,
            id: RequestId,
            method: String,
            params: Option<Value>,
        }

        let repr = RequestRepr::deserialize(deserializer)?;
        validate_jsonrpc_field::<D::Error>(&repr.jsonrpc)?;

        Ok(Self {
            id: repr.id,
            method: repr.method,
            params: repr.params,
        })
    }
}

/// The payload of a JSON-RPC 2.0 response — either a success result or an error.
#[derive(Debug, Clone)]
pub enum ResponseBody {
    /// A successful response with a JSON result value.
    Success(Value),
    /// A failed response with a structured JSON-RPC error.
    Error(ResponseError),
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
/// # Invariants
///
/// A `Response` always has exactly one of `result` or `error` — never both,
/// never neither. This is enforced by the constructors and the `ResponseBody` enum.
/// Direct field construction is not possible; use [`Response::ok`], [`Response::err`],
/// or [`Response::parse_error`].
///
/// # Examples
///
/// ```
/// use lsp_server_tokio::{Response, ResponseBody, ResponseError, ErrorCode};
/// use serde_json::json;
///
/// // Success response
/// let ok = Response::ok(1, json!({"items": []}));
/// assert!(ok.is_ok());
/// assert!(ok.result().is_some());
/// assert!(ok.error().is_none());
/// assert!(matches!(ok.body(), ResponseBody::Success(_)));
///
/// // Error response
/// let err = Response::err(1, ResponseError::new(ErrorCode::MethodNotFound, "Method not found"));
/// assert!(err.is_err());
/// assert!(err.result().is_none());
/// assert!(err.error().is_some());
/// assert!(matches!(err.body(), ResponseBody::Error(_)));
/// ```
///
/// ```compile_fail
/// use lsp_server_tokio::Response;
/// use serde_json::json;
///
/// // Cannot construct directly — fields are private
/// let _ = Response {
///     id: Some(1.into()),
///     body: lsp_server_tokio::ResponseBody::Success(json!("ok")),
/// };
/// ```
#[derive(Debug, Clone)]
pub struct Response {
    /// The request id. None for parse error responses.
    pub id: Option<RequestId>,
    /// The response payload.
    body: ResponseBody,
}

impl Serialize for Response {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // Count fields: jsonrpc + body field + optional id
        let field_count = 2 + usize::from(self.id.is_some());
        let mut state = serializer.serialize_struct("Response", field_count)?;
        state.serialize_field("jsonrpc", JSONRPC_VERSION)?;
        if let Some(id) = &self.id {
            state.serialize_field("id", id)?;
        }
        match &self.body {
            ResponseBody::Success(result) => state.serialize_field("result", result)?,
            ResponseBody::Error(error) => state.serialize_field("error", error)?,
        }
        state.end()
    }
}

// Custom deserializer for Response that ensures either result or error is present
// and correctly distinguishes between "result": null (present) vs result field absent
impl<'de> Deserialize<'de> for Response {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        // Deserialize as a generic Value first to check field presence
        let v = Value::deserialize(deserializer)?;
        let obj = v
            .as_object()
            .ok_or_else(|| D::Error::custom("expected object"))?;

        // Check if result or error field is present (not just non-null)
        let has_result = obj.contains_key("result");
        let has_error = obj.contains_key("error");

        // A valid Response MUST have either result or error field present
        if !has_result && !has_error {
            return Err(D::Error::custom(
                "Response must have either 'result' or 'error' field",
            ));
        }

        // Validate jsonrpc
        let jsonrpc = obj
            .get("jsonrpc")
            .and_then(|v| v.as_str())
            .ok_or_else(|| D::Error::custom("missing jsonrpc field"))?;
        validate_jsonrpc_field::<D::Error>(jsonrpc)?;

        // Extract id (can be null/absent for parse errors)
        let id = if let Some(id_val) = obj.get("id") {
            if id_val.is_null() {
                None
            } else {
                Some(
                    serde_json::from_value::<RequestId>(id_val.clone())
                        .map_err(D::Error::custom)?,
                )
            }
        } else {
            None
        };

        // Determine the body — error takes precedence if both are present.
        // Per JSON-RPC 2.0, having both result and error is invalid, but some
        // implementations produce it. We handle it gracefully by preferring error,
        // matching the spec's intent that error indicates failure.
        let body = if has_error {
            let error_val = obj.get("error").cloned().unwrap_or(Value::Null);
            if error_val.is_null() && has_result {
                // error field is null but result is present — treat as success
                let result = obj.get("result").cloned().unwrap_or(Value::Null);
                ResponseBody::Success(result)
            } else if error_val.is_null() {
                return Err(D::Error::custom(
                    "Response has error field but it is null and no result field",
                ));
            } else {
                let error =
                    serde_json::from_value::<ResponseError>(error_val).map_err(D::Error::custom)?;
                ResponseBody::Error(error)
            }
        } else {
            // has_result must be true (checked above)
            let result = obj.get("result").cloned().unwrap_or(Value::Null);
            ResponseBody::Success(result)
        };

        Ok(Response { id, body })
    }
}

impl Response {
    /// Creates a successful response with the given id and result.
    pub fn ok(id: impl Into<RequestId>, result: Value) -> Self {
        Self {
            id: Some(id.into()),
            body: ResponseBody::Success(result),
        }
    }

    /// Creates an error response with the given id and error.
    pub fn err(id: impl Into<RequestId>, error: ResponseError) -> Self {
        Self {
            id: Some(id.into()),
            body: ResponseBody::Error(error),
        }
    }

    /// Creates a parse error response where the request id could not be determined.
    ///
    /// Per JSON-RPC 2.0 spec, the id MUST be null when the request id cannot be parsed.
    #[must_use]
    pub fn parse_error(error: ResponseError) -> Self {
        Self {
            id: None,
            body: ResponseBody::Error(error),
        }
    }

    /// Returns the response body.
    #[must_use]
    pub fn body(&self) -> &ResponseBody {
        &self.body
    }

    /// Consumes the response and returns the body.
    #[must_use]
    pub fn into_body(self) -> ResponseBody {
        self.body
    }

    /// Returns `true` if this is a success response.
    #[must_use]
    pub fn is_ok(&self) -> bool {
        matches!(self.body, ResponseBody::Success(_))
    }

    /// Returns `true` if this is an error response.
    #[must_use]
    pub fn is_err(&self) -> bool {
        matches!(self.body, ResponseBody::Error(_))
    }

    /// Returns the result value, if this is a success response.
    #[must_use]
    pub fn result(&self) -> Option<&Value> {
        match &self.body {
            ResponseBody::Success(v) => Some(v),
            ResponseBody::Error(_) => None,
        }
    }

    /// Returns the error, if this is an error response.
    #[must_use]
    pub fn error(&self) -> Option<&ResponseError> {
        match &self.body {
            ResponseBody::Success(_) => None,
            ResponseBody::Error(e) => Some(e),
        }
    }

    /// Consumes the response and returns the result value, if success.
    #[must_use]
    pub fn into_result(self) -> Option<Value> {
        match self.body {
            ResponseBody::Success(v) => Some(v),
            ResponseBody::Error(_) => None,
        }
    }

    /// Consumes the response and returns the error, if error.
    #[must_use]
    pub fn into_error(self) -> Option<ResponseError> {
        match self.body {
            ResponseBody::Success(_) => None,
            ResponseBody::Error(e) => Some(e),
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
#[derive(Debug, Clone)]
pub struct Notification {
    /// The method to be invoked.
    pub method: String,
    /// The notification's params.
    pub params: Option<Value>,
}

impl Notification {
    /// Creates a new Notification with the given method and optional params.
    pub fn new(method: impl Into<String>, params: Option<Value>) -> Self {
        Self {
            method: method.into(),
            params,
        }
    }
}

impl Serialize for Notification {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let field_count = if self.params.is_some() { 3 } else { 2 };
        let mut state = serializer.serialize_struct("Notification", field_count)?;
        state.serialize_field("jsonrpc", JSONRPC_VERSION)?;
        state.serialize_field("method", &self.method)?;
        if let Some(params) = &self.params {
            state.serialize_field("params", params)?;
        }
        state.end()
    }
}

impl<'de> Deserialize<'de> for Notification {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct NotificationRepr {
            jsonrpc: String,
            method: String,
            params: Option<Value>,
        }

        let repr = NotificationRepr::deserialize(deserializer)?;
        validate_jsonrpc_field::<D::Error>(&repr.jsonrpc)?;

        Ok(Self {
            method: repr.method,
            params: repr.params,
        })
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
    #[must_use]
    pub fn is_request(&self) -> bool {
        matches!(self, Message::Request(_))
    }

    /// Returns `true` if this is a Response message.
    #[must_use]
    pub fn is_response(&self) -> bool {
        matches!(self, Message::Response(_))
    }

    /// Returns `true` if this is a Notification message.
    #[must_use]
    pub fn is_notification(&self) -> bool {
        matches!(self, Message::Notification(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;
    use serde_json::json;

    // ============== Request Tests ==============

    #[test]
    fn request_roundtrip() {
        let req = Request::new(1, "test/method", None);
        let json = serde_json::to_string(&req).unwrap();
        let parsed: Request = serde_json::from_str(&json).unwrap();

        assert!(json.contains("\"jsonrpc\":\"2.0\""));
        assert_eq!(parsed.id, RequestId::Integer(1));
        assert_eq!(parsed.method, "test/method");
        assert!(parsed.params.is_none());
    }

    #[test]
    fn request_with_params_roundtrip() {
        let params = json!({"textDocument": {"uri": "file:///test.rs"}});
        let req = Request::new(42, "textDocument/completion", Some(params.clone()));
        let json = serde_json::to_string(&req).unwrap();
        let parsed: Request = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.id, RequestId::Integer(42));
        assert_eq!(parsed.method, "textDocument/completion");
        assert_eq!(parsed.params.unwrap(), params);
    }

    #[test]
    fn request_without_params_omits_field() {
        let req = Request::new(1, "test", None);
        let json = serde_json::to_string(&req).unwrap();
        // params field should be omitted when None
        assert!(!json.contains("params"));
    }

    #[test]
    fn request_with_string_id() {
        let req = Request::new("abc-123", "test", None);
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"abc-123\""));

        let parsed: Request = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.id, RequestId::String("abc-123".to_string()));
    }

    #[test]
    fn request_deserialization_rejects_missing_jsonrpc() {
        let json = r#"{"id":1,"method":"test"}"#;
        let error = serde_json::from_str::<Request>(json).unwrap_err();
        assert!(error.to_string().contains("missing field `jsonrpc`"));
    }

    #[test]
    fn request_deserialization_rejects_wrong_jsonrpc_version() {
        let json = r#"{"jsonrpc":"1.0","id":1,"method":"test"}"#;
        let error = serde_json::from_str::<Request>(json).unwrap_err();
        assert!(error
            .to_string()
            .contains("expected jsonrpc field to equal \"2.0\""));
    }

    // ============== Response Tests ==============

    #[test]
    fn response_ok_roundtrip() {
        let result = json!({"items": [], "isIncomplete": false});
        let resp = Response::ok(1, result.clone());
        let json = serde_json::to_string(&resp).unwrap();
        let parsed: Response = serde_json::from_str(&json).unwrap();

        assert!(json.contains("\"jsonrpc\":\"2.0\""));
        assert_eq!(parsed.id, Some(RequestId::Integer(1)));
        assert!(parsed.error().is_none());
        assert_eq!(parsed.into_result().unwrap(), result);
    }

    #[test]
    fn response_serialization_hardcodes_jsonrpc_version() {
        let json = serde_json::to_string(&Response::ok(1, json!(null))).unwrap();
        assert!(json.contains("\"jsonrpc\":\"2.0\""));
    }

    #[test]
    fn response_deserialization_rejects_missing_jsonrpc() {
        let json = r#"{"id":1,"result":null}"#;
        let error = serde_json::from_str::<Response>(json).unwrap_err();
        assert!(error.to_string().contains("missing jsonrpc field"));
    }

    #[test]
    fn response_err_roundtrip() {
        let error = ResponseError::new(ErrorCode::MethodNotFound, "Method not found");
        let resp = Response::err(1, error);
        let json = serde_json::to_string(&resp).unwrap();
        let parsed: Response = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.id, Some(RequestId::Integer(1)));
        assert!(parsed.result().is_none());
        let err = parsed.into_error().unwrap();
        assert_eq!(err.code, -32601);
        assert_eq!(err.message, "Method not found");
    }

    #[test]
    fn response_parse_error_has_null_id() {
        let error = ResponseError::new(ErrorCode::ParseError, "Parse error");
        let resp = Response::parse_error(error);
        let json = serde_json::to_string(&resp).unwrap();

        // id should be omitted (null semantically)
        assert!(!json.contains("\"id\""));

        let parsed: Response = serde_json::from_str(&json).unwrap();
        assert!(parsed.id.is_none());
        assert!(parsed.error().is_some());
    }

    #[test]
    fn response_with_string_id() {
        let resp = Response::ok("uuid-123", json!(null));
        let json = serde_json::to_string(&resp).unwrap();
        let parsed: Response = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.id, Some(RequestId::String("uuid-123".to_string())));
    }

    #[test]
    fn response_ok_omits_error_field() {
        let resp = Response::ok(1, json!(null));
        let json = serde_json::to_string(&resp).unwrap();
        assert!(!json.contains("error"));
    }

    #[test]
    fn response_err_omits_result_field() {
        let error = ResponseError::new(ErrorCode::InternalError, "Error");
        let resp = Response::err(1, error);
        let json = serde_json::to_string(&resp).unwrap();
        assert!(!json.contains("result"));
    }

    // ============== Notification Tests ==============

    #[test]
    fn notification_roundtrip() {
        let notif = Notification::new("textDocument/didOpen", None);
        let json = serde_json::to_string(&notif).unwrap();
        let parsed: Notification = serde_json::from_str(&json).unwrap();

        assert!(json.contains("\"jsonrpc\":\"2.0\""));
        assert_eq!(parsed.method, "textDocument/didOpen");
        assert!(parsed.params.is_none());
    }

    #[test]
    fn notification_with_params_roundtrip() {
        let params = json!({"textDocument": {"uri": "file:///test.rs"}});
        let notif = Notification::new("textDocument/didOpen", Some(params.clone()));
        let json = serde_json::to_string(&notif).unwrap();
        let parsed: Notification = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.method, "textDocument/didOpen");
        assert_eq!(parsed.params.unwrap(), params);
    }

    #[test]
    fn notification_deserialization_rejects_wrong_jsonrpc_version() {
        let json = r#"{"jsonrpc":"1.0","method":"test"}"#;
        let error = serde_json::from_str::<Notification>(json).unwrap_err();
        assert!(error
            .to_string()
            .contains("expected jsonrpc field to equal \"2.0\""));
    }

    #[test]
    fn notification_without_params_omits_field() {
        let notif = Notification::new("test", None);
        let json = serde_json::to_string(&notif).unwrap();
        // params field should be omitted when None
        assert!(!json.contains("params"));
    }

    // ============== Message Discrimination Tests ==============

    #[test]
    fn message_discriminates_request() {
        let json = r#"{"jsonrpc":"2.0","id":1,"method":"test"}"#;
        let msg: Message = serde_json::from_str(json).unwrap();
        assert!(msg.is_request());
        assert!(!msg.is_response());
        assert!(!msg.is_notification());

        if let Message::Request(req) = msg {
            assert_eq!(req.id, RequestId::Integer(1));
            assert_eq!(req.method, "test");
        } else {
            panic!("Expected Request variant");
        }
    }

    #[test]
    fn message_discriminates_request_with_params() {
        let json = r#"{"jsonrpc":"2.0","id":1,"method":"test","params":{"key":"value"}}"#;
        let msg: Message = serde_json::from_str(json).unwrap();
        assert!(msg.is_request());

        if let Message::Request(req) = msg {
            assert!(req.params.is_some());
        } else {
            panic!("Expected Request variant");
        }
    }

    #[test]
    fn message_discriminates_response_ok() {
        let json = r#"{"jsonrpc":"2.0","id":1,"result":null}"#;
        let msg: Message = serde_json::from_str(json).unwrap();
        assert!(msg.is_response());
        assert!(!msg.is_request());
        assert!(!msg.is_notification());

        if let Message::Response(resp) = msg {
            assert_eq!(resp.id, Some(RequestId::Integer(1)));
            assert!(resp.result().is_some());
            assert!(resp.error().is_none());
        } else {
            panic!("Expected Response variant");
        }
    }

    #[test]
    fn message_discriminates_response_err() {
        let json =
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32600,"message":"Invalid Request"}}"#;
        let msg: Message = serde_json::from_str(json).unwrap();
        assert!(msg.is_response());

        if let Message::Response(resp) = msg {
            assert_eq!(resp.id, Some(RequestId::Integer(1)));
            assert!(resp.result().is_none());
            let err = resp.into_error().unwrap();
            assert_eq!(err.code, -32600);
        } else {
            panic!("Expected Response variant");
        }
    }

    #[test]
    fn message_discriminates_notification() {
        let json = r#"{"jsonrpc":"2.0","method":"test"}"#;
        let msg: Message = serde_json::from_str(json).unwrap();
        assert!(msg.is_notification());
        assert!(!msg.is_request());
        assert!(!msg.is_response());

        if let Message::Notification(notif) = msg {
            assert_eq!(notif.method, "test");
        } else {
            panic!("Expected Notification variant");
        }
    }

    #[test]
    fn message_discriminates_notification_with_params() {
        let json = r#"{"jsonrpc":"2.0","method":"test","params":{"key":"value"}}"#;
        let msg: Message = serde_json::from_str(json).unwrap();
        assert!(msg.is_notification());

        if let Message::Notification(notif) = msg {
            assert!(notif.params.is_some());
        } else {
            panic!("Expected Notification variant");
        }
    }

    // ============== Edge Cases ==============

    #[test]
    fn request_with_string_id_discrimination() {
        let json = r#"{"jsonrpc":"2.0","id":"abc-123","method":"test"}"#;
        let msg: Message = serde_json::from_str(json).unwrap();
        assert!(msg.is_request());

        if let Message::Request(req) = msg {
            assert_eq!(req.id, RequestId::String("abc-123".to_string()));
        } else {
            panic!("Expected Request");
        }
    }

    #[test]
    fn response_with_null_id_discrimination() {
        // This tests the parse error case where id cannot be determined
        // The JSON has no id field at all (not null)
        let json = r#"{"jsonrpc":"2.0","error":{"code":-32700,"message":"Parse error"}}"#;
        let msg: Message = serde_json::from_str(json).unwrap();
        assert!(msg.is_response());

        if let Message::Response(resp) = msg {
            assert!(resp.id.is_none());
            assert!(resp.error().is_some());
        } else {
            panic!("Expected Response");
        }
    }

    #[test]
    fn response_with_explicit_null_id() {
        // Some clients might send explicit null for parse errors
        let json = r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"Parse error"}}"#;
        let parsed: Response = serde_json::from_str(json).unwrap();
        // null should deserialize to None
        assert!(parsed.id.is_none());
    }

    #[test]
    fn message_roundtrip_request() {
        let req = Request::new(
            42,
            "textDocument/hover",
            Some(json!({"position": {"line": 0}})),
        );
        let msg = Message::Request(req);
        let json = serde_json::to_string(&msg).unwrap();
        let parsed: Message = serde_json::from_str(&json).unwrap();
        assert!(parsed.is_request());
    }

    #[test]
    fn message_roundtrip_response() {
        let resp = Response::ok(42, json!({"contents": "documentation"}));
        let msg = Message::Response(resp);
        let json = serde_json::to_string(&msg).unwrap();
        let parsed: Message = serde_json::from_str(&json).unwrap();
        assert!(parsed.is_response());
    }

    #[test]
    fn message_roundtrip_notification() {
        let notif = Notification::new("$/cancelRequest", Some(json!({"id": 42})));
        let msg = Message::Notification(notif);
        let json = serde_json::to_string(&msg).unwrap();
        let parsed: Message = serde_json::from_str(&json).unwrap();
        assert!(parsed.is_notification());
    }

    #[test]
    fn response_with_complex_result() {
        let result = json!({
            "capabilities": {
                "textDocumentSync": 1,
                "completionProvider": {"triggerCharacters": [".", ":"]},
                "hoverProvider": true
            }
        });
        let resp = Response::ok(0, result.clone());
        let json = serde_json::to_string(&resp).unwrap();
        let parsed: Response = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.into_result().unwrap(), result);
    }

    #[test]
    fn response_error_with_data() {
        let error = ResponseError::new(ErrorCode::InvalidParams, "Missing field")
            .with_data(json!({"missing": ["uri", "position"]}));
        let resp = Response::err(1, error);
        let json = serde_json::to_string(&resp).unwrap();
        let parsed: Response = serde_json::from_str(&json).unwrap();

        let err = parsed.into_error().unwrap();
        assert!(err.data.is_some());
        assert_eq!(err.data.unwrap()["missing"][0], "uri");
    }

    // ============== ResponseBody Deserialization Edge Cases ==============

    #[test]
    fn response_both_result_and_error_prefers_error() {
        // JSON-RPC 2.0 forbids both, but some implementations produce it.
        // We prefer error (the failure signal).
        let json =
            r#"{"jsonrpc":"2.0","id":1,"result":"ok","error":{"code":-32600,"message":"bad"}}"#;
        let resp: Response = serde_json::from_str(json).unwrap();
        assert!(resp.error().is_some());
        assert_eq!(resp.error().unwrap().code, -32600);
    }

    #[test]
    fn response_error_null_with_result_treats_as_success() {
        // "error": null but "result" present → success
        let json = r#"{"jsonrpc":"2.0","id":1,"result":"ok","error":null}"#;
        let resp: Response = serde_json::from_str(json).unwrap();
        assert!(resp.result().is_some());
        assert_eq!(resp.result().unwrap(), "ok");
    }

    #[test]
    fn response_neither_result_nor_error_rejected() {
        let json = r#"{"jsonrpc":"2.0","id":1}"#;
        let err = serde_json::from_str::<Response>(json).unwrap_err();
        assert!(err.to_string().contains("result"));
    }

    #[test]
    fn response_error_null_without_result_rejected() {
        // "error": null and no "result" → invalid
        let json = r#"{"jsonrpc":"2.0","id":1,"error":null}"#;
        let err = serde_json::from_str::<Response>(json).unwrap_err();
        assert!(err.to_string().contains("null"));
    }

    #[test]
    fn response_result_null_is_valid_success() {
        // "result": null is a valid success response (e.g., shutdown response)
        let json = r#"{"jsonrpc":"2.0","id":1,"result":null}"#;
        let resp: Response = serde_json::from_str(json).unwrap();
        assert!(resp.result().is_some());
        assert!(resp.result().unwrap().is_null());
    }
}
