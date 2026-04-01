//! Message routing infrastructure for LSP communication.
//!
//! This module provides types and functions for classifying incoming messages
//! and routing responses to pending outgoing requests.
//!
//! # Overview
//!
//! The routing infrastructure handles two main concerns:
//!
//! 1. **Message Classification**: The [`IncomingMessage`] enum categorizes received
//!    messages into requests that need responses, notifications that are fire-and-forget,
//!    or responses that were either routed to pending requests or are unknown.
//!
//! 2. **Response Routing**: When the server sends requests to the client, responses
//!    are automatically routed to the waiting receivers via oneshot channels.
//!
//! # Example
//!
//! ```
//! use lsp_server_tokio::{IncomingMessage, Message, Request, Notification, Response};
//!
//! // Classify incoming messages
//! let req = Message::Request(Request::new(1, "textDocument/hover", None));
//! // After calling route(), you get IncomingMessage::Request(req)
//!
//! let notif = Message::Notification(Notification::new("initialized", None));
//! // After calling route(), you get IncomingMessage::Notification(notif)
//! ```

use tokio_util::sync::CancellationToken;

use crate::error::{ErrorCode, ResponseError};
use crate::message::{Notification, Request, Response};

/// Represents a classified incoming message after routing.
///
/// When a [`Message`](crate::Message) is received from the transport, it is classified
/// into one of these variants by the `Connection::route()` method.
///
/// # Variants
///
/// - [`Request`](IncomingMessage::Request): A request requiring a response. Includes
///   a [`CancellationToken`] that is triggered on `$/cancelRequest` or shutdown.
/// - [`Notification`](IncomingMessage::Notification): A fire-and-forget notification.
///   No response is expected.
/// - [`ResponseRouted`](IncomingMessage::ResponseRouted): A response that was successfully
///   delivered to a pending outgoing request's receiver.
/// - [`ResponseUnknown`](IncomingMessage::ResponseUnknown): A response for which no pending
///   outgoing request was found. This typically indicates a protocol error or a timed-out request.
///
/// # Example
///
/// ```
/// use lsp_server_tokio::{IncomingMessage, Message, Request, Response};
///
/// fn handle_message(incoming: IncomingMessage) {
///     match incoming {
///         IncomingMessage::Request(req, token) => {
///             println!("Handle request: {}", req.method);
///             // Use token for cooperative cancellation
///             // Send response back
///         }
///         IncomingMessage::Notification(notif) => {
///             println!("Handle notification: {}", notif.method);
///         }
///         IncomingMessage::CancelHandled => {
///             // `$/cancelRequest` was applied automatically
///         }
///         IncomingMessage::ResponseRouted => {
///             // Response was delivered to awaiting task, nothing to do
///         }
///         IncomingMessage::ResponseUnknown(resp) => {
///             println!("Unknown response for id: {:?}", resp.id);
///             // Log or handle the unexpected response
///         }
///     }
/// }
/// ```
#[derive(Debug)]
pub enum IncomingMessage {
    /// A request that needs a response.
    ///
    /// The server must send a response with the same request ID.
    /// The included [`CancellationToken`] is triggered when:
    /// - A `$/cancelRequest` notification is received for this request
    /// - The connection is shutting down
    ///
    /// Use the token for cooperative cancellation of long-running operations.
    Request(Request, CancellationToken),

    /// A notification (fire-and-forget).
    ///
    /// No response is expected or allowed.
    Notification(Notification),

    /// A `$/cancelRequest` notification that was automatically processed.
    ///
    /// The cancellation token for the referenced request (if pending) has already
    /// been triggered. No further action is needed.
    CancelHandled,

    /// A response that was successfully delivered to a pending outgoing request.
    ///
    /// The response was sent to the oneshot channel registered when the
    /// outgoing request was created. The awaiting task will receive it.
    ResponseRouted,

    /// A response for an unknown request ID.
    ///
    /// This occurs when:
    /// - The response ID doesn't match any pending outgoing request
    /// - The request timed out and was removed before the response arrived
    /// - The client sent an unsolicited response
    /// - The response has a null ID (parse error response)
    ResponseUnknown(Response),
}

impl IncomingMessage {
    /// Returns `true` if this message is a routed request.
    #[must_use]
    pub fn is_request(&self) -> bool {
        matches!(self, Self::Request(_, _))
    }

    /// Returns `true` if this message is a notification.
    #[must_use]
    pub fn is_notification(&self) -> bool {
        matches!(self, Self::Notification(_))
    }

    /// Returns `true` if this message is an automatically handled cancellation notification.
    #[must_use]
    pub fn is_cancel_handled(&self) -> bool {
        matches!(self, Self::CancelHandled)
    }

    /// Returns `true` if this message is a response routed to a pending request.
    #[must_use]
    pub fn is_response_routed(&self) -> bool {
        matches!(self, Self::ResponseRouted)
    }

    /// Returns `true` if this message is a response for an unknown request.
    #[must_use]
    pub fn is_response_unknown(&self) -> bool {
        matches!(self, Self::ResponseUnknown(_))
    }
}

/// Creates a `MethodNotFound` error response for an unhandled request.
///
/// This helper creates a properly formatted JSON-RPC 2.0 error response
/// with error code `-32601` (`MethodNotFound`) and a message indicating
/// which method was not found.
///
/// # Arguments
///
/// * `request` - The request for which no handler was found
///
/// # Returns
///
/// A [`Response`] with an error containing the `MethodNotFound` code.
///
/// # Example
///
/// ```
/// use lsp_server_tokio::{method_not_found_response, Request, ErrorCode};
///
/// let request = Request::new(42, "unknown/method", None);
/// let response = method_not_found_response(&request);
///
/// assert!(response.error().is_some());
/// let error = response.into_error().unwrap();
/// assert_eq!(error.code, ErrorCode::MethodNotFound as i32);
/// assert!(error.message.contains("unknown/method"));
/// ```
#[must_use]
pub fn method_not_found_response(request: &Request) -> Response {
    let error = ResponseError::new(
        ErrorCode::MethodNotFound,
        format!("Method not found: {}", request.method),
    );
    Response::err(request.id.clone(), error)
}

/// Creates a `RequestCancelled` error response for a cancelled request.
///
/// This helper creates a properly formatted JSON-RPC 2.0 error response
/// with error code `-32800` (`RequestCancelled`) and a standard message.
/// Use this when a request has been cancelled via $/cancelRequest.
///
/// # Arguments
///
/// * `id` - The request ID of the cancelled request
///
/// # Returns
///
/// A [`Response`] with an error containing the `RequestCancelled` code.
///
/// # Example
///
/// ```
/// use lsp_server_tokio::{cancelled_response, RequestId, ErrorCode};
///
/// let id: RequestId = 42.into();
/// let response = cancelled_response(id);
///
/// assert!(response.error().is_some());
/// let error = response.into_error().unwrap();
/// assert_eq!(error.code, ErrorCode::RequestCancelled as i32);
/// ```
pub fn cancelled_response(id: impl Into<crate::RequestId>) -> Response {
    let error = ResponseError::new(ErrorCode::RequestCancelled, "Request was cancelled");
    Response::err(id, error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ErrorCode;
    use serde_json::json;

    #[test]
    fn method_not_found_response_creates_correct_error() {
        let request = Request::new(42, "textDocument/unknown", Some(json!({"key": "value"})));
        let response = method_not_found_response(&request);

        // Verify it's an error response
        assert!(response.error().is_some());
        assert!(response.result().is_none());

        // Verify correct ID
        assert_eq!(response.id, Some(42.into()));

        // Verify error details
        let error = response.into_error().unwrap();
        assert_eq!(error.code, ErrorCode::MethodNotFound as i32);
        assert_eq!(error.code, -32601);
    }

    #[test]
    fn method_not_found_response_includes_method_name() {
        let request = Request::new(1, "custom/myMethod", None);
        let response = method_not_found_response(&request);

        let error = response.into_error().unwrap();
        assert!(
            error.message.contains("custom/myMethod"),
            "Error message should contain the method name"
        );
        assert_eq!(error.message, "Method not found: custom/myMethod");
    }

    #[test]
    fn method_not_found_response_with_string_id() {
        let request = Request::new("request-abc-123", "test/method", None);
        let response = method_not_found_response(&request);

        assert_eq!(
            response.id,
            Some(crate::RequestId::String("request-abc-123".to_string()))
        );
    }

    #[test]
    fn incoming_message_variants_constructible() {
        use tokio_util::sync::CancellationToken;

        // Test that all variants can be constructed
        let request = Request::new(1, "test", None);
        let notification = Notification::new("test", None);
        let response = Response::ok(1, json!(null));
        let token = CancellationToken::new();

        let _req = IncomingMessage::Request(request, token);
        let _notif = IncomingMessage::Notification(notification);
        let _routed = IncomingMessage::ResponseRouted;
        let _unknown = IncomingMessage::ResponseUnknown(response);
    }

    #[test]
    fn incoming_message_is_debug() {
        use tokio_util::sync::CancellationToken;

        let request = Request::new(1, "test", None);
        let token = CancellationToken::new();
        let incoming = IncomingMessage::Request(request, token);
        let debug_str = format!("{incoming:?}");
        assert!(debug_str.contains("Request"));
    }

    #[test]
    fn incoming_message_accessors_match_variants() {
        let request = IncomingMessage::Request(Request::new(1, "test", None), CancellationToken::new());
        assert!(request.is_request());
        assert!(!request.is_notification());
        assert!(!request.is_response_routed());
        assert!(!request.is_response_unknown());
        assert!(!request.is_cancel_handled());

        let notification = IncomingMessage::Notification(Notification::new("test", None));
        assert!(notification.is_notification());

        let routed = IncomingMessage::ResponseRouted;
        assert!(routed.is_response_routed());

        let unknown = IncomingMessage::ResponseUnknown(Response::ok(1, json!(null)));
        assert!(unknown.is_response_unknown());

        let cancelled = IncomingMessage::CancelHandled;
        assert!(cancelled.is_cancel_handled());
    }

    // ============== cancelled_response Tests ==============

    use super::cancelled_response;

    #[test]
    fn cancelled_response_creates_correct_error() {
        let response = cancelled_response(42);

        assert!(response.error().is_some());
        assert!(response.result().is_none());
        assert_eq!(response.id, Some(42.into()));

        let error = response.into_error().unwrap();
        assert_eq!(error.code, ErrorCode::RequestCancelled as i32);
        assert_eq!(error.code, -32800);
        assert!(error.message.contains("cancelled"));
    }

    #[test]
    fn cancelled_response_with_string_id() {
        let response = cancelled_response("req-xyz");

        assert_eq!(
            response.id,
            Some(crate::RequestId::String("req-xyz".to_string()))
        );
        assert!(response.error().is_some());
    }
}
