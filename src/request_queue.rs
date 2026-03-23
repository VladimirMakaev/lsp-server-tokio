//! Request queue types for tracking pending LSP requests.
//!
//! This module provides types for tracking request-response correlation in both
//! directions of LSP communication:
//!
//! - [`IncomingRequests`] - Tracks requests received from clients (server needs to send responses)
//! - [`OutgoingRequests`] - Tracks requests sent to clients (server is waiting for responses)
//! - [`RequestQueue`] - Combines both for complete request lifecycle management
//!
//! # Usage Pattern
//!
//! ```
//! use lsp_server_tokio::{RequestQueue, RequestId};
//! use tokio_util::sync::CancellationToken;
//!
//! // Create a queue with custom metadata types
//! let mut queue: RequestQueue<String, String> = RequestQueue::new();
//!
//! // Track an incoming request (from client) with a cancellation token
//! let request_id: RequestId = 1.into();
//! let token = CancellationToken::new();
//! queue.incoming.register(request_id.clone(), "handler_context".to_string(), token);
//! assert!(queue.incoming.is_pending(&request_id));
//!
//! // When ready to respond, complete the request
//! let metadata = queue.incoming.complete(&request_id);
//! assert_eq!(metadata, Some("handler_context".to_string()));
//! ```
//!
//! # Server-Initiated Requests
//!
//! ```
//! use lsp_server_tokio::{RequestQueue, RequestId};
//!
//! # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
//! let mut queue: RequestQueue<(), String> = RequestQueue::new();
//!
//! // Register an outgoing request (to client) and get a receiver
//! let request_id: RequestId = 42.into();
//! let rx = queue.outgoing.register(request_id.clone());
//!
//! // When response arrives, complete the request
//! let sent = queue.outgoing.complete(&request_id, "response data".to_string());
//! assert!(sent);
//!
//! // The receiver gets the response
//! let response = rx.await.unwrap();
//! assert_eq!(response, "response data");
//! # });
//! ```

use std::collections::HashMap;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use crate::request_id::RequestId;

/// The method name for cancel request notifications per LSP specification.
pub const CANCEL_REQUEST_METHOD: &str = "$/cancelRequest";

/// Parses the request ID from $/cancelRequest notification params.
///
/// According to the LSP specification, $/cancelRequest has params:
/// ```json
/// { "id": number | string }
/// ```
///
/// Returns `None` if params are missing, malformed, or the ID is not
/// a valid integer or string.
///
/// # Example
///
/// ```
/// use lsp_server_tokio::parse_cancel_params;
/// use serde_json::json;
///
/// // Integer ID
/// let params = Some(json!({"id": 42}));
/// let id = parse_cancel_params(&params);
/// assert_eq!(id, Some(42.into()));
///
/// // String ID
/// let params = Some(json!({"id": "request-abc"}));
/// let id = parse_cancel_params(&params);
/// assert_eq!(id, Some("request-abc".into()));
///
/// // Missing params
/// let id = parse_cancel_params(&None);
/// assert!(id.is_none());
/// ```
#[must_use] 
pub fn parse_cancel_params(params: &Option<serde_json::Value>) -> Option<RequestId> {
    let params = params.as_ref()?;
    let id_value = params.get("id")?;

    match id_value {
        serde_json::Value::Number(n) => n
            .as_i64()
            .and_then(|i| i32::try_from(i).ok())
            .map(RequestId::Integer),
        serde_json::Value::String(s) => Some(RequestId::String(s.clone())),
        _ => None,
    }
}

/// Tracks requests received from clients (incoming to the server).
///
/// When the server receives a request, it registers the request ID along with
/// any metadata needed to process the response. When the server is ready to
/// send a response, it completes the request to retrieve the metadata.
///
/// Each request is also associated with a [`CancellationToken`] that can be
/// used to signal cancellation (e.g., when receiving `$/cancelRequest`).
///
/// The generic parameter `I` represents user-defined metadata associated with
/// each incoming request (e.g., handler context, timing info, request origin).
///
/// # Example
///
/// ```
/// use lsp_server_tokio::{IncomingRequests, RequestId};
/// use tokio_util::sync::CancellationToken;
///
/// let mut incoming: IncomingRequests<String> = IncomingRequests::new();
///
/// // Register a request with metadata and cancellation token
/// let token1 = CancellationToken::new();
/// let token2 = CancellationToken::new();
/// incoming.register(1.into(), "textDocument/hover".to_string(), token1);
/// incoming.register(2.into(), "textDocument/completion".to_string(), token2);
///
/// assert_eq!(incoming.pending_count(), 2);
/// assert!(incoming.is_pending(&1.into()));
///
/// // Cancel a request
/// incoming.cancel(&2.into());
///
/// // Complete request and get metadata back
/// let method = incoming.complete(&1.into());
/// assert_eq!(method, Some("textDocument/hover".to_string()));
/// assert_eq!(incoming.pending_count(), 1);
/// ```
#[derive(Debug)]
pub struct IncomingRequests<I> {
    pending: HashMap<RequestId, (I, CancellationToken)>,
}

impl<I> IncomingRequests<I> {
    /// Creates a new empty incoming request tracker.
    #[must_use] 
    pub fn new() -> Self {
        Self {
            pending: HashMap::new(),
        }
    }

    /// Registers an incoming request with associated metadata and cancellation token.
    ///
    /// The metadata can be any user-defined type that you want to associate
    /// with this request until it's completed. The cancellation token can be
    /// used to signal request cancellation to async handlers.
    pub fn register(&mut self, id: RequestId, data: I, token: CancellationToken) {
        self.pending.insert(id, (data, token));
    }

    /// Completes an incoming request, removing it from tracking and returning the metadata.
    ///
    /// Returns `Some(metadata)` if the request was pending, `None` otherwise.
    /// The cancellation token is dropped when the request is completed.
    pub fn complete(&mut self, id: &RequestId) -> Option<I> {
        self.pending.remove(id).map(|(data, _)| data)
    }

    /// Returns `true` if the request is currently pending.
    #[must_use] 
    pub fn is_pending(&self, id: &RequestId) -> bool {
        self.pending.contains_key(id)
    }

    /// Cancels a pending request by triggering its cancellation token.
    ///
    /// Returns `true` if the request was found and cancelled, `false` if the
    /// request ID was not pending. Note that cancelling an already-cancelled
    /// token is a no-op.
    #[must_use] 
    pub fn cancel(&self, id: &RequestId) -> bool {
        if let Some((_, token)) = self.pending.get(id) {
            token.cancel();
            true
        } else {
            false
        }
    }

    /// Returns a clone of the cancellation token for a pending request.
    ///
    /// Returns `None` if the request is not pending. The returned token
    /// can be passed to async handlers for cooperative cancellation.
    #[must_use] 
    pub fn get_token(&self, id: &RequestId) -> Option<CancellationToken> {
        self.pending.get(id).map(|(_, token)| token.clone())
    }

    /// Returns the number of currently pending requests.
    #[must_use] 
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }
}

impl<I> Default for IncomingRequests<I> {
    fn default() -> Self {
        Self::new()
    }
}

/// Tracks requests sent to clients (outgoing from the server).
///
/// When the server sends a request to the client, it registers the request ID
/// and receives a oneshot receiver. When the client's response arrives, the
/// server completes the request, sending the response to the waiting receiver.
///
/// The generic parameter `O` represents the response type that will be delivered
/// when the request completes.
///
/// # Example
///
/// ```
/// use lsp_server_tokio::{OutgoingRequests, RequestId};
///
/// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
/// let mut outgoing: OutgoingRequests<String> = OutgoingRequests::new();
///
/// // Register an outgoing request
/// let rx = outgoing.register(1.into());
/// assert!(outgoing.is_pending(&1.into()));
///
/// // Simulate receiving a response
/// let completed = outgoing.complete(&1.into(), "result".to_string());
/// assert!(completed);
///
/// // Receiver gets the response
/// let result = rx.await.unwrap();
/// assert_eq!(result, "result");
/// # });
/// ```
#[derive(Debug)]
pub struct OutgoingRequests<O> {
    pending: HashMap<RequestId, oneshot::Sender<O>>,
}

impl<O> OutgoingRequests<O> {
    /// Creates a new empty outgoing request tracker.
    #[must_use] 
    pub fn new() -> Self {
        Self {
            pending: HashMap::new(),
        }
    }

    /// Registers an outgoing request and returns a receiver for the response.
    ///
    /// The returned receiver will receive the response value when [`complete`](Self::complete)
    /// is called with a matching ID. If the request is cancelled via [`cancel`](Self::cancel),
    /// the receiver will return a `RecvError`.
    pub fn register(&mut self, id: RequestId) -> oneshot::Receiver<O> {
        let (tx, rx) = oneshot::channel();
        self.pending.insert(id, tx);
        rx
    }

    /// Completes an outgoing request by sending the response to the waiting receiver.
    ///
    /// Returns `true` if the request was pending and the response was sent,
    /// `false` if the request was not found.
    ///
    /// Note: This returns `true` even if the receiver was dropped (the response is
    /// still considered "completed" from the queue's perspective).
    pub fn complete(&mut self, id: &RequestId, response: O) -> bool {
        if let Some(tx) = self.pending.remove(id) {
            // Ignore send error - receiver may have been dropped
            let _ = tx.send(response);
            true
        } else {
            false
        }
    }

    /// Cancels an outgoing request without sending a response.
    ///
    /// The sender is dropped, causing the receiver to return `RecvError`.
    ///
    /// Returns `true` if the request was pending, `false` otherwise.
    pub fn cancel(&mut self, id: &RequestId) -> bool {
        self.pending.remove(id).is_some()
    }

    /// Returns `true` if the request is currently pending.
    #[must_use] 
    pub fn is_pending(&self, id: &RequestId) -> bool {
        self.pending.contains_key(id)
    }

    /// Returns the number of currently pending requests.
    #[must_use] 
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }
}

impl<O> Default for OutgoingRequests<O> {
    fn default() -> Self {
        Self::new()
    }
}

/// Combined request queue tracking both incoming and outgoing requests.
///
/// This is the primary type for managing LSP request-response correlation.
/// It provides separate tracking for:
///
/// - `incoming`: Requests received from clients that need responses
/// - `outgoing`: Requests sent to clients that are awaiting responses
///
/// # Type Parameters
///
/// - `I`: Metadata type for incoming requests (e.g., handler context)
/// - `O`: Response type for outgoing requests
///
/// # Example
///
/// ```
/// use lsp_server_tokio::{RequestQueue, RequestId};
/// use tokio_util::sync::CancellationToken;
///
/// // Create a queue for a server that tracks method names for incoming
/// // requests and expects JSON responses for outgoing requests
/// let mut queue: RequestQueue<String, serde_json::Value> = RequestQueue::new();
///
/// // Track incoming request with cancellation token
/// let token = CancellationToken::new();
/// queue.incoming.register(1.into(), "textDocument/hover".to_string(), token);
///
/// // Operations on incoming don't affect outgoing
/// assert_eq!(queue.incoming.pending_count(), 1);
/// assert_eq!(queue.outgoing.pending_count(), 0);
/// ```
#[derive(Debug)]
pub struct RequestQueue<I, O> {
    /// Tracker for requests received from clients.
    pub incoming: IncomingRequests<I>,
    /// Tracker for requests sent to clients.
    pub outgoing: OutgoingRequests<O>,
}

impl<I, O> RequestQueue<I, O> {
    /// Creates a new empty request queue.
    #[must_use] 
    pub fn new() -> Self {
        Self {
            incoming: IncomingRequests::new(),
            outgoing: OutgoingRequests::new(),
        }
    }
}

impl<I, O> Default for RequestQueue<I, O> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_util::sync::CancellationToken;

    // ============== IncomingRequests Tests ==============

    #[test]
    fn incoming_register_and_complete() {
        let mut incoming: IncomingRequests<String> = IncomingRequests::new();
        let token = CancellationToken::new();

        incoming.register(1.into(), "metadata".to_string(), token);
        let data = incoming.complete(&1.into());

        assert_eq!(data, Some("metadata".to_string()));
        assert!(!incoming.is_pending(&1.into()));
    }

    #[test]
    fn incoming_complete_unknown_returns_none() {
        let mut incoming: IncomingRequests<String> = IncomingRequests::new();

        let data = incoming.complete(&999.into());
        assert_eq!(data, None);
    }

    #[test]
    fn incoming_is_pending() {
        let mut incoming: IncomingRequests<()> = IncomingRequests::new();

        assert!(!incoming.is_pending(&1.into()));

        let token = CancellationToken::new();
        incoming.register(1.into(), (), token);
        assert!(incoming.is_pending(&1.into()));

        incoming.complete(&1.into());
        assert!(!incoming.is_pending(&1.into()));
    }

    #[test]
    fn incoming_pending_count() {
        let mut incoming: IncomingRequests<i32> = IncomingRequests::new();

        assert_eq!(incoming.pending_count(), 0);

        let token1 = CancellationToken::new();
        incoming.register(1.into(), 100, token1);
        assert_eq!(incoming.pending_count(), 1);

        let token2 = CancellationToken::new();
        incoming.register(2.into(), 200, token2);
        assert_eq!(incoming.pending_count(), 2);

        incoming.complete(&1.into());
        assert_eq!(incoming.pending_count(), 1);

        incoming.complete(&2.into());
        assert_eq!(incoming.pending_count(), 0);
    }

    #[test]
    fn incoming_default() {
        let incoming: IncomingRequests<()> = IncomingRequests::default();
        assert_eq!(incoming.pending_count(), 0);
    }

    #[test]
    fn incoming_cancel_triggers_token() {
        let mut incoming: IncomingRequests<String> = IncomingRequests::new();
        let token = CancellationToken::new();
        let token_clone = token.clone();

        incoming.register(1.into(), "data".to_string(), token);

        // Cancel the request
        assert!(incoming.cancel(&1.into()));

        // Token should be cancelled
        assert!(token_clone.is_cancelled());
    }

    #[test]
    fn incoming_cancel_unknown_returns_false() {
        let incoming: IncomingRequests<()> = IncomingRequests::new();
        assert!(!incoming.cancel(&999.into()));
    }

    #[test]
    fn incoming_cancel_idempotent() {
        let mut incoming: IncomingRequests<()> = IncomingRequests::new();
        let token = CancellationToken::new();

        incoming.register(1.into(), (), token);

        // Cancel twice - both should succeed
        assert!(incoming.cancel(&1.into()));
        assert!(incoming.cancel(&1.into())); // Still returns true, request still pending
    }

    #[test]
    fn incoming_get_token_returns_clone() {
        let mut incoming: IncomingRequests<String> = IncomingRequests::new();
        let original_token = CancellationToken::new();

        incoming.register(1.into(), "data".to_string(), original_token.clone());

        // Get the token
        let retrieved = incoming.get_token(&1.into());
        assert!(retrieved.is_some());

        // Cancel via retrieved token
        retrieved.unwrap().cancel();

        // Original should also be cancelled (they're the same underlying token)
        assert!(original_token.is_cancelled());
    }

    #[test]
    fn incoming_get_token_unknown_returns_none() {
        let incoming: IncomingRequests<()> = IncomingRequests::new();
        assert!(incoming.get_token(&999.into()).is_none());
    }

    #[test]
    fn incoming_complete_after_cancel_returns_data() {
        let mut incoming: IncomingRequests<String> = IncomingRequests::new();
        let token = CancellationToken::new();

        incoming.register(1.into(), "cancelled_data".to_string(), token);

        // Cancel first
        let _ = incoming.cancel(&1.into());

        // Complete should still return the data
        let data = incoming.complete(&1.into());
        assert_eq!(data, Some("cancelled_data".to_string()));
    }

    // ============== OutgoingRequests Tests ==============

    #[tokio::test]
    async fn outgoing_register_and_complete() {
        let mut outgoing: OutgoingRequests<String> = OutgoingRequests::new();
        let rx = outgoing.register(1.into());

        assert!(outgoing.complete(&1.into(), "response".to_string()));
        assert_eq!(rx.await.unwrap(), "response");
    }

    #[test]
    fn outgoing_complete_unknown_returns_false() {
        let mut outgoing: OutgoingRequests<String> = OutgoingRequests::new();

        let result = outgoing.complete(&999.into(), "response".to_string());
        assert!(!result);
    }

    #[tokio::test]
    async fn outgoing_cancel_drops_sender() {
        let mut outgoing: OutgoingRequests<String> = OutgoingRequests::new();
        let rx = outgoing.register(1.into());

        assert!(outgoing.cancel(&1.into()));
        assert!(!outgoing.is_pending(&1.into()));

        // Receiver should get an error since sender was dropped
        assert!(rx.await.is_err());
    }

    #[test]
    fn outgoing_cancel_unknown_returns_false() {
        let mut outgoing: OutgoingRequests<String> = OutgoingRequests::new();

        assert!(!outgoing.cancel(&999.into()));
    }

    #[test]
    fn outgoing_is_pending() {
        let mut outgoing: OutgoingRequests<()> = OutgoingRequests::new();

        assert!(!outgoing.is_pending(&1.into()));

        let _rx = outgoing.register(1.into());
        assert!(outgoing.is_pending(&1.into()));

        outgoing.complete(&1.into(), ());
        assert!(!outgoing.is_pending(&1.into()));
    }

    #[test]
    fn outgoing_pending_count() {
        let mut outgoing: OutgoingRequests<i32> = OutgoingRequests::new();

        assert_eq!(outgoing.pending_count(), 0);

        let _rx1 = outgoing.register(1.into());
        assert_eq!(outgoing.pending_count(), 1);

        let _rx2 = outgoing.register(2.into());
        assert_eq!(outgoing.pending_count(), 2);

        outgoing.complete(&1.into(), 100);
        assert_eq!(outgoing.pending_count(), 1);

        outgoing.cancel(&2.into());
        assert_eq!(outgoing.pending_count(), 0);
    }

    #[test]
    fn outgoing_default() {
        let outgoing: OutgoingRequests<()> = OutgoingRequests::default();
        assert_eq!(outgoing.pending_count(), 0);
    }

    // ============== RequestQueue Tests ==============

    #[test]
    fn queue_new_creates_empty() {
        let queue: RequestQueue<(), ()> = RequestQueue::new();

        assert_eq!(queue.incoming.pending_count(), 0);
        assert_eq!(queue.outgoing.pending_count(), 0);
    }

    #[test]
    fn queue_incoming_outgoing_independent() {
        let mut queue: RequestQueue<String, String> = RequestQueue::new();

        // Register on incoming
        let token = CancellationToken::new();
        queue
            .incoming
            .register(1.into(), "incoming".to_string(), token);
        assert_eq!(queue.incoming.pending_count(), 1);
        assert_eq!(queue.outgoing.pending_count(), 0);

        // Register on outgoing
        let _rx = queue.outgoing.register(2.into());
        assert_eq!(queue.incoming.pending_count(), 1);
        assert_eq!(queue.outgoing.pending_count(), 1);

        // Complete incoming doesn't affect outgoing
        queue.incoming.complete(&1.into());
        assert_eq!(queue.incoming.pending_count(), 0);
        assert_eq!(queue.outgoing.pending_count(), 1);
    }

    #[test]
    fn queue_default() {
        let queue: RequestQueue<(), ()> = RequestQueue::default();
        assert_eq!(queue.incoming.pending_count(), 0);
        assert_eq!(queue.outgoing.pending_count(), 0);
    }

    #[test]
    fn queue_with_string_request_id() {
        let mut queue: RequestQueue<i32, i32> = RequestQueue::new();

        let str_id: RequestId = "abc-123".into();
        let token = CancellationToken::new();
        queue.incoming.register(str_id.clone(), 42, token);

        assert!(queue.incoming.is_pending(&str_id));
        assert_eq!(queue.incoming.complete(&str_id), Some(42));
    }

    // ============== parse_cancel_params Tests ==============

    use super::parse_cancel_params;

    #[test]
    fn parse_cancel_params_integer_id() {
        let params = Some(serde_json::json!({"id": 42}));
        let id = parse_cancel_params(&params);
        assert_eq!(id, Some(RequestId::Integer(42)));
    }

    #[test]
    fn parse_cancel_params_string_id() {
        let params = Some(serde_json::json!({"id": "request-123"}));
        let id = parse_cancel_params(&params);
        assert_eq!(id, Some(RequestId::String("request-123".to_string())));
    }

    #[test]
    fn parse_cancel_params_missing_params() {
        let id = parse_cancel_params(&None);
        assert!(id.is_none());
    }

    #[test]
    fn parse_cancel_params_missing_id_field() {
        let params = Some(serde_json::json!({"other": "field"}));
        let id = parse_cancel_params(&params);
        assert!(id.is_none());
    }

    #[test]
    fn parse_cancel_params_invalid_id_type() {
        let params = Some(serde_json::json!({"id": true}));
        let id = parse_cancel_params(&params);
        assert!(id.is_none());
    }

    #[test]
    fn parse_cancel_params_null_id() {
        let params = Some(serde_json::json!({"id": null}));
        let id = parse_cancel_params(&params);
        assert!(id.is_none());
    }
}
