//! Connection abstraction for bidirectional LSP message communication.
//!
//! This module provides the [`Connection`] type which is the primary entry point
//! for LSP communication. It wraps a transport and provides:
//!
//! - Split sender/receiver halves for async message passing
//! - Request queue for tracking pending requests in both directions
//! - Lifecycle state management for LSP initialization and shutdown
//!
//! # Overview
//!
//! A [`Connection`] is constructed from any `AsyncRead + AsyncWrite` stream and
//! provides separate [`Sender`] and [`Receiver`] halves that can be used independently
//! in concurrent async tasks. It also contains a [`RequestQueue`] for tracking
//! pending requests.
//!
//! # Examples
//!
//! ## Basic usage with duplex streams
//!
//! ```
//! use futures::{SinkExt, StreamExt};
//! use lsp_server_tokio::{Connection, Message, Request};
//!
//! # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
//! let (client_stream, server_stream) = tokio::io::duplex(4096);
//!
//! // Create connections
//! let mut client: Connection<_, (), ()> = Connection::new(client_stream);
//! let mut server: Connection<_, (), ()> = Connection::new(server_stream);
//!
//! // Send a request from client
//! let request = Message::Request(Request::new(1, "textDocument/hover", None));
//! client.sender.send(request).await.unwrap();
//!
//! // Receive on server
//! let msg = server.receiver.next().await.unwrap().unwrap();
//! assert!(msg.is_request());
//! # });
//! ```
//!
//! ## With request queue tracking
//!
//! ```
//! use lsp_server_tokio::{Connection, RequestQueue};
//!
//! # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
//! let (stream, _) = tokio::io::duplex(4096);
//!
//! // Create connection with typed request queue
//! let mut conn: Connection<_, String, String> = Connection::new(stream);
//!
//! // Track an incoming request with a cancellation token
//! use tokio_util::sync::CancellationToken;
//! let token = CancellationToken::new();
//! conn.request_queue.incoming.register(1.into(), "textDocument/hover".to_string(), token);
//! assert!(conn.request_queue.incoming.is_pending(&1.into()));
//! # });
//! ```
//!
//! ## Stdio for LSP servers
//!
//! ```no_run
//! use lsp_server_tokio::Connection;
//!
//! # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
//! // Create a connection over stdin/stdout for typical LSP server usage
//! let conn = Connection::stdio();
//! // Use conn.sender to write responses, conn.receiver to read requests
//! # });
//! ```

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::stream::{SplitSink, SplitStream};
use futures::StreamExt;
use pin_project_lite::pin_project;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf, Stdin, Stdout};
use tokio_util::sync::CancellationToken;

use crate::lifecycle::{ExitCode, LifecycleState, ProtocolError};
use crate::{transport, Message, RequestQueue, Transport};

/// A [`Connection`] backed by [`StdioTransport`].
///
/// This alias is useful when building stdio-based servers that want to use
/// custom metadata types for request tracking without repeating the full
/// `Connection<StdioTransport, I, O>` generic.
///
/// # Examples
///
/// ```no_run
/// use lsp_server_tokio::{connection::StdioTransport, Connection, StdioConnection};
///
/// let conn: StdioConnection<String, String> = Connection::new(StdioTransport::new());
/// ```
pub type StdioConnection<I = (), O = ()> = Connection<StdioTransport, I, O>;

/// The sender half of an LSP connection.
///
/// This type can be used to send LSP messages over the transport.
/// It implements `Sink<Message, Error=io::Error>`.
///
/// # Example
///
/// ```
/// use futures::SinkExt;
/// use lsp_server_tokio::{Connection, Message, Request, Sender};
///
/// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
/// let (client_stream, server_stream) = tokio::io::duplex(4096);
/// let client: Connection<_, (), ()> = Connection::new(client_stream);
/// let _server: Connection<_, (), ()> = Connection::new(server_stream);
///
/// // Move sender to a task
/// let mut sender: Sender<_> = client.sender;
/// sender.send(Message::Request(Request::new(1, "test", None))).await.unwrap();
/// # });
/// ```
pub type Sender<T> = SplitSink<Transport<T>, Message>;

/// The receiver half of an LSP connection.
///
/// This type can be used to receive LSP messages from the transport.
/// It implements `Stream<Item=Result<Message, io::Error>>`.
///
/// # Example
///
/// ```
/// use futures::StreamExt;
/// use lsp_server_tokio::{Connection, Receiver};
///
/// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
/// let (stream, _) = tokio::io::duplex(4096);
/// let conn: Connection<_, (), ()> = Connection::new(stream);
///
/// // Move receiver to a task
/// let mut receiver: Receiver<_> = conn.receiver;
/// // receiver.next().await would block waiting for a message
/// # });
/// ```
pub type Receiver<T> = SplitStream<Transport<T>>;

/// A bidirectional LSP connection with split sender/receiver and request tracking.
///
/// `Connection` is the primary type for LSP communication. It provides:
///
/// - [`sender`](Connection::sender): A sink for sending outbound messages
/// - [`receiver`](Connection::receiver): A stream for receiving inbound messages
/// - [`request_queue`](Connection::request_queue): Tracking for pending requests
///
/// The sender and receiver can be moved to separate async tasks for concurrent
/// bidirectional communication.
///
/// # Type Parameters
///
/// - `T`: The underlying I/O stream type (`AsyncRead + AsyncWrite`)
/// - `I`: Metadata type for incoming requests (default: `()`)
/// - `O`: Response type for outgoing requests (default: `()`)
///
/// # Examples
///
/// ## Basic construction
///
/// ```
/// use lsp_server_tokio::Connection;
///
/// let (stream, _) = tokio::io::duplex(4096);
///
/// // Simple connection with unit metadata types
/// let conn: Connection<_, (), ()> = Connection::new(stream);
/// ```
///
/// ## With custom metadata types
///
/// ```
/// use lsp_server_tokio::Connection;
///
/// let (stream, _) = tokio::io::duplex(4096);
///
/// // Connection tracking method names for incoming, JSON values for outgoing
/// let conn: Connection<_, String, serde_json::Value> = Connection::new(stream);
/// ```
pub struct Connection<T, I = (), O = ()>
where
    T: AsyncRead + AsyncWrite,
{
    /// The sender half for outbound messages.
    ///
    /// Use this to send requests, responses, and notifications to the other end.
    pub sender: Sender<T>,

    /// The receiver half for inbound messages.
    ///
    /// Use this to receive requests, responses, and notifications from the other end.
    pub receiver: Receiver<T>,

    /// Request queue for tracking pending incoming and outgoing requests.
    ///
    /// - `request_queue.incoming`: Track requests you've received and need to respond to
    /// - `request_queue.outgoing`: Track requests you've sent and are awaiting responses for
    pub request_queue: RequestQueue<I, O>,

    /// The current lifecycle state of the connection.
    lifecycle_state: LifecycleState,

    /// Cancellation token that is triggered when shutdown is requested.
    shutdown_token: CancellationToken,
}

impl<T, I, O> Connection<T, I, O>
where
    T: AsyncRead + AsyncWrite,
{
    /// Creates a new connection from an async I/O stream.
    ///
    /// This constructor wraps the stream with [`crate::LspCodec`] for Content-Length
    /// framing, splits it into sender/receiver halves, and creates an empty
    /// request queue.
    ///
    /// # Arguments
    ///
    /// * `io` - Any async I/O stream implementing `AsyncRead + AsyncWrite`
    ///
    /// # Example
    ///
    /// ```
    /// use lsp_server_tokio::Connection;
    ///
    /// let (stream, _) = tokio::io::duplex(4096);
    /// let conn: Connection<_, (), ()> = Connection::new(stream);
    /// ```
    pub fn new(io: T) -> Self {
        let transport = transport(io);
        Self::from_transport(transport)
    }

    /// Creates a new connection from an existing transport.
    ///
    /// This is useful when you already have a [`Transport`] and want to
    /// upgrade it to a full `Connection` with request tracking.
    ///
    /// # Arguments
    ///
    /// * `transport` - An existing LSP transport
    ///
    /// # Example
    ///
    /// ```
    /// use lsp_server_tokio::{transport, Connection};
    ///
    /// let (stream, _) = tokio::io::duplex(4096);
    /// let transport = transport(stream);
    /// let conn: Connection<_, (), ()> = Connection::from_transport(transport);
    /// ```
    pub fn from_transport(transport: Transport<T>) -> Self {
        let (sender, receiver) = transport.split();
        Self {
            sender,
            receiver,
            request_queue: RequestQueue::new(),
            lifecycle_state: LifecycleState::default(),
            shutdown_token: CancellationToken::new(),
        }
    }

    /// Creates a new connection with a pre-existing request queue.
    ///
    /// This constructor allows you to provide your own [`RequestQueue`], which
    /// is useful for:
    /// - Testing with pre-populated request state
    /// - Migrating state between connections
    /// - Using custom metadata types
    ///
    /// # Arguments
    ///
    /// * `io` - Any async I/O stream implementing `AsyncRead + AsyncWrite`
    /// * `request_queue` - A pre-existing request queue
    ///
    /// # Example
    ///
    /// ```
    /// use lsp_server_tokio::{Connection, RequestQueue};
    ///
    /// let (stream, _) = tokio::io::duplex(4096);
    ///
    /// // Create a queue with some pre-registered requests
    /// use tokio_util::sync::CancellationToken;
    /// let mut queue: RequestQueue<u32, u32> = RequestQueue::new();
    /// let token = CancellationToken::new();
    /// queue.incoming.register(1.into(), 100, token);
    ///
    /// let conn = Connection::with_request_queue(stream, queue);
    /// assert!(conn.request_queue.incoming.is_pending(&1.into()));
    /// ```
    pub fn with_request_queue(io: T, request_queue: RequestQueue<I, O>) -> Self {
        let transport = transport(io);
        let (sender, receiver) = transport.split();
        Self {
            sender,
            receiver,
            request_queue,
            lifecycle_state: LifecycleState::default(),
            shutdown_token: CancellationToken::new(),
        }
    }

    /// Returns the current lifecycle state.
    #[must_use]
    pub fn lifecycle_state(&self) -> LifecycleState {
        self.lifecycle_state
    }

    /// Returns a token that is cancelled when shutdown is requested.
    ///
    /// Use this to gracefully stop background tasks when the server is
    /// shutting down.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use lsp_server_tokio::Connection;
    /// use tokio_util::sync::CancellationToken;
    ///
    /// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
    /// let (stream, _) = tokio::io::duplex(4096);
    /// let conn: Connection<_, (), ()> = Connection::new(stream);
    ///
    /// let token = conn.shutdown_token();
    /// tokio::spawn(async move {
    ///     loop {
    ///         tokio::select! {
    ///             _ = token.cancelled() => {
    ///                 // Clean up and exit
    ///                 break;
    ///             }
    ///             // ... other work ...
    ///         }
    ///     }
    /// });
    /// # });
    /// ```
    #[must_use]
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown_token.clone()
    }

    /// Returns a future that completes when shutdown is requested.
    ///
    /// This is equivalent to `self.shutdown_token().cancelled()` but more
    /// convenient for simple use cases.
    pub fn on_shutdown(&self) -> impl std::future::Future<Output = ()> + '_ {
        self.shutdown_token.cancelled()
    }
}

// Message routing methods
impl<T, I> Connection<T, I, crate::Response>
where
    T: AsyncRead + AsyncWrite,
    I: Default,
{
    /// Routes an incoming message, delivering responses to pending outgoing requests.
    ///
    /// Call this method for each message received from the transport to classify it
    /// and automatically deliver responses to their corresponding outgoing request receivers.
    ///
    /// For [`IncomingMessage::Request`](crate::IncomingMessage::Request) variants, the request
    /// is automatically registered with a [`CancellationToken`] that is:
    /// - Cancelled when `$/cancelRequest` is received for this request ID
    /// - Cancelled when the connection shuts down
    ///
    /// # Returns
    ///
    /// - [`IncomingMessage::Request`](crate::IncomingMessage::Request) - Handle the request and send a response
    /// - [`IncomingMessage::Notification`](crate::IncomingMessage::Notification) - Handle the notification
    /// - [`IncomingMessage::ResponseRouted`](crate::IncomingMessage::ResponseRouted) - Response was delivered to awaiting receiver
    /// - [`IncomingMessage::ResponseUnknown`](crate::IncomingMessage::ResponseUnknown) - No pending request for this response ID
    ///
    /// # Example
    ///
    /// ```no_run
    /// use lsp_server_tokio::{Connection, Message, IncomingMessage, Response};
    /// use futures::StreamExt;
    ///
    /// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
    /// let (stream, _) = tokio::io::duplex(4096);
    /// let mut conn: Connection<_, (), Response> = Connection::new(stream);
    ///
    /// while let Some(Ok(msg)) = conn.receiver.next().await {
    ///     match conn.route(msg) {
    ///         IncomingMessage::Request(req, token) => {
    ///             // Handle request with cooperative cancellation
    ///             println!("Request: {}", req.method);
    ///             // Use token.cancelled().await in select! for cancellation
    ///             // After handling, call conn.request_queue.incoming.complete(&req.id)
    ///         }
    ///         IncomingMessage::Notification(notif) => {
    ///             // Handle notification
    ///             println!("Notification: {}", notif.method);
    ///         }
    ///         IncomingMessage::ResponseRouted => {
    ///             // Response delivered to awaiting task
    ///         }
    ///         IncomingMessage::ResponseUnknown(resp) => {
    ///             // Log unexpected response
    ///             eprintln!("Unknown response: {:?}", resp.id);
    ///         }
    ///     }
    /// }
    /// # });
    /// ```
    #[allow(deprecated)]
    pub fn route(&mut self, message: Message) -> crate::IncomingMessage {
        match message {
            Message::Request(req) => {
                let token = self.register_cancellable_request(req.id.clone(), I::default());
                crate::IncomingMessage::Request(req, token)
            }
            Message::Notification(notif) => crate::IncomingMessage::Notification(notif),
            Message::Response(resp) => {
                if let Some(id) = resp.id.clone() {
                    // Check if there's a pending request for this response
                    if self.request_queue.outgoing.is_pending(&id) {
                        // Complete the request - this sends the response to the awaiting receiver
                        self.request_queue.outgoing.complete(&id, resp);
                        crate::IncomingMessage::ResponseRouted
                    } else {
                        // No pending request for this ID
                        crate::IncomingMessage::ResponseUnknown(resp)
                    }
                } else {
                    // Response with null ID (parse error response)
                    crate::IncomingMessage::ResponseUnknown(resp)
                }
            }
        }
    }

    /// Cancels an incoming request by request ID.
    ///
    /// This is a convenience method that cancels the [`CancellationToken`] for a registered
    /// incoming request. Use this when you receive a `$/cancelRequest` notification.
    ///
    /// # Arguments
    ///
    /// * `id` - The request ID to cancel
    ///
    /// # Returns
    ///
    /// `true` if the request was found and cancelled, `false` if not found.
    ///
    /// # Example
    ///
    /// ```
    /// use lsp_server_tokio::Connection;
    ///
    /// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
    /// let (stream, _) = tokio::io::duplex(4096);
    /// let mut conn: Connection<_, (), lsp_server_tokio::Response> = Connection::new(stream);
    ///
    /// // After receiving $/cancelRequest for ID 42:
    /// let was_cancelled = conn.cancel_incoming(42);
    /// # });
    /// ```
    pub fn cancel_incoming(&mut self, id: impl Into<crate::RequestId>) -> bool {
        self.request_queue.incoming.cancel(&id.into())
    }
}

// Cancel request handling methods
impl<T, I, O> Connection<T, I, O>
where
    T: AsyncRead + AsyncWrite,
{
    /// Registers an incoming request with automatic cancellation token creation.
    ///
    /// # Deprecated
    ///
    /// This method is deprecated. Use [`route()`](Connection::route) instead, which
    /// automatically registers incoming requests and returns the token in the
    /// [`IncomingMessage::Request`](crate::IncomingMessage::Request) variant.
    ///
    /// # Details
    ///
    /// This is a convenience method that creates a child token from the connection's
    /// shutdown token and registers the request. The returned token:
    /// - Is cancelled when $/cancelRequest is received for this ID
    /// - Is cancelled when the server shuts down (inherits from `shutdown_token`)
    ///
    /// Pass this token to your request handler for cooperative cancellation.
    ///
    /// # Arguments
    ///
    /// * `id` - The request ID
    /// * `data` - User-defined metadata for this request
    ///
    /// # Returns
    ///
    /// A [`CancellationToken`] that will be triggered on cancel or shutdown.
    #[deprecated(
        since = "0.1.0",
        note = "Use route() which auto-registers requests. The token is now included in IncomingMessage::Request."
    )]
    pub fn register_cancellable_request(
        &mut self,
        id: crate::RequestId,
        data: I,
    ) -> CancellationToken {
        let token = self.shutdown_token.child_token();
        self.request_queue
            .incoming
            .register(id, data, token.clone());
        token
    }

    /// Handles a $/cancelRequest notification.
    ///
    /// If the notification is a $/cancelRequest, parses the request ID from params
    /// and cancels the corresponding request's token. If the request is not pending
    /// (already completed or never registered), this is a no-op.
    ///
    /// # Arguments
    ///
    /// * `notification` - The notification to check
    ///
    /// # Returns
    ///
    /// - `Some(true)` if this was $/cancelRequest and the request was found and cancelled
    /// - `Some(false)` if this was $/cancelRequest but the request was not pending
    /// - `None` if this was not a $/cancelRequest notification
    ///
    /// # Example
    ///
    /// ```
    /// use lsp_server_tokio::{Connection, Notification, IncomingMessage};
    ///
    /// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
    /// let (stream, _) = tokio::io::duplex(4096);
    /// let mut conn: Connection<_, String, ()> = Connection::new(stream);
    ///
    /// // Register a request
    /// let _token = conn.register_cancellable_request(42.into(), "test".to_string());
    ///
    /// // Simulate receiving $/cancelRequest
    /// let cancel_notif = Notification::new(
    ///     "$/cancelRequest",
    ///     Some(serde_json::json!({"id": 42})),
    /// );
    ///
    /// let result = conn.handle_cancel_request(&cancel_notif);
    /// assert_eq!(result, Some(true));
    /// # });
    /// ```
    pub fn handle_cancel_request(&mut self, notification: &crate::Notification) -> Option<bool> {
        use crate::request_queue::{parse_cancel_params, CANCEL_REQUEST_METHOD};

        if notification.method != CANCEL_REQUEST_METHOD {
            return None;
        }

        let id = parse_cancel_params(&notification.params)?;
        Some(self.request_queue.incoming.cancel(&id))
    }
}

// Outgoing request cancellation methods
impl<T, I, O> Connection<T, I, O>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    /// Cancels an outgoing request by sending $/cancelRequest and removing it from the queue.
    ///
    /// This method is for cancelling requests that **this connection sent** (outgoing requests).
    /// It is the inverse of [`handle_cancel_request`](Self::handle_cancel_request), which handles
    /// cancellation requests **received from** the other end (for incoming requests).
    ///
    /// # Behavior
    ///
    /// 1. Sends a `$/cancelRequest` notification with the given request ID
    /// 2. Removes the request from the outgoing queue (the awaiting receiver will get `RecvError`)
    ///
    /// # Returns
    ///
    /// - `Ok(true)` if the request was pending and cancelled
    /// - `Ok(false)` if the request was not found in the queue
    /// - `Err` if sending the notification failed
    ///
    /// # Errors
    ///
    /// Returns an error if the cancellation notification cannot be sent to the peer.
    ///
    /// # Example
    ///
    /// ```
    /// use lsp_server_tokio::{Connection, Response};
    /// use futures::SinkExt;
    ///
    /// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
    /// let (client_stream, server_stream) = tokio::io::duplex(4096);
    /// let mut conn: Connection<_, (), Response> = Connection::new(client_stream);
    ///
    /// // Register an outgoing request
    /// let rx = conn.request_queue.outgoing.register(42.into());
    ///
    /// // Cancel it
    /// let was_pending = conn.cancel(42).await.unwrap();
    /// assert!(was_pending);
    ///
    /// // The receiver will get an error
    /// assert!(rx.await.is_err());
    /// # });
    /// ```
    pub async fn cancel(
        &mut self,
        id: impl Into<crate::RequestId>,
    ) -> Result<bool, std::io::Error> {
        use crate::request_queue::CANCEL_REQUEST_METHOD;
        use futures::SinkExt;

        let id = id.into();

        // Build the $/cancelRequest notification
        let notification =
            crate::Notification::new(CANCEL_REQUEST_METHOD, Some(serde_json::json!({"id": id})));

        // Send the notification
        self.sender
            .send(Message::Notification(notification))
            .await?;

        // Cancel in the queue (returns true if was pending)
        Ok(self.request_queue.outgoing.cancel(&id))
    }
}

// Lifecycle management methods
impl<T, I, O> Connection<T, I, O>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    /// Waits for the initialize request from the client.
    ///
    /// This method blocks until an initialize request is received, rejecting
    /// any other requests with `ServerNotInitialized` error and dropping
    /// notifications (except exit which disconnects).
    ///
    /// Returns the request ID and params for the initialize request.
    /// You must call [`initialize_finish()`](Self::initialize_finish) with the
    /// same ID to complete the handshake.
    ///
    /// # Errors
    ///
    /// - [`ProtocolError::Disconnected`] if the connection is closed or exit notification received
    /// - [`ProtocolError::Io`] if an I/O error occurs
    ///
    /// # Example
    ///
    /// ```no_run
    /// use lsp_server_tokio::Connection;
    ///
    /// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
    /// let (stream, _) = tokio::io::duplex(4096);
    /// let mut conn: Connection<_, (), ()> = Connection::new(stream);
    ///
    /// let (id, params) = conn.initialize_start().await.unwrap();
    /// // Process params, build capabilities...
    /// let capabilities = serde_json::json!({"textDocumentSync": 1});
    /// conn.initialize_finish(id, capabilities).await.unwrap();
    /// # });
    /// ```
    pub async fn initialize_start(
        &mut self,
    ) -> Result<(crate::RequestId, serde_json::Value), ProtocolError> {
        use futures::SinkExt;

        loop {
            match self.receiver.next().await {
                Some(Ok(Message::Request(req))) => {
                    if req.method == "initialize" {
                        self.lifecycle_state = LifecycleState::Initializing;
                        return Ok((req.id, req.params.unwrap_or(serde_json::Value::Null)));
                    }

                    // Reject non-initialize requests with ServerNotInitialized
                    let error = crate::ResponseError::new(
                        crate::ErrorCode::ServerNotInitialized,
                        "Server not yet initialized",
                    );
                    let response = Message::Response(crate::Response::err(req.id, error));
                    if let Err(e) = self.sender.send(response).await {
                        return Err(ProtocolError::Io(e));
                    }
                    // Continue waiting for initialize
                }
                Some(Ok(Message::Notification(notif))) => {
                    if notif.method == "exit" {
                        return Err(ProtocolError::Disconnected);
                    }
                    // Drop other notifications silently
                }
                Some(Ok(Message::Response(_))) => {
                    // Unexpected response, ignore
                }
                Some(Err(e)) => {
                    return Err(ProtocolError::Io(e));
                }
                None => {
                    return Err(ProtocolError::Disconnected);
                }
            }
        }
    }

    /// Completes the initialization handshake.
    ///
    /// Sends the `InitializeResult` response and waits for the initialized
    /// notification from the client. After this returns `Ok(())`, the
    /// connection is in Running state and ready for normal operation.
    ///
    /// # Arguments
    ///
    /// * `id` - The request ID from [`initialize_start()`](Self::initialize_start)
    /// * `server_capabilities` - The server's capabilities as JSON
    ///
    /// # Errors
    ///
    /// - [`ProtocolError::Disconnected`] if the connection is closed
    /// - [`ProtocolError::InitializeTimeout`] if the client does not send
    ///   `initialized` within 60 seconds
    /// - [`ProtocolError::Io`] if an I/O error occurs
    ///
    /// # Example
    ///
    /// ```no_run
    /// use lsp_server_tokio::Connection;
    ///
    /// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
    /// let (stream, _) = tokio::io::duplex(4096);
    /// let mut conn: Connection<_, (), ()> = Connection::new(stream);
    ///
    /// let (id, _params) = conn.initialize_start().await.unwrap();
    /// let capabilities = serde_json::json!({"textDocumentSync": 1});
    /// conn.initialize_finish(id, capabilities).await.unwrap();
    ///
    /// assert!(conn.is_running());
    /// # });
    /// ```
    pub async fn initialize_finish(
        &mut self,
        id: crate::RequestId,
        server_capabilities: serde_json::Value,
    ) -> Result<(), ProtocolError> {
        use futures::SinkExt;
        use std::time::Duration;

        // Build InitializeResult
        let result = serde_json::json!({
            "capabilities": server_capabilities
        });

        // Send the response
        let response = Message::Response(crate::Response::ok(id, result));
        if let Err(e) = self.sender.send(response).await {
            return Err(ProtocolError::Io(e));
        }

        tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                match self.receiver.next().await {
                    Some(Ok(Message::Notification(notif))) => {
                        if notif.method == "initialized" {
                            self.lifecycle_state = LifecycleState::Running;
                            return Ok(());
                        }
                        // Drop other notifications silently
                    }
                    Some(Ok(Message::Request(req))) => {
                        // Still initializing, reject with ServerNotInitialized
                        let error = crate::ResponseError::new(
                            crate::ErrorCode::ServerNotInitialized,
                            "Server not yet initialized",
                        );
                        let response = Message::Response(crate::Response::err(req.id, error));
                        if let Err(e) = self.sender.send(response).await {
                            return Err(ProtocolError::Io(e));
                        }
                    }
                    Some(Ok(Message::Response(_))) => {
                        // Ignore unexpected responses
                    }
                    Some(Err(e)) => {
                        return Err(ProtocolError::Io(e));
                    }
                    None => {
                        return Err(ProtocolError::Disconnected);
                    }
                }
            }
        })
        .await
        .map_err(|_| ProtocolError::InitializeTimeout)?
    }

    /// Performs complete LSP initialization handshake.
    ///
    /// This is a convenience method that calls [`initialize_start()`](Self::initialize_start)
    /// followed by [`initialize_finish()`](Self::initialize_finish).
    /// Returns the initialize params from the client.
    ///
    /// # Arguments
    ///
    /// * `server_capabilities` - The server's capabilities as JSON
    ///
    /// # Errors
    ///
    /// - [`ProtocolError::Disconnected`] if the connection is closed
    /// - [`ProtocolError::Io`] if an I/O error occurs
    ///
    /// # Example
    ///
    /// ```no_run
    /// use lsp_server_tokio::Connection;
    ///
    /// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
    /// let (stream, _) = tokio::io::duplex(4096);
    /// let mut conn: Connection<_, (), ()> = Connection::new(stream);
    ///
    /// let capabilities = serde_json::json!({"textDocumentSync": 1});
    /// let client_params = conn.initialize(capabilities).await.unwrap();
    /// println!("Client capabilities: {}", client_params);
    /// # });
    /// ```
    pub async fn initialize(
        &mut self,
        server_capabilities: serde_json::Value,
    ) -> Result<serde_json::Value, ProtocolError> {
        let (id, params) = self.initialize_start().await?;
        self.initialize_finish(id, server_capabilities).await?;
        Ok(params)
    }

    /// Handles a shutdown request.
    ///
    /// Transitions to `ShuttingDown` state, cancels the shutdown token,
    /// and sends a null response. After this, only exit notification
    /// should be received.
    ///
    /// # Arguments
    ///
    /// * `id` - The request ID of the shutdown request
    ///
    /// # Errors
    ///
    /// - [`ProtocolError::Io`] if sending the response fails
    ///
    /// # Example
    ///
    /// ```no_run
    /// use lsp_server_tokio::{Connection, Message};
    /// use futures::StreamExt;
    ///
    /// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
    /// let (stream, _) = tokio::io::duplex(4096);
    /// let mut conn: Connection<_, (), ()> = Connection::new(stream);
    ///
    /// // ... after initialization ...
    /// // When shutdown request is received:
    /// // if let Message::Request(req) = msg && req.method == "shutdown" {
    /// //     conn.handle_shutdown(req.id).await.unwrap();
    /// //     assert!(conn.is_shutting_down());
    /// // }
    /// # });
    /// ```
    pub async fn handle_shutdown(&mut self, id: crate::RequestId) -> Result<(), ProtocolError> {
        use futures::SinkExt;

        // Cancel shutdown token first to notify waiting tasks
        self.shutdown_token.cancel();

        // Transition state
        self.lifecycle_state = LifecycleState::ShuttingDown;

        // Send null response
        let response = Message::Response(crate::Response::ok(id, serde_json::Value::Null));
        if let Err(e) = self.sender.send(response).await {
            return Err(ProtocolError::Io(e));
        }

        Ok(())
    }

    /// Handles the exit notification.
    ///
    /// Returns [`ExitCode::Success`] (exit code 0) if shutdown was received first,
    /// or [`ExitCode::Error`] (exit code 1) if exit came without shutdown.
    ///
    /// # Example
    ///
    /// ```
    /// use lsp_server_tokio::{Connection, ExitCode, LifecycleState};
    ///
    /// let (stream, _) = tokio::io::duplex(4096);
    /// let mut conn: Connection<_, (), ()> = Connection::new(stream);
    ///
    /// // Exit without shutdown - dirty exit
    /// let code = conn.handle_exit();
    /// assert_eq!(code, ExitCode::Error);
    /// ```
    pub fn handle_exit(&mut self) -> ExitCode {
        let was_shutting_down = self.lifecycle_state == LifecycleState::ShuttingDown;
        self.lifecycle_state = LifecycleState::Exited;

        if was_shutting_down {
            ExitCode::Success
        } else {
            ExitCode::Error
        }
    }

    /// Returns true if the connection is in Running state.
    ///
    /// The connection is in Running state after successful initialization
    /// and before shutdown is requested.
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.lifecycle_state == LifecycleState::Running
    }

    /// Returns true if shutdown has been requested.
    ///
    /// After shutdown, the server should only expect the exit notification.
    #[must_use]
    pub fn is_shutting_down(&self) -> bool {
        self.lifecycle_state == LifecycleState::ShuttingDown
    }
}

pin_project! {
    /// A combined stdin/stdout stream for LSP server communication.
    ///
    /// This type wraps tokio's [`Stdin`] and [`Stdout`] into a single type that
    /// implements both [`AsyncRead`] and [`AsyncWrite`], suitable for use with
    /// [`Connection`].
    ///
    /// This is typically used internally by [`Connection::stdio()`] and doesn't
    /// need to be constructed directly.
    pub struct StdioTransport {
        #[pin]
        stdin: Stdin,
        #[pin]
        stdout: Stdout,
    }
}

impl StdioTransport {
    /// Creates a new stdio transport from stdin and stdout.
    #[must_use]
    pub fn new() -> Self {
        Self {
            stdin: tokio::io::stdin(),
            stdout: tokio::io::stdout(),
        }
    }
}

impl Default for StdioTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl AsyncRead for StdioTransport {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.project().stdin.poll_read(cx, buf)
    }
}

impl AsyncWrite for StdioTransport {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.project().stdout.poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.project().stdout.poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.project().stdout.poll_shutdown(cx)
    }
}

impl Connection<StdioTransport, (), ()> {
    /// Creates a connection using stdin for reading and stdout for writing.
    ///
    /// This is the typical constructor for LSP servers that communicate
    /// over standard I/O. Uses unit types `()` for request queue metadata.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use futures::StreamExt;
    /// use lsp_server_tokio::{Connection, Message};
    ///
    /// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
    /// let mut conn = Connection::stdio();
    ///
    /// // Process incoming messages
    /// while let Some(result) = conn.receiver.next().await {
    ///     match result {
    ///         Ok(Message::Request(req)) => {
    ///             println!("Received request: {}", req.method);
    ///             // Handle request...
    ///         }
    ///         Ok(Message::Notification(notif)) => {
    ///             println!("Received notification: {}", notif.method);
    ///         }
    ///         Ok(Message::Response(resp)) => {
    ///             println!("Received response for: {:?}", resp.id);
    ///         }
    ///         Err(e) => {
    ///             eprintln!("Error: {}", e);
    ///             break;
    ///         }
    ///     }
    /// }
    /// # });
    /// ```
    ///
    /// # Note
    ///
    /// This constructor uses unit types `()` for both incoming and outgoing
    /// request metadata. If you need custom metadata types, use
    /// [`Connection::new()`] with a [`StdioTransport`] directly:
    ///
    /// ```no_run
    /// use lsp_server_tokio::{Connection, connection::StdioTransport};
    ///
    /// let conn: Connection<StdioTransport, String, String> = Connection::new(StdioTransport::new());
    /// ```
    #[must_use]
    pub fn stdio() -> Self {
        Self::new(StdioTransport::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Request, Response, StdioConnection};
    use futures::SinkExt;
    use serde_json::json;
    use std::time::Duration;

    #[tokio::test]
    async fn connection_from_duplex_test() {
        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let mut client: Connection<_, (), ()> = Connection::new(client_stream);
        let mut server: Connection<_, (), ()> = Connection::new(server_stream);

        // Send request from client
        let request = Message::Request(Request::new(1, "test", None));
        client.sender.send(request).await.unwrap();

        // Receive on server
        let received = server.receiver.next().await.unwrap().unwrap();
        assert!(received.is_request());
        if let Message::Request(req) = received {
            assert_eq!(req.method, "test");
            assert_eq!(req.id, 1.into());
        } else {
            panic!("Expected Request");
        }
    }

    #[tokio::test]
    async fn connection_bidirectional_test() {
        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let mut client: Connection<_, (), ()> = Connection::new(client_stream);
        let mut server: Connection<_, (), ()> = Connection::new(server_stream);

        // Client sends request
        let request = Message::Request(Request::new(
            1,
            "textDocument/hover",
            Some(json!({
                "textDocument": {"uri": "file:///test.rs"},
                "position": {"line": 10, "character": 5}
            })),
        ));
        client.sender.send(request).await.unwrap();

        // Server receives request
        let received = server.receiver.next().await.unwrap().unwrap();
        assert!(received.is_request());

        // Server sends response
        let response = Message::Response(Response::ok(
            1,
            json!({
                "contents": "fn main()"
            }),
        ));
        server.sender.send(response).await.unwrap();

        // Client receives response
        let received = client.receiver.next().await.unwrap().unwrap();
        assert!(received.is_response());
        if let Message::Response(resp) = received {
            assert_eq!(resp.id, Some(1.into()));
            assert!(resp.result.is_some());
        } else {
            panic!("Expected Response");
        }
    }

    #[tokio::test]
    async fn connection_from_transport_test() {
        let (client_stream, server_stream) = tokio::io::duplex(4096);

        // Create transports manually
        let client_transport = transport(client_stream);
        let server_transport = transport(server_stream);

        // Create connections from transports
        let mut client: Connection<_, (), ()> = Connection::from_transport(client_transport);
        let mut server: Connection<_, (), ()> = Connection::from_transport(server_transport);

        // Verify functionality
        let request = Message::Request(Request::new(42, "test", None));
        client.sender.send(request).await.unwrap();

        let received = server.receiver.next().await.unwrap().unwrap();
        assert!(received.is_request());
        if let Message::Request(req) = received {
            assert_eq!(req.id, 42.into());
        }
    }

    #[tokio::test]
    async fn connection_multiple_messages_test() {
        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let mut client: Connection<_, (), ()> = Connection::new(client_stream);
        let mut server: Connection<_, (), ()> = Connection::new(server_stream);

        // Send 3 messages in sequence
        let msg1 = Message::Request(Request::new(1, "first", None));
        let msg2 = Message::Request(Request::new(2, "second", None));
        let msg3 = Message::Request(Request::new(3, "third", None));

        client.sender.send(msg1).await.unwrap();
        client.sender.send(msg2).await.unwrap();
        client.sender.send(msg3).await.unwrap();

        // Receive all 3 - order must be preserved
        let recv1 = server.receiver.next().await.unwrap().unwrap();
        let recv2 = server.receiver.next().await.unwrap().unwrap();
        let recv3 = server.receiver.next().await.unwrap().unwrap();

        if let Message::Request(r) = recv1 {
            assert_eq!(r.method, "first");
            assert_eq!(r.id, 1.into());
        } else {
            panic!("Expected request");
        }

        if let Message::Request(r) = recv2 {
            assert_eq!(r.method, "second");
            assert_eq!(r.id, 2.into());
        } else {
            panic!("Expected request");
        }

        if let Message::Request(r) = recv3 {
            assert_eq!(r.method, "third");
            assert_eq!(r.id, 3.into());
        } else {
            panic!("Expected request");
        }
    }

    #[tokio::test]
    async fn sender_receiver_independent_test() {
        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let client: Connection<_, (), ()> = Connection::new(client_stream);
        let server: Connection<_, (), ()> = Connection::new(server_stream);

        // Move sender and receiver to separate tasks
        let mut client_sender = client.sender;
        let mut server_receiver = server.receiver;

        let send_task = tokio::spawn(async move {
            client_sender
                .send(Message::Request(Request::new(1, "test", None)))
                .await
        });

        let recv_task = tokio::spawn(async move { server_receiver.next().await });

        let (send_result, recv_result) = tokio::join!(send_task, recv_task);
        send_result.unwrap().unwrap();
        assert!(recv_result.unwrap().unwrap().unwrap().is_request());
    }

    #[tokio::test]
    async fn connection_has_request_queue_test() {
        let (stream, _) = tokio::io::duplex(4096);
        let mut conn: Connection<_, String, String> = Connection::new(stream);

        // Use request queue to track an incoming request
        let token = CancellationToken::new();
        conn.request_queue
            .incoming
            .register(1.into(), "handler_data".to_string(), token);
        assert!(conn.request_queue.incoming.is_pending(&1.into()));

        // Complete it
        let data = conn.request_queue.incoming.complete(&1.into());
        assert_eq!(data, Some("handler_data".to_string()));
    }

    #[tokio::test]
    async fn connection_with_request_queue_test() {
        let (stream, _) = tokio::io::duplex(4096);
        let mut queue: RequestQueue<u32, u32> = RequestQueue::new();
        let token = CancellationToken::new();
        queue.incoming.register(42.into(), 100, token);

        let conn = Connection::with_request_queue(stream, queue);
        assert!(conn.request_queue.incoming.is_pending(&42.into()));
    }

    #[tokio::test]
    async fn connection_outgoing_request_queue_test() {
        let (stream, _) = tokio::io::duplex(4096);
        let mut conn: Connection<_, (), String> = Connection::new(stream);

        // Register an outgoing request
        let rx = conn.request_queue.outgoing.register(1.into());
        assert!(conn.request_queue.outgoing.is_pending(&1.into()));

        // Complete it with a response
        let completed = conn
            .request_queue
            .outgoing
            .complete(&1.into(), "response data".to_string());
        assert!(completed);

        // Receiver gets the response
        let response = rx.await.unwrap();
        assert_eq!(response, "response data");
    }

    // Note: Connection::stdio() cannot be tested in unit tests as it requires
    // actual stdin/stdout. It will be tested in E2E tests in Phase 9.
    // However, we can test that StdioTransport can be constructed.
    #[test]
    fn stdio_transport_constructible() {
        // Just verify it compiles and can be constructed
        // We can't actually test I/O without real stdio
        let _transport = StdioTransport::new();
        let _transport_default = StdioTransport::default();
    }

    // =========================================================================
    // Lifecycle Tests
    // =========================================================================

    use crate::{ExitCode, LifecycleState, Notification, ProtocolError};

    #[tokio::test]
    async fn test_initialize_handshake() {
        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let mut client: Connection<_, (), ()> = Connection::new(client_stream);
        let mut server: Connection<_, (), ()> = Connection::new(server_stream);

        // Client sends initialize request
        let init_params = json!({"processId": 1234, "capabilities": {}});
        let init_request =
            Message::Request(Request::new(1, "initialize", Some(init_params.clone())));
        client.sender.send(init_request).await.unwrap();

        // Server waits for initialize
        let (id, params) = server.initialize_start().await.unwrap();
        assert_eq!(id, 1.into());
        assert_eq!(params["processId"], 1234);
        assert_eq!(server.lifecycle_state(), LifecycleState::Initializing);

        // Spawn task to handle server's initialize_finish
        let server_task = tokio::spawn(async move {
            let capabilities = json!({"textDocumentSync": 1});
            server.initialize_finish(id, capabilities).await.unwrap();
            server
        });

        // Client receives InitializeResult
        let response = client.receiver.next().await.unwrap().unwrap();
        assert!(response.is_response());
        if let Message::Response(resp) = response {
            assert_eq!(resp.id, Some(1.into()));
            assert!(resp.result.is_some());
            let result = resp.result.unwrap();
            assert_eq!(result["capabilities"]["textDocumentSync"], 1);
        }

        // Client sends initialized notification
        let initialized = Message::Notification(Notification::new("initialized", None));
        client.sender.send(initialized).await.unwrap();

        // Server's initialize_finish completes
        let server = server_task.await.unwrap();
        assert!(server.is_running());
        assert_eq!(server.lifecycle_state(), LifecycleState::Running);
    }

    #[test]
    fn stdio_connection_alias_constructs_with_custom_metadata() {
        let conn: StdioConnection<String, Response> = Connection::new(StdioTransport::new());

        assert_eq!(conn.lifecycle_state(), LifecycleState::Uninitialized);
        assert!(!conn.request_queue.incoming.is_pending(&1.into()));
        assert!(!conn.request_queue.outgoing.is_pending(&1.into()));
    }

    #[tokio::test]
    async fn test_initialize_rejects_non_init_requests() {
        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let mut client: Connection<_, (), ()> = Connection::new(client_stream);
        let mut server: Connection<_, (), ()> = Connection::new(server_stream);

        // Client sends a non-initialize request first
        let hover_request = Message::Request(Request::new(1, "textDocument/hover", None));
        client.sender.send(hover_request).await.unwrap();

        // Spawn server's initialize_start
        let server_task = tokio::spawn(async move {
            server.initialize_start().await.unwrap();
            server
        });

        // Client receives ServerNotInitialized error
        let response = client.receiver.next().await.unwrap().unwrap();
        assert!(response.is_response());
        if let Message::Response(resp) = response {
            assert_eq!(resp.id, Some(1.into()));
            assert!(resp.error.is_some());
            let error = resp.error.unwrap();
            assert_eq!(error.code, crate::ErrorCode::ServerNotInitialized as i32);
        }

        // Now client sends initialize - should be accepted
        let init_request = Message::Request(Request::new(2, "initialize", None));
        client.sender.send(init_request).await.unwrap();

        let server = server_task.await.unwrap();
        assert_eq!(server.lifecycle_state(), LifecycleState::Initializing);
    }

    #[tokio::test(start_paused = true)]
    async fn test_initialize_finish_times_out_without_initialized() {
        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let mut client: Connection<_, (), ()> = Connection::new(client_stream);
        let mut server: Connection<_, (), ()> = Connection::new(server_stream);

        client
            .sender
            .send(Message::Request(Request::new(1, "initialize", None)))
            .await
            .unwrap();

        let (id, _params) = server.initialize_start().await.unwrap();

        let server_task =
            tokio::spawn(async move { server.initialize_finish(id, json!({})).await });

        let response = client.receiver.next().await.unwrap().unwrap();
        assert!(response.is_response());

        tokio::time::advance(Duration::from_secs(61)).await;

        let result = server_task.await.unwrap();
        assert!(matches!(result, Err(ProtocolError::InitializeTimeout)));
    }

    #[tokio::test]
    async fn test_initialize_drops_notifications() {
        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let mut client: Connection<_, (), ()> = Connection::new(client_stream);
        let mut server: Connection<_, (), ()> = Connection::new(server_stream);

        // Client sends random notification before init
        let random_notif = Message::Notification(Notification::new("textDocument/didOpen", None));
        client.sender.send(random_notif).await.unwrap();

        // Client sends initialize request
        let init_request = Message::Request(Request::new(1, "initialize", None));
        client.sender.send(init_request).await.unwrap();

        // Server's initialize_start should skip notification and find initialize
        let (id, _params) = server.initialize_start().await.unwrap();
        assert_eq!(id, 1.into());
        assert_eq!(server.lifecycle_state(), LifecycleState::Initializing);
    }

    #[tokio::test]
    async fn test_exit_during_init_disconnects() {
        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let mut client: Connection<_, (), ()> = Connection::new(client_stream);
        let mut server: Connection<_, (), ()> = Connection::new(server_stream);

        // Client sends exit notification instead of initialize
        let exit_notif = Message::Notification(Notification::new("exit", None));
        client.sender.send(exit_notif).await.unwrap();

        // Server's initialize_start should return Disconnected
        let result = server.initialize_start().await;
        assert!(matches!(result, Err(ProtocolError::Disconnected)));
    }

    #[tokio::test]
    async fn test_shutdown_then_exit() {
        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let mut client: Connection<_, (), ()> = Connection::new(client_stream);
        let mut server: Connection<_, (), ()> = Connection::new(server_stream);

        // Complete initialization
        let init_request = Message::Request(Request::new(1, "initialize", None));
        client.sender.send(init_request).await.unwrap();

        let (id, _params) = server.initialize_start().await.unwrap();

        let server_task = tokio::spawn(async move {
            server.initialize_finish(id, json!({})).await.unwrap();
            server
        });

        let _ = client.receiver.next().await; // Receive InitializeResult
        let initialized = Message::Notification(Notification::new("initialized", None));
        client.sender.send(initialized).await.unwrap();

        let mut server = server_task.await.unwrap();
        assert!(server.is_running());

        // Client sends shutdown request
        let shutdown_request = Message::Request(Request::new(2, "shutdown", None));
        client.sender.send(shutdown_request).await.unwrap();

        // Server receives and handles shutdown
        let msg = server.receiver.next().await.unwrap().unwrap();
        if let Message::Request(req) = msg {
            assert_eq!(req.method, "shutdown");
            server.handle_shutdown(req.id).await.unwrap();
        } else {
            panic!("Expected shutdown request");
        }

        // Verify shutdown state
        assert!(server.is_shutting_down());
        assert!(server.shutdown_token().is_cancelled());

        // Client receives null response
        let response = client.receiver.next().await.unwrap().unwrap();
        if let Message::Response(resp) = response {
            assert_eq!(resp.id, Some(2.into()));
            assert_eq!(resp.result, Some(serde_json::Value::Null));
        }

        // Client sends exit
        // Server handles exit
        let exit_code = server.handle_exit();
        assert_eq!(exit_code, ExitCode::Success);
        assert_eq!(server.lifecycle_state(), LifecycleState::Exited);
    }

    #[tokio::test]
    async fn test_exit_without_shutdown() {
        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let mut client: Connection<_, (), ()> = Connection::new(client_stream);
        let mut server: Connection<_, (), ()> = Connection::new(server_stream);

        // Complete initialization
        let init_request = Message::Request(Request::new(1, "initialize", None));
        client.sender.send(init_request).await.unwrap();

        let (id, _params) = server.initialize_start().await.unwrap();

        let server_task = tokio::spawn(async move {
            server.initialize_finish(id, json!({})).await.unwrap();
            server
        });

        let _ = client.receiver.next().await;
        let initialized = Message::Notification(Notification::new("initialized", None));
        client.sender.send(initialized).await.unwrap();

        let mut server = server_task.await.unwrap();
        assert!(server.is_running());

        // Server receives exit without shutdown - dirty exit
        let exit_code = server.handle_exit();
        assert_eq!(exit_code, ExitCode::Error);
        assert_eq!(server.lifecycle_state(), LifecycleState::Exited);
    }

    #[tokio::test]
    async fn test_on_shutdown_future() {
        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let mut client: Connection<_, (), ()> = Connection::new(client_stream);
        let mut server: Connection<_, (), ()> = Connection::new(server_stream);

        // Complete initialization
        let init_request = Message::Request(Request::new(1, "initialize", None));
        client.sender.send(init_request).await.unwrap();

        let (id, _params) = server.initialize_start().await.unwrap();

        let server_task = tokio::spawn(async move {
            server.initialize_finish(id, json!({})).await.unwrap();
            server
        });

        let _ = client.receiver.next().await;
        let initialized = Message::Notification(Notification::new("initialized", None));
        client.sender.send(initialized).await.unwrap();

        let mut server = server_task.await.unwrap();

        // Spawn a task waiting on shutdown
        let token = server.shutdown_token();
        let wait_task = tokio::spawn(async move {
            token.cancelled().await;
            "shutdown received"
        });

        // Send shutdown
        let shutdown_request = Message::Request(Request::new(2, "shutdown", None));
        client.sender.send(shutdown_request).await.unwrap();

        let msg = server.receiver.next().await.unwrap().unwrap();
        if let Message::Request(req) = msg {
            server.handle_shutdown(req.id).await.unwrap();
        }

        // The wait task should complete now
        let result = tokio::time::timeout(std::time::Duration::from_millis(100), wait_task)
            .await
            .expect("wait task should complete quickly")
            .unwrap();

        assert_eq!(result, "shutdown received");
    }

    // =========================================================================
    // Routing Tests
    // =========================================================================

    use crate::IncomingMessage;

    #[test]
    fn route_request_returns_incoming_request() {
        let (stream, _) = tokio::io::duplex(4096);
        let mut conn: Connection<_, (), Response> = Connection::new(stream);

        let request = Request::new(42, "textDocument/hover", Some(json!({"line": 10})));
        let message = Message::Request(request);

        let result = conn.route(message);
        match result {
            IncomingMessage::Request(req, token) => {
                assert_eq!(req.id, 42.into());
                assert_eq!(req.method, "textDocument/hover");
                // Token should not be cancelled yet
                assert!(!token.is_cancelled());
                // Request should be auto-registered
                assert!(conn.request_queue.incoming.is_pending(&42.into()));
            }
            _ => panic!("Expected IncomingMessage::Request"),
        }
    }

    #[test]
    fn route_notification_returns_incoming_notification() {
        let (stream, _) = tokio::io::duplex(4096);
        let mut conn: Connection<_, (), Response> = Connection::new(stream);

        let notification = Notification::new(
            "textDocument/didOpen",
            Some(json!({"uri": "file:///test.rs"})),
        );
        let message = Message::Notification(notification);

        let result = conn.route(message);
        match result {
            IncomingMessage::Notification(notif) => {
                assert_eq!(notif.method, "textDocument/didOpen");
            }
            _ => panic!("Expected IncomingMessage::Notification"),
        }
    }

    #[tokio::test]
    async fn route_response_to_pending_outgoing_request() {
        let (stream, _) = tokio::io::duplex(4096);
        let mut conn: Connection<_, (), Response> = Connection::new(stream);

        // Register an outgoing request
        let rx = conn.request_queue.outgoing.register(42.into());

        // Create a matching response
        let response = Response::ok(42, json!({"result": "success"}));
        let message = Message::Response(response);

        // Route it
        let result = conn.route(message);
        assert!(
            matches!(result, IncomingMessage::ResponseRouted),
            "Expected ResponseRouted, got {:?}",
            result
        );

        // Verify receiver got the response
        let received = rx.await.expect("Should receive response");
        assert_eq!(received.id, Some(42.into()));
        assert!(received.result.is_some());
        assert_eq!(received.result.unwrap()["result"], "success");
    }

    #[test]
    fn route_response_for_unknown_id_returns_response_unknown() {
        let (stream, _) = tokio::io::duplex(4096);
        let mut conn: Connection<_, (), Response> = Connection::new(stream);

        // Create a response for an ID that was never registered
        let response = Response::ok(999, json!({"unexpected": true}));
        let message = Message::Response(response);

        let result = conn.route(message);
        match result {
            IncomingMessage::ResponseUnknown(resp) => {
                assert_eq!(resp.id, Some(999.into()));
            }
            _ => panic!("Expected IncomingMessage::ResponseUnknown"),
        }
    }

    #[test]
    fn route_response_with_null_id_returns_response_unknown() {
        let (stream, _) = tokio::io::duplex(4096);
        let mut conn: Connection<_, (), Response> = Connection::new(stream);

        // Create a parse error response (null id)
        let response = Response::parse_error(crate::ResponseError::new(
            crate::ErrorCode::ParseError,
            "Parse error",
        ));
        let message = Message::Response(response);

        let result = conn.route(message);
        match result {
            IncomingMessage::ResponseUnknown(resp) => {
                assert!(
                    resp.id.is_none(),
                    "Expected null id for parse error response"
                );
                assert!(resp.error.is_some());
            }
            _ => panic!("Expected IncomingMessage::ResponseUnknown"),
        }
    }

    #[tokio::test]
    async fn route_response_with_string_id() {
        let (stream, _) = tokio::io::duplex(4096);
        let mut conn: Connection<_, (), Response> = Connection::new(stream);

        // Register with string ID
        let rx = conn.request_queue.outgoing.register("request-abc".into());

        // Create matching response
        let response = Response::ok("request-abc", json!(null));
        let message = Message::Response(response);

        let result = conn.route(message);
        assert!(matches!(result, IncomingMessage::ResponseRouted));

        let received = rx.await.expect("Should receive response");
        assert_eq!(
            received.id,
            Some(crate::RequestId::String("request-abc".to_string()))
        );
    }

    #[tokio::test]
    async fn route_multiple_responses_to_different_requests() {
        let (stream, _) = tokio::io::duplex(4096);
        let mut conn: Connection<_, (), Response> = Connection::new(stream);

        // Register multiple outgoing requests
        let rx1 = conn.request_queue.outgoing.register(1.into());
        let rx2 = conn.request_queue.outgoing.register(2.into());
        let rx3 = conn.request_queue.outgoing.register(3.into());

        // Route responses out of order
        let result2 = conn.route(Message::Response(Response::ok(2, json!("second"))));
        assert!(matches!(result2, IncomingMessage::ResponseRouted));

        let result1 = conn.route(Message::Response(Response::ok(1, json!("first"))));
        assert!(matches!(result1, IncomingMessage::ResponseRouted));

        let result3 = conn.route(Message::Response(Response::ok(3, json!("third"))));
        assert!(matches!(result3, IncomingMessage::ResponseRouted));

        // Verify all receivers got correct responses
        let resp1 = rx1.await.unwrap();
        assert_eq!(resp1.result.unwrap(), json!("first"));

        let resp2 = rx2.await.unwrap();
        assert_eq!(resp2.result.unwrap(), json!("second"));

        let resp3 = rx3.await.unwrap();
        assert_eq!(resp3.result.unwrap(), json!("third"));
    }

    #[test]
    fn route_error_response_to_pending_request() {
        let (stream, _) = tokio::io::duplex(4096);
        let mut conn: Connection<_, (), Response> = Connection::new(stream);

        // Register an outgoing request (we won't await it in this sync test)
        let _rx = conn.request_queue.outgoing.register(42.into());

        // Route an error response
        let response = Response::err(
            42,
            crate::ResponseError::new(crate::ErrorCode::MethodNotFound, "Not found"),
        );
        let message = Message::Response(response);

        let result = conn.route(message);
        assert!(matches!(result, IncomingMessage::ResponseRouted));
    }

    // =========================================================================
    // Cancellation Tests
    // =========================================================================

    #[test]
    #[allow(deprecated)]
    fn register_cancellable_request_creates_child_token() {
        let (stream, _) = tokio::io::duplex(4096);
        let mut conn: Connection<_, String, ()> = Connection::new(stream);

        let token = conn.register_cancellable_request(1.into(), "test".to_string());

        // Request should be pending
        assert!(conn.request_queue.incoming.is_pending(&1.into()));

        // Token should not be cancelled yet
        assert!(!token.is_cancelled());

        // Cancelling shutdown should cancel the child token
        conn.shutdown_token.cancel();
        assert!(token.is_cancelled());
    }

    #[test]
    #[allow(deprecated)]
    fn register_cancellable_request_stores_metadata() {
        let (stream, _) = tokio::io::duplex(4096);
        let mut conn: Connection<_, String, ()> = Connection::new(stream);

        conn.register_cancellable_request(1.into(), "hover_context".to_string());

        let data = conn.request_queue.incoming.complete(&1.into());
        assert_eq!(data, Some("hover_context".to_string()));
    }

    #[test]
    #[allow(deprecated)]
    fn handle_cancel_request_cancels_pending() {
        let (stream, _) = tokio::io::duplex(4096);
        let mut conn: Connection<_, String, ()> = Connection::new(stream);

        // Register a cancellable request
        let token = conn.register_cancellable_request(42.into(), "test".to_string());
        assert!(!token.is_cancelled());

        // Create cancel notification
        let cancel_notif = Notification::new("$/cancelRequest", Some(json!({"id": 42})));

        // Handle it
        let result = conn.handle_cancel_request(&cancel_notif);
        assert_eq!(result, Some(true));

        // Token should be cancelled
        assert!(token.is_cancelled());
    }

    #[test]
    fn handle_cancel_request_unknown_id_returns_false() {
        let (stream, _) = tokio::io::duplex(4096);
        let mut conn: Connection<_, String, ()> = Connection::new(stream);

        let cancel_notif = Notification::new("$/cancelRequest", Some(json!({"id": 999})));

        let result = conn.handle_cancel_request(&cancel_notif);
        assert_eq!(result, Some(false));
    }

    #[test]
    fn handle_cancel_request_wrong_method_returns_none() {
        let (stream, _) = tokio::io::duplex(4096);
        let mut conn: Connection<_, String, ()> = Connection::new(stream);

        let other_notif = Notification::new(
            "textDocument/didOpen",
            Some(json!({"uri": "file:///test.rs"})),
        );

        let result = conn.handle_cancel_request(&other_notif);
        assert_eq!(result, None);
    }

    #[test]
    fn handle_cancel_request_malformed_params_returns_none() {
        let (stream, _) = tokio::io::duplex(4096);
        let mut conn: Connection<_, String, ()> = Connection::new(stream);

        // Missing id field
        let cancel_notif = Notification::new("$/cancelRequest", Some(json!({"other": "field"})));

        let result = conn.handle_cancel_request(&cancel_notif);
        assert_eq!(result, None);
    }

    #[tokio::test]
    #[allow(deprecated)]
    async fn cancellation_propagates_to_spawned_handler() {
        let (stream, _) = tokio::io::duplex(4096);
        let mut conn: Connection<_, String, ()> = Connection::new(stream);

        // Register a cancellable request
        let token = conn.register_cancellable_request(1.into(), "test".to_string());

        // Spawn a handler that waits for cancellation
        let handle = tokio::spawn(async move {
            token.cancelled().await;
            "cancelled"
        });

        // Cancel the request
        let _ = conn.request_queue.incoming.cancel(&1.into());

        // Handler should complete quickly
        let result = tokio::time::timeout(std::time::Duration::from_millis(100), handle)
            .await
            .expect("Handler should complete quickly")
            .unwrap();

        assert_eq!(result, "cancelled");
    }

    #[test]
    fn route_request_auto_registers_and_cancellation_works() {
        let (stream, _) = tokio::io::duplex(4096);
        let mut conn: Connection<_, (), Response> = Connection::new(stream);

        // Route a request
        let request = Request::new(42, "test", None);
        let result = conn.route(Message::Request(request));

        let token = match result {
            IncomingMessage::Request(_, token) => token,
            _ => panic!("Expected IncomingMessage::Request"),
        };

        // Token should not be cancelled
        assert!(!token.is_cancelled());

        // Cancel via cancel_incoming
        let was_cancelled = conn.cancel_incoming(42);
        assert!(was_cancelled);

        // Token should now be cancelled
        assert!(token.is_cancelled());
    }

    // =========================================================================
    // Outgoing Cancel Tests
    // =========================================================================

    #[tokio::test]
    async fn test_cancel_outgoing_request() {
        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let mut client: Connection<_, (), Response> = Connection::new(client_stream);
        let mut server: Connection<_, (), ()> = Connection::new(server_stream);

        // Register an outgoing request on client
        let rx = client.request_queue.outgoing.register(42.into());

        // Cancel it - should send notification and remove from queue
        let was_pending = client.cancel(42).await.unwrap();
        assert!(was_pending);
        assert!(!client.request_queue.outgoing.is_pending(&42.into()));

        // Server should receive the $/cancelRequest notification
        let msg = server.receiver.next().await.unwrap().unwrap();
        assert!(msg.is_notification());
        if let Message::Notification(notif) = msg {
            assert_eq!(notif.method, "$/cancelRequest");
            assert_eq!(notif.params.unwrap()["id"], 42);
        } else {
            panic!("Expected notification");
        }

        // The receiver should get an error (sender dropped)
        assert!(rx.await.is_err());
    }

    #[tokio::test]
    async fn test_cancel_unknown_outgoing_request() {
        let (client_stream, _server_stream) = tokio::io::duplex(4096);
        let mut client: Connection<_, (), Response> = Connection::new(client_stream);

        // Cancel a request that was never registered
        let was_pending = client.cancel(999).await.unwrap();
        assert!(!was_pending);
    }

    #[tokio::test]
    async fn test_cancel_with_string_id() {
        let (client_stream, server_stream) = tokio::io::duplex(4096);
        let mut client: Connection<_, (), Response> = Connection::new(client_stream);
        let mut server: Connection<_, (), ()> = Connection::new(server_stream);

        // Register with string ID
        let rx = client.request_queue.outgoing.register("req-abc".into());

        // Cancel it
        let was_pending = client.cancel("req-abc").await.unwrap();
        assert!(was_pending);

        // Server should receive the notification with string ID
        let msg = server.receiver.next().await.unwrap().unwrap();
        if let Message::Notification(notif) = msg {
            assert_eq!(notif.params.unwrap()["id"], "req-abc");
        } else {
            panic!("Expected notification");
        }

        assert!(rx.await.is_err());
    }
}
