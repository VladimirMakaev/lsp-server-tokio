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
//! // Track an incoming request
//! conn.request_queue.incoming.register(1.into(), "textDocument/hover".to_string());
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
    /// let mut queue: RequestQueue<u32, u32> = RequestQueue::new();
    /// queue.incoming.register(1.into(), 100);
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
    pub fn stdio() -> Self {
        Self::new(StdioTransport::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Request, Response};
    use futures::SinkExt;
    use serde_json::json;

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
        let request = Message::Request(Request::new(1, "textDocument/hover", Some(json!({
            "textDocument": {"uri": "file:///test.rs"},
            "position": {"line": 10, "character": 5}
        }))));
        client.sender.send(request).await.unwrap();

        // Server receives request
        let received = server.receiver.next().await.unwrap().unwrap();
        assert!(received.is_request());

        // Server sends response
        let response = Message::Response(Response::ok(1, json!({
            "contents": "fn main()"
        })));
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
        conn.request_queue
            .incoming
            .register(1.into(), "handler_data".to_string());
        assert!(conn.request_queue.incoming.is_pending(&1.into()));

        // Complete it
        let data = conn.request_queue.incoming.complete(&1.into());
        assert_eq!(data, Some("handler_data".to_string()));
    }

    #[tokio::test]
    async fn connection_with_request_queue_test() {
        let (stream, _) = tokio::io::duplex(4096);
        let mut queue: RequestQueue<u32, u32> = RequestQueue::new();
        queue.incoming.register(42.into(), 100);

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
}
