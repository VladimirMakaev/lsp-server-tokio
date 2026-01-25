//! Transport abstraction for LSP message I/O.
//!
//! This module provides [`Transport`], a type alias for bidirectional LSP message
//! passing over any async I/O stream, plus factory functions for creating transports.
//!
//! # Overview
//!
//! The transport layer wraps any `AsyncRead + AsyncWrite` stream with [`LspCodec`]
//! to provide a `Stream<Item=Result<Message, io::Error>>` for receiving messages
//! and a `Sink<Message, Error=io::Error>` for sending messages.
//!
//! # Example
//!
//! ```ignore
//! use futures::{SinkExt, StreamExt};
//! use lsp_server_tokio::{transport, Message, Request};
//!
//! // Wrap any async I/O stream
//! let transport = transport(io_stream);
//!
//! // Receive messages
//! while let Some(msg) = transport.next().await {
//!     match msg? {
//!         Message::Request(req) => { /* handle request */ }
//!         Message::Response(resp) => { /* handle response */ }
//!         Message::Notification(notif) => { /* handle notification */ }
//!     }
//! }
//! ```
//!
//! # Testing
//!
//! For testing without real I/O, use [`duplex_transport`] to create a pair of
//! connected in-memory transports:
//!
//! ```ignore
//! let (mut client, mut server) = duplex_transport(1024);
//!
//! // Messages sent on one side are received by the other
//! client.send(request).await?;
//! let received = server.next().await.unwrap()?;
//! ```

use tokio::io::{AsyncRead, AsyncWrite, DuplexStream};
use tokio_util::codec::Framed;

use crate::LspCodec;

/// A bidirectional LSP transport over any async I/O stream.
///
/// This type provides a `Stream<Item=Result<Message, io::Error>>` for receiving
/// messages and a `Sink<Message, Error=io::Error>` for sending messages.
///
/// The transport handles Content-Length based message framing per the LSP
/// wire protocol specification via the underlying [`LspCodec`].
///
/// # Type Parameter
///
/// * `T` - The underlying I/O stream type, must implement `AsyncRead + AsyncWrite`
///
/// # Example
///
/// ```ignore
/// use futures::{SinkExt, StreamExt};
/// use lsp_server_tokio::{transport, Message, Request};
///
/// async fn handle_connection<T: AsyncRead + AsyncWrite + Unpin>(io: T) {
///     let mut transport = transport(io);
///
///     // Receive messages
///     while let Some(result) = transport.next().await {
///         let msg = result.expect("I/O error");
///         // Process message...
///     }
/// }
/// ```
pub type Transport<T> = Framed<T, LspCodec>;

/// Creates an LSP transport from an async I/O stream.
///
/// The returned transport wraps the stream with [`LspCodec`] for
/// Content-Length based message framing per the LSP specification.
///
/// # Arguments
///
/// * `io` - Any async I/O stream implementing `AsyncRead + AsyncWrite`
///
/// # Returns
///
/// A [`Transport`] providing `Stream` and `Sink` interfaces for LSP messages.
///
/// # Example
///
/// ```ignore
/// use tokio::net::TcpStream;
/// use lsp_server_tokio::transport;
///
/// let stream = TcpStream::connect("127.0.0.1:8080").await?;
/// let mut transport = transport(stream);
/// ```
pub fn transport<T>(io: T) -> Transport<T>
where
    T: AsyncRead + AsyncWrite,
{
    Framed::new(io, LspCodec::new())
}

/// Creates a pair of connected in-memory transports for testing.
///
/// Messages sent on one transport will be received by the other,
/// enabling bidirectional communication testing without real I/O.
///
/// # Arguments
///
/// * `buffer_size` - Size of the internal buffer in bytes (1024-8192 recommended)
///
/// # Returns
///
/// A tuple of two connected [`Transport`]s. The first transport's output
/// is connected to the second transport's input, and vice versa.
///
/// # Example
///
/// ```ignore
/// use futures::{SinkExt, StreamExt};
/// use lsp_server_tokio::{duplex_transport, Message, Request};
///
/// let (mut client, mut server) = duplex_transport(1024);
///
/// // Send from client
/// let request = Message::Request(Request::new(1, "test/method", None));
/// client.send(request).await?;
///
/// // Receive on server
/// let received = server.next().await.unwrap()?;
/// assert!(received.is_request());
/// ```
pub fn duplex_transport(buffer_size: usize) -> (Transport<DuplexStream>, Transport<DuplexStream>) {
    let (a, b) = tokio::io::duplex(buffer_size);
    (transport(a), transport(b))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Message, Notification, Request, Response};
    use futures::{SinkExt, StreamExt};
    use serde_json::json;

    // ============== Integration Tests ==============

    #[tokio::test]
    async fn duplex_request_response_test() {
        let (mut client, mut server) = duplex_transport(4096);

        // Client sends request
        let request = Message::Request(Request::new(1, "textDocument/hover", Some(json!({
            "textDocument": {"uri": "file:///test.rs"},
            "position": {"line": 10, "character": 5}
        }))));
        client.send(request).await.expect("send request");

        // Server receives request
        let received = server.next().await.expect("receive").expect("decode");
        assert!(received.is_request());
        if let Message::Request(req) = &received {
            assert_eq!(req.method, "textDocument/hover");
            assert_eq!(req.id, 1.into());
        }

        // Server sends response
        let response = Message::Response(Response::ok(1, json!({
            "contents": "fn main()"
        })));
        server.send(response).await.expect("send response");

        // Client receives response
        let received = client.next().await.expect("receive").expect("decode");
        assert!(received.is_response());
        if let Message::Response(resp) = &received {
            assert_eq!(resp.id, Some(1.into()));
            assert!(resp.result.is_some());
        }
    }

    #[tokio::test]
    async fn duplex_notification_test() {
        let (mut client, mut server) = duplex_transport(4096);

        // Client sends notification (no id, no response expected)
        let notification = Message::Notification(Notification::new(
            "textDocument/didOpen",
            Some(json!({
                "textDocument": {
                    "uri": "file:///test.rs",
                    "languageId": "rust",
                    "version": 1,
                    "text": "fn main() {}"
                }
            })),
        ));
        client.send(notification).await.expect("send notification");

        // Server receives notification
        let received = server.next().await.expect("receive").expect("decode");
        assert!(received.is_notification());
        if let Message::Notification(notif) = &received {
            assert_eq!(notif.method, "textDocument/didOpen");
        }

        // No response expected for notifications
    }

    #[tokio::test]
    async fn duplex_multiple_messages_test() {
        let (mut client, mut server) = duplex_transport(4096);

        // Send 3 messages in sequence
        let msg1 = Message::Request(Request::new(1, "first", None));
        let msg2 = Message::Request(Request::new(2, "second", None));
        let msg3 = Message::Request(Request::new(3, "third", None));

        client.send(msg1).await.expect("send 1");
        client.send(msg2).await.expect("send 2");
        client.send(msg3).await.expect("send 3");

        // Receive all 3 on server - order must be preserved
        let recv1 = server.next().await.expect("receive 1").expect("decode 1");
        let recv2 = server.next().await.expect("receive 2").expect("decode 2");
        let recv3 = server.next().await.expect("receive 3").expect("decode 3");

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
    async fn duplex_bidirectional_concurrent_test() {
        let (mut client, mut server) = duplex_transport(4096);

        // Send from both sides concurrently
        let client_msg = Message::Request(Request::new(1, "client/message", None));
        let server_msg = Message::Notification(Notification::new("server/notification", None));

        let (client_send, server_send) = tokio::join!(
            client.send(client_msg),
            server.send(server_msg)
        );

        client_send.expect("client send");
        server_send.expect("server send");

        // Receive on both sides
        let (from_client, from_server) = tokio::join!(
            server.next(),
            client.next()
        );

        // Server receives client's message
        let from_client = from_client.expect("receive from client").expect("decode");
        assert!(from_client.is_request());
        if let Message::Request(r) = from_client {
            assert_eq!(r.method, "client/message");
        }

        // Client receives server's message
        let from_server = from_server.expect("receive from server").expect("decode");
        assert!(from_server.is_notification());
        if let Message::Notification(n) = from_server {
            assert_eq!(n.method, "server/notification");
        }
    }

    #[tokio::test]
    async fn large_message_test() {
        let (mut client, mut server) = duplex_transport(16384);

        // Create a message with large params (~10KB of data)
        let large_data: String = "x".repeat(10_000);
        let request = Message::Request(Request::new(42, "large/data", Some(json!({
            "content": large_data.clone()
        }))));

        client.send(request).await.expect("send large message");

        // Receive and verify content integrity
        let received = server.next().await.expect("receive").expect("decode");
        assert!(received.is_request());
        if let Message::Request(req) = received {
            assert_eq!(req.method, "large/data");
            let params = req.params.expect("has params");
            let content = params["content"].as_str().expect("content is string");
            assert_eq!(content.len(), 10_000);
            assert_eq!(content, large_data);
        }
    }

    #[tokio::test]
    async fn response_with_error_test() {
        use crate::{ErrorCode, ResponseError};

        let (mut client, mut server) = duplex_transport(4096);

        // Client sends request
        let request = Message::Request(Request::new(1, "unknown/method", None));
        client.send(request).await.expect("send request");

        // Server receives and sends error response
        let _received = server.next().await.expect("receive").expect("decode");

        let error = ResponseError::new(ErrorCode::MethodNotFound, "Method not found");
        let response = Message::Response(Response::err(1, error));
        server.send(response).await.expect("send error response");

        // Client receives error response
        let received = client.next().await.expect("receive").expect("decode");
        assert!(received.is_response());
        if let Message::Response(resp) = received {
            assert_eq!(resp.id, Some(1.into()));
            assert!(resp.error.is_some());
            let err = resp.error.unwrap();
            assert_eq!(err.code, -32601); // MethodNotFound
            assert_eq!(err.message, "Method not found");
        }
    }
}
