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
