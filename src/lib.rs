#![warn(clippy::all, clippy::pedantic)]

//! # lsp-server-tokio
//!
//! An async-first Rust crate for building LSP (Language Server Protocol) servers using Tokio.
//!
//! This crate provides transport-agnostic async LSP server infrastructure that handles
//! protocol concerns so developers can focus on language-specific logic.
//!
//! ## Quick Start
//!
//! ```
//! use lsp_server_tokio::{duplex_transport, Message, Request, Response};
//! use futures::{SinkExt, StreamExt};
//!
//! # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
//! // Create in-memory transport pair for testing
//! let (mut client, mut server) = duplex_transport(4096);
//!
//! // Send a request from client
//! let request = Message::Request(Request::new(1, "textDocument/hover", None));
//! client.send(request).await.unwrap();
//!
//! // Receive on server
//! if let Some(Ok(Message::Request(req))) = server.next().await {
//!     println!("Received: {}", req.method);
//!
//!     // Send response back
//!     let response = Message::Response(Response::ok(1, serde_json::json!({"contents": "Hello"})));
//!     server.send(response).await.unwrap();
//! }
//!
//! // Receive response on client
//! if let Some(Ok(Message::Response(resp))) = client.next().await {
//!     println!("Got response for id: {:?}", resp.id);
//! }
//! # });
//! ```
//!
//! ## Core Types
//!
//! - [`RequestId`] - Identifies requests/responses (supports both integer and string IDs)
//! - [`ErrorCode`] - LSP specification error codes
//! - [`ResponseError`] - Error responses with code, message, and optional data
//! - [`Message`] - Discriminated union of Request, Response, and Notification
//! - [`Request`] - JSON-RPC request with id and method
//! - [`Response`] - JSON-RPC response with result or error
//! - [`Notification`] - JSON-RPC notification (no id, no response)
//!
//! ## Transport Layer
//!
//! - [`Transport`] - Type alias for `Framed<T, LspCodec>` providing Stream + Sink
//! - [`transport()`] - Factory function wrapping any `AsyncRead` + `AsyncWrite`
//! - [`duplex_transport()`] - Creates connected in-memory transports for testing
//! - [`LspCodec`] - Encoder/Decoder for Content-Length message framing
//!
//! ## Request Routing
//!
//! The [`IncomingMessage`] enum classifies messages received from [`Connection::route()`]:
//! - [`IncomingMessage::Request`] - A request with automatic [`CancellationToken`] for cooperative cancellation
//! - [`IncomingMessage::Notification`] - A notification (no response expected)
//! - [`IncomingMessage::ResponseRouted`] - A response delivered to an awaiting receiver
//! - [`IncomingMessage::ResponseUnknown`] - A response for an unknown request ID

pub mod client_sender;
pub mod codec;
pub mod connection;
pub mod error;
pub mod lifecycle;
pub mod message;
pub mod request_id;
pub mod request_queue;
pub mod routing;
pub mod transport;

pub use client_sender::{ClientSender, SendError};
pub use codec::LspCodec;
pub use connection::{Connection, Receiver, StdioConnection};
pub use error::{ErrorCode, ResponseError};
pub use lifecycle::{ExitCode, LifecycleState, ProtocolError};
pub use message::{Message, Notification, Request, Response, ResponseBody};
pub use request_id::RequestId;
pub use request_queue::{
    parse_cancel_params, IncomingRequests, OutgoingRequests, RequestQueue, CANCEL_REQUEST_METHOD,
};
pub use routing::{cancelled_response, method_not_found_response, IncomingMessage};
pub use transport::{duplex_transport, transport, Transport};

// Re-export CancellationToken for ergonomic use with IncomingMessage::Request
pub use tokio_util::sync::CancellationToken;
