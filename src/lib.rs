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
//! ```no_run
//! use futures::StreamExt;
//! use lsp_server_tokio::{Connection, IncomingMessage, Response};
//!
//! # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
//! let mut conn = Connection::stdio();
//! let capabilities = serde_json::json!({
//!     "documentFormattingProvider": true
//! });
//!
//! let _client_params = conn.initialize(capabilities).await?;
//! let sender = conn.client_sender();
//!
//! while let Some(result) = conn.receiver.next().await {
//!     let msg = result?;
//!
//!     match conn.route(msg) {
//!         IncomingMessage::Request(req, _) if req.method == "shutdown" => {
//!             conn.handle_shutdown(req.id)?;
//!         }
//!         IncomingMessage::Request(req, _) => {
//!             sender.respond(Response::ok(req.id, serde_json::Value::Null))?;
//!         }
//!         IncomingMessage::Notification(notif) if notif.method == "exit" => {
//!             break;
//!         }
//!         IncomingMessage::CancelHandled => {}
//!         IncomingMessage::Notification(_) => {}
//!         IncomingMessage::ResponseRouted | IncomingMessage::ResponseUnknown(_) => {}
//!         _ => {}
//!     }
//! }
//! # Ok::<(), Box<dyn std::error::Error>>(()) });
//! ```
//!
//! ## Testing
//!
//! Use [`duplex_transport()`] when you want connected in-memory transports for unit
//! and integration tests without stdio:
//!
//! ```
//! use futures::{SinkExt, StreamExt};
//! use lsp_server_tokio::{duplex_transport, Message, Request, Response};
//!
//! # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
//! let (mut client, mut server) = duplex_transport(4096);
//!
//! client
//!     .send(Message::Request(Request::new(1, "textDocument/hover", None)))
//!     .await
//!     .unwrap();
//!
//! if let Some(Ok(Message::Request(req))) = server.next().await {
//!     server
//!         .send(Message::Response(Response::ok(
//!             req.id,
//!             serde_json::json!({"contents": "Hello"}),
//!         )))
//!         .await
//!         .unwrap();
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
//! - [`IncomingMessage::CancelHandled`] - A `$/cancelRequest` that was applied automatically
//! - [`IncomingMessage::ResponseRouted`] - A response delivered to an awaiting receiver
//! - [`IncomingMessage::ResponseUnknown`] - A response for an unknown request ID

mod client_sender;
mod codec;
mod connection;
mod error;
mod lifecycle;
mod message;
mod request_id;
mod request_queue;
mod routing;
mod transport;

pub use client_sender::{ClientSender, SendError};
pub use codec::LspCodec;
pub use connection::{Connection, Receiver, StdioConnection, StdioTransport};
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
