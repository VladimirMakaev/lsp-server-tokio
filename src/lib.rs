//! # lsp-server-tokio
//!
//! An async-first Rust crate for building LSP (Language Server Protocol) servers using Tokio.
//!
//! This crate provides transport-agnostic async LSP server infrastructure that handles
//! protocol concerns so developers can focus on language-specific logic.
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
//! - [`transport()`] - Factory function wrapping any AsyncRead + AsyncWrite
//! - [`duplex_transport`] - Creates connected in-memory transports for testing
//! - [`LspCodec`] - Encoder/Decoder for Content-Length message framing

pub mod codec;
pub mod transport;
pub mod error;
pub mod message;
pub mod request_id;

pub use codec::LspCodec;
pub use error::{ErrorCode, ResponseError};
pub use message::{Message, Notification, Request, Response};
pub use request_id::RequestId;
pub use transport::{duplex_transport, transport, Transport};
