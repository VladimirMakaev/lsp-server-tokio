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

pub mod error;
pub mod request_id;

pub use error::{ErrorCode, ResponseError};
pub use request_id::RequestId;
