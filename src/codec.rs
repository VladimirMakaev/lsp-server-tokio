//! LSP wire protocol codec for Content-Length framed message I/O.
//!
//! This module provides [`LspCodec`], which implements the tokio-util codec traits
//! for encoding and decoding LSP messages according to the wire protocol specification.
//!
//! # Wire Protocol Format
//!
//! LSP uses HTTP-style Content-Length framing:
//! ```text
//! Content-Length: {byte_count}\r\n\r\n{json_body}
//! ```
//!
//! The Content-Length header specifies the byte length of the UTF-8 encoded JSON body.
//! Headers are ASCII-encoded, followed by a blank line (CRLF CRLF), then the JSON body.
//!
//! # Example
//!
//! ```rust,no_run
//! use bytes::BytesMut;
//! use tokio_util::codec::{Decoder, Encoder};
//! use lsp_server_tokio::{LspCodec, Message, Request};
//!
//! let mut codec = LspCodec::new();
//! let mut buf = BytesMut::new();
//!
//! // Encode a message
//! let msg = Message::Request(Request::new(1, "test", None));
//! codec.encode(msg, &mut buf).unwrap();
//!
//! // Decode it back
//! let decoded = codec.decode(&mut buf).unwrap().unwrap();
//! assert!(decoded.is_request());
//! ```

use bytes::{Buf, BufMut, BytesMut};
use std::io::{self, Write};
use tokio_util::codec::{Decoder, Encoder};

use crate::Message;

/// The header terminator sequence for LSP wire protocol (CRLF CRLF).
const HEADER_TERMINATOR: &[u8] = b"\r\n\r\n";

/// The default maximum Content-Length value (10 MB).
const DEFAULT_MAX_CONTENT_LENGTH: usize = 10 * 1024 * 1024;

/// LSP wire protocol codec implementing Content-Length framing.
///
/// This codec handles encoding and decoding of LSP [`Message`] types using
/// the Content-Length header framing specified by the LSP wire protocol.
///
/// # Encoding
///
/// Messages are serialized to JSON, then prefixed with a Content-Length header:
/// ```text
/// Content-Length: {byte_length}\r\n\r\n{json_body}
/// ```
///
/// # Decoding
///
/// The decoder handles partial reads by maintaining state between calls:
/// - Returns `Ok(None)` if the header is incomplete
/// - Returns `Ok(None)` if the body is incomplete
/// - Returns `Ok(Some(message))` when a complete message is available
///
/// A maximum Content-Length guard (default 10 MB) prevents memory exhaustion
/// from malformed or malicious input.
///
/// # Thread Safety
///
/// `LspCodec` maintains internal parsing state and should not be shared between
/// concurrent readers. Use one codec instance per direction (read/write) or
/// use `Framed` which handles this correctly.
#[derive(Debug)]
pub struct LspCodec {
    /// The content length parsed from headers, None if still reading headers.
    content_length: Option<usize>,
    /// Maximum allowed Content-Length value.
    max_content_length: usize,
}

impl LspCodec {
    /// Creates a new `LspCodec` with the default max content length (10 MB).
    #[must_use]
    pub fn new() -> Self {
        Self {
            content_length: None,
            max_content_length: DEFAULT_MAX_CONTENT_LENGTH,
        }
    }

    /// Creates a new `LspCodec` with a custom max content length.
    ///
    /// # Arguments
    ///
    /// * `max` - Maximum allowed Content-Length in bytes
    #[must_use]
    pub fn with_max_content_length(max: usize) -> Self {
        Self {
            content_length: None,
            max_content_length: max,
        }
    }
}

impl Default for LspCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for LspCodec {
    type Item = Message;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // If we don't have content length yet, parse headers
        if self.content_length.is_none() {
            // Look for header terminator
            let Some(header_end) = find_subsequence(src, HEADER_TERMINATOR) else {
                return Ok(None); // Need more data
            };

            // Parse Content-Length from headers
            let headers = &src[..header_end];
            let content_length = parse_content_length(headers)?;

            // Reject messages that exceed the maximum allowed size
            if content_length > self.max_content_length {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Content-Length {content_length} exceeds maximum {}",
                        self.max_content_length
                    ),
                ));
            }

            // Remove headers from buffer (including terminator)
            src.advance(header_end + HEADER_TERMINATOR.len());
            self.content_length = Some(content_length);
        }

        // Now we have content length, check if body is complete
        let Some(content_length) = self.content_length else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "missing Content-Length state: expected parsed headers before body, received empty decoder state",
            ));
        };
        if src.len() < content_length {
            return Ok(None); // Need more data
        }

        // Extract body and parse
        let body = src.split_to(content_length);
        self.content_length = None; // Reset for next message

        let message: Message = serde_json::from_slice(&body)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(Some(message))
    }
}

impl Encoder<Message> for LspCodec {
    type Error = io::Error;

    fn encode(&mut self, item: Message, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let json =
            serde_json::to_vec(&item).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        // Reserve space for header + body
        // Header format: "Content-Length: {n}\r\n\r\n" (max ~30 bytes for reasonable sizes)
        dst.reserve(32 + json.len());

        // Write header using BufMut::writer() for std::io::Write compatibility
        write!(dst.writer(), "Content-Length: {}\r\n\r\n", json.len())?;

        // Write body
        dst.extend_from_slice(&json);

        Ok(())
    }
}

/// Finds the position of `needle` in `haystack`.
///
/// Returns the index of the first byte of the first occurrence of `needle`
/// within `haystack`, or `None` if not found.
fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// Parses the Content-Length value from HTTP-style headers.
///
/// Headers are expected to be ASCII-encoded with CRLF line endings.
/// The function is case-sensitive for "Content-Length" per the LSP specification,
/// but the LSP spec recommends being lenient with header casing for interoperability.
fn parse_content_length(headers: &[u8]) -> io::Result<usize> {
    // Headers are ASCII, so this is safe
    let headers_str =
        std::str::from_utf8(headers).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    for line in headers_str.split("\r\n") {
        // Case-insensitive match for robustness per LSP spec recommendation
        let line_lower = line.to_ascii_lowercase();
        if line_lower.strip_prefix("content-length:").is_some() {
            // Get the actual value from the original line (after the colon)
            let value = &line["content-length:".len()..];
            return value
                .trim()
                .parse()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e));
        }
    }

    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "Missing Content-Length header",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ErrorCode, Notification, Request, Response, ResponseError};
    use serde_json::json;

    // ============== Encoder Tests ==============

    #[test]
    fn encode_request_test() {
        let mut codec = LspCodec::new();
        let mut buf = BytesMut::new();

        let req = Request::new(1, "test/method", None);
        let msg = Message::Request(req);
        codec.encode(msg, &mut buf).unwrap();

        let output = std::str::from_utf8(&buf).unwrap();

        // Verify header format
        assert!(output.starts_with("Content-Length: "));
        assert!(output.contains("\r\n\r\n"));

        // Split header and body
        let parts: Vec<&str> = output.splitn(2, "\r\n\r\n").collect();
        assert_eq!(parts.len(), 2);

        // Verify body is valid JSON
        let body = parts[1];
        let parsed: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(parsed["method"], "test/method");
        assert_eq!(parsed["id"], 1);
        assert_eq!(parsed["jsonrpc"], "2.0");

        // Verify Content-Length matches body byte length
        let header = parts[0];
        let content_length: usize = header
            .strip_prefix("Content-Length: ")
            .unwrap()
            .parse()
            .unwrap();
        assert_eq!(content_length, body.len());
    }

    #[test]
    fn encode_response_test() {
        let mut codec = LspCodec::new();
        let mut buf = BytesMut::new();

        let resp = Response::ok(42, json!({"result": "value"}));
        let msg = Message::Response(resp);
        codec.encode(msg, &mut buf).unwrap();

        let output = std::str::from_utf8(&buf).unwrap();
        assert!(output.starts_with("Content-Length: "));
        assert!(output.contains("\r\n\r\n"));

        // Verify body
        let body = output.split("\r\n\r\n").nth(1).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(parsed["id"], 42);
        assert!(parsed.get("result").is_some());
    }

    #[test]
    fn encode_notification_test() {
        let mut codec = LspCodec::new();
        let mut buf = BytesMut::new();

        let notif = Notification::new("textDocument/didOpen", Some(json!({"uri": "file:///test"})));
        let msg = Message::Notification(notif);
        codec.encode(msg, &mut buf).unwrap();

        let output = std::str::from_utf8(&buf).unwrap();
        assert!(output.starts_with("Content-Length: "));

        // Verify body
        let body = output.split("\r\n\r\n").nth(1).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(parsed["method"], "textDocument/didOpen");
        assert!(parsed.get("id").is_none());
    }

    // ============== Decoder Tests ==============

    #[test]
    fn decode_complete_message_test() {
        let mut codec = LspCodec::new();
        let mut buf = BytesMut::new();

        let json_body = r#"{"jsonrpc":"2.0","id":1,"method":"test"}"#;
        let framed = format!("Content-Length: {}\r\n\r\n{}", json_body.len(), json_body);
        buf.extend_from_slice(framed.as_bytes());

        let msg = codec.decode(&mut buf).unwrap().unwrap();
        assert!(msg.is_request());

        if let Message::Request(req) = msg {
            assert_eq!(req.method, "test");
        }
    }

    #[test]
    fn decode_partial_header_test() {
        let mut codec = LspCodec::new();
        let mut buf = BytesMut::new();

        // Feed partial header
        buf.extend_from_slice(b"Content-Length: ");
        assert!(codec.decode(&mut buf).unwrap().is_none());

        // Feed more header
        buf.extend_from_slice(b"40\r\n");
        assert!(codec.decode(&mut buf).unwrap().is_none());

        // Complete header
        buf.extend_from_slice(b"\r\n");
        assert!(codec.decode(&mut buf).unwrap().is_none()); // Still no body

        // Now add body
        let json_body = r#"{"jsonrpc":"2.0","id":1,"method":"test"}"#;
        assert_eq!(json_body.len(), 40);
        buf.extend_from_slice(json_body.as_bytes());

        let msg = codec.decode(&mut buf).unwrap().unwrap();
        assert!(msg.is_request());
    }

    #[test]
    fn decode_partial_body_test() {
        let mut codec = LspCodec::new();
        let mut buf = BytesMut::new();

        let json_body = r#"{"jsonrpc":"2.0","id":1,"method":"test"}"#;

        // Feed complete header but partial body
        buf.extend_from_slice(format!("Content-Length: {}\r\n\r\n", json_body.len()).as_bytes());
        buf.extend_from_slice(&json_body.as_bytes()[..20]);
        assert!(codec.decode(&mut buf).unwrap().is_none());

        // Feed remaining body
        buf.extend_from_slice(&json_body.as_bytes()[20..]);
        let msg = codec.decode(&mut buf).unwrap().unwrap();
        assert!(msg.is_request());
    }

    #[test]
    fn decode_multiple_messages_test() {
        let mut codec = LspCodec::new();
        let mut buf = BytesMut::new();

        // Add two complete messages
        let json1 = r#"{"jsonrpc":"2.0","id":1,"method":"first"}"#;
        let json2 = r#"{"jsonrpc":"2.0","id":2,"method":"second"}"#;

        buf.extend_from_slice(
            format!("Content-Length: {}\r\n\r\n{}", json1.len(), json1).as_bytes(),
        );
        buf.extend_from_slice(
            format!("Content-Length: {}\r\n\r\n{}", json2.len(), json2).as_bytes(),
        );

        // Decode first
        let msg1 = codec.decode(&mut buf).unwrap().unwrap();
        if let Message::Request(req) = msg1 {
            assert_eq!(req.method, "first");
        } else {
            panic!("Expected request");
        }

        // Buffer should still contain second message
        assert!(!buf.is_empty());

        // Decode second
        let msg2 = codec.decode(&mut buf).unwrap().unwrap();
        if let Message::Request(req) = msg2 {
            assert_eq!(req.method, "second");
        } else {
            panic!("Expected request");
        }

        // Buffer should be empty
        assert!(buf.is_empty());
    }

    #[test]
    fn encode_decode_roundtrip_test() {
        let mut codec = LspCodec::new();
        let mut buf = BytesMut::new();

        // Create various message types
        let request = Message::Request(Request::new(
            123,
            "textDocument/completion",
            Some(json!({"position": {"line": 10}})),
        ));
        let response = Message::Response(Response::ok(456, json!({"items": []})));
        let notification = Message::Notification(Notification::new("textDocument/didSave", None));

        // Encode all
        codec.encode(request.clone(), &mut buf).unwrap();
        codec.encode(response.clone(), &mut buf).unwrap();
        codec.encode(notification.clone(), &mut buf).unwrap();

        // Decode and verify
        let decoded_request = codec.decode(&mut buf).unwrap().unwrap();
        assert!(decoded_request.is_request());
        if let (Message::Request(orig), Message::Request(dec)) = (&request, &decoded_request) {
            assert_eq!(orig.id, dec.id);
            assert_eq!(orig.method, dec.method);
        }

        let decoded_response = codec.decode(&mut buf).unwrap().unwrap();
        assert!(decoded_response.is_response());

        let decoded_notification = codec.decode(&mut buf).unwrap().unwrap();
        assert!(decoded_notification.is_notification());

        assert!(buf.is_empty());
    }

    #[test]
    fn content_length_byte_count_test() {
        let mut codec = LspCodec::new();
        let mut buf = BytesMut::new();

        // Create a message with Unicode content
        // The method name contains non-ASCII characters
        // "test/mehod" with a Japanese character (3 bytes in UTF-8)
        let req = Request::new(1, "test/\u{65E5}\u{672C}", None); // "test/日本" - 2 Japanese chars (6 bytes each encoded)
        let msg = Message::Request(req);
        codec.encode(msg, &mut buf).unwrap();

        let output = std::str::from_utf8(&buf).unwrap();

        // Split header and body
        let parts: Vec<&str> = output.splitn(2, "\r\n\r\n").collect();
        let header = parts[0];
        let body = parts[1];

        // Extract Content-Length
        let content_length: usize = header
            .strip_prefix("Content-Length: ")
            .unwrap()
            .parse()
            .unwrap();

        // Content-Length should be BYTE count, not character count
        assert_eq!(content_length, body.len());

        // Verify the body contains more bytes than characters due to UTF-8 encoding
        assert!(body.len() > body.chars().count());
    }

    #[test]
    fn case_insensitive_header_parsing() {
        let mut codec = LspCodec::new();
        let mut buf = BytesMut::new();

        // Use lowercase header (some clients might send this)
        let json_body = r#"{"jsonrpc":"2.0","id":1,"method":"test"}"#;
        let framed = format!("content-length: {}\r\n\r\n{}", json_body.len(), json_body);
        buf.extend_from_slice(framed.as_bytes());

        let msg = codec.decode(&mut buf).unwrap().unwrap();
        assert!(msg.is_request());
    }

    #[test]
    fn response_error_roundtrip() {
        let mut codec = LspCodec::new();
        let mut buf = BytesMut::new();

        let error = ResponseError::new(ErrorCode::MethodNotFound, "Method not found");
        let resp = Message::Response(Response::err(1, error));
        codec.encode(resp, &mut buf).unwrap();

        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        if let Message::Response(r) = decoded {
            assert!(r.error().is_some());
            assert_eq!(r.into_error().unwrap().code, -32601);
        } else {
            panic!("Expected response");
        }
    }

    #[test]
    fn decode_invalid_json_returns_error() {
        let mut codec = LspCodec::new();
        let mut buf = BytesMut::new();

        let invalid_json = "{ not valid json }";
        let framed = format!(
            "Content-Length: {}\r\n\r\n{}",
            invalid_json.len(),
            invalid_json
        );
        buf.extend_from_slice(framed.as_bytes());

        let result = codec.decode(&mut buf);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn decode_missing_content_length_returns_error() {
        let mut codec = LspCodec::new();
        let mut buf = BytesMut::new();

        // No Content-Length header
        let framed = "Some-Other-Header: value\r\n\r\n{}";
        buf.extend_from_slice(framed.as_bytes());

        let result = codec.decode(&mut buf);
        assert!(result.is_err());
    }

    // ============== Content-Length Guard Tests ==============

    #[test]
    fn decode_at_max_limit_passes() {
        let json_body = r#"{"jsonrpc":"2.0","id":1,"method":"test"}"#;
        // Set max to exactly the body size
        let mut codec = LspCodec::with_max_content_length(json_body.len());
        let mut buf = BytesMut::new();

        let framed = format!("Content-Length: {}\r\n\r\n{}", json_body.len(), json_body);
        buf.extend_from_slice(framed.as_bytes());

        let msg = codec.decode(&mut buf).unwrap().unwrap();
        assert!(msg.is_request());
    }

    #[test]
    fn decode_over_max_limit_rejected() {
        let json_body = r#"{"jsonrpc":"2.0","id":1,"method":"test"}"#;
        // Set max to one less than body size
        let mut codec = LspCodec::with_max_content_length(json_body.len() - 1);
        let mut buf = BytesMut::new();

        let framed = format!("Content-Length: {}\r\n\r\n{}", json_body.len(), json_body);
        buf.extend_from_slice(framed.as_bytes());

        let result = codec.decode(&mut buf);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("exceeds maximum"));
    }

    #[test]
    fn decode_custom_limit_works() {
        // Very small custom limit
        let mut codec = LspCodec::with_max_content_length(10);
        let mut buf = BytesMut::new();

        let json_body = r#"{"jsonrpc":"2.0","id":1,"method":"test"}"#;
        let framed = format!("Content-Length: {}\r\n\r\n{}", json_body.len(), json_body);
        buf.extend_from_slice(framed.as_bytes());

        let result = codec.decode(&mut buf);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("exceeds maximum"));
    }

    #[test]
    fn default_max_content_length_is_10mb() {
        let codec = LspCodec::new();
        assert_eq!(codec.max_content_length, 10 * 1024 * 1024);

        let codec_default = LspCodec::default();
        assert_eq!(codec_default.max_content_length, 10 * 1024 * 1024);
    }
}
