//! Shared E2E test utilities for LSP server testing.
//!
//! Provides a `TestClient` for spawning and communicating with the
//! formatter_server example over stdio using the LSP protocol.

use std::io;
use std::process::Stdio;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::time::timeout;

/// Default timeout for reading messages from the server.
const READ_TIMEOUT: Duration = Duration::from_secs(5);

/// A test client for communicating with an LSP server over stdio.
///
/// Spawns the formatter_server example as a child process and provides
/// methods for sending requests/notifications and reading responses.
pub struct TestClient {
    /// Stdin handle for sending messages to the server.
    stdin: ChildStdin,
    /// Buffered stdout for reading messages from the server.
    stdout: BufReader<ChildStdout>,
    /// The child process handle.
    child: Child,
    /// Atomic counter for generating unique request IDs.
    next_id: AtomicI64,
}

impl TestClient {
    /// Spawns the formatter_server example and returns a test client.
    ///
    /// The server is spawned with piped stdin/stdout for communication
    /// and `kill_on_drop(true)` to prevent zombie processes.
    pub async fn spawn() -> io::Result<Self> {
        let mut child = Command::new("cargo")
            .args(["run", "--example", "formatter_server", "--quiet"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit()) // Show server errors for debugging
            .kill_on_drop(true)
            .spawn()?;

        let stdin = child.stdin.take().expect("Failed to take stdin");
        let stdout = child.stdout.take().expect("Failed to take stdout");
        let stdout = BufReader::new(stdout);

        Ok(Self {
            stdin,
            stdout,
            child,
            next_id: AtomicI64::new(1),
        })
    }

    /// Sends a JSON-RPC request to the server and returns the request ID.
    ///
    /// The request is encoded with Content-Length header and flushed.
    pub async fn send_request(&mut self, method: &str, params: Value) -> io::Result<i64> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params
        });
        self.send_message(&request).await?;
        Ok(id)
    }

    /// Sends a JSON-RPC notification to the server.
    ///
    /// Notifications have no ID and expect no response.
    pub async fn send_notification(&mut self, method: &str, params: Value) -> io::Result<()> {
        let notification = serde_json::json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params
        });
        self.send_message(&notification).await
    }

    /// Sends an LSP message (request or notification) to the server.
    async fn send_message(&mut self, content: &Value) -> io::Result<()> {
        let encoded = encode_lsp_message(content);
        self.stdin.write_all(&encoded).await?;
        self.stdin.flush().await?;
        Ok(())
    }

    /// Reads a single LSP message from the server.
    ///
    /// Parses the Content-Length header, reads the message body,
    /// and returns the parsed JSON value.
    pub async fn read_message(&mut self) -> io::Result<Value> {
        self.read_message_with_timeout(READ_TIMEOUT).await
    }

    /// Reads a message with a custom timeout.
    pub async fn read_message_with_timeout(&mut self, duration: Duration) -> io::Result<Value> {
        timeout(duration, self.read_message_internal())
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "Read timeout"))?
    }

    /// Internal message reading without timeout.
    async fn read_message_internal(&mut self) -> io::Result<Value> {
        // Read Content-Length header
        let mut header_line = String::new();
        self.stdout.read_line(&mut header_line).await?;

        let content_length = parse_content_length(&header_line)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Invalid Content-Length header"))?;

        // Read blank line after header
        let mut blank_line = String::new();
        self.stdout.read_line(&mut blank_line).await?;

        // Read message body
        let mut body = vec![0u8; content_length];
        self.stdout.read_exact(&mut body).await?;

        // Parse JSON
        serde_json::from_slice(&body)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    /// Reads the next response message from the server.
    ///
    /// Skips any notifications (log messages) and returns the first response.
    pub async fn read_response(&mut self) -> io::Result<Value> {
        loop {
            let msg = self.read_message().await?;
            // Responses have an "id" field; notifications have "method" but no "id"
            if msg.get("id").is_some() {
                return Ok(msg);
            }
            // Skip notifications and continue reading
        }
    }

    /// Performs the shutdown/exit sequence.
    ///
    /// Sends shutdown request, waits for response, sends exit notification,
    /// and waits for the server to exit cleanly.
    pub async fn shutdown(&mut self) -> io::Result<()> {
        // Send shutdown request
        self.send_request("shutdown", serde_json::json!(null)).await?;

        // Wait for shutdown response
        let response = self.read_response().await?;
        if response.get("error").is_some() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("Shutdown failed: {:?}", response),
            ));
        }

        // Send exit notification
        self.send_notification("exit", serde_json::json!(null)).await?;

        // Wait for process to exit (with timeout)
        match timeout(Duration::from_secs(5), self.child.wait()).await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(_) => {
                // Timeout - force kill
                self.child.kill().await?;
                Ok(())
            }
        }
    }

    /// Kills the server process immediately.
    #[allow(dead_code)]
    pub async fn kill(&mut self) -> io::Result<()> {
        self.child.kill().await
    }
}

/// Encodes a JSON value as an LSP message with Content-Length header.
pub fn encode_lsp_message(content: &Value) -> Vec<u8> {
    let json = serde_json::to_string(content).expect("Failed to serialize JSON");
    format!("Content-Length: {}\r\n\r\n{}", json.len(), json).into_bytes()
}

/// Parses the Content-Length value from a header line.
///
/// Returns None if the header is invalid or missing.
pub fn parse_content_length(line: &str) -> Option<usize> {
    let line = line.trim();
    let prefix = "Content-Length:";
    if line.to_ascii_lowercase().starts_with(&prefix.to_ascii_lowercase()) {
        let value = line[prefix.len()..].trim();
        value.parse().ok()
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_lsp_message() {
        let content = serde_json::json!({"key": "value"});
        let encoded = encode_lsp_message(&content);
        let expected = b"Content-Length: 15\r\n\r\n{\"key\":\"value\"}";
        assert_eq!(encoded, expected);
    }

    #[test]
    fn test_parse_content_length_valid() {
        assert_eq!(parse_content_length("Content-Length: 123\r\n"), Some(123));
        assert_eq!(parse_content_length("Content-Length:456"), Some(456));
        assert_eq!(parse_content_length("content-length: 789"), Some(789));
    }

    #[test]
    fn test_parse_content_length_invalid() {
        assert_eq!(parse_content_length("Invalid-Header: 123"), None);
        assert_eq!(parse_content_length("Content-Length: abc"), None);
        assert_eq!(parse_content_length(""), None);
    }
}
