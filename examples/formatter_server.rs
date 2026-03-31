//! Example LSP server demonstrating lsp-server-tokio features.
//!
//! This server provides document formatting that capitalizes all text.
//! It demonstrates:
//!
//! - Full LSP lifecycle (initialize/initialized/shutdown/exit)
//! - Document synchronization (didOpen, didChange, didClose)
//! - Request handling with typed lsp-types structures
//! - Cancellation support for long-running requests
//! - Server-to-client logging via window/logMessage
//!
//! # Usage
//!
//! Run the server and communicate via stdin/stdout using the LSP protocol:
//!
//! ```bash
//! cargo run --example formatter_server
//! ```
//!
//! The server expects LSP messages with Content-Length headers on stdin
//! and writes LSP responses to stdout.

use std::collections::HashMap;

use futures::StreamExt;
use lsp_server_tokio::{
    cancelled_response, method_not_found_response, ClientSender, Connection, IncomingMessage,
    Notification, Response,
};
use lsp_types::{
    DidChangeTextDocumentParams, DidCloseTextDocumentParams, DidOpenTextDocumentParams,
    DocumentFormattingParams, InitializeParams, LogMessageParams, MessageType, Position, Range,
    ServerCapabilities, TextDocumentSyncCapability, TextDocumentSyncKind, TextEdit, Uri,
};
use tokio_util::sync::CancellationToken;

/// Tracks the state of an open document.
#[derive(Debug, Clone)]
struct DocumentState {
    /// The full content of the document.
    content: String,
    /// The document version for ordering edits.
    version: i32,
}

/// Holds all server state during a session.
struct ServerState {
    /// Map of document URIs to their current state.
    documents: HashMap<Uri, DocumentState>,
}

impl ServerState {
    fn new() -> Self {
        Self {
            documents: HashMap::new(),
        }
    }
}

#[tokio::main]
async fn main() {
    // Create a connection over stdio for LSP communication
    let mut conn: Connection<_, String> =
        Connection::new(lsp_server_tokio::connection::StdioTransport::new());

    // Perform LSP initialization handshake
    let capabilities = match serde_json::to_value(server_capabilities()) {
        Ok(capabilities) => capabilities,
        Err(err) => {
            eprintln!(
                "Failed to serialize server capabilities: expected JSON value, received serialization error: {err}"
            );
            return;
        }
    };

    match conn.initialize(capabilities).await {
        Ok(params) => {
            let sender = conn.client_sender().expect("client sender should be available");

            // Parse client's initialize params to understand client capabilities
            if let Ok(init_params) = serde_json::from_value::<InitializeParams>(params) {
                log_message(
                    &sender,
                    MessageType::INFO,
                    format!("Client initialized: {:?}", init_params.client_info),
                );
            }

            run_server(conn, sender).await;
        }
        Err(e) => {
            eprintln!("Initialization failed: {:?}", e);
        }
    }
}

async fn run_server<T>(mut conn: Connection<T, String>, sender: ClientSender)
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    // Initialize server state
    let mut state = ServerState::new();

    log_message(
        &sender,
        MessageType::INFO,
        "Formatter server ready".to_string(),
    );

    // Main message loop
    while let Some(result) = conn.receiver.next().await {
        let msg = match result {
            Ok(msg) => msg,
            Err(e) => {
                eprintln!("Error receiving message: {:?}", e);
                break;
            }
        };

        // Route the message and handle it
        match conn.route(msg) {
            IncomingMessage::Request(req, token) => {
                // Handle shutdown specially
                if req.method == "shutdown" {
                    conn.handle_shutdown(req.id).await.ok();
                    continue;
                }

                // Token is now automatically provided by route()
                let response = handle_request(&sender, &mut state, &req, token).await;
                if let Err(e) = sender.respond(response) {
                    eprintln!("Error sending response: {:?}", e);
                    break;
                }

                // Complete the request in the queue
                conn.request_queue.incoming.complete(&req.id);
            }
            IncomingMessage::Notification(notif) => {
                // Handle exit notification
                if notif.method == "exit" {
                    let exit_code = conn.handle_exit();
                    std::process::exit(exit_code as i32);
                }

                // Handle other notifications
                handle_notification(&mut state, &notif);
            }
            IncomingMessage::CancelHandled => {}
            IncomingMessage::ResponseRouted => {
                // Response was delivered to awaiting receiver
            }
            IncomingMessage::ResponseUnknown(resp) => {
                eprintln!("Received unexpected response: {:?}", resp.id);
            }
        }
    }
}

/// Returns the server capabilities advertised to the client.
///
/// This server supports:
/// - Text document sync with incremental changes
/// - Document formatting
fn server_capabilities() -> ServerCapabilities {
    ServerCapabilities {
        text_document_sync: Some(TextDocumentSyncCapability::Kind(
            TextDocumentSyncKind::INCREMENTAL,
        )),
        document_formatting_provider: Some(lsp_types::OneOf::Left(true)),
        ..Default::default()
    }
}

/// Handles an incoming request and returns a response.
async fn handle_request(
    sender: &ClientSender,
    state: &mut ServerState,
    req: &lsp_server_tokio::Request,
    token: CancellationToken,
) -> Response {
    match req.method.as_str() {
        "textDocument/formatting" => {
            // Parse the formatting params
            let params: DocumentFormattingParams = match req
                .params
                .as_ref()
                .and_then(|p| serde_json::from_value(p.clone()).ok())
            {
                Some(p) => p,
                None => {
                    return Response::err(
                        req.id.clone(),
                        lsp_server_tokio::ResponseError::new(
                            lsp_server_tokio::ErrorCode::InvalidParams,
                            "Invalid formatting params",
                        ),
                    );
                }
            };

            // Format the document with cancellation support
            match format_document(sender, state, &params, token).await {
                Ok(edits) => match serde_json::to_value(edits) {
                    Ok(edits) => Response::ok(req.id.clone(), edits),
                    Err(err) => Response::err(
                        req.id.clone(),
                        lsp_server_tokio::ResponseError::new(
                            lsp_server_tokio::ErrorCode::InternalError,
                            format!(
                                "Failed to serialize formatting edits: expected JSON value, received serialization error: {err}"
                            ),
                        ),
                    ),
                },
                Err(FormatError::Cancelled) => cancelled_response(req.id.clone()),
                Err(FormatError::DocumentNotFound) => Response::err(
                    req.id.clone(),
                    lsp_server_tokio::ResponseError::new(
                        lsp_server_tokio::ErrorCode::InvalidParams,
                        "Document not found",
                    ),
                ),
            }
        }
        _ => method_not_found_response(req),
    }
}

/// Errors that can occur during formatting.
enum FormatError {
    /// The request was cancelled.
    Cancelled,
    /// The document was not found in the state.
    DocumentNotFound,
}

/// Formats a document by capitalizing all text.
///
/// Includes a 200ms delay to demonstrate cancellation handling.
async fn format_document(
    sender: &ClientSender,
    state: &ServerState,
    params: &DocumentFormattingParams,
    token: CancellationToken,
) -> Result<Vec<TextEdit>, FormatError> {
    let uri = &params.text_document.uri;

    // Get the document content
    let doc = state
        .documents
        .get(uri)
        .ok_or(FormatError::DocumentNotFound)?;

    log_message(
        sender,
        MessageType::LOG,
        format!("Formatting document: {:?}", uri),
    );

    // Simulate a long-running operation (200ms delay)
    // This allows cancellation to be tested
    tokio::select! {
        _ = tokio::time::sleep(std::time::Duration::from_millis(200)) => {}
        _ = token.cancelled() => {
            log_message(sender, MessageType::LOG, "Formatting cancelled".to_string());
            return Err(FormatError::Cancelled);
        }
    }

    // Check cancellation again after the delay
    if token.is_cancelled() {
        return Err(FormatError::Cancelled);
    }

    // Create a text edit that replaces the entire document with uppercase content
    let edits = create_uppercase_edit(&doc.content);

    log_message(
        sender,
        MessageType::LOG,
        format!("Formatting complete: {} edits", edits.len()),
    );

    Ok(edits)
}

/// Creates text edits to replace the entire document with uppercase content.
fn create_uppercase_edit(content: &str) -> Vec<TextEdit> {
    if content.is_empty() {
        return vec![];
    }

    let range = Range {
        start: Position {
            line: 0,
            character: 0,
        },
        end: end_position_utf16(content),
    };

    vec![TextEdit {
        range,
        new_text: content.to_uppercase(),
    }]
}

/// Handles an incoming notification.
fn handle_notification(state: &mut ServerState, notif: &Notification) {
    match notif.method.as_str() {
        "textDocument/didOpen" => {
            if let Some(params) = parse_notification_params::<DidOpenTextDocumentParams>(notif) {
                state.documents.insert(
                    params.text_document.uri,
                    DocumentState {
                        content: params.text_document.text,
                        version: params.text_document.version,
                    },
                );
            }
        }
        "textDocument/didChange" => {
            if let Some(params) = parse_notification_params::<DidChangeTextDocumentParams>(notif) {
                if let Some(doc) = state.documents.get_mut(&params.text_document.uri) {
                    apply_incremental_changes(doc, params.content_changes);
                    doc.version = params.text_document.version;
                }
            }
        }
        "textDocument/didClose" => {
            if let Some(params) = parse_notification_params::<DidCloseTextDocumentParams>(notif) {
                state.documents.remove(&params.text_document.uri);
            }
        }
        "initialized" => {
            // Required by protocol but no action needed
        }
        _ => {
            // Unknown notification, ignore
        }
    }
}

/// Parses notification params into a typed structure.
fn parse_notification_params<T: serde::de::DeserializeOwned>(notif: &Notification) -> Option<T> {
    notif
        .params
        .as_ref()
        .and_then(|p| serde_json::from_value(p.clone()).ok())
}

/// Applies incremental changes to a document.
///
/// Each change can be:
/// - A full replacement (no range): replaces entire content
/// - An incremental edit (with range): replaces specified range
///
/// Note: This implementation uses byte offsets and assumes ASCII content
/// for simplicity. A production server would need proper Unicode handling.
fn apply_incremental_changes(
    doc: &mut DocumentState,
    changes: Vec<lsp_types::TextDocumentContentChangeEvent>,
) {
    for change in changes {
        match change.range {
            Some(range) => {
                // Incremental change: replace the specified range
                let start_offset = position_to_offset(&doc.content, range.start);
                let end_offset = position_to_offset(&doc.content, range.end);

                if start_offset <= end_offset && end_offset <= doc.content.len() {
                    let mut new_content = String::with_capacity(
                        doc.content.len() - (end_offset - start_offset) + change.text.len(),
                    );
                    new_content.push_str(&doc.content[..start_offset]);
                    new_content.push_str(&change.text);
                    new_content.push_str(&doc.content[end_offset..]);
                    doc.content = new_content;
                }
            }
            None => {
                // Full sync fallback: replace entire content
                doc.content = change.text;
            }
        }
    }
}

fn position_to_offset(content: &str, pos: Position) -> usize {
    let line_start = line_start_offset(content, pos.line);
    if line_start >= content.len() {
        return content.len();
    }

    let mut offset = line_start;
    let mut utf16_units = 0u32;

    for ch in content[line_start..].chars() {
        if matches!(ch, '\n' | '\r') {
            break;
        }

        let next_units = utf16_units + ch.len_utf16() as u32;
        if next_units > pos.character {
            break;
        }

        utf16_units = next_units;
        offset += ch.len_utf8();
    }

    offset
}

fn line_start_offset(content: &str, target_line: u32) -> usize {
    let bytes = content.as_bytes();
    let mut offset = 0;
    let mut current_line = 0;

    while offset < bytes.len() && current_line < target_line {
        match bytes[offset] {
            b'\n' => {
                offset += 1;
                current_line += 1;
            }
            b'\r' => {
                offset += 1;
                if offset < bytes.len() && bytes[offset] == b'\n' {
                    offset += 1;
                }
                current_line += 1;
            }
            _ => {
                offset += content[offset..]
                    .chars()
                    .next()
                    .map_or(1, |ch| ch.len_utf8());
            }
        }
    }

    if current_line == target_line {
        offset
    } else {
        content.len()
    }
}

fn end_position_utf16(content: &str) -> Position {
    let bytes = content.as_bytes();
    let mut offset = 0;
    let mut position = Position {
        line: 0,
        character: 0,
    };

    while offset < bytes.len() {
        match bytes[offset] {
            b'\n' => {
                offset += 1;
                position.line += 1;
                position.character = 0;
            }
            b'\r' => {
                offset += 1;
                if offset < bytes.len() && bytes[offset] == b'\n' {
                    offset += 1;
                }
                position.line += 1;
                position.character = 0;
            }
            _ => {
                let ch = match content[offset..].chars().next() {
                    Some(ch) => ch,
                    None => break,
                };
                position.character += ch.len_utf16() as u32;
                offset += ch.len_utf8();
            }
        }
    }

    position
}

/// Sends a window/logMessage notification to the client.
fn log_message(sender: &ClientSender, typ: MessageType, message: String) {
    let params = LogMessageParams { typ, message };
    // Ignore errors - logging is best-effort
    let value = match serde_json::to_value(params) {
        Ok(value) => value,
        Err(err) => {
            eprintln!(
                "Failed to serialize log message params: expected JSON value, received serialization error: {err}"
            );
            return;
        }
    };
    let _ = sender.notify("window/logMessage", Some(value));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_position_to_offset_single_line() {
        let content = "hello world";
        assert_eq!(
            position_to_offset(
                content,
                Position {
                    line: 0,
                    character: 0
                }
            ),
            0
        );
        assert_eq!(
            position_to_offset(
                content,
                Position {
                    line: 0,
                    character: 5
                }
            ),
            5
        );
        assert_eq!(
            position_to_offset(
                content,
                Position {
                    line: 0,
                    character: 11
                }
            ),
            11
        );
    }

    #[test]
    fn test_position_to_offset_multi_line() {
        let content = "line1\nline2\nline3";
        assert_eq!(
            position_to_offset(
                content,
                Position {
                    line: 0,
                    character: 0
                }
            ),
            0
        );
        assert_eq!(
            position_to_offset(
                content,
                Position {
                    line: 1,
                    character: 0
                }
            ),
            6
        );
        assert_eq!(
            position_to_offset(
                content,
                Position {
                    line: 2,
                    character: 0
                }
            ),
            12
        );
        assert_eq!(
            position_to_offset(
                content,
                Position {
                    line: 2,
                    character: 5
                }
            ),
            17
        );
    }

    #[test]
    fn test_position_to_offset_beyond_content() {
        let content = "short";
        // Beyond last character on line
        assert_eq!(
            position_to_offset(
                content,
                Position {
                    line: 0,
                    character: 100
                }
            ),
            5
        );
        // Beyond last line
        assert_eq!(
            position_to_offset(
                content,
                Position {
                    line: 10,
                    character: 0
                }
            ),
            5
        );
    }

    #[test]
    fn test_create_uppercase_edit_empty() {
        let edits = create_uppercase_edit("");
        assert!(edits.is_empty());
    }

    #[test]
    fn test_create_uppercase_edit_single_line() {
        let edits = create_uppercase_edit("hello world");
        assert_eq!(edits.len(), 1);
        assert_eq!(edits[0].new_text, "HELLO WORLD");
        assert_eq!(edits[0].range.start.line, 0);
        assert_eq!(edits[0].range.start.character, 0);
        assert_eq!(edits[0].range.end.line, 0);
        assert_eq!(edits[0].range.end.character, 11);
    }

    #[test]
    fn test_create_uppercase_edit_multi_line() {
        let edits = create_uppercase_edit("line1\nline2");
        assert_eq!(edits.len(), 1);
        assert_eq!(edits[0].new_text, "LINE1\nLINE2");
        assert_eq!(edits[0].range.end.line, 1);
        assert_eq!(edits[0].range.end.character, 5);
    }

    #[test]
    fn test_apply_incremental_changes_full_sync() {
        let mut doc = DocumentState {
            content: "original".to_string(),
            version: 1,
        };
        let changes = vec![lsp_types::TextDocumentContentChangeEvent {
            range: None,
            range_length: None,
            text: "replaced".to_string(),
        }];
        apply_incremental_changes(&mut doc, changes);
        assert_eq!(doc.content, "replaced");
    }

    #[test]
    fn test_apply_incremental_changes_partial() {
        let mut doc = DocumentState {
            content: "hello world".to_string(),
            version: 1,
        };
        let changes = vec![lsp_types::TextDocumentContentChangeEvent {
            range: Some(Range {
                start: Position {
                    line: 0,
                    character: 6,
                },
                end: Position {
                    line: 0,
                    character: 11,
                },
            }),
            range_length: None,
            text: "rust".to_string(),
        }];
        apply_incremental_changes(&mut doc, changes);
        assert_eq!(doc.content, "hello rust");
    }

    #[test]
    fn test_apply_incremental_changes_insert() {
        let mut doc = DocumentState {
            content: "hello".to_string(),
            version: 1,
        };
        let changes = vec![lsp_types::TextDocumentContentChangeEvent {
            range: Some(Range {
                start: Position {
                    line: 0,
                    character: 5,
                },
                end: Position {
                    line: 0,
                    character: 5,
                },
            }),
            range_length: None,
            text: " world".to_string(),
        }];
        apply_incremental_changes(&mut doc, changes);
        assert_eq!(doc.content, "hello world");
    }
}
