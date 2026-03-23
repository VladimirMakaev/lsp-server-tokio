//! End-to-end tests for the formatter_server example.
//!
//! These tests spawn the real formatter_server binary and communicate
//! with it over stdio using the LSP protocol. They validate the full
//! lifecycle, formatting, cancellation, and multi-document handling.

mod common;

use std::str::FromStr;
use std::time::Duration;

use common::TestClient;
use lsp_types::{
    DidChangeTextDocumentParams, DidCloseTextDocumentParams, DidOpenTextDocumentParams,
    DocumentFormattingParams, FormattingOptions, InitializeParams, Position, Range,
    TextDocumentContentChangeEvent, TextDocumentIdentifier, TextDocumentItem, Uri,
    VersionedTextDocumentIdentifier, WorkspaceFolder,
};
use serde_json::{json, Value};
use tokio::time::timeout;

/// Helper to create InitializeParams for tests.
#[allow(deprecated)]
fn create_initialize_params() -> Value {
    serde_json::to_value(InitializeParams {
        process_id: Some(std::process::id()),
        root_path: None,
        root_uri: None,
        initialization_options: None,
        capabilities: lsp_types::ClientCapabilities::default(),
        trace: None,
        workspace_folders: Some(vec![WorkspaceFolder {
            uri: Uri::from_str("file:///tmp/test-workspace").unwrap(),
            name: "test-workspace".to_string(),
        }]),
        client_info: Some(lsp_types::ClientInfo {
            name: "test-client".to_string(),
            version: Some("1.0.0".to_string()),
        }),
        locale: None,
        work_done_progress_params: Default::default(),
    })
    .unwrap()
}

/// Helper to create didOpen params.
fn create_did_open_params(uri: &str, text: &str) -> Value {
    serde_json::to_value(DidOpenTextDocumentParams {
        text_document: TextDocumentItem {
            uri: Uri::from_str(uri).unwrap(),
            language_id: "plaintext".to_string(),
            version: 1,
            text: text.to_string(),
        },
    })
    .unwrap()
}

/// Helper to create didChange params with incremental change.
fn create_did_change_params(uri: &str, version: i32, range: Range, text: &str) -> Value {
    serde_json::to_value(DidChangeTextDocumentParams {
        text_document: VersionedTextDocumentIdentifier {
            uri: Uri::from_str(uri).unwrap(),
            version,
        },
        content_changes: vec![TextDocumentContentChangeEvent {
            range: Some(range),
            range_length: None,
            text: text.to_string(),
        }],
    })
    .unwrap()
}

/// Helper to create didClose params.
fn create_did_close_params(uri: &str) -> Value {
    serde_json::to_value(DidCloseTextDocumentParams {
        text_document: TextDocumentIdentifier {
            uri: Uri::from_str(uri).unwrap(),
        },
    })
    .unwrap()
}

/// Helper to create formatting params.
fn create_formatting_params(uri: &str) -> Value {
    serde_json::to_value(DocumentFormattingParams {
        text_document: TextDocumentIdentifier {
            uri: Uri::from_str(uri).unwrap(),
        },
        options: FormattingOptions {
            tab_size: 4,
            insert_spaces: true,
            properties: Default::default(),
            trim_trailing_whitespace: None,
            insert_final_newline: None,
            trim_final_newlines: None,
        },
        work_done_progress_params: Default::default(),
    })
    .unwrap()
}

/// Performs the initialize handshake with the server.
async fn initialize(client: &mut TestClient) -> std::io::Result<Value> {
    client
        .send_request("initialize", create_initialize_params())
        .await?;
    let response = client.read_response().await?;

    // Send initialized notification
    client.send_notification("initialized", json!({})).await?;

    Ok(response)
}

/// Tests the full LSP lifecycle: initialize, initialized, shutdown, exit.
#[tokio::test]
async fn test_full_lifecycle() {
    let mut client = TestClient::spawn().await.expect("Failed to spawn server");

    // Initialize
    let init_response = initialize(&mut client).await.expect("Initialize failed");

    // Verify response structure
    assert!(init_response.get("id").is_some());
    assert!(init_response.get("result").is_some());

    let result = init_response.get("result").unwrap();
    let capabilities = result.get("capabilities").expect("Missing capabilities");

    // Verify server advertises formatting
    assert!(capabilities.get("documentFormattingProvider").is_some());
    assert!(capabilities.get("textDocumentSync").is_some());

    // Shutdown and exit
    client.shutdown().await.expect("Shutdown failed");
}

/// Tests document formatting with uppercase transformation.
#[tokio::test]
async fn test_format_document() {
    let mut client = TestClient::spawn().await.expect("Failed to spawn server");

    initialize(&mut client).await.expect("Initialize failed");

    // Open a document
    let uri = "file:///tmp/test.txt";
    client
        .send_notification(
            "textDocument/didOpen",
            create_did_open_params(uri, "hello world"),
        )
        .await
        .expect("didOpen failed");

    // Request formatting
    client
        .send_request("textDocument/formatting", create_formatting_params(uri))
        .await
        .expect("Formatting request failed");

    let response = client.read_response().await.expect("Read response failed");

    // Verify formatting result
    let result = response.get("result").expect("Missing result");
    let edits = result.as_array().expect("Result should be array");
    assert!(!edits.is_empty(), "Should have at least one edit");

    let edit = &edits[0];
    let new_text = edit
        .get("newText")
        .and_then(|v| v.as_str())
        .expect("Missing newText");
    assert_eq!(new_text, "HELLO WORLD", "Text should be uppercase");

    client.shutdown().await.expect("Shutdown failed");
}

/// Tests incremental document changes.
#[tokio::test]
async fn test_incremental_changes() {
    let mut client = TestClient::spawn().await.expect("Failed to spawn server");

    initialize(&mut client).await.expect("Initialize failed");

    // Open document with initial content
    let uri = "file:///tmp/test.txt";
    client
        .send_notification(
            "textDocument/didOpen",
            create_did_open_params(uri, "line one\nline two"),
        )
        .await
        .expect("didOpen failed");

    // Apply incremental change: replace "one" with "ONE"
    let change_params = create_did_change_params(
        uri,
        2,
        Range {
            start: Position {
                line: 0,
                character: 5,
            },
            end: Position {
                line: 0,
                character: 8,
            },
        },
        "ONE",
    );
    client
        .send_notification("textDocument/didChange", change_params)
        .await
        .expect("didChange failed");

    // Format document
    client
        .send_request("textDocument/formatting", create_formatting_params(uri))
        .await
        .expect("Formatting request failed");

    let response = client.read_response().await.expect("Read response failed");

    // Verify result includes the change
    let result = response.get("result").expect("Missing result");
    let edits = result.as_array().expect("Result should be array");
    assert!(!edits.is_empty());

    let new_text = edits[0]
        .get("newText")
        .and_then(|v| v.as_str())
        .expect("Missing newText");

    // After incremental change "one" -> "ONE", then uppercase:
    // "line ONE\nline two" -> "LINE ONE\nLINE TWO"
    assert!(new_text.contains("LINE ONE"), "Should contain LINE ONE");

    client.shutdown().await.expect("Shutdown failed");
}

/// Tests multi-document workspace handling.
#[tokio::test]
async fn test_multi_document_workspace() {
    let mut client = TestClient::spawn().await.expect("Failed to spawn server");

    initialize(&mut client).await.expect("Initialize failed");

    // Open two documents
    let uri1 = "file:///tmp/doc1.txt";
    let uri2 = "file:///tmp/doc2.txt";

    client
        .send_notification(
            "textDocument/didOpen",
            create_did_open_params(uri1, "first"),
        )
        .await
        .expect("didOpen doc1 failed");

    client
        .send_notification(
            "textDocument/didOpen",
            create_did_open_params(uri2, "second"),
        )
        .await
        .expect("didOpen doc2 failed");

    // Format doc1
    client
        .send_request("textDocument/formatting", create_formatting_params(uri1))
        .await
        .expect("Formatting doc1 failed");

    let response1 = client.read_response().await.expect("Read response failed");
    let edits1 = response1
        .get("result")
        .and_then(|r| r.as_array())
        .expect("Missing result array");
    assert_eq!(
        edits1[0].get("newText").and_then(|v| v.as_str()),
        Some("FIRST")
    );

    // Format doc2
    client
        .send_request("textDocument/formatting", create_formatting_params(uri2))
        .await
        .expect("Formatting doc2 failed");

    let response2 = client.read_response().await.expect("Read response failed");
    let edits2 = response2
        .get("result")
        .and_then(|r| r.as_array())
        .expect("Missing result array");
    assert_eq!(
        edits2[0].get("newText").and_then(|v| v.as_str()),
        Some("SECOND")
    );

    // Close doc1
    client
        .send_notification("textDocument/didClose", create_did_close_params(uri1))
        .await
        .expect("didClose failed");

    // doc2 should still be accessible
    client
        .send_request("textDocument/formatting", create_formatting_params(uri2))
        .await
        .expect("Formatting doc2 after close failed");

    let response3 = client.read_response().await.expect("Read response failed");
    assert!(response3.get("result").is_some(), "doc2 should still work");

    client.shutdown().await.expect("Shutdown failed");
}

/// Tests request cancellation.
#[tokio::test]
async fn test_cancellation() {
    let mut client = TestClient::spawn().await.expect("Failed to spawn server");

    initialize(&mut client).await.expect("Initialize failed");

    // Open a document
    let uri = "file:///tmp/test.txt";
    client
        .send_notification(
            "textDocument/didOpen",
            create_did_open_params(uri, "test content"),
        )
        .await
        .expect("didOpen failed");

    // Send formatting request (has 200ms delay)
    let req_id = client
        .send_request("textDocument/formatting", create_formatting_params(uri))
        .await
        .expect("Formatting request failed");

    // Immediately send cancel request
    client
        .send_notification("$/cancelRequest", json!({ "id": req_id }))
        .await
        .expect("Cancel request failed");

    // Read response - should be cancelled error
    let response = client.read_response().await.expect("Read response failed");

    // Verify response is either cancelled or completed (race condition)
    if let Some(error) = response.get("error") {
        let code = error.get("code").and_then(|c| c.as_i64()).unwrap_or(0);
        // -32800 is RequestCancelled
        assert_eq!(code, -32800, "Should be RequestCancelled error");
    } else {
        // Request completed before cancellation - that's also valid
        assert!(response.get("result").is_some());
    }

    client.shutdown().await.expect("Shutdown failed");
}

/// Tests handling of unknown methods.
#[tokio::test]
async fn test_unknown_method() {
    let mut client = TestClient::spawn().await.expect("Failed to spawn server");

    initialize(&mut client).await.expect("Initialize failed");

    // Send request with unknown method
    client
        .send_request("unknown/method", json!({}))
        .await
        .expect("Request failed");

    let response = client.read_response().await.expect("Read response failed");

    // Verify MethodNotFound error
    let error = response.get("error").expect("Should have error");
    let code = error.get("code").and_then(|c| c.as_i64()).unwrap_or(0);
    // -32601 is MethodNotFound
    assert_eq!(code, -32601, "Should be MethodNotFound error");

    client.shutdown().await.expect("Shutdown failed");
}

/// Tests shutdown during a pending request.
#[tokio::test]
async fn test_shutdown_during_request() {
    let mut client = TestClient::spawn().await.expect("Failed to spawn server");

    initialize(&mut client).await.expect("Initialize failed");

    // Open a document
    let uri = "file:///tmp/test.txt";
    client
        .send_notification("textDocument/didOpen", create_did_open_params(uri, "test"))
        .await
        .expect("didOpen failed");

    // Send formatting request (has 200ms delay)
    client
        .send_request("textDocument/formatting", create_formatting_params(uri))
        .await
        .expect("Formatting request failed");

    // Wait a bit, then send shutdown
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send shutdown request
    client
        .send_request("shutdown", json!(null))
        .await
        .expect("Shutdown request failed");

    // Read responses - we should get both formatting response and shutdown response
    // The order may vary based on timing
    let mut got_format_response = false;
    let mut got_shutdown_response = false;

    for _ in 0..2 {
        let response = match timeout(Duration::from_secs(5), client.read_response()).await {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => panic!("Read error: {:?}", e),
            Err(_) => panic!("Timeout reading response"),
        };

        // Identify response type by checking if it's the shutdown response
        // Shutdown response has null result and no error (typically)
        if response.get("result") == Some(&json!(null)) && response.get("error").is_none() {
            got_shutdown_response = true;
        } else if response.get("result").is_some() || response.get("error").is_some() {
            // Format response (either success or cancelled)
            got_format_response = true;
        }
    }

    assert!(got_format_response, "Should receive format response");
    assert!(got_shutdown_response, "Should receive shutdown response");

    // Send exit notification
    client
        .send_notification("exit", json!(null))
        .await
        .expect("Exit failed");

    // Wait for process to exit
    tokio::time::sleep(Duration::from_millis(100)).await;
}

/// Tests that formatting an unknown document returns an error.
#[tokio::test]
async fn test_format_unknown_document() {
    let mut client = TestClient::spawn().await.expect("Failed to spawn server");

    initialize(&mut client).await.expect("Initialize failed");

    // Request formatting for a document that was never opened
    let uri = "file:///tmp/unknown.txt";
    client
        .send_request("textDocument/formatting", create_formatting_params(uri))
        .await
        .expect("Formatting request failed");

    let response = client.read_response().await.expect("Read response failed");

    // Should get an error (document not found)
    assert!(
        response.get("error").is_some(),
        "Should have error for unknown document"
    );

    client.shutdown().await.expect("Shutdown failed");
}
