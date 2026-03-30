# lsp-server-tokio

[![crates.io](https://img.shields.io/crates/v/lsp-server-tokio.svg)](https://crates.io/crates/lsp-server-tokio)
[![docs.rs](https://img.shields.io/docsrs/lsp-server-tokio)](https://docs.rs/lsp-server-tokio)
[![CI](https://github.com/VladimirMakaev/lsp-server-tokio/actions/workflows/ci.yml/badge.svg)](https://github.com/VladimirMakaev/lsp-server-tokio/actions/workflows/ci.yml)

`lsp-server-tokio` is an async-first Rust crate for building Language Server Protocol servers on Tokio. It sits between `lsp-server` and `tower-lsp`: lower-level and transport-agnostic like `lsp-server`, but designed for async I/O, explicit routing, cooperative cancellation, and testable server infrastructure without a framework trait or `tower` stack.

## Quick Start

Add the crate to `Cargo.toml`:

```toml
[dependencies]
lsp-server-tokio = "0.3"
serde_json = "1"
```

```rust,no_run
use futures::StreamExt;
use lsp_server_tokio::{Connection, IncomingMessage, Response};

# tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
let mut conn = Connection::stdio();
let capabilities = serde_json::json!({
    "documentFormattingProvider": true
});

let _client_params = conn.initialize(capabilities).await?;
let sender = conn.client_sender();

while let Some(result) = conn.receiver.next().await {
    let msg = result?;

    match conn.route(msg) {
        IncomingMessage::Request(req, _) if req.method == "shutdown" => {
            conn.handle_shutdown(req.id).await?;
        }
        IncomingMessage::Request(req, _) => {
            let response = Response::ok(req.id, serde_json::Value::Null);
            sender.respond(response)?;
        }
        IncomingMessage::Notification(notif) if notif.method == "exit" => {
            break;
        }
        IncomingMessage::Notification(_) => {}
        IncomingMessage::ResponseRouted | IncomingMessage::ResponseUnknown(_) => {}
    }
}
# Ok::<(), Box<dyn std::error::Error>>(()) });
```

See `examples/formatter_server.rs` for a fuller stdio server with typed `lsp-types` requests.

## Features

- Async-first connection management with `Connection::stdio()` and `Connection::new(io)`
- Cloneable `ClientSender` for non-blocking server→client requests, responses, and notifications
- Explicit message classification through `conn.route(msg)` and `IncomingMessage`
- First-class request cancellation via re-exported `CancellationToken`
- Transport-agnostic I/O over stdio, TCP, pipes, or custom streams
- In-memory testing with `duplex_transport()`
- Clean lifecycle helpers for initialize, shutdown, exit, and protocol errors
- Minimal runtime dependencies with no `tower` requirement

## Server→Client Communication

After initialization, call `conn.client_sender()` to upgrade outbound traffic to a cloneable `ClientSender`. This lets spawned tasks notify the client without holding `&mut Connection`.

```rust,no_run
use lsp_server_tokio::{ClientSender, Connection};
use lsp_types::{LogMessageParams, MessageType};

fn log_message(sender: &ClientSender, message: impl Into<String>) {
    let params = LogMessageParams {
        typ: MessageType::INFO,
        message: message.into(),
    };

    sender
        .notify("window/logMessage", Some(serde_json::to_value(params).unwrap()))
        .ok();
}

# tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
let mut conn = Connection::stdio();
let capabilities = serde_json::json!({});
let _ = conn.initialize(capabilities).await?;

let sender = conn.client_sender();
let background_sender = sender.clone();

tokio::spawn(async move {
    log_message(&background_sender, "background indexing started");
});

log_message(&sender, "server ready");
# Ok::<(), Box<dyn std::error::Error>>(()) });
```

Use `ClientSender::respond()` for request handlers and `ClientSender::request()` when the server needs to initiate its own JSON-RPC requests to the client.

## Architecture

The crate keeps transport, lifecycle, and routing separate. `Transport<T>` handles framed JSON-RPC messages, `Connection<T, I, O>` adds request tracking and lifecycle state, and `conn.route(msg)` classifies inbound messages while wiring responses and cancellation into the request queue.

## Comparison

| Crate | Async I/O | Server model | Cancellation | Testing story | Opinionation |
| --- | --- | --- | --- | --- | --- |
| `lsp-server-tokio` | Native Tokio | Explicit dispatch with `Connection` | `CancellationToken` per request | `duplex_transport()` and custom transports | Low |
| `lsp-server` | Primarily sync | Minimal message loop | Manual | Custom harness required | Very low |
| `tower-lsp` | Async | Trait-based framework on `tower` | Framework-managed | Good, but tied to framework | Higher |

## Testing

Use `duplex_transport()` to connect a client and server in memory without stdio:

```rust
use futures::{SinkExt, StreamExt};
use lsp_server_tokio::{duplex_transport, Message, Request, Response};

# tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
let (mut client, mut server) = duplex_transport(4096);

client
    .send(Message::Request(Request::new(1, "textDocument/hover", None)))
    .await
    .unwrap();

if let Some(Ok(Message::Request(req))) = server.next().await {
    server
        .send(Message::Response(Response::ok(
            req.id,
            serde_json::json!({"contents": "Hello from tests"}),
        )))
        .await
        .unwrap();
}
# });
```

## License

Licensed under either of:

- MIT license
- Apache License, Version 2.0
