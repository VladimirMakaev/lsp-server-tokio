# Changelog

All notable changes to this project will be documented in this file.

## [0.4.0] - 2026-04-10

### Breaking Changes
- **Removed `I` type parameter** from `Connection<T, I>`, `RequestQueue`, `IncomingRequests` — incoming request metadata is no longer generic. Use `HashMap<RequestId, MyData>` externally if needed. (#17)
- **Removed `O` type parameter** from `Connection` and `RequestQueue` — outgoing request tracking is now untyped (#9)
- **`Response` uses `ResponseBody` enum** (`.body: ResponseBody`) instead of separate `result`/`error` fields, enforcing the JSON-RPC invariant of exactly one of result or error (#9)
- **`receiver` field is now private** — use `receiver_mut()` or `into_receiver()` (#9)
- **`Receiver` type renamed to `MessageStream`** (#9)
- **`cancel()` and `handle_shutdown()` are now sync** (no longer async) (#9)
- **`register()` no longer takes a `data` parameter**, `complete()` returns `bool` instead of `Option<I>` (#17)
- **`ErrorCode` and `LifecycleState` are `#[non_exhaustive]`** (#9)
- **MSRV raised to 1.91** (#9)

### Added
- `ClientSender` for cloneable, non-blocking server→client communication (#1)
- `Connection::client_sender()` to create a `ClientSender` handle from an initialized connection (#1)
- Auto-handle `$/cancelRequest` in `route()` — returns `IncomingMessage::CancelHandled` (#9)
- `ResponseBody` enum enforcing JSON-RPC response invariants (#9)
- `drain_alive` disconnection detection — `ClientSender::request()` races response vs transport death (#9)
- Emit `"id": null` for parse errors per JSON-RPC 2.0 spec (#9)
- Well-known LSP method name constants: `INITIALIZE_METHOD`, `INITIALIZED_METHOD`, `SHUTDOWN_METHOD`, `EXIT_METHOD` (#18)
- `From<Request/Response/Notification>` for `Message` enabling `.into()` (#18)
- `Message` accessor methods: `as_request()`, `as_response()`, `as_notification()`, `into_request()`, `into_response()`, `into_notification()` (#18)
- `#[non_exhaustive]` on `IncomingMessage` (#9)
- `parking_lot::Mutex` replaces `std::sync::Mutex` (no poisoning) (#9)
- E2E tests for UTF-16 position encoding, CRLF line endings, and trailing newlines (#9)

### Changed
- `Connection.sender` access now goes through `Connection::sender()` method (#1)
- `client_sender()` is idempotent (can be called multiple times) (#9)
- `send()` is now sync, channel-based, and non-blocking (#9)
- Improved `lib.rs` quick-start documentation and doc examples (#9)

### Fixed
- Parse error responses now include `"id": null` per JSON-RPC 2.0 spec (#9)
- `bytes` updated 1.11.0 → 1.11.1 (CVE-2026-25541, RUSTSEC-2026-0007) (#9)

### Deprecated
- `Connection::send_request()` in favor of `ClientSender::request()` (#1)

## [0.3.0] - 2026-03-30

### Added
- `ClientSender` for cloneable, non-blocking server→client communication
- `Connection::client_sender()` to create a `ClientSender` handle from an initialized connection

### Changed
- `Connection.sender` access now goes through the `Connection::sender()` method

### Deprecated
- `Connection::send_request()` in favor of `ClientSender::request()`

## [0.1.0] - 2026-03-23

### Added
- Async LSP server infrastructure using Tokio
- `Connection` type with split sender/receiver for concurrent I/O
- `StdioConnection` type alias for ergonomic stdio-based servers
- `LspCodec` for Content-Length message framing
- `Transport` and `duplex_transport()` for transport-agnostic I/O
- Full LSP lifecycle state machine (Uninitialized → Running → ShuttingDown → Exited)
- `RequestQueue` with `IncomingRequests` and `OutgoingRequests` tracking
- Cooperative cancellation via `CancellationToken` per request
- `IncomingMessage` routing with auto-registration
- 60-second timeout on `initialize_finish` for robustness
- Complete set of LSP and JSON-RPC error codes
- Example formatter server with E2E tests
